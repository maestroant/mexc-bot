"""PAPER на живом потоке, без ордеров: те же движки ядра, что у REPLAY, — основная стратегия (core.cap_breakout.MainEngine:
контрольный BASELINE по ТЗ v2.1 и малая часть сетки MAIN_EARLY) и CAP BREAKOUT (CapEngine: FAST, CONFIRMED, EARLY).

Основная стратегия стартует с T0 по единой модели ядра (официальное время открытия firstOpenTime; нет — первый действительный
стакан или первая сделка) и ведётся до T0 + --main-duration с. Потолок CAP BREAKOUT берётся
из 30010 в логе пробы (probe_listing --cap-test), проверка раз в 2 с до T+--cap-wait мин; нашёлся — CapEngine ведёт стакан
и после снятия потолка — ветки до снятия + --after-release с. Потолка в первые секунды PAPER ещё не знает, поэтому
основная стратегия на замороженном листинге идёт без ограничения потолком (вход там отсекает спред 7–24 %).

Исполнение — модель ядра по живому стакану (задержка, частичный BUY, полоса цены, minNotional, комиссии инструмента),
ордера не ставятся. Лог — logs/paper_<SYMBOL>_<время>.jsonl (OPEN, CAP, START, ENTRY, EXIT, NO_ENTRY, SUMMARY; у каждого
события поле engine: main или cap); итог — в Telegram с пометкой PAPER, раздельно по BASELINE, MAIN_EARLY и режимам
CAP BREAKOUT. Задержка: время обработки кадра движками и запаздывание кадров относительно sendTime биржи сверх лучшего
наблюдённого (часы сервера и биржи расходятся на сотни мс, абсолютное значение не показательно).

Журнал кадров P0 (logs/paperrec_*): соединение A полностью, теневое B — метаданные, события диагностики. Окно — от T0 − 2 мин
до T0 + 5 мин; на замороженном листинге (известен потолок, клиент 11.11) — до снятия потолка, завершения всех веток
CAP BREAKOUT и ещё 120 с, а без снятия до T0 + --release-wait мин — до этого срока со статусом
NO_RELEASE_OBSERVED_WITHIN_WINDOW (он не значит, что снятия не было позже). Время каждого этапа обработки кадра (разбор,
стакан, запуск веток, ветки, учёт веток с логом, журнал), задержки цикла событий > 20 мс с местом блокировки (стек
главного потока и занятых потоков в момент блокировки); > 50 мс — ещё и в лог PAPER (для разбора).

Обычно запускает наблюдатель вместе с пробой и записью: .venv/bin/python -u tools/paper.py SYMBOL
"""

import argparse
import asyncio
import gc
import gzip
import json
import os
import signal
import sys
import threading
import time
from argparse import Namespace
from collections import deque
from datetime import datetime

from cap_replay import brief
from core.cap_breakout import (EARLY_USDT, EXEC_MODEL_ID, MAIN_EARLY_PAPER, ORDER_USDT, PREREG_ID, CapEngine,
                               MainEngine, Opts, find_cap, market_params, no_entry_reason)
from core.clock import measure, mono_ms
from core.feed import events_of, frame_meta
from core.live import LiveFeed
from notify import notify
from probe_listing import ROOT, Client, load_dotenv

LATENCY_MS = 50  # задержка ордера в модели — как в основном прогоне REPLAY
JOIN_MARGIN_MS = 250  # Time Guard: запас на погрешность поправки часов при сравнении firstOpenTime со стартом (19.11)
JOURNAL_BEFORE_MS, JOURNAL_AFTER_MS = 120_000, 300_000  # журнал кадров PAPER: T0 − 2 мин … T0 + 5 мин
CAP_TAIL_MS = 120_000  # замороженный листинг: журнал ещё 120 с после завершения всех веток CAP BREAKOUT (клиент 11.11)
LOOP_TICK_MS, LOOP_LAG_MS, SLOW_FRAME_MS = 10, 20, 20  # контроль цикла событий и медленных кадров (в журнал)
REVIEW_MS = 50  # задержка цикла или обработка кадра дольше — ещё и в лог PAPER, для отдельного разбора
PROC_FROM_MS, PROC_TO_MS = -2_000, 30_000  # время обработки каждого кадра A в журнал — в этом окне от T0
# (у CAP BREAKOUT — от первой сделки выше потолка до снятия + PROC_TO_MS)
STAGES = ("parse", "book", "start", "eval", "base", "exp", "track", "jw", "total", "q")
# eval — все ветки; base — ветки BASELINE (ТЗ v2.1), exp — остальные (экспериментальные); q — кадров в очереди
# websockets, уже принятых, но ещё не прочитанных, в момент чтения этого кадра (шаг 0 оптимизаций, клиент 12.11)


class GcWatch:
    """Паузы сборщика мусора Python (gc.callbacks): начало, конец, поколение — для разбора задержек цикла событий."""

    def __init__(self):
        self.started: float | None = None
        self.events: deque = deque(maxlen=2000)  # (начало, конец, поколение)
        self.stats: dict[int, list[float]] = {0: [], 1: [], 2: []}

    def __call__(self, phase: str, info: dict) -> None:
        now = time.perf_counter()
        if phase == "start":
            self.started = now
        elif self.started is not None:
            gen = info.get("generation", 0)
            self.events.append((self.started, now, gen))
            self.stats.setdefault(gen, []).append(1000 * (now - self.started))
            self.started = None

    def within(self, start: float, end: float) -> dict:
        got = [(b, e, g) for b, e, g in self.events if e >= start and b <= end]
        if not got:
            return {}
        return dict(gc_ms=round(sum(1000 * (e - b) for b, e, g in got), 1), gc_gens=sorted({g for _, _, g in got}))

    def summary(self) -> dict:
        return {f"gen{g}": dict(n=len(v), p99_ms=round(pct(v, 0.99), 2), max_ms=round(max(v, default=0.0), 2))
                for g, v in self.stats.items()}


class StallSampler(threading.Thread):
    """Где стоит цикл событий: если он не отмечался дольше threshold_s, раз в tick_s снимается стек главного потока
    и занятых других потоков (sys._current_frames). Только диагностика: потоку нужен GIL на доли миллисекунды."""

    def __init__(self, threshold_s: float = (LOOP_TICK_MS + 5) / 1000, tick_s: float = 0.005):
        super().__init__(daemon=True, name="stall-sampler")
        self.main_id = threading.main_thread().ident
        self.threshold_s, self.tick_s = threshold_s, tick_s
        self.beat = time.perf_counter()
        self.halt = threading.Event()
        self.lock = threading.Lock()
        self.samples: list[tuple[float, str, tuple]] = []

    @staticmethod
    def where(frame, depth: int = 10) -> str:
        parts = []
        while frame is not None and len(parts) < depth:
            code = frame.f_code
            short = "/".join(code.co_filename.replace("\\", "/").split("/")[-2:])
            parts.append(f"{short}:{code.co_name}:{frame.f_lineno}")
            frame = frame.f_back
        return " <- ".join(parts)

    @staticmethod
    def idle(frame) -> bool:
        name = frame.f_code.co_filename.replace("\\", "/")
        return name.endswith(("threading.py", "queue.py", "selectors.py")) or frame.f_code.co_name == "_worker"

    def run(self) -> None:
        while not self.halt.wait(self.tick_s):
            if time.perf_counter() - self.beat < self.threshold_s:
                continue
            me = threading.get_ident()
            frames = sys._current_frames()
            main = self.where(frames.get(self.main_id), 6)
            others = tuple(self.where(f, 3) for tid, f in frames.items()
                           if tid not in (self.main_id, me) and not self.idle(f))
            with self.lock:
                self.samples.append((time.perf_counter(), main, others))
                del self.samples[:-400]

    def take(self, start: float, end: float) -> dict:
        """Снимки за интервал задержки: самое частое место главного потока и занятые потоки."""
        with self.lock:
            got = [x for x in self.samples if start <= x[0] <= end]
            self.samples = [x for x in self.samples if x[0] > end]
        if not got:
            return dict(samples=0)
        mains: dict[str, int] = {}
        others: dict[str, int] = {}
        for _, m, o in got:
            mains[m] = mains.get(m, 0) + 1
            for x in o:
                others[x] = others.get(x, 0) + 1
        return dict(samples=len(got), where=max(mains, key=mains.get), threads=sorted(others, key=others.get)[-3:])


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else 0.0


class Paper:
    def __init__(self, args, client: Client):
        self.args, self.sym, self.client = args, args.symbol, client
        self.cap_cfg = Namespace(modes=["FAST", "CONFIRMED", "EARLY"], early={}, early_sweep=False,
                                 latency=[LATENCY_MS], usdt=[ORDER_USDT], early_usdt=list(EARLY_USDT))
        self.main_cfg = Namespace(latency=[LATENCY_MS], usdt=[ORDER_USDT], sweep=False, main_early=MAIN_EARLY_PAPER)
        self.stop = asyncio.Event()
        self.feed = LiveFeed(self.sym, client, self.on_entry, on_log=self.log, on_shadow=self.on_shadow,
                             on_event=self.on_feed_event)
        self.started_ms = self.feed.t()
        self.info: dict = {}
        self.open_ms: int | None = None
        self.cap: tuple[float, str] | None = None
        self.cap_checked = False  # потолка нет и ждать его больше не нужно
        self.cap_status: dict = {}  # NO_CAP_OBSERVED / CAP_UNKNOWN и причина (решение клиента 12.11, C4)
        # основная стратегия ведёт стакан с запуска, T0 — по модели MainEngine; cap — CAP BREAKOUT, когда известен потолок
        # наблюдение — с запуска процесса (join_ms): firstOpenTime раньше него — LATE_JOIN, веток нет (решение 19.11)
        self.engines: dict[str, CapEngine] = {"main": MainEngine(self.main_cfg, Opts(), None, {},
                                                                 join_ms=self.started_ms, join_margin_ms=JOIN_MARGIN_MS)}
        self.t0_seen = 0  # сколько переходов состояния T0 уже записано в журнал
        self.states: dict[str, list[str]] = {}
        self.done: dict[str, bool] = {}
        self.proc_ms: list[float] = []
        self.delay_ms: list[float] = []  # t получения − sendTime биржи
        stamp = f"{datetime.now():%Y%m%d_%H%M%S}"
        path = ROOT / "logs" / f"paper_{self.sym}_{stamp}.jsonl"
        self.out = path.open("a", encoding="utf-8")
        self.path = path
        # P0: журнал кадров в формате записи (стратегия — соединение A, полностью; теневое B — только метаданные),
        # события диагностики; до открытия — кольцевой буфер последних JOURNAL_BEFORE_MS
        self.journal_path = ROOT / "logs" / f"paperrec_{self.sym}_{stamp}.jsonl.gz"
        self.journal = None
        self.ring: deque = deque()
        self.journal_closed = False
        self.loop_lag_max = 0.0
        self.slow_frames = 0  # с 11.11: полное время обработки кадра (с журналом, логом, разбором) > SLOW_FRAME_MS
        self.slow_engine_frames = 0  # как до 11.11: только движки > SLOW_FRAME_MS — для сравнения с MTX, USDT/BRL
        self.first_after_open: dict[str, int] = {}  # соединение → время первого WS-кадра после T0
        self.depth_recv: dict[str, dict[int, int]] = {"A": {}, "B": {}}  # toVersion → время получения (окно журнала)
        # P0, 11.11: окно журнала CAP BREAKOUT, нагрузка журнала, этапы обработки кадра, место блокировки цикла
        self.cap_done_ms: int | None = None
        self.cap_seen: dict[str, bool] = {}  # отмечено в журнале: первая сделка выше потолка, снятие
        self.journal_status: str | None = None
        self.j_lines = self.j_bytes = 0
        self.jw_s = self.log_s = 0.0
        self.jw_ms: list[float] = []
        self.stage: dict[str, list[float]] = {k: [] for k in STAGES}
        self.last_end: float | None = None
        self.loop_lag_at: int | None = None
        self.loop_lag_where: dict = {}
        self.sampler = StallSampler()
        self.gcw = GcWatch()
        self.tp_seen: dict[str, list[int]] = {}

    def log(self, msg: str) -> None:
        print(f"[{datetime.now():%H:%M:%S}] {self.sym} {msg}", flush=True)

    def write(self, event: str, **data) -> None:
        p0 = time.perf_counter()
        self.out.write(json.dumps({"t": self.feed.t(), "event": event, **data}, ensure_ascii=False, default=str) + "\n")
        self.out.flush()
        self.log_s += time.perf_counter() - p0

    # --- журнал кадров (P0) ---
    def t0_hint(self) -> int | None:
        return self.open_ms or self.feed.first_open or (int(self.info["firstOpenTime"])
                                                         if self.info.get("firstOpenTime") else None)

    def journal_end(self) -> int | None:
        """Конец окна журнала (время в шкале биржи); None — ещё не известен (CAP BREAKOUT идёт или T0 не известно)."""
        t0 = self.t0_hint()
        if t0 is None:
            return None
        cap = self.engines.get("cap")
        if cap is None:
            return t0 + JOURNAL_AFTER_MS
        if self.cap_done_ms is None:
            return None
        return self.cap_done_ms + (CAP_TAIL_MS if cap.release is not None else 0)

    def jwrite(self, entry: dict) -> None:
        """Строка журнала; до T0 − JOURNAL_BEFORE_MS — в кольцевой буфер, после конца окна — не пишется."""
        if self.journal_closed:
            return
        p0 = time.perf_counter()
        line = json.dumps(entry, ensure_ascii=False, separators=(",", ":"), default=str) + "\n"
        t0, t = self.t0_hint(), entry["t"]
        if t0 is None or t < t0 - JOURNAL_BEFORE_MS:
            self.ring.append((t, line))
            while self.ring and self.ring[0][0] < t - JOURNAL_BEFORE_MS:
                self.ring.popleft()
            self.jw_s += time.perf_counter() - p0
            return
        end = self.journal_end()
        if end is not None and t > end:
            self.close_journal()
            return
        if self.journal is None:
            self.journal = gzip.open(self.journal_path, "wt", encoding="utf-8")
            for rt, ln in self.ring:
                if rt >= t0 - JOURNAL_BEFORE_MS:
                    self.journal.write(ln)
                    self.j_lines += 1
                    self.j_bytes += len(ln)
            self.ring.clear()
        self.journal.write(line)
        self.j_lines += 1
        self.j_bytes += len(line)
        dt = time.perf_counter() - p0
        self.jw_s += dt
        self.jw_ms.append(1000 * dt)

    def close_journal(self) -> None:
        if self.journal_closed:
            return
        cap = self.engines.get("cap")
        if self.journal_status is None:
            self.journal_status = ("NO_CAP_KNOWN" if cap is None else "RELEASE_OBSERVED" if cap.release is not None
                                   else "NO_RELEASE_OBSERVED_WITHIN_WINDOW" if self.cap_done_ms else "STOPPED")
        if self.journal is not None:
            t0, now = self.t0_hint(), self.feed.t()
            self.journal.write(json.dumps({"t": now, "src": "event", "ch": "JOURNAL_END", "data": dict(
                status=self.journal_status, end_rel_ms=now - t0 if t0 else None), "m": mono_ms()},
                ensure_ascii=False, separators=(",", ":")) + "\n")  # мимо jwrite: окно уже закончилось
            self.journal.close()
            self.journal_closed = True
            info = self.journal_stats()
            self.write("JOURNAL_CLOSED", **info)
            self.log(f"журнал кадров: {self.journal_path} ({info})")
        self.journal_closed = True

    def journal_stats(self) -> dict:
        size = self.journal_path.stat().st_size if self.journal_path.exists() else 0
        return dict(path=self.journal_path.name, status=self.journal_status, lines=self.j_lines,
                    raw_mb=round(self.j_bytes / 1e6, 2), gz_mb=round(size / 1e6, 2),
                    write_p50_ms=round(pct(self.jw_ms, 0.5), 3), write_p99_ms=round(pct(self.jw_ms, 0.99), 3),
                    write_max_ms=round(max(self.jw_ms, default=0.0), 3))

    def jevent(self, name: str, t: int | None = None, **data) -> None:
        self.jwrite({"t": self.feed.t() if t is None else t, "src": "event", "ch": name, "data": data, "m": mono_ms()})

    def on_feed_event(self, name: str, **data) -> None:
        self.write(name, **data)
        self.jevent(name, **data)

    def note_frame(self, entry: dict) -> None:
        """Время получения кадра по соединению: первый WS-кадр после T0 и diff-ы по toVersion для сравнения A/B."""
        data, conn, t = entry.get("data"), entry.get("conn", "A"), entry["t"]
        if entry.get("src") != "ws_pub" or not isinstance(data, dict):
            return
        t0 = self.t0_hint()
        if t0 is not None and t >= t0 and conn not in self.first_after_open:
            self.first_after_open[conn] = t
            self.jevent("FIRST_FRAME_AFTER_T0", conn=conn, rel_ms=t - t0, send_rel_ms=int(data.get("sendTime", 0)) - t0)
        dep = data.get("publicAggreDepths")
        if dep and t0 is not None and t0 - JOURNAL_BEFORE_MS <= t <= t0 + JOURNAL_AFTER_MS:
            self.depth_recv[conn].setdefault(int(dep["toVersion"]), t)

    def on_shadow(self, entry: dict) -> None:
        """Теневое соединение B: только метаданные кадра в журнал."""
        self.note_frame(entry)
        data = entry.get("data") or {}
        dep, deals = data.get("publicAggreDepths"), data.get("publicAggreDeals")
        meta = dict(sendTime=data.get("sendTime"))
        if dep:
            meta.update(fromVersion=dep.get("fromVersion"), toVersion=dep.get("toVersion"))
        if deals:  # сделки без идентификаторов: сравнение соединений — по (время, цена, количество)
            meta.update(deals=[[d.get("time"), d.get("price"), d.get("quantity")] for d in deals.get("deals", [])])
        self.jwrite({"t": entry["t"], "src": "ws_shadow", "ch": entry["ch"], "conn": "B", "data": meta,
                     "m": entry.get("m")})

    async def loop_monitor(self) -> None:
        """Задержка цикла событий: насколько позже срабатывает sleep(LOOP_TICK_MS); больше LOOP_LAG_MS — в журнал
        с местом блокировки (StallSampler), больше REVIEW_MS — ещё и в лог PAPER."""
        self.sampler.start()
        gc.callbacks.append(self.gcw)
        try:
            while not self.stop.is_set():
                started = time.perf_counter()
                self.sampler.beat = started
                await asyncio.sleep(LOOP_TICK_MS / 1000)
                now = time.perf_counter()
                self.sampler.beat = now
                lag = 1000 * (now - started) - LOOP_TICK_MS
                if lag <= LOOP_LAG_MS:
                    self.loop_lag_max = max(self.loop_lag_max, lag)
                    continue
                where = {**self.sampler.take(started, now), **self.gcw.within(started, now)}
                if lag > self.loop_lag_max:
                    self.loop_lag_max, self.loop_lag_at, self.loop_lag_where = lag, self.feed.t(), where
                self.jevent("LOOP_LAG", lag_ms=round(lag, 1), **where)
                if lag > REVIEW_MS:
                    t0 = self.t0_hint()
                    self.write("LOOP_LAG", lag_ms=round(lag, 1), rel_ms=self.feed.t() - t0 if t0 else None, **where)
        finally:
            self.sampler.halt.set()
            if self.gcw in gc.callbacks:
                gc.callbacks.remove(self.gcw)

    def ab_compare(self) -> dict:
        """Одни и те же diff-ы (по toVersion) на соединениях A и B: на сколько B получал раньше/позже A."""
        a, b = self.depth_recv["A"], self.depth_recv["B"]
        common = sorted(set(a) & set(b))
        diffs = [a[v] - b[v] for v in common]
        t0 = self.t0_hint()
        first = {c: (v - t0 if t0 else None) for c, v in self.first_after_open.items()}
        return dict(versions=len(common), a_minus_b_p50_ms=pct(diffs, 0.5), a_minus_b_p01_ms=pct(diffs, 0.01),
                    a_minus_b_p99_ms=pct(diffs, 0.99), first_frame_after_t0_ms=first)

    def t0_note(self) -> None:
        """Новые переходы состояния T0 основного движка — в журнал (T0_STATE) и лог; LATE_JOIN — завершение без веток
        (решение клиента 19.11): процесс запущен после открытия, окно открытия не наблюдалось, новых входов нет."""
        main = self.engines["main"]
        for rec in main.t0_log[self.t0_seen:]:
            self.write("T0_STATE", known_at=rec.get("t"), **{k: v for k, v in rec.items() if k != "t"},
                       first_msg_ms=main.first_msg_ms, started_ms=self.started_ms,
                       clock_offset_ms=getattr(self.client, "time_offset_ms", None))
            self.jevent("T0_STATE", t=rec.get("t") or self.feed.t(), **{k: v for k, v in rec.items() if k != "t"})
            self.log(f"T0: {rec['state']} — {rec['why']}")
        self.t0_seen = len(main.t0_log)
        if main.t0_state == MainEngine.LATE_JOIN and not self.stop.is_set():
            self.finish("late_join")

    # --- поток ---
    def on_entry(self, entry: dict) -> None:
        s0 = time.perf_counter()
        jw0, log0 = self.jw_s, self.log_s
        gap = None if self.last_end is None else round(1000 * (s0 - self.last_end), 3)  # пауза после прошлого кадра
        self.jwrite(entry)
        self.note_frame(entry)
        data = entry.get("data")
        if entry["ch"] == "exchangeInfo" and isinstance(data, dict):
            fresh = (data.get("symbols") or [{}])[0]
            self.info = fresh or self.info
            for name, eng in self.engines.items():
                if isinstance(eng, MainEngine):
                    if fresh:  # версия справочника — в момент получения ответа (known_at = время строки)
                        eng.on_info(entry["t"], fresh, "rest")
                elif eng.release is None:
                    eng.info = self.info  # шаги цены, полоса и комиссии — по последнему exchangeInfo до старта веток
            self.t0_note()
            self.last_end = time.perf_counter()
            return
        if isinstance(data, dict) and data.get("sendTime"):
            self.delay_ms.append(entry["t"] - int(data["sendTime"]))
        events = events_of(entry)
        if not events:
            self.last_end = time.perf_counter()
            return
        meta = frame_meta(entry)
        book = self.engines["main"].book
        was_valid = book.valid
        cap = self.engines.get("cap")
        cap_valid = cap.book.valid if cap else None
        book0 = sum(e.book_s for e in self.engines.values())
        start0 = sum(e.start_s for e in self.engines.values())
        base0 = sum(e.base_s for e in self.engines.values())
        started = time.perf_counter()
        for name, eng in self.engines.items():
            if not self.done.get(name):
                for t, kind, ev in events:
                    eng.on_event(t, kind, ev, *meta)
        proc = 1000 * (time.perf_counter() - started)
        self.proc_ms.append(proc)
        self.slow_engine_frames += proc > SLOW_FRAME_MS
        book_ms = 1000 * (sum(e.book_s for e in self.engines.values()) - book0)
        start_ms = 1000 * (sum(e.start_s for e in self.engines.values()) - start0)
        base_ms = 1000 * (sum(e.base_s for e in self.engines.values()) - base0)
        t0 = self.t0_hint()
        if book.valid != was_valid:  # стакан стал (не)действительным: разрыв версий или снимок
            self.jevent("BOOK_VALID" if book.valid else "BOOK_INVALID", t=entry["t"], version=book.version,
                        pending=len(book.pending), gaps=book.gaps, by=entry["src"])
        if cap is not None:
            self.cap_marks(cap, cap_valid, entry)
        main = self.engines["main"]  # T0 — по единой модели ядра: официальное время, иначе стакан или первая сделка
        if self.open_ms is None and main.open_ms is not None and events[-1][0] >= main.open_ms:
            self.open_ms = main.open_ms
            self.write("OPEN", open_ms=self.open_ms, exchange_open_ms=main.exchange_open_ms())
            self.jevent("OPEN_DETECTED", t=events[-1][0], open_ms=self.open_ms)  # по нему журнал читают main_replay и paper_vs_replay
            self.log("открытие: основная стратегия идёт, жду потолок из лога пробы")
        p2 = time.perf_counter()
        for name in self.engines:
            self.track(name)
        end = time.perf_counter()
        parse = entry.get("parse_ms") or 0.0
        ev = proc - book_ms - start_ms
        st = dict(parse=parse, book=book_ms, start=start_ms, eval=ev, base=base_ms, exp=ev - base_ms,
                  track=1000 * (end - p2), jw=1000 * (self.jw_s - jw0), total=1000 * (end - s0) + parse,
                  q=float(entry.get("q") or 0))
        for k, v in st.items():
            self.stage[k].append(v)
        st = {k: round(v, 3) for k, v in st.items()}
        st.update(log=round(1000 * (self.log_s - log0), 3), gap=gap)
        in_cap = cap is not None and cap.cand_ms is not None and (
            cap.release is None or entry["t"] <= cap.release + PROC_TO_MS)
        if t0 is not None and (t0 + PROC_FROM_MS <= entry["t"] <= t0 + PROC_TO_MS or in_cap):
            self.jevent("PROC", t=entry["t"], ms=round(proc, 3), version=book.version, valid=book.valid, **st)
        if st["total"] > SLOW_FRAME_MS:
            self.slow_frames += 1
            info = dict(proc_ms=round(proc, 1), kinds=sorted({k for _, k, _ in events}), src=entry["src"],
                        sims=sum(len(e.sims) for e in self.engines.values()), **st, **self.gcw.within(s0, end))
            self.jevent("SLOW_FRAME", **info)
            if st["total"] > REVIEW_MS:
                self.write("SLOW_FRAME", rel_ms=entry["t"] - t0 if t0 else None, **info)
        self.last_end = time.perf_counter()

    def cap_marks(self, cap: CapEngine, was_valid: bool, entry: dict) -> None:
        """Вехи CAP BREAKOUT в журнал: стакан CapEngine (не)действителен, первая сделка выше потолка, снятие."""
        t = entry["t"]
        if cap.book.valid != was_valid:
            self.jevent("CAP_BOOK_VALID" if cap.book.valid else "CAP_BOOK_INVALID", t=t, version=cap.book.version,
                        after_release=cap.release is not None)
        if cap.cand_ms is not None and not self.cap_seen.get("cand"):
            self.cap_seen["cand"] = True
            self.jevent("CAP_CAND", t=cap.cand_ms, cap=cap.cap)
        if cap.release is not None and not self.cap_seen.get("release"):
            self.cap_seen["release"] = True
            bb, ba = cap.book.best()
            self.jevent("CAP_RELEASE", t=cap.release, cap=cap.cap, bid=bb, ask=ba, version=cap.book.version,
                        sims=len(cap.sims))
            self.write("CAP_RELEASE", release_ms=cap.release, bid=bb, ask=ba, version=cap.book.version)

    def track(self, name: str) -> None:
        """Записать переходы веток движка: старт, вход, выход, отказ от входа."""
        eng = self.engines[name]
        if eng.release is None:
            return
        if name not in self.states:
            self.states[name] = ["SCAN"] * len(eng.sims)
            fees = dict(taker=eng.sims[0].opts.fee, maker=eng.sims[0].opts.maker_fee) if eng.sims else None
            self.write("START", engine=name, start_ms=eng.release, sims=len(eng.sims), prereg=PREREG_ID,
                       exec_model=EXEC_MODEL_ID, fees=fees, code=code_version(),
                       t_from_open_s=(eng.release - self.open_ms) / 1000)
            self.log(f"{name}: ветки запущены на T+{(eng.release - self.open_ms) / 1000:.1f} с, веток {len(eng.sims)}")
        states = self.states[name]
        tps = self.tp_seen.setdefault(name, [0] * len(eng.sims))
        decided: dict[tuple, list] = {}  # (время решения, версия стакана) → режимы веток
        for i, s in enumerate(eng.sims):
            prev, now = states[i], s.state
            if s.tp_fills != tps[i]:  # исполнение TP (в т. ч. частичное) — в журнал
                tps[i] = s.tp_fills
                self.jevent("SIM_TP", t=eng.last_t, engine=name, i=i, mode=s.mode, tp_fills=s.tp_fills, left_qty=s.Q)
            if prev == now:
                continue
            states[i] = now
            tag = dict(engine=name, mode=s.mode, variant=s.variant, usdt=s.usdt, profile=s.profile.name)
            r = s.result
            if prev == "SCAN" and r.get("decision_ms"):  # момент решения: время модели, получения кадра и биржи
                self.write("DECISION", **tag, decision_ms=r["decision_ms"], recv_ms=r.get("decision_recv_ms"),
                           exch_ms=r.get("decision_exch_ms"), version=eng.book.version, ask=s.decision_ask,
                           limit=s.buy_limit)
                decided.setdefault((r["decision_ms"], eng.book.version), []).append(s.mode)
            if now == "POSITION" and prev != "POSITION":
                self.write("ENTRY", **tag, entry_ms=r["entry_ms"], entry_px=r["entry_px"], buy_usdt=r["buy_usdt"],
                           fill_pct=r["fill_pct"], slippage_pct=r["slippage_pct"],
                           rise_from_first_ask_pct=r.get("rise_from_first_ask_pct"),
                           spread_pct=r.get("decision_spread_pct"), bid07_usdt=r.get("decision_bid07_usdt"))
                self.jevent("SIM_ENTRY", t=r["entry_ms"], i=i, **tag, entry_px=r["entry_px"], fill_pct=r["fill_pct"])
            elif now == "EXITING":
                self.jevent("SIM_EXIT_START", t=eng.last_t, engine=name, i=i, mode=s.mode, reason=s.exit_reason)
            elif now == "DONE":
                if "pnl_usdt" in r:
                    self.write("EXIT", **tag, reason=r["reason"], pnl_usdt=r["pnl_usdt"], pnl_pct=r["pnl_pct"],
                               hold_s=r["hold_s"], left_usdt=r["left_usdt"])
                    self.jevent("SIM_EXIT", t=r["exit_ms"], engine=name, i=i, mode=s.mode, reason=r["reason"],
                                pnl_usdt=r["pnl_usdt"])
                else:
                    self.write("NO_ENTRY", **tag, reason=r.get("reason"), skips=r.get("skips"),
                               buy_misses=r.get("buy_misses"))
        for (t, version), modes in decided.items():
            self.jevent("DECISION", t=t, engine=name, version=version, branches=len(modes), modes=sorted(set(modes)))

    # --- управление ---
    async def cap_loop(self) -> None:
        """Потолок из 30010 в логе пробы; CapEngine создаётся, как только он известен."""
        while not self.stop.is_set():
            await asyncio.sleep(2)
            if self.open_ms is None:
                continue
            cap = await asyncio.to_thread(find_cap, self.sym)
            if cap:
                self.start_cap(cap)
                return
            if self.feed.t() > self.open_ms + self.args.cap_wait * 60_000:
                self.cap_checked = True
                self.cap_status = cap_probe_status(self.sym)
                self.write("CAP_STATUS", **self.cap_status)
                self.jevent("CAP_STATUS", **self.cap_status)
                self.log(f"потолок: {self.cap_status['status']} — {self.cap_status['reason']}")
                return

    def start_cap(self, cap: tuple[float, str]) -> None:
        self.cap = cap
        self.engines["cap"] = CapEngine(self.cap_cfg, Opts(), cap[0], self.open_ms, self.info)
        self.write("CAP", cap=cap[0], src=cap[1], journal_open=not self.journal_closed)
        # по этому событию p0_three_way повторяет путь PAPER: движок CAP создаётся между кадрами
        self.jevent("CAP_ENGINE", cap=cap[0], src=cap[1], open_ms=self.open_ms)
        self.log(f"потолок {cap[0]} ({cap[1]}) — CAP BREAKOUT ведёт стакан, жду снятия")

    def engine_over(self, name: str, now: int) -> bool:
        eng = self.engines[name]
        if name == "main":
            return eng.done or now > self.open_ms + self.args.main_duration * 1000
        if eng.release is None:
            return now > self.open_ms + self.args.release_wait * 60_000
        return eng.done or now > eng.release + self.args.after_release * 1000

    async def watchdog(self) -> None:
        while not self.stop.is_set():
            await asyncio.sleep(1)
            self.tick(self.feed.t())

    def tick(self, now: int) -> None:
        """Раз в секунду: завершение движков по сроку, конец окна журнала, итог."""
        if self.open_ms is None:
            if now - self.started_ms > self.args.max_wait * 3_600_000:
                self.finish("not_opened")
            return
        for name, eng in list(self.engines.items()):
            if not self.done.get(name) and self.engine_over(name, now):
                if eng.release is not None:
                    eng.finish(now)
                    self.track(name)
                self.done[name] = True
                self.log(f"{name}: завершён")
                if name == "cap":
                    self.cap_done_ms = now
                    status = "RELEASE_OBSERVED" if eng.release is not None else "NO_RELEASE_OBSERVED_WITHIN_WINDOW"
                    self.jevent("CAP_DONE", t=now, status=status, release_ms=eng.release,
                                tail_ms=CAP_TAIL_MS if eng.release is not None else 0)
                    self.write("CAP_DONE", status=status, release_ms=eng.release, cand_ms=eng.cand_ms,
                               journal_until_ms=self.journal_end())
        end = self.journal_end()
        if not self.journal_closed and end is not None and now > end and self.journal is not None:
            self.close_journal()
        cap_pending = "cap" not in self.engines and not self.cap_checked
        journal_pending = "cap" in self.engines and not self.journal_closed and self.journal is not None
        if self.engines and all(self.done.get(n) for n in self.engines) and not cap_pending and not journal_pending:
            self.finish("done")

    def finish(self, result: str) -> None:
        if self.stop.is_set():
            return
        self.stop.set()
        rows = {name: eng.rows(self.sym) for name, eng in self.engines.items() if eng.release is not None}
        base = min(self.delay_ms) if self.delay_ms else 0
        lag = dict(frames=len(self.delay_ms), proc_p50_ms=pct(self.proc_ms, 0.5), proc_p99_ms=pct(self.proc_ms, 0.99),
                   proc_max_ms=max(self.proc_ms, default=0.0),
                   delay_over_best_p50_ms=pct(self.delay_ms, 0.5) - base,
                   delay_over_best_p99_ms=pct(self.delay_ms, 0.99) - base,
                   loop_lag_max_ms=round(self.loop_lag_max, 1), slow_frames=self.slow_frames, slow_engine_frames=self.slow_engine_frames,
                   clock_offset_ms=self.client.time_offset_ms, ab=self.ab_compare(),
                   loop_lag_max_rel_ms=(self.loop_lag_at - self.open_ms if self.loop_lag_at and self.open_ms else None),
                   loop_lag_max_where=self.loop_lag_where, gc=self.gcw.summary(),
                   stages={k: dict(p50=round(pct(v, 0.5), 3), p99=round(pct(v, 0.99), 3),
                                   max=round(max(v, default=0.0), 3)) for k, v in self.stage.items()})
        self.close_journal()
        self.write("SUMMARY", result=result, cap=self.cap, open_ms=self.open_ms, lag=lag, rows=rows, prereg=PREREG_ID,
                   times=self.engines["main"].times(), journal=self.journal_stats())
        self.log(f"готово: {result}; задержки {lag}")
        if self.open_ms is not None:
            notify(self.text(rows, lag))

    def text(self, rows: dict, lag: dict) -> str:
        """Итог PAPER для Telegram: BASELINE, MAIN_EARLY, CAP BREAKOUT — раздельно."""
        zones = market_params(self.info)[3] if self.info else ""
        lines = [f"🧪 PAPER — {self.sym}" + (f" [{zones}]" if zones else "")]
        tm = self.engines["main"].times()
        exec10 = (tm["first_exec"].get(f"{ORDER_USDT:g}") or {}).get("rel_ms")
        lines.append(f"от T0 ({'официальное время' if tm['exchange_open_ms'] else 'нет официального времени'}): "
                     f"стакан +{tm['first_book_rel_ms']} мс, первая сделка +{tm['first_trade_rel_ms']} мс, "
                     f"BUY {ORDER_USDT:g} USDT исполним с +{exec10} мс")
        main = [r for r in rows.get("main", []) if r["latency"] == LATENCY_MS]
        r0 = next((r for r in main if r["mode"] == "MAIN" and r["variant"] == "base" and r["profile"] == "MAIN_V21"
                   and r["usdt"] == ORDER_USDT), None)
        if r0 is None:
            lines.append("BASELINE: ветки не запустились (стакан не стал действительным)")
        elif "pnl_pct" in r0:
            lines.append(f"BASELINE (ТЗ v2.1): вход +{(r0['entry_ms'] - self.open_ms) / 1000:.1f} с, "
                         f"куплено {r0['buy_usdt']:.2f} USDT → {r0['pnl_pct']:+.2f} % ({r0['reason']}, {r0['hold_s']:.0f} с)")
        else:
            lines.append(f"BASELINE (ТЗ v2.1): нет входа ({no_entry_reason(r0)})")
        early = [r for r in main if r["mode"] == "MAIN_EARLY"]
        if early:
            got = [r for r in early if "pnl_usdt" in r]
            lines.append(f"MAIN_EARLY (эксперимент, {len(early)} веток): входов {len(got)}, итог "
                         f"{sum(r['pnl_usdt'] for r in got):+.2f} USDT"
                         + ("; " + ", ".join(f"{r['variant']} {r['profile'].removeprefix('MAIN_')} {r['pnl_pct']:+.1f} %"
                                              for r in got[:4]) if got else ""))
        if "cap" in rows:
            eng = self.engines["cap"]
            meta = dict(symbol=self.sym, status="released", zones=zones, cap=self.cap[0], src=self.cap[1],
                        release_ms=eng.release, open_ms=self.open_ms)
            body = brief(meta, rows["cap"], LATENCY_MS, ORDER_USDT).split("\n")[1:-1]
            lines += ["CAP BREAKOUT:"] + ["  " + x for x in body]
        elif self.cap:
            lines.append(f"CAP BREAKOUT: потолок {self.cap[0]}, снятия в пределах {self.args.release_wait:.0f} мин не было")
        else:
            st = self.cap_status or {"status": "NO_CAP_OBSERVED", "reason": ""}
            lines.append("CAP BREAKOUT: " + ("потолка нет (проба LIMIT_MAKER без 30010)" if st["status"] == "NO_CAP_OBSERVED"
                                             else f"потолок неизвестен — CAP PAPER не запущен: {st['reason']}"))
        lines.append(f"обработка кадра p99 {lag['proc_p99_ms']:.1f} мс (макс {lag['proc_max_ms']:.0f}), "
                     f"цикл событий макс {lag['loop_lag_max_ms']:.0f} мс, "
                     f"запаздывание кадров p99 {lag['delay_over_best_p99_ms']:.0f} мс сверх лучшего; ордеров нет")
        ab = lag["ab"]
        lines.append(f"соединения A/B: первый кадр после T0 {ab['first_frame_after_t0_ms']}, "
                     f"A−B по одним diff-ам p50 {ab['a_minus_b_p50_ms']:+.0f} мс ({ab['versions']} версий)")
        return "\n".join(lines)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: self.finish("stopped"))
        self.write("START_PAPER", symbol=self.sym, main_cfg=vars(self.main_cfg), cap_cfg=vars(self.cap_cfg),
                   prereg=PREREG_ID, code=code_version(), clock_offset_ms=self.client.time_offset_ms)
        self.log(f"PAPER запущен, лог {self.path}")
        await asyncio.gather(self.feed.run(self.stop), self.cap_loop(), self.watchdog(), self.loop_monitor())
        self.close_journal()
        self.out.close()


def cap_probe_status(symbol: str) -> dict:
    """Состояние потолка по журналу пробы (решение клиента 13.11, A–E, п. 3.4) — строго наблюдательно:
    CAP_FOUND — реально получен 30010; NO_CAP_OBSERVED — для перечисленных фактически отправленных цен и моментов 30010
    не получен (не CAP_ABSENT и не «потолка нет ниже цены»); CAP_UNKNOWN — пригодных проб нет или только отказы.
    Дополнительно — число подавленных пилотом G2 проб выше принятой цены (CAP_MAY_HAVE_BEEN_MISSED).
    30010 не выдумывается. Читаются все логи пробы пары за последние 6 ч (перезапуск)."""
    logs = sorted((ROOT / "logs").glob(f"probe_{symbol}_2*.jsonl"))
    if not logs:
        return {"status": "CAP_UNKNOWN", "reason": "нет лога пробы"}
    fresh = [p for p in logs if time.time() - p.stat().st_mtime < 6 * 3600] or logs[-1:]
    sent, rejected, skipped, missed = [], {}, None, 0
    for path in fresh:
        with path.open(encoding="utf-8") as f:
            for line in f:
                if '"check": "cap_suppressed"' in line:
                    missed += bool(json.loads(line).get("cap_may_have_been_missed"))
                    continue
                if '"check": "cap_maker"' not in line:
                    continue
                e = json.loads(line)
                state = e.get("state", "")
                if state == "ACCEPTED":
                    rel = e.get("rel_ms")
                    sent.append(f"{(e.get('params') or {}).get('price')}"
                                + (f" (T0{rel / 1000:+.0f} с)" if rel is not None else ""))
                elif "30010" in state:
                    return {"status": "CAP_FOUND", "reason": "30010 в логе пробы", "may_have_been_missed": missed}
                elif state.startswith(("skipped", "stop", "waiting")):
                    skipped = state
                else:  # 10007, 30004 и т. п. о потолке ничего не говорят
                    code = state.split()[1] if state.startswith("ERR ") else state
                    rejected[code] = rejected.get(code, 0) + 1
    tail = f"; CAP_MAY_HAVE_BEEN_MISSED: {missed}" if missed else ""
    if sent:
        return {"status": "NO_CAP_OBSERVED", "sent": sent, "may_have_been_missed": missed,
                "reason": f"NO_CAP_OBSERVED: для отправленных цен {', '.join(sent)} 30010 не получен — наблюдение, "
                          f"не CAP_ABSENT{tail}"}
    why = skipped or (f"LIMIT_MAKER только отклонялся: {rejected}" if rejected else "LIMIT_MAKER после открытия не подавался")
    return {"status": "CAP_UNKNOWN", "reason": why + tail, "may_have_been_missed": missed}


def code_version() -> str:
    """Версия кода для журнала (клиент 08.11): метка из файла VERSION (выкладка на сервер, где нет git) или коммит git,
    и всегда — отпечаток содержимого исполняемого кода (tools/paper.py, tools/core/*.py), по которому версию можно
    сверить с репозиторием."""
    import hashlib
    import subprocess
    h = hashlib.sha256()
    for f in [ROOT / "tools" / "paper.py"] + sorted((ROOT / "tools" / "core").glob("*.py")):
        h.update(f.name.encode() + b"\0" + f.read_bytes())
    digest = h.hexdigest()[:12]
    version_file = ROOT / "VERSION"
    if version_file.exists():
        return f"{version_file.read_text(encoding='utf-8').strip()}#{digest}"
    try:
        head = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                              timeout=5).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=no"],
                               capture_output=True, text=True, timeout=5).stdout.strip()
        return f"{head + ('+dirty' if dirty else '') if head else 'unknown'}#{digest}"
    except (OSError, subprocess.SubprocessError):
        return f"unknown#{digest}"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("symbol")
    p.add_argument("--max-wait", type=float, default=72, help="сколько часов ждать открытия (72)")
    p.add_argument("--main-duration", type=float, default=120, help="основная стратегия — до T0 + N с (120)")
    p.add_argument("--cap-wait", type=float, default=12, help="ждать 30010 в логе пробы до T+N мин (12)")
    p.add_argument("--release-wait", type=float, default=30, help="ждать снятия потолка до T+N мин (30)")
    p.add_argument("--after-release", type=float, default=170, help="вести ветки CAP до снятия + N с (170, как REPLAY)")
    args = p.parse_args()
    args.symbol = args.symbol.upper()
    load_dotenv(ROOT / ".env")
    client = Client(os.environ.get("MEXC_API_KEY"), os.environ.get("MEXC_API_SECRET"), 10.0)
    res = measure()  # то же правило, что у записи и у пересчёта в LiveFeed (core/clock.py)
    if res:
        client.time_offset_ms = res["offset_ms"]
    else:
        client.sync_time()
    asyncio.run(Paper(args, client).run())


if __name__ == "__main__":
    main()
