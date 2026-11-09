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

Обычно запускает наблюдатель вместе с пробой и записью: .venv/bin/python -u tools/paper.py SYMBOL
"""

import argparse
import asyncio
import gzip
import json
import os
import signal
import time
from argparse import Namespace
from collections import deque
from datetime import datetime

from cap_replay import brief
from core.cap_breakout import (EARLY_USDT, EXEC_MODEL_ID, MAIN_EARLY_PAPER, ORDER_USDT, PREREG_ID, CapEngine,
                               MainEngine, Opts, find_cap, market_params, no_entry_reason)
from core.feed import events_of, frame_meta
from core.live import LiveFeed
from notify import notify
from probe_listing import ROOT, Client, load_dotenv

LATENCY_MS = 50  # задержка ордера в модели — как в основном прогоне REPLAY
JOURNAL_BEFORE_MS, JOURNAL_AFTER_MS = 120_000, 300_000  # журнал кадров PAPER: T0 − 2 мин … T0 + 5 мин
LOOP_TICK_MS, LOOP_LAG_MS, SLOW_FRAME_MS = 10, 20, 20  # контроль цикла событий и медленных кадров


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
        # основная стратегия ведёт стакан с запуска, T0 — по модели MainEngine; cap — CAP BREAKOUT, когда известен потолок
        self.engines: dict[str, CapEngine] = {"main": MainEngine(self.main_cfg, Opts(), None, {})}
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
        self.slow_frames = 0
        self.first_after_open: dict[str, int] = {}  # соединение → время первого WS-кадра после T0
        self.depth_recv: dict[str, dict[int, int]] = {"A": {}, "B": {}}  # toVersion → время получения (окно журнала)

    def log(self, msg: str) -> None:
        print(f"[{datetime.now():%H:%M:%S}] {self.sym} {msg}", flush=True)

    def write(self, event: str, **data) -> None:
        self.out.write(json.dumps({"t": self.feed.t(), "event": event, **data}, ensure_ascii=False, default=str) + "\n")
        self.out.flush()

    # --- журнал кадров (P0) ---
    def t0_hint(self) -> int | None:
        return self.open_ms or self.feed.first_open or (int(self.info["firstOpenTime"])
                                                         if self.info.get("firstOpenTime") else None)

    def jwrite(self, entry: dict) -> None:
        """Строка журнала; до T0 − JOURNAL_BEFORE_MS — в кольцевой буфер, после T0 + JOURNAL_AFTER_MS — не пишется."""
        if self.journal_closed:
            return
        line = json.dumps(entry, ensure_ascii=False, separators=(",", ":"), default=str) + "\n"
        t0, t = self.t0_hint(), entry["t"]
        if t0 is None or t < t0 - JOURNAL_BEFORE_MS:
            self.ring.append((t, line))
            while self.ring and self.ring[0][0] < t - JOURNAL_BEFORE_MS:
                self.ring.popleft()
            return
        if t > t0 + JOURNAL_AFTER_MS:
            self.close_journal()
            return
        if self.journal is None:
            self.journal = gzip.open(self.journal_path, "wt", encoding="utf-8")
            for rt, ln in self.ring:
                if rt >= t0 - JOURNAL_BEFORE_MS:
                    self.journal.write(ln)
            self.ring.clear()
        self.journal.write(line)

    def close_journal(self) -> None:
        if self.journal is not None and not self.journal_closed:
            self.journal.close()
            self.log(f"журнал кадров: {self.journal_path}")
        self.journal_closed = True

    def jevent(self, name: str, t: int | None = None, **data) -> None:
        self.jwrite({"t": self.feed.t() if t is None else t, "src": "event", "ch": name, "data": data})

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
        if deals:
            meta.update(deals=len(deals.get("deals", [])))
        self.jwrite({"t": entry["t"], "src": "ws_shadow", "ch": entry["ch"], "conn": "B", "data": meta})

    async def loop_monitor(self) -> None:
        """Задержка цикла событий: насколько позже срабатывает sleep(LOOP_TICK_MS); больше LOOP_LAG_MS — в журнал."""
        while not self.stop.is_set():
            started = time.perf_counter()
            await asyncio.sleep(LOOP_TICK_MS / 1000)
            lag = 1000 * (time.perf_counter() - started) - LOOP_TICK_MS
            self.loop_lag_max = max(self.loop_lag_max, lag)
            if lag > LOOP_LAG_MS:
                self.jevent("LOOP_LAG", lag_ms=round(lag, 1))

    def ab_compare(self) -> dict:
        """Одни и те же diff-ы (по toVersion) на соединениях A и B: на сколько B получал раньше/позже A."""
        a, b = self.depth_recv["A"], self.depth_recv["B"]
        common = sorted(set(a) & set(b))
        diffs = [a[v] - b[v] for v in common]
        t0 = self.t0_hint()
        first = {c: (v - t0 if t0 else None) for c, v in self.first_after_open.items()}
        return dict(versions=len(common), a_minus_b_p50_ms=pct(diffs, 0.5), a_minus_b_p01_ms=pct(diffs, 0.01),
                    a_minus_b_p99_ms=pct(diffs, 0.99), first_frame_after_t0_ms=first)

    # --- поток ---
    def on_entry(self, entry: dict) -> None:
        self.jwrite(entry)
        self.note_frame(entry)
        data = entry.get("data")
        if entry["ch"] == "exchangeInfo" and isinstance(data, dict):
            self.info = (data.get("symbols") or [{}])[0] or self.info
            for eng in self.engines.values():
                if eng.release is None:
                    eng.info = self.info  # шаги цены, полоса и комиссии — по последнему exchangeInfo до старта веток
            return
        if isinstance(data, dict) and data.get("sendTime"):
            self.delay_ms.append(entry["t"] - int(data["sendTime"]))
        events = events_of(entry)
        if not events:
            return
        meta = frame_meta(entry)
        started = time.perf_counter()
        for name, eng in self.engines.items():
            if not self.done.get(name):
                for t, kind, ev in events:
                    eng.on_event(t, kind, ev, *meta)
        proc = 1000 * (time.perf_counter() - started)
        self.proc_ms.append(proc)
        if proc > SLOW_FRAME_MS:
            self.slow_frames += 1
            self.jevent("SLOW_FRAME", proc_ms=round(proc, 1), kinds=sorted({k for _, k, _ in events}),
                        sims=sum(len(e.sims) for e in self.engines.values()))
        main = self.engines["main"]  # T0 — по единой модели ядра: официальное время, иначе стакан или первая сделка
        if self.open_ms is None and main.open_ms is not None and events[-1][0] >= main.open_ms:
            self.open_ms = main.open_ms
            self.write("OPEN", open_ms=self.open_ms, exchange_open_ms=main.exchange_open_ms())
            self.jevent("OPEN_DETECTED", t=events[-1][0], open_ms=self.open_ms)  # по нему журнал читают main_replay и paper_vs_replay
            self.log("открытие: основная стратегия идёт, жду потолок из лога пробы")
        for name in self.engines:
            self.track(name)

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
        for i, s in enumerate(eng.sims):
            prev, now = states[i], s.state
            if prev == now:
                continue
            states[i] = now
            tag = dict(engine=name, mode=s.mode, variant=s.variant, usdt=s.usdt, profile=s.profile.name)
            r = s.result
            if now == "POSITION" and prev != "POSITION":
                self.write("ENTRY", **tag, entry_ms=r["entry_ms"], entry_px=r["entry_px"], buy_usdt=r["buy_usdt"],
                           fill_pct=r["fill_pct"], slippage_pct=r["slippage_pct"],
                           rise_from_first_ask_pct=r.get("rise_from_first_ask_pct"),
                           spread_pct=r.get("decision_spread_pct"), bid07_usdt=r.get("decision_bid07_usdt"))
            elif now == "DONE":
                if "pnl_usdt" in r:
                    self.write("EXIT", **tag, reason=r["reason"], pnl_usdt=r["pnl_usdt"], pnl_pct=r["pnl_pct"],
                               hold_s=r["hold_s"], left_usdt=r["left_usdt"])
                else:
                    self.write("NO_ENTRY", **tag, reason=r.get("reason"), skips=r.get("skips"),
                               buy_misses=r.get("buy_misses"))

    # --- управление ---
    async def cap_loop(self) -> None:
        """Потолок из 30010 в логе пробы; CapEngine создаётся, как только он известен."""
        while not self.stop.is_set():
            await asyncio.sleep(2)
            if self.open_ms is None:
                continue
            self.cap = await asyncio.to_thread(find_cap, self.sym)
            if self.cap:
                self.engines["cap"] = CapEngine(self.cap_cfg, Opts(), self.cap[0], self.open_ms, self.info)
                self.write("CAP", cap=self.cap[0], src=self.cap[1])
                self.log(f"потолок {self.cap[0]} ({self.cap[1]}) — CAP BREAKOUT ведёт стакан, жду снятия")
                return
            if self.feed.t() > self.open_ms + self.args.cap_wait * 60_000:
                self.cap_checked = True
                self.log("потолка нет — CAP BREAKOUT не нужен")
                return

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
            now = self.feed.t()
            if self.open_ms is None:
                if now - self.started_ms > self.args.max_wait * 3_600_000:
                    self.finish("not_opened")
                continue
            for name, eng in self.engines.items():
                if not self.done.get(name) and self.engine_over(name, now):
                    if eng.release is not None:
                        eng.finish(now)
                        self.track(name)
                    self.done[name] = True
                    self.log(f"{name}: завершён")
            cap_pending = "cap" not in self.engines and not self.cap_checked
            if self.engines and all(self.done.get(n) for n in self.engines) and not cap_pending:
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
                   loop_lag_max_ms=round(self.loop_lag_max, 1), slow_frames=self.slow_frames,
                   clock_offset_ms=self.client.time_offset_ms, ab=self.ab_compare())
        self.write("SUMMARY", result=result, cap=self.cap, open_ms=self.open_ms, lag=lag, rows=rows, prereg=PREREG_ID,
                   times=self.engines["main"].times())
        self.log(f"готово: {result}; задержки {lag}")
        self.close_journal()
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
            lines.append("CAP BREAKOUT: потолка нет")
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
    client.sync_time()
    asyncio.run(Paper(args, client).run())


if __name__ == "__main__":
    main()
