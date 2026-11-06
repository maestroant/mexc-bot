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
import json
import os
import signal
import time
from argparse import Namespace
from datetime import datetime

from cap_replay import brief
from core.cap_breakout import (EARLY_USDT, MAIN_EARLY_PAPER, ORDER_USDT, PREREG_ID, CapEngine, MainEngine, Opts,
                               find_cap, market_params, no_entry_reason)
from core.feed import events_of, frame_meta
from core.live import LiveFeed
from notify import notify
from probe_listing import ROOT, Client, load_dotenv

LATENCY_MS = 50  # задержка ордера в модели — как в основном прогоне REPLAY


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
        self.feed = LiveFeed(self.sym, client, self.on_entry, on_log=self.log)
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
        path = ROOT / "logs" / f"paper_{self.sym}_{datetime.now():%Y%m%d_%H%M%S}.jsonl"
        self.out = path.open("a", encoding="utf-8")
        self.path = path

    def log(self, msg: str) -> None:
        print(f"[{datetime.now():%H:%M:%S}] {self.sym} {msg}", flush=True)

    def write(self, event: str, **data) -> None:
        self.out.write(json.dumps({"t": self.feed.t(), "event": event, **data}, ensure_ascii=False, default=str) + "\n")
        self.out.flush()

    # --- поток ---
    def on_entry(self, entry: dict) -> None:
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
        self.proc_ms.append(1000 * (time.perf_counter() - started))
        main = self.engines["main"]  # T0 — по единой модели ядра: официальное время, иначе стакан или первая сделка
        if self.open_ms is None and main.open_ms is not None and events[-1][0] >= main.open_ms:
            self.open_ms = main.open_ms
            self.write("OPEN", open_ms=self.open_ms, exchange_open_ms=main.exchange_open_ms())
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
            self.write("START", engine=name, start_ms=eng.release, sims=len(eng.sims), prereg=PREREG_ID,
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
                   delay_over_best_p99_ms=pct(self.delay_ms, 0.99) - base)
        self.write("SUMMARY", result=result, cap=self.cap, open_ms=self.open_ms, lag=lag, rows=rows, prereg=PREREG_ID,
                   times=self.engines["main"].times())
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
            lines.append("CAP BREAKOUT: потолка нет")
        lines.append(f"обработка кадра p99 {lag['proc_p99_ms']:.1f} мс (макс {lag['proc_max_ms']:.0f}), "
                     f"запаздывание кадров p99 {lag['delay_over_best_p99_ms']:.0f} мс сверх лучшего; ордеров нет")
        return "\n".join(lines)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: self.finish("stopped"))
        self.write("START_PAPER", symbol=self.sym, main_cfg=vars(self.main_cfg), cap_cfg=vars(self.cap_cfg))
        self.log(f"PAPER запущен, лог {self.path}")
        await asyncio.gather(self.feed.run(self.stop), self.cap_loop(), self.watchdog())
        self.out.close()


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
