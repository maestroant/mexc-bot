"""PAPER CAP BREAKOUT: тот же движок, что у REPLAY (core.cap_breakout.CapEngine), на живом потоке, без ордеров.

Ждёт первую сделку пары (T=0). Потолок берёт из 30010 в логе пробы (probe_listing --cap-test), проверяя раз в 2 с
до T+--cap-wait мин; потолка нет — листинг не замороженный, выход. После снятия потолка ведёт те же ветки, что
REPLAY (FAST, CONFIRMED, EARLY с профилями, размерами и вариантами конкуренции), в реальном времени: исполнение —
модель по живому стакану (задержка, частичный BUY, полоса цены, minNotional), ордера не ставятся.

Пишет logs/paper_<SYMBOL>_<время>.jsonl: OPEN, CAP, RELEASE, ENTRY и EXIT каждой ветки, SUMMARY (строки результатов
и задержки обработки); итог — в Telegram с пометкой PAPER, отдельно от REPLAY и LIVE.
Задержка: время обработки кадра движком и запаздывание кадра относительно sendTime биржи сверх лучшего наблюдённого
(часы сервера и биржи расходятся на сотни мс, поэтому абсолютное значение не показательно).

Обычно запускает наблюдатель вместе с пробой и записью: .venv/bin/python -u tools/paper_cap.py SYMBOL
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
from core.cap_breakout import EARLY_USDT, ORDER_USDT, CapEngine, Opts, find_cap, market_params
from core.feed import events_of
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
        self.cfg = Namespace(modes=["FAST", "CONFIRMED", "EARLY"], early={}, early_sweep=False, latency=[LATENCY_MS],
                             usdt=[ORDER_USDT], early_usdt=list(EARLY_USDT))
        self.stop = asyncio.Event()
        self.feed = LiveFeed(self.sym, client, self.on_entry, on_log=self.log)
        self.started_ms = self.feed.t()
        self.info: dict = {}
        self.open_ms: int | None = None
        self.cap: tuple[float, str] | None = None
        self.engine: CapEngine | None = None
        self.states: list[str] = []
        self.proc_ms: list[float] = []
        self.delay_ms: list[float] = []  # t получения − sendTime биржи
        self.result = "running"
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
            return
        if isinstance(data, dict) and data.get("sendTime"):
            self.delay_ms.append(entry["t"] - int(data["sendTime"]))
        events = events_of(entry)
        if not events:
            return
        if self.open_ms is None and any(kind == "deal" for _, kind, _ in events):
            self.open_ms = events[0][0]
            self.write("OPEN", open_ms=self.open_ms)
            self.log("открытие (первая сделка), жду потолок из лога пробы")
        if self.engine is None:
            return
        started = time.perf_counter()
        for t, kind, ev in events:
            self.engine.on_event(t, kind, ev)
        self.proc_ms.append(1000 * (time.perf_counter() - started))
        self.track()

    def track(self) -> None:
        """Записать переходы веток: вход, выход, отказ от входа."""
        eng = self.engine
        if eng.release is not None and not self.states:
            self.states = ["SCAN"] * len(eng.sims)
            self.write("RELEASE", release_ms=eng.release, cand_ms=eng.cand_ms, sims=len(eng.sims),
                       t_from_open_s=(eng.release - self.open_ms) / 1000)
            self.log(f"потолок снят на T+{(eng.release - self.open_ms) / 1000:.1f} с, веток {len(eng.sims)}")
        for i, s in enumerate(eng.sims):
            prev, now = self.states[i], s.state
            if prev == now:
                continue
            self.states[i] = now
            tag = dict(mode=s.mode, variant=s.variant, usdt=s.usdt, profile=s.profile.name)
            if now == "POSITION" and prev != "POSITION":
                r = s.result
                self.write("ENTRY", **tag, entry_ms=r["entry_ms"], entry_px=r["entry_px"], buy_usdt=r["buy_usdt"],
                           fill_pct=r["fill_pct"], slippage_pct=r["slippage_pct"])
            elif now == "DONE":
                r = s.result
                if "pnl_usdt" in r:
                    self.write("EXIT", **tag, reason=r["reason"], pnl_usdt=r["pnl_usdt"], pnl_pct=r["pnl_pct"],
                               hold_s=r["hold_s"], left_usdt=r["left_usdt"])
                else:
                    self.write("NO_ENTRY", **tag, reason=r.get("reason"), skips=r.get("skips"),
                               buy_misses=r.get("buy_misses"))

    # --- управление ---
    async def cap_loop(self) -> None:
        """Потолок из 30010 в логе пробы; движок создаётся, как только он известен."""
        while not self.stop.is_set():
            await asyncio.sleep(2)
            if self.open_ms is None:
                continue
            self.cap = await asyncio.to_thread(find_cap, self.sym)
            if self.cap:
                self.engine = CapEngine(self.cfg, Opts(), self.cap[0], self.open_ms, self.info)
                self.write("CAP", cap=self.cap[0], src=self.cap[1])
                self.log(f"потолок {self.cap[0]} ({self.cap[1]}) — веду стакан, жду снятия")
                return
            if self.feed.t() > self.open_ms + self.args.cap_wait * 60_000:
                self.finish("no_cap")
                return

    async def watchdog(self) -> None:
        while not self.stop.is_set():
            await asyncio.sleep(1)
            now = self.feed.t()
            eng = self.engine
            if self.open_ms is None and now - self.started_ms > self.args.max_wait * 3_600_000:
                self.finish("not_opened")
            elif eng is not None and eng.release is not None and (
                    eng.done or now > eng.release + self.args.after_release * 1000):
                self.finish("released")
            elif eng is not None and eng.release is None and now > self.open_ms + self.args.release_wait * 60_000:
                self.finish("not_released")
            if eng is not None:
                eng.info = self.info or eng.info  # шаги цены и полоса — по последнему exchangeInfo до снятия

    def finish(self, result: str) -> None:
        if self.stop.is_set():
            return
        self.result = result
        self.stop.set()
        eng = self.engine
        rows: list[dict] = []
        if eng is not None and eng.release is not None:
            eng.finish(self.feed.t())
            self.track()
            rows = eng.rows(self.sym)
        base = min(self.delay_ms) if self.delay_ms else 0
        lag = dict(frames=len(self.delay_ms), proc_p50_ms=pct(self.proc_ms, 0.5), proc_p99_ms=pct(self.proc_ms, 0.99),
                   proc_max_ms=max(self.proc_ms, default=0.0),
                   delay_over_best_p50_ms=pct(self.delay_ms, 0.5) - base,
                   delay_over_best_p99_ms=pct(self.delay_ms, 0.99) - base)
        self.write("SUMMARY", result=result, cap=self.cap, open_ms=self.open_ms, lag=lag, rows=rows)
        self.log(f"готово: {result}; задержки {lag}")
        if result == "released" or (self.cap and result == "not_released"):
            zones = market_params(self.info)[3] if self.info else ""
            meta = dict(symbol=self.sym, status=result, zones=zones, cap=self.cap[0], src=self.cap[1],
                        release_ms=eng.release if eng else None, open_ms=self.open_ms)
            text = brief(meta, rows, LATENCY_MS, ORDER_USDT).replace("📊 REPLAY", "🧪 PAPER", 1)
            notify(text + f"\nPAPER: обработка кадра p99 {lag['proc_p99_ms']:.1f} мс (макс {lag['proc_max_ms']:.0f}), "
                          f"запаздывание кадров p99 {lag['delay_over_best_p99_ms']:.0f} мс сверх лучшего")

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: self.finish("stopped"))
        self.write("START", symbol=self.sym, cfg=vars(self.cfg))
        self.log(f"PAPER запущен, лог {self.path}")
        await asyncio.gather(self.feed.run(self.stop), self.cap_loop(), self.watchdog())
        self.out.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("symbol")
    p.add_argument("--max-wait", type=float, default=72, help="сколько часов ждать открытия (72)")
    p.add_argument("--cap-wait", type=float, default=12, help="ждать 30010 в логе пробы до T+N мин (12)")
    p.add_argument("--release-wait", type=float, default=30, help="ждать снятия потолка до T+N мин (30)")
    p.add_argument("--after-release", type=float, default=170, help="вести ветки до снятия + N с (170, как REPLAY)")
    args = p.parse_args()
    args.symbol = args.symbol.upper()
    load_dotenv(ROOT / ".env")
    client = Client(os.environ.get("MEXC_API_KEY"), os.environ.get("MEXC_API_SECRET"), 10.0)
    client.sync_time()
    asyncio.run(Paper(args, client).run())


if __name__ == "__main__":
    main()
