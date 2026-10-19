"""Запись всех рыночных данных MEXC по паре вокруг листинга.

Пишет в logs/rec_<SYMBOL>_<время>.jsonl.gz по строке на событие:
  src=ws_pub   публичные WS-потоки (protobuf, декодированные):
               сделки 10ms, diff-стакан 10ms, лучшие цены 10ms, стакан 20 уровней, свеча 1m, мини-тикер
  src=ws_priv  приватные WS-потоки нашего аккаунта (ордера, сделки, баланс), если есть ключи
  src=rest     снимки REST: exchangeInfo, стакан 1000 уровней, последние сделки, тикеры
  src=event    служебные события: подключения, подписки, OPEN_DETECTED, ошибки
Поле t — локальное время получения (ms, с поправкой на часы биржи), у WS есть и время биржи внутри data.

До открытия пары (status != 1) REST опрашивается раз в --pre-interval секунд, после — раз в --interval.
Открытие (T=0) — первая сделка или первый двусторонний bookTicker.
Запись идёт до T + --duration секунд; если пара не открылась за --max-wait часов — завершение.

Нужен venv с websockets и protobuf и скомпилированные tools/mexc_pb (см. deploy).
  .venv/bin/python tools/listing_recorder.py MTOUSDT --duration 3600
"""

import argparse
import asyncio
import base64
import gzip
import json
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

import websockets
from google.protobuf.json_format import MessageToDict

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "mexc_pb"))
from PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper  # noqa: E402

from notify import notify  # noqa: E402
from probe_listing import ROOT, Client, load_dotenv, now_ms  # noqa: E402

WS_URL = "wss://wbs-api.mexc.com/ws"


def public_channels(sym: str) -> list[str]:
    return [
        f"spot@public.aggre.deals.v3.api.pb@10ms@{sym}",
        f"spot@public.aggre.depth.v3.api.pb@10ms@{sym}",
        f"spot@public.aggre.bookTicker.v3.api.pb@10ms@{sym}",
        f"spot@public.limit.depth.v3.api.pb@{sym}@20",
        f"spot@public.kline.v3.api.pb@{sym}@Min1",
        f"spot@public.miniTicker.v3.api.pb@{sym}@UTC+8",
    ]


PRIVATE_CHANNELS = [
    "spot@private.orders.v3.api.pb",
    "spot@private.deals.v3.api.pb",
    "spot@private.account.v3.api.pb",
]


class Recorder:
    def __init__(self, args, client: Client):
        self.args = args
        self.sym = args.symbol
        self.client = client
        self.started_ms = now_ms()
        self.open_ms: int | None = None
        self.online = False  # status == 1 в exchangeInfo
        self.absent_since: int | None = None  # с какого момента пары нет в exchangeInfo
        self.stop = asyncio.Event()
        self.counts: dict[str, int] = {}
        log_dir = ROOT / "logs"
        log_dir.mkdir(exist_ok=True)
        self.path = log_dir / f"rec_{self.sym}_{datetime.now():%Y%m%d_%H%M%S}.jsonl.gz"
        self.out = gzip.open(self.path, "at", encoding="utf-8", compresslevel=5)
        self.last_flush = time.monotonic()

    def t(self) -> int:
        return now_ms() + self.client.time_offset_ms

    def write(self, src: str, ch: str, data, **extra) -> None:
        entry = {"t": self.t(), "src": src, "ch": ch, "data": data, **extra}
        self.out.write(json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n")
        key = f"{src}:{ch.split('@')[1]}" if "@" in ch else f"{src}:{ch}"
        self.counts[key] = self.counts.get(key, 0) + 1
        if time.monotonic() - self.last_flush > 2:
            self.out.flush()
            self.last_flush = time.monotonic()

    def event(self, name: str, **info) -> None:
        self.write("event", name, info)
        print(f"[{datetime.now():%H:%M:%S}] {self.sym} {name} {info if info else ''}", flush=True)

    def mark_open(self, reason: str) -> None:
        if self.open_ms is None:
            self.open_ms = self.t()
            self.event("OPEN_DETECTED", reason=reason, open_ms=self.open_ms)
            notify(f"🟢 {self.sym}: открытие торгов обнаружено ({reason}), запись идёт {self.args.duration:.0f} с")

    # --- WebSocket ------------------------------------------------------------

    def handle_frame(self, src: str, frame) -> None:
        if isinstance(frame, bytes):
            try:
                msg = PushDataV3ApiWrapper.FromString(frame)
                data = MessageToDict(msg, preserving_proto_field_name=True)
            except Exception as e:
                self.write(src, "undecoded", base64.b64encode(frame).decode(), error=str(e))
                return
            ch = data.get("channel", "?")
            self.write(src, ch, data)
            if src == "ws_pub" and self.open_ms is None:
                deals = data.get("publicAggreDeals", {}).get("deals")
                bt = data.get("publicAggreBookTicker", {})
                if deals:
                    self.mark_open("first_deal")
                # у приостановленной старой пары (status=2) стакан тоже двусторонний — это не открытие
                elif self.online and float(bt.get("bidPrice") or 0) > 0 and float(bt.get("askPrice") or 0) > 0:
                    self.mark_open("two_sided_book")
        else:
            try:
                data = json.loads(frame)
            except ValueError:
                data = frame
            if not (isinstance(data, dict) and data.get("msg") == "PONG"):
                self.write(src, "text", data)

    async def ws_loop(self, src: str, url: str, channels: list[str]) -> None:
        while not self.stop.is_set():
            try:
                async with websockets.connect(url, ping_interval=None, max_size=None, open_timeout=15) as ws:
                    self.event("WS_CONNECTED", src=src)
                    await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": channels}))
                    pinger = asyncio.create_task(self.ws_pinger(ws))
                    try:
                        while not self.stop.is_set():
                            try:
                                frame = await asyncio.wait_for(ws.recv(), timeout=1)
                            except asyncio.TimeoutError:
                                continue
                            self.handle_frame(src, frame)
                    finally:
                        pinger.cancel()
            except Exception as e:
                if self.stop.is_set():
                    break
                self.event("WS_ERROR", src=src, error=f"{type(e).__name__}: {e}")
                await asyncio.sleep(1)

    async def ws_pinger(self, ws) -> None:
        while True:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"method": "PING"}))

    async def private_loop(self) -> None:
        r = await asyncio.to_thread(self.client.call, "POST", "/api/v3/userDataStream", None, True)
        key = r.body.get("listenKey") if r.ok and isinstance(r.body, dict) else None
        if not key:
            self.event("LISTENKEY_ERROR", error=r.label(), body=r.body)
            return
        keepalive = asyncio.create_task(self.listenkey_keepalive(key))
        try:
            await self.ws_loop("ws_priv", f"{WS_URL}?listenKey={key}", PRIVATE_CHANNELS)
        finally:
            keepalive.cancel()
            await asyncio.to_thread(self.client.call, "DELETE", "/api/v3/userDataStream", {"listenKey": key}, True)

    async def listenkey_keepalive(self, key: str) -> None:
        while True:
            await asyncio.sleep(25 * 60)
            await asyncio.to_thread(self.client.call, "PUT", "/api/v3/userDataStream", {"listenKey": key}, True)

    # --- REST -----------------------------------------------------------------

    async def rest(self, ch: str, path: str, params: dict) -> object:
        r = await asyncio.to_thread(self.client.call, "GET", path, params)
        self.write("rest", ch, r.body, http=r.http, latency_ms=round(r.latency_ms, 1))
        return r.body if r.ok else None

    async def rest_loop(self) -> None:
        sym = {"symbol": self.sym}
        last_info = 0.0
        while not self.stop.is_set():
            started = time.monotonic()
            if not self.online or started - last_info >= 10:
                info = await self.rest("exchangeInfo", "/api/v3/exchangeInfo", sym)
                last_info = started
                symbols = info.get("symbols") if isinstance(info, dict) else None
                status = symbols[0].get("status") if symbols else None
                if isinstance(info, dict) and not symbols:  # пару убрали из exchangeInfo (листинг отменён)
                    self.absent_since = self.absent_since or self.t()
                    if self.t() - self.absent_since > self.args.absent_stop * 60 * 1000:
                        self.event("DONE", reason="symbol_absent", minutes=self.args.absent_stop)
                        self.stop.set()
                        return
                elif symbols:
                    self.absent_since = None
                if status == "1" and not self.online:
                    self.online = True
                    self.event("STATUS_ONLINE", api_allowed=symbols[0].get("isSpotTradingAllowed"))
            if self.online:
                await asyncio.gather(
                    self.rest("depth", "/api/v3/depth", {**sym, "limit": 1000}),
                    self.rest("trades", "/api/v3/trades", {**sym, "limit": 1000}),
                    self.rest("ticker24h", "/api/v3/ticker/24hr", sym),
                    self.rest("bookTicker", "/api/v3/ticker/bookTicker", sym),
                )
            interval = self.args.interval if self.online else self.args.pre_interval
            await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))

    # --- управление -----------------------------------------------------------

    async def watchdog(self) -> None:
        while not self.stop.is_set():
            now = self.t()
            if self.open_ms is not None and now >= self.open_ms + self.args.duration * 1000:
                self.event("DONE", reason="duration")
                break
            if self.open_ms is None and now - self.started_ms > self.args.max_wait * 3600 * 1000:
                self.event("DONE", reason="max_wait")
                break
            await asyncio.sleep(1)
        self.stop.set()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop.set)
        self.event("START", file=str(self.path), duration=self.args.duration, max_wait_h=self.args.max_wait)
        tasks = [
            asyncio.create_task(self.ws_loop("ws_pub", WS_URL, public_channels(self.sym))),
            asyncio.create_task(self.rest_loop()),
            asyncio.create_task(self.watchdog()),
        ]
        if self.client.has_keys:
            tasks.append(asyncio.create_task(self.private_loop()))
        await self.stop.wait()
        await asyncio.sleep(1.5)  # дать циклам выйти по флагу
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.write("event", "COUNTS", self.counts)
        self.out.close()
        size_mb = self.path.stat().st_size / 1e6
        print(f"Записано: {self.path} ({size_mb:.1f} MB)\n{json.dumps(self.counts, indent=1)}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("symbol")
    p.add_argument("--duration", type=float, default=3600, help="секунд записи после открытия (3600)")
    p.add_argument("--max-wait", type=float, default=72, help="сколько часов ждать открытия (72)")
    p.add_argument("--interval", type=float, default=1.0, help="интервал REST-снимков после status=1, с (1.0)")
    p.add_argument("--pre-interval", type=float, default=10.0, help="интервал опроса exchangeInfo до status=1, с (10)")
    p.add_argument("--absent-stop", type=float, default=30,
                   help="завершить, если пары нет в exchangeInfo дольше N минут (листинг отменён) (30)")
    args = p.parse_args()
    args.symbol = args.symbol.upper().replace("/", "").replace("_", "").replace("-", "")

    load_dotenv(ROOT / ".env")
    client = Client(os.environ.get("MEXC_API_KEY"), os.environ.get("MEXC_API_SECRET"), 10.0)
    client.sync_time()
    asyncio.run(Recorder(args, client).run())


if __name__ == "__main__":
    main()
