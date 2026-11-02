"""Живой поток пары для PAPER (и будущего LIVE) в формате записи listing_recorder.

WebSocket: сделки и diff-стакан aggre 10ms (protobuf); REST: exchangeInfo раз в info_s секунд, после status=1 —
снимок стакана 1000 уровней раз в snapshot_s секунд (из него и diff-ов строится локальный стакан, как в записи).
Каждый кадр уходит в on_entry({"t", "src", "ch", "data"}) сразу по приходу; t — локальное время с поправкой на часы
биржи. Нужны websockets, protobuf и скомпилированные tools/mexc_pb (как у listing_recorder)."""

import asyncio
import json
import sys
import time
from pathlib import Path

import websockets
from google.protobuf.json_format import MessageToDict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "mexc_pb"))
from PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper  # noqa: E402

WS_URL = "wss://wbs-api.mexc.com/ws"


class LiveFeed:
    def __init__(self, symbol: str, client, on_entry, on_log=print, snapshot_s: float = 1.0, info_s: float = 10.0):
        self.sym, self.client, self.on_entry, self.log = symbol, client, on_entry, on_log
        self.snapshot_s, self.info_s = snapshot_s, info_s
        self.online = False  # status == 1 в exchangeInfo

    def t(self) -> int:
        return int(time.time() * 1000) + self.client.time_offset_ms

    def emit(self, src: str, ch: str, data) -> None:
        self.on_entry({"t": self.t(), "src": src, "ch": ch, "data": data})

    async def run(self, stop: asyncio.Event) -> None:
        tasks = [asyncio.create_task(self.ws_loop(stop)), asyncio.create_task(self.rest_loop(stop))]
        await stop.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def ws_loop(self, stop: asyncio.Event) -> None:
        channels = [f"spot@public.aggre.deals.v3.api.pb@10ms@{self.sym}",
                    f"spot@public.aggre.depth.v3.api.pb@10ms@{self.sym}"]
        while not stop.is_set():
            try:
                async with websockets.connect(WS_URL, ping_interval=None, max_size=None, open_timeout=15) as ws:
                    self.log(f"WS подключён: {', '.join(channels)}")
                    await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": channels}))
                    pinger = asyncio.create_task(self.pinger(ws))
                    try:
                        while not stop.is_set():
                            try:
                                frame = await asyncio.wait_for(ws.recv(), timeout=1)
                            except asyncio.TimeoutError:
                                continue
                            if isinstance(frame, bytes):
                                data = MessageToDict(PushDataV3ApiWrapper.FromString(frame),
                                                     preserving_proto_field_name=True)
                                self.emit("ws_pub", data.get("channel", "?"), data)
                    finally:
                        pinger.cancel()
            except Exception as e:  # обрыв — переподключение; стакан станет действительным со следующим снимком
                if stop.is_set():
                    break
                self.log(f"WS ошибка: {type(e).__name__}: {e}")
                await asyncio.sleep(1)

    @staticmethod
    async def pinger(ws) -> None:
        while True:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"method": "PING"}))

    async def rest(self, ch: str, path: str, params: dict):
        r = await asyncio.to_thread(self.client.call, "GET", path, params)
        if r.ok:
            self.emit("rest", ch, r.body)
        return r.body if r.ok else None

    async def rest_loop(self, stop: asyncio.Event) -> None:
        sym = {"symbol": self.sym}
        last_info = 0.0
        while not stop.is_set():
            started = time.monotonic()
            if not self.online or started - last_info >= self.info_s:
                info = await self.rest("exchangeInfo", "/api/v3/exchangeInfo", sym)
                last_info = started
                symbols = info.get("symbols") if isinstance(info, dict) else None
                self.online = bool(symbols) and symbols[0].get("status") == "1"
            if self.online:
                await self.rest("depth", "/api/v3/depth", {**sym, "limit": 1000})
            interval = self.snapshot_s if self.online else self.info_s
            await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))
