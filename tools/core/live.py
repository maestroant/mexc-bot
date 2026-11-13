"""Живой поток пары для PAPER (и будущего LIVE) в формате записи listing_recorder.

WebSocket: сделки и diff-стакан aggre 10ms (protobuf); REST: exchangeInfo раз в info_s секунд, после status=1 —
снимок стакана 1000 уровней раз в snapshot_s секунд (из него и diff-ов строится локальный стакан, как в записи).
Каждый кадр уходит в on_entry({"t", "src", "ch", "conn", "data"}) сразу по приходу; t — локальное время с поправкой
на часы биржи. Диагностика (P0): теневое WS-соединение B с теми же подписками — его кадры идут в on_shadow и в стратегию
не попадают (сравнение доставки двух соединений по одним часам); при подключении — адрес шлюза; поправка часов —
core/clock.py (одно правило с записью: пересчёт каждые clock_s секунд, заморозка за 5 мин до открытия). У каждого кадра
m — локальное монотонное время получения (до разбора protobuf), у WS-кадров ещё parse_ms — время разбора (P0, 11.11).
Нужны websockets, protobuf и скомпилированные tools/mexc_pb (как у listing_recorder)."""

import asyncio
import json
import ssl
import sys
import time
from pathlib import Path

import websockets
from google.protobuf.json_format import MessageToDict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "mexc_pb"))
from PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper  # noqa: E402

from core.clock import CLOCK_S, Clock, mono_ms  # noqa: E402

WS_URL = "wss://wbs-api.mexc.com/ws"
SSL_CONTEXT_REUSE = True


def queued(ws) -> int | None:
    """Кадров в очереди websockets, уже принятых, но ещё не прочитанных (websockets ≥ 13, asyncio-реализация);
    байты в буфере сокета ядра сюда не входят. None — версия библиотеки не даёт этого счётчика."""
    try:
        return len(ws.recv_messages.frames)
    except (AttributeError, TypeError):
        return None


class LiveFeed:
    def __init__(self, symbol: str, client, on_entry, on_log=print, snapshot_s: float = 1.0, info_s: float = 10.0,
                 on_shadow=None, on_event=None, clock_s: float = CLOCK_S):
        self.sym, self.client, self.on_entry, self.log = symbol, client, on_entry, on_log
        self.on_shadow, self.on_event = on_shadow, on_event or (lambda name, **data: None)
        self.snapshot_s, self.info_s, self.clock_s = snapshot_s, info_s, clock_s
        self.online = False  # status == 1 в exchangeInfo
        self.first_open: int | None = None  # firstOpenTime из exchangeInfo
        self.clock = Clock(client, self.on_event)
        # шаг 1 оптимизаций (клиент 12.11): SSL-контекст создаётся один раз при запуске и переиспользуется при каждом
        # подключении и переподключении WS (раньше websockets создавал новый на каждое: ~27 мс блокировки цикла,
        # 140–240 мс при старте вместе с HTTPS в другом потоке). Проверка сертификатов та же (системные CA на момент
        # запуска процесса); правила переподключения не меняются. Откат: SSL_CONTEXT_REUSE = False.
        self.ssl_ctx = ssl.create_default_context() if SSL_CONTEXT_REUSE else None

    def t(self) -> int:
        return int(time.time() * 1000) + self.client.time_offset_ms

    def emit(self, src: str, ch: str, data, conn: str = "A", m: float | None = None, **extra) -> None:
        entry = {"t": self.t(), "src": src, "ch": ch, "conn": conn, "data": data, **extra,
                 "m": mono_ms() if m is None else m}
        (self.on_entry if conn == "A" else self.on_shadow)(entry)

    async def run(self, stop: asyncio.Event) -> None:
        tasks = [asyncio.create_task(self.ws_loop(stop)), asyncio.create_task(self.rest_loop(stop)),
                 asyncio.create_task(self.clock_loop(stop))]
        if self.on_shadow:
            tasks.append(asyncio.create_task(self.ws_loop(stop, "B")))
        await stop.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def ws_loop(self, stop: asyncio.Event, conn: str = "A") -> None:
        channels = [f"spot@public.aggre.deals.v3.api.pb@10ms@{self.sym}",
                    f"spot@public.aggre.depth.v3.api.pb@10ms@{self.sym}"]
        while not stop.is_set():
            try:
                tls = {"ssl": self.ssl_ctx} if self.ssl_ctx is not None else {}
                async with websockets.connect(WS_URL, ping_interval=None, max_size=None, open_timeout=15, **tls) as ws:
                    self.log(f"WS {conn} подключён ({ws.remote_address}): {', '.join(channels)}")
                    self.on_event("WS_CONNECTED", conn=conn, remote=str(ws.remote_address))
                    await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": channels}))
                    pinger = asyncio.create_task(self.pinger(ws))
                    try:
                        while not stop.is_set():
                            try:
                                frame = await asyncio.wait_for(ws.recv(), timeout=1)
                            except asyncio.TimeoutError:
                                continue
                            if isinstance(frame, bytes):
                                m = mono_ms()
                                q = queued(ws)
                                data = MessageToDict(PushDataV3ApiWrapper.FromString(frame),
                                                     preserving_proto_field_name=True)
                                self.emit("ws_pub", data.get("channel", "?"), data, conn, m=m,
                                          parse_ms=round(mono_ms() - m, 3), q=q)
                    finally:
                        pinger.cancel()
            except Exception as e:  # обрыв — переподключение; стакан станет действительным со следующим снимком
                if stop.is_set():
                    break
                self.log(f"WS {conn} ошибка: {type(e).__name__}: {e}")
                self.on_event("WS_ERROR", conn=conn, error=f"{type(e).__name__}: {e}")
                await asyncio.sleep(1)

    @staticmethod
    async def pinger(ws) -> None:
        while True:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"method": "PING"}))

    async def rest(self, ch: str, path: str, params: dict):
        r = await asyncio.to_thread(self.client.call, "GET", path, params)
        if r.ok:  # http и latency_ms — как в записи listing_recorder (время ответа REST, P0)
            self.emit("rest", ch, r.body, http=r.http, latency_ms=round(r.latency_ms, 1))
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
                if symbols and symbols[0].get("firstOpenTime"):
                    self.first_open = int(symbols[0]["firstOpenTime"])
            if self.online:
                await self.rest("depth", "/api/v3/depth", {**sym, "limit": 1000})
            interval = self.snapshot_s if self.online else self.info_s
            await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))

    async def clock_loop(self, stop: asyncio.Event) -> None:
        await self.clock.loop(stop, lambda: self.first_open, self.t, self.clock_s)
