"""Поправка часов к бирже — одно правило для записи (listing_recorder) и PAPER (core/live.py); P0, клиент 11.11.

Поправка = serverTime биржи − середина запроса по локальным часам. Измеряется на одном открытом HTTPS-соединении:
первый запрос открывает его (TCP + TLS — его время в оценку не идёт), затем SAMPLES запросов /api/v3/time, берётся
запрос с наименьшим RTT. Раньше запись мерила один холодный запрос при старте (время установки соединения попадало
в RTT/2 — на USDT/BRL 11.11 поправка записи была на ~34 мс больше, чем у PAPER).

Пересчёт раз в CLOCK_S секунд; за FREEZE_MS до firstOpenTime и позже поправка не меняется (время кадров на открытии —
в одной шкале). Каждое применение — событие CLOCK: поправка, изменение, RTT лучшего запроса, все RTT и поправки
выборки, локальное монотонное время применения (applied_m) и причина. Кадры задним числом не пересчитываются: стратегия
видит время кадра с той поправкой, что действовала при его получении.

Для сравнения процессов на одном сервере у каждого кадра есть поле m — CLOCK_MONOTONIC в мс (общий для всех процессов
сервера, не зависит от поправок и перевода системных часов)."""

import asyncio
import http.client
import json
import time

HOST = "api.mexc.com"
SAMPLES = 5
CLOCK_S = 600.0
FREEZE_MS = 300_000


def mono_ms() -> float:
    """Локальное монотонное время сервера, мс (CLOCK_MONOTONIC: одно для всех процессов на сервере)."""
    return round(time.monotonic() * 1000, 3)


def measure(samples: int = SAMPLES, timeout: float = 5.0) -> dict | None:
    """Лучшая по RTT оценка поправки на тёплом соединении; None — биржа не ответила."""
    conn = http.client.HTTPSConnection(HOST, timeout=timeout)
    got = []  # (rtt, поправка)
    try:
        for i in range(samples + 1):
            wall = time.time() * 1000
            p0 = time.perf_counter()
            conn.request("GET", "/api/v3/time")
            resp = conn.getresponse()
            body = resp.read()
            rtt = (time.perf_counter() - p0) * 1000
            if resp.status != 200:
                continue
            server = int(json.loads(body)["serverTime"])
            if i:  # первый запрос — установка соединения
                got.append((rtt, server - (wall + rtt / 2)))
    except (OSError, http.client.HTTPException, ValueError, KeyError):
        pass
    finally:
        conn.close()
    if not got:
        return None
    rtt, off = min(got)
    return dict(offset_ms=round(off), rtt_ms=round(rtt, 1), rtts=[round(r, 1) for r, _ in got],
                offsets=[round(o) for _, o in got])


class Clock:
    """Поправка часов процесса: client.time_offset_ms; on_event(name, **data) — журнал процесса."""

    def __init__(self, client, on_event):
        self.client, self.on_event = client, on_event
        self.frozen = False

    def apply(self, res: dict | None, reason: str) -> None:
        if res is None:
            self.on_event("CLOCK_ERROR", reason=reason, applied_m=mono_ms())
            return
        old = self.client.time_offset_ms
        self.client.time_offset_ms = res["offset_ms"]
        self.on_event("CLOCK", offset_ms=res["offset_ms"], change_ms=res["offset_ms"] - old, rtt_ms=res["rtt_ms"],
                      rtts=res["rtts"], offsets=res["offsets"], applied_m=mono_ms(), reason=reason)

    def sync_now(self) -> None:
        """Синхронно, до запуска цикла событий (старт процесса)."""
        self.apply(measure(), "start")

    async def loop(self, stop: asyncio.Event, first_open, now_t, period_s: float = CLOCK_S) -> None:
        """first_open() — firstOpenTime или None; now_t() — время процесса в шкале биржи."""
        while not stop.is_set():
            fo = first_open()
            if fo is None or now_t() < fo - FREEZE_MS:
                self.frozen = False  # открытие перенесли позже — снова пересчитываем
                self.apply(await asyncio.to_thread(measure), "period")
            elif not self.frozen:
                self.frozen = True
                self.on_event("CLOCK_FROZEN", offset_ms=self.client.time_offset_ms, first_open=fo, applied_m=mono_ms())
            try:
                await asyncio.wait_for(stop.wait(), timeout=period_s)
            except asyncio.TimeoutError:
                pass
