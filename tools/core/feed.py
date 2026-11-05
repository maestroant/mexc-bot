"""Поток событий в формате записи listing_recorder: строка {"t", "src", "ch", "data"} → события движка.

Одинаково для файла записи (REPLAY) и живых кадров (PAPER, core/live.py):
  snap — REST-снимок стакана (src=rest, ch=depth), diff — aggre.depth, deal — (цена, количество, сторона)."""

import gzip
from pathlib import Path


def line_t(line: str) -> int:
    return int(line[5:18])  # строки записи начинаются с {"t":<13 цифр>,


def iter_lines(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        try:
            for line in f:
                if line.endswith("\n"):
                    yield line
        except EOFError:  # запись прервана — читаем, что есть
            return


STALL_MS = 200  # запаздывание WS-кадра сверх обычного, после которого время получения считается искажённым


def ws_base_lag(entries) -> int | None:
    """Обычное запаздывание получения WS-кадра относительно sendTime биржи в записи (минимум по кадрам).
    Часы сервера и биржи расходятся, поэтому само значение может быть и отрицательным."""
    lags = [e["t"] - int(e["data"]["sendTime"]) for e in entries
            if e.get("src") == "ws_pub" and isinstance(e.get("data"), dict) and e["data"].get("sendTime")]
    return min(lags) if lags else None


def frame_time(entry: dict, base_lag: int | None) -> int:
    """Время кадра для движка. Если процесс записи стоял (до 26.10 синхронная отправка в Telegram на открытии
    останавливала приём на ~2 с: LALA, KAMIRAI, GANG), кадр получен позже, чем его увидел бы работающий бот;
    тогда берётся sendTime + обычное запаздывание."""
    t = entry["t"]
    data = entry.get("data")
    if base_lag is None or entry.get("src") != "ws_pub" or not isinstance(data, dict) or not data.get("sendTime"):
        return t
    fixed = int(data["sendTime"]) + base_lag
    return fixed if t - fixed > STALL_MS else t


def events_of(entry: dict, base_lag: int | None = None) -> list[tuple]:
    """События движка из одной строки записи (или живого кадра того же вида); base_lag — см. frame_time."""
    t, data = frame_time(entry, base_lag), entry.get("data")
    if not isinstance(data, dict):
        return []
    if entry.get("src") == "rest" and entry.get("ch") == "depth":
        return [(t, "snap", data)] if "lastUpdateId" in data else []
    if "publicAggreDepths" in data:
        return [(t, "diff", data["publicAggreDepths"])]
    if "publicAggreDeals" in data:
        return [(t, "deal", (float(d["price"]), float(d["quantity"]), int(d.get("tradeType") or 0)))
                for d in data["publicAggreDeals"].get("deals", [])]
    return []
