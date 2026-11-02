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


def events_of(entry: dict) -> list[tuple]:
    """События движка из одной строки записи (или живого кадра того же вида)."""
    t, data = entry["t"], entry.get("data")
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
