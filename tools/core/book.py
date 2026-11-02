"""Локальный стакан из REST-снимка (lastUpdateId) и diff-потока aggre.depth (fromVersion/toVersion) и расчёты по уровням."""

import math
from collections import deque


class Book:
    def __init__(self):
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.version = 0
        self.valid = False
        self.pending: deque = deque(maxlen=500)  # diff-ы, пришедшие, пока стакан недействителен
        self.updates = 0  # число применённых изменений (для «различных обновлений» CONFIRMED)
        self.gaps = 0
        self._sorted: dict[str, tuple[int, list]] = {}  # сторона → (updates, уровни по цене) — общий для всех веток

    def snapshot(self, d: dict) -> None:
        if self.valid and int(d["lastUpdateId"]) <= self.version:
            return  # снимок старее стакана, собранного из diff-ов
        self.bids = {float(p): float(q) for p, q in d.get("bids", [])}
        self.asks = {float(p): float(q) for p, q in d.get("asks", [])}
        self.version = int(d["lastUpdateId"])
        self.valid = True
        self.updates += 1
        for diff in list(self.pending):
            if int(diff["toVersion"]) > self.version:
                self.diff(diff)
        self.pending.clear()

    def diff(self, d: dict) -> bool:
        fr, to = int(d["fromVersion"]), int(d["toVersion"])
        if not self.valid:
            self.pending.append(d)
            return False
        if to <= self.version:
            return False
        if fr > self.version + 1:
            self.valid, self.gaps = False, self.gaps + 1
            self.pending.append(d)
            return False
        for side, book in (("bids", self.bids), ("asks", self.asks)):
            for lvl in d.get(side, []):
                p, q = float(lvl["price"]), float(lvl.get("quantity") or 0)
                if q > 0:
                    book[p] = q
                else:
                    book.pop(p, None)
        self.version = to
        self.updates += 1
        return True

    def levels(self, side: str) -> list:
        cached = self._sorted.get(side)
        if cached is None or cached[0] != self.updates:
            book = self.bids if side == "bids" else self.asks
            cached = self.updates, sorted(book.items(), reverse=side == "bids")
            self._sorted[side] = cached
        return cached[1]

    def best(self) -> tuple[float, float]:
        bids, asks = self.levels("bids"), self.levels("asks")
        return (bids[0][0] if bids else 0.0), (asks[0][0] if asks else 0.0)

    def sorted_bids(self):
        return self.levels("bids")

    def sorted_asks(self):
        return self.levels("asks")


def vwap(levels, qty: float, limit: float | None = None, is_buy: bool = True):
    """Исполнение qty по уровням (asks для покупки, bids для продажи) не хуже limit: (кол-во, сумма)."""
    left, cost = qty, 0.0
    for p, q in levels:
        if limit is not None and ((is_buy and p > limit) or (not is_buy and p < limit)):
            break
        take = min(left, q)
        cost += take * p
        left -= take
        if left <= qty * 1e-9:
            return qty, cost
    return qty - left, cost


def depth_usdt(levels, lo: float, hi: float) -> float:
    return sum(p * q for p, q in levels if lo <= p <= hi)


def floor_step(x: float, step: float) -> float:
    return math.floor(x / step + 1e-9) * step


def ceil_step(x: float, step: float) -> float:
    return math.ceil(x / step - 1e-9) * step
