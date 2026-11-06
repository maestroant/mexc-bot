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
        # активность и свежесть (ТЗ v2.1, 2.9–2.10): времена обновлений и изменений лучших уровней
        self.upd_ts: deque = deque()
        self.best_ts: deque = deque()
        self.last_t = 0
        self._last_best: tuple | None = None
        self.px: deque = deque()  # (t, bestBid, bestAsk) после каждого обновления — для изменений за 100–500 мс
        self.first_trade: float | None = None  # цена первой сделки листинга (её ставит движок)
        self.first_ask: float | None = None  # первый действительный bestAsk после открытия (firstValidAsk ТЗ v2.1)
        self._ago_cache: dict = {}
        self.recv_t: int | None = None  # текущий кадр: время получения (localReceiveTime) и биржи (sendTime)
        self.exch_t: int | None = None
        self._depth_cache: dict = {}

    def stamp(self, t: int, before: int) -> None:
        """Отметить время, если событие изменило стакан (before — updates до события)."""
        if self.updates == before or not self.valid:
            return
        self.last_t = t
        self.upd_ts.append(t)
        bids, asks = self.levels("bids"), self.levels("asks")
        best = (bids[0] if bids else None, asks[0] if asks else None)
        if bids and asks:
            self.px.append((t, bids[0][0], asks[0][0]))
        if best != self._last_best:
            self.best_ts.append(t)
            self._last_best = best

    def px_ago(self, t: int, ms: int, keep_ms: int = 2000):
        """(t, bestBid, bestAsk) не позже t − ms (последнее такое) или None; старше keep_ms — отбрасываются.
        Результат кешируется на момент t: его спрашивают все ветки на одном и том же событии."""
        key = (t, ms)
        if key in self._ago_cache:
            return self._ago_cache[key]
        if self._ago_cache and next(iter(self._ago_cache))[0] != t:
            self._ago_cache.clear()
        while len(self.px) > 1 and self.px[1][0] <= t - keep_ms:
            self.px.popleft()
        val = None
        for row in self.px:
            if row[0] <= t - ms:
                val = row
            else:
                break
        self._ago_cache[key] = val
        return val

    def activity(self, t: int, ms: int = 1000) -> tuple[int, int]:
        """updatesPerSec и bestUpdatesPerSec за последние ms."""
        for q in (self.upd_ts, self.best_ts):
            while q and q[0] <= t - ms:
                q.popleft()
        return len(self.upd_ts), len(self.best_ts)

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

    def depth(self, side: str, coef: float) -> float:
        """Глубина в USDT от лучшей цены стороны на долю coef вглубь (bids — вниз от bestBid, asks — вверх от bestAsk);
        кешируется до следующего изменения стакана."""
        key = (self.updates, side, coef)
        cached = self._depth_cache.get(key)
        if cached is None:
            if len(self._depth_cache) > 64:
                self._depth_cache.clear()
            lv = self.levels(side)
            if not lv:
                cached = 0.0
            elif side == "bids":
                cached = depth_usdt(lv, lv[0][0] * (1 - coef), lv[0][0])
            else:
                cached = depth_usdt(lv, lv[0][0], lv[0][0] * (1 + coef))
            self._depth_cache[key] = cached
        return cached

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
