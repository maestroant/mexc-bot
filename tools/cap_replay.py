"""REPLAY стратегии CAP BREAKOUT по записям listing_recorder (ТЗ v2 + решения клиента, doc/cap_breakout_resheniya.md).

Для каждой записи замороженного листинга:
  потолок      из ответа 30010 в логе пробы (logs/probe_<SYMBOL>_*.jsonl) или --cap SYMBOL=цена;
  снятие       первая сделка выше потолка, затем bid локального стакана выше потолка;
  вход         FAST (окно 10 с, без стабилизации) и CONFIRMED (окно 30 с, фильтры держатся ≥200 мс и ≥3 версии стакана);
               фильтры §4.3: спред, depthBid0.7/1.5, depthAsk0.7, проскальзывание выхода, tickPct, entryStressLoss
               (по buyVWAP с резервом 0,5 % или — для сравнения — по bestAsk +1 %); по желанию — фильтр падения
               bid-VWAP перед покупкой; LIMIT BUY по bestAsk +1 % исполняется по asks в момент прихода ордера;
  позиция      профили CAP_DYNAMIC (свой для FAST и CONFIRMED), CAP_NO_TIME_STOPS (тот же без TIME_STOP_*) и
               LEGACY_STOPS (старые стопы, TP +2/+4 %, удержание 15 с); TP — LIMIT в очереди своего ценового уровня;
  выход        отмена TP, затем LIMIT SELL по bestBid −0,5 %, до 3 перестановок через 400 мс, затем продажа по bids
               до −20 % (предел PERCENT_PRICE_BY_SIDE; веб-fallback не моделируется), непроданное — остаток.
Локальный стакан: REST-снимок (lastUpdateId) + поток aggre.depth с проверкой fromVersion/toVersion;
при разрыве версий стакан недействителен до следующего снимка (запись делает их раз в секунду).

Исполнение: количество округляется вниз до шага пары, ордер меньше 1 USDT не ставится (остаток — пыль);
TP исполняется сделкой выше его цены целиком, сделкой по его цене — после очереди, стоявшей на уровне при постановке;
комиссии — taker 0,05 % (BUY — в монете), TP — maker 0 %. Наш ордер не меняет стакан.

Запуск на сервере (записи лежат там):
  python3 tools/cap_replay.py logs/rec_LALAUSDT_*.jsonl.gz --latency 20,50,100,200
  python3 tools/cap_replay.py logs/rec_CHOMPUSDT_*.jsonl.gz --cap CHOMPUSDT=5 --depth-persist-ms 2000 --summary-only
"""

import argparse
import gzip
import json
import math
import re
from collections import deque
from dataclasses import dataclass, field, replace
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"

ORDER_USDT = 10.0
BUY_FEE = SELL_FEE = 0.0005  # taker; BUY-комиссия списывается в купленной монете (mxDeduct выключен)
TP_FEE = 0.0  # maker
MIN_NOTIONAL = 1.0  # минимальная сумма ордера, USDT (30002)
FILTERS = dict(max_spread=1.2, bid07=15, bid15=30, ask07=8, max_exit_slip=1.5, max_tick=0.3, max_stress_loss=3.0,
               buy_aggressive=1.0)
ENTRY = {"FAST": dict(window_ms=10_000, stable_ms=0, min_updates=1),
         "CONFIRMED": dict(window_ms=30_000, stable_ms=200, min_updates=3)}
EXIT_OFFSET, EXIT_TIMEOUT_MS, EXIT_REPRICES, CRITICAL_FLOOR = 0.005, 400, 3, 0.8


@dataclass(frozen=True)
class Profile:
    name: str
    hard: float  # все пороги — в % netPnL позиции
    window_dump: float  # падение цены сделок за 1 с
    bid_vwap_drop: float | None  # падение exitVWAP(Q) за 1 с
    fast_drop: float  # в первые 3 с
    be_act: float
    be_floor: float
    time_neg: tuple[int, float]  # (мс, порог)
    no_edge: tuple[int, float]
    no_edge_plus_spread: bool  # LEGACY: порог NO_EDGE + текущий спред
    no_momentum_ms: int
    depth_x: float | None  # DEPTH_DISAPPEARED: глубина bids < depth_x × стоимости остатка
    trail_act: float | None  # trailing до TP1 (по netPnL с резервом)
    trail_pp: float | None
    tp: tuple[tuple[float, float], ...]  # ((цель %, доля позиции), ...)
    trail_after_tp1: float | None  # % от пика exitVWAP остатка
    guard_target: float | None  # защита общего netPnL после TP1
    stress: float  # резерв проскальзывания для trailing и защиты, %
    max_hold_ms: int
    time_stops: bool = True  # TIME_STOP_NEGATIVE / NO_EDGE / NO_MOMENTUM


CAP_DYNAMIC = {
    "FAST": Profile("CAP_DYNAMIC", -6, -5, -4, -3, 5, 0.5, (3000, -0.5), (6000, 0.5), False, 10_000, 5, 4, 3,
                    ((10, 0.5),), 15, 0.5, 1.5, 45_000),
    "CONFIRMED": Profile("CAP_DYNAMIC", -5, -4, -3, -2.5, 3, 0.3, (3000, -0.3), (5000, 0.3), False, 8_000, 8, 2.5, 2,
                         ((5, 0.5),), 10, 0.3, 1.0, 60_000),
}
CAP_NO_TIME = {m: replace(p, name="CAP_NO_TIME_STOPS", time_stops=False) for m, p in CAP_DYNAMIC.items()}
LEGACY = Profile("LEGACY_STOPS", -8, -6, None, -2.5, 1.5, 0.15, (3000, -0.3), (5000, 0.3), True, 8_000, None, None,
                 None, ((2, 0.5), (4, 0.5)), None, None, 0.0, 15_000)


# --- данные ---------------------------------------------------------------------

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


def find_cap(symbol: str) -> tuple[float, str] | None:
    """Потолок из ответа 30010 в логах пробы: "_extend": {"price": "0.5USDT"}."""
    for p in sorted(LOGS.glob(f"probe_{symbol}_2*.jsonl")):
        with p.open(encoding="utf-8") as f:
            for line in f:
                if '"code": 30010' in line:
                    m = re.search(r'"_extend": \{"price": "([0-9.]+)', line)
                    if m:
                        return float(m.group(1)), "30010"
    return None


def load(path: Path, cap: float, before_ms=30_000, after_ms=170_000):
    """Два прохода: найти снятие потолка по сделкам, затем взять все события стакана и сделки вокруг него."""
    open_ms, cand_ms = None, None
    for line in iter_lines(path):
        if open_ms is None:
            if '"OPEN_DETECTED"' in line:
                e = json.loads(line)
                open_ms = e["data"].get("open_ms") or e["t"]
            continue
        if "aggre.deals" in line and '"ch":"text"' not in line and line_t(line) > open_ms + 60_000:
            e = json.loads(line)
            if any(float(d["price"]) > cap * 1.000001 for d in e["data"]["publicAggreDeals"].get("deals", [])):
                cand_ms = e["t"]
                break
    if cand_ms is None:
        return open_ms, None, [], {}
    lo, hi = cand_ms - before_ms, cand_ms + after_ms
    events, info = [], {}
    for line in iter_lines(path):
        t = line_t(line)
        if t < lo - 60_000:
            continue
        if t > hi:
            break
        if '"ch":"exchangeInfo"' in line:
            syms = (json.loads(line)["data"] or {}).get("symbols") or []
            if syms:
                info = syms[0]
        elif t < lo:
            continue
        elif '"src":"rest","ch":"depth"' in line:
            events.append((t, "snap", json.loads(line)["data"]))
        elif "aggre.depth" in line and '"ch":"text"' not in line:
            events.append((t, "diff", json.loads(line)["data"]["publicAggreDepths"]))
        elif "aggre.deals" in line and '"ch":"text"' not in line:
            for d in json.loads(line)["data"]["publicAggreDeals"].get("deals", []):
                events.append((t, "deal", (float(d["price"]), float(d["quantity"]), int(d.get("tradeType") or 0))))
    events.sort(key=lambda e: e[0])
    return open_ms, cand_ms, events, info


# --- локальный стакан -------------------------------------------------------------

class Book:
    def __init__(self):
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.version = 0
        self.valid = False
        self.pending: deque = deque(maxlen=500)  # diff-ы, пришедшие, пока стакан недействителен
        self.updates = 0  # число применённых изменений (для «различных обновлений» CONFIRMED)
        self.gaps = 0

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

    def best(self) -> tuple[float, float]:
        return (max(self.bids) if self.bids else 0.0), (min(self.asks) if self.asks else 0.0)

    def sorted_bids(self):
        return sorted(self.bids.items(), reverse=True)

    def sorted_asks(self):
        return sorted(self.asks.items())


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


@dataclass
class Opts:
    stops_stressed: bool
    depth_band: float  # доля от bestBid
    depth_persist_ms: int
    entry_stress: float  # %
    entry_basis: str  # vwap | max
    pre_buy_drop: bool


@dataclass
class TP:
    price: float
    qty: float
    left: float
    queue: float  # объём, стоявший на уровне до нас


# --- симуляция одной ветки -----------------------------------------------------------

@dataclass
class Sim:
    mode: str
    profile: Profile
    latency: int
    opts: Opts
    release_ms: int
    step: float
    qty_step: float
    # состояние
    state: str = "SCAN"  # SCAN → BUY_SENT → POSITION → EXITING → DONE
    stable_since: int | None = None
    stable_updates: int = 0
    seen_updates: int = -1  # book.updates при прошлой проверке: без новой версии стакана проверка не повторяется
    skip_reasons: dict = field(default_factory=dict)
    scan_px: deque = field(default_factory=lambda: deque(maxlen=5000))  # (t, exitVWAP плановой покупки) до входа
    buy_at: int = 0
    buy_limit: float = 0.0
    buy_qty: float = 0.0
    decision_ask: float = 0.0
    entry_ms: int = 0
    C: float = 0.0
    R: float = 0.0
    Q: float = 0.0
    fees: float = 0.0
    tps: list = field(default_factory=list)
    tp1_done: bool = False
    tp_fills: int = 0
    mfe_trail: float = -1e9
    mfe_stop: float = -1e9
    mfe: float = -1e9
    mae: float = 1e9
    min_d07: float = 1e9
    min_d15: float = 1e9
    trail_armed: bool = False
    peak_ev: float = 0.0
    depth_low_since: int | None = None
    trades: deque = field(default_factory=lambda: deque(maxlen=5000))  # (t, цена сделки)
    evs: deque = field(default_factory=lambda: deque(maxlen=5000))  # (t, exitVWAP(Q))
    last_bid: float = 0.0
    exit_reason: str = ""
    exit_ms: int = 0
    exit_detail: str = ""
    exit_ev: float = 0.0
    exit_round: int = 0
    exit_next: int = 0
    exit_sold: float = 0.0
    exit_rev: float = 0.0
    critical_used: bool = False
    result: dict = field(default_factory=dict)

    # --- вход ---
    def check_entry(self, t: int, book: Book) -> None:
        cfg = ENTRY[self.mode]
        if t > self.release_ms + cfg["window_ms"]:
            self.finish(t, "SKIP_WINDOW_END")
            return
        if book.updates == self.seen_updates:
            return
        self.seen_updates = book.updates
        self.track_scan_px(t, book)
        ok, why, plan = self.filters(t, book)
        if not ok:
            self.skip_reasons[why] = self.skip_reasons.get(why, 0) + 1
            self.stable_since, self.stable_updates = None, 0
            return
        if self.stable_since is None:
            self.stable_since, self.stable_updates = t, 0
        self.stable_updates += 1
        if t - self.stable_since < cfg["stable_ms"] or self.stable_updates < cfg["min_updates"]:
            return
        self.buy_limit, self.buy_qty, self.decision_ask, drop = plan
        self.result["drop_1s_at_decision"] = drop
        self.buy_at = t + self.latency
        self.state = "BUY_SENT"

    def track_scan_px(self, t: int, book: Book) -> None:
        """exitVWAP плановой покупки на каждом обновлении стакана до входа — для фильтра падения перед BUY."""
        if not book.valid:
            return
        bb, ba = book.best()
        if bb <= 0 or ba <= 0:
            return
        qty = ORDER_USDT / (ba * 1.01)
        sold, rev = vwap(book.sorted_bids(), qty, None, False)
        if sold >= qty * 0.999:
            self.scan_px.append((t, rev / qty))

    def filters(self, t: int, book: Book):
        if not book.valid:
            return False, "book_invalid", None
        bb, ba = book.best()
        if bb <= 0 or ba <= bb:
            return False, "bad_book", None
        f = FILTERS
        mid = (bb + ba) / 2
        if 100 * (ba - bb) / mid > f["max_spread"]:
            return False, "spread", None
        bids, asks = book.sorted_bids(), book.sorted_asks()
        if depth_usdt(bids, bb * (1 - 0.007), bb) < ORDER_USDT * f["bid07"]:
            return False, "depthBid0.7", None
        if depth_usdt(bids, bb * (1 - 0.015), bb) < ORDER_USDT * f["bid15"]:
            return False, "depthBid1.5", None
        if depth_usdt(asks, ba, ba * (1 + 0.007)) < ORDER_USDT * f["ask07"]:
            return False, "depthAsk0.7", None
        if 100 * self.step / ba >= f["max_tick"]:
            return False, "tick", None
        limit = floor_step(ba * (1 + f["buy_aggressive"] / 100), self.step)
        qty = floor_step(ORDER_USDT / limit, self.qty_step)
        if qty * limit < MIN_NOTIONAL:
            return False, "min_notional", None
        got, cost = vwap(asks, qty, limit, True)
        if got < qty * 0.999:
            return False, "buy_depth", None
        q_net = qty * (1 - BUY_FEE)
        sold, rev = vwap(bids, q_net, None, False)
        if sold < q_net * 0.999:
            return False, "exit_vwap", None
        exit_px = rev / q_net
        if 100 * (bb - exit_px) / bb > f["max_exit_slip"]:
            return False, "exit_slippage", None
        base = cost if self.opts.entry_basis == "vwap" else qty * limit
        stressed_exit = rev * (1 - SELL_FEE) * (1 - self.opts.entry_stress / 100)
        if 100 * (stressed_exit - base) / base < -f["max_stress_loss"]:
            return False, "entry_stress", None
        ago = self.ago(self.scan_px, t, 1000)
        drop = 100 * (exit_px / ago - 1) if ago else None
        if self.opts.pre_buy_drop and drop is not None and drop <= CAP_DYNAMIC[self.mode].bid_vwap_drop:
            return False, "pre_buy_drop", None
        return True, "", (limit, qty, ba, drop)

    def fill_buy(self, t: int, book: Book) -> None:
        got_raw, cost_raw = vwap(book.sorted_asks(), self.buy_qty, self.buy_limit, True)
        got = floor_step(got_raw, self.qty_step)
        if got <= 0:
            self.finish(t, "BUY_NOT_FILLED")
            return
        cost = cost_raw * got / got_raw
        avg = cost / got
        self.entry_ms, self.C, self.Q = t, cost, got * (1 - BUY_FEE)
        self.fees += got * BUY_FEE * avg
        placed, n = 0.0, len(self.profile.tp)
        for i, (pct, share) in enumerate(self.profile.tp):
            price = ceil_step(avg * (1 + pct / 100), self.step)
            rest = n > 1 and i == n - 1  # последний из нескольких TP — на весь остаток
            qty = floor_step(self.Q - placed if rest else self.Q * share, self.qty_step)
            if qty * price < MIN_NOTIONAL:
                continue
            queue = sum(q for p, q in book.asks.items() if abs(p - price) < self.step * 0.01)
            self.tps.append(TP(price, qty, qty, queue))
            placed += qty
        self.state = "POSITION"
        self.result.update(entry_ms=t, entry_px=avg, buy_usdt=cost, decision_ask=self.decision_ask,
                           slippage_pct=100 * (avg / self.decision_ask - 1))

    # --- позиция ---
    def on_deal(self, t: int, price: float, qty: float, side: int) -> None:
        self.trades.append((t, price))
        if self.state != "POSITION":
            return
        for tp in self.tps:
            if tp.left <= 0:
                continue
            fill = 0.0
            if price > tp.price + self.step * 0.01:
                fill = tp.left  # цена прошла выше — уровень съеден целиком
            elif abs(price - tp.price) <= self.step * 0.01 and side == 1:
                if tp.queue >= qty:
                    tp.queue -= qty
                else:
                    fill = min(tp.left, qty - tp.queue)
                    tp.queue = 0.0
            if fill > 0:
                fill = min(fill, self.Q)
                tp.left -= fill
                self.R += fill * tp.price * (1 - TP_FEE)
                self.fees += fill * tp.price * TP_FEE
                self.Q -= fill
                self.tp_fills += 1
                if not self.tp1_done:  # первое (в т. ч. частичное) исполнение TP1
                    self.tp1_done = True
                    self.evs.clear()  # exitVWAP другого количества несравним
                    self.peak_ev = 0.0
        if all(tp.left <= 1e-12 for tp in self.tps) and self.tps:
            if floor_step(self.Q, self.qty_step) * price < MIN_NOTIONAL:
                self.close(t, "TP_FILLED")

    def pnl(self, ev: float, stressed: bool) -> float:
        s = self.profile.stress / 100 if stressed else 0.0
        return 100 * (self.R + self.Q * ev * (1 - SELL_FEE) * (1 - s) - self.C) / self.C

    @staticmethod
    def ago(hist: deque, t: int, ms: int):
        """Значение не позже t − ms (последнее такое) или None."""
        val = None
        for ht, hv in hist:
            if ht <= t - ms:
                val = hv
            else:
                break
        return val

    def check_exit(self, t: int, book: Book) -> None:
        p = self.profile
        bb, ba = book.best()
        self.last_bid = bb or self.last_bid
        bids = book.sorted_bids()
        sold, rev = vwap(bids, self.Q, None, False)
        ev = rev / self.Q if sold >= self.Q * 0.999 and self.Q > 0 else None
        elapsed = t - self.entry_ms
        if ev is None:
            self.start_exit(t, "DEPTH_DISAPPEARED", "exitVWAP не считается", 0.0)
            return
        self.evs.append((t, ev))
        plain, stressed = self.pnl(ev, False), self.pnl(ev, True)
        basis = stressed if self.opts.stops_stressed and p.name != "LEGACY_STOPS" else plain
        self.mfe_trail = max(self.mfe_trail, stressed)
        self.mfe_stop = max(self.mfe_stop, basis)
        self.mfe, self.mae = max(self.mfe, plain), min(self.mae, plain)
        value = self.Q * bb
        if value > 0:
            d07, d15 = depth_usdt(bids, bb * 0.993, bb), depth_usdt(bids, bb * 0.985, bb)
            self.min_d07, self.min_d15 = min(self.min_d07, d07 / value), min(self.min_d15, d15 / value)
        depth_hit = False
        if p.depth_x is not None:
            low = depth_usdt(bids, bb * (1 - self.opts.depth_band), bb) < p.depth_x * value
            self.depth_low_since = (self.depth_low_since or t) if low else None
            depth_hit = low and t - self.depth_low_since >= self.opts.depth_persist_ms
        last_trade = self.trades[-1][1] if self.trades else None
        trade_1s = self.ago(self.trades, t, 1000)
        ev_1s, ev_2s = self.ago(self.evs, t, 1000), self.ago(self.evs, t, 2000)
        if not self.tp1_done and p.trail_act is not None and self.mfe_trail >= p.trail_act:
            self.trail_armed = True
        if self.tp1_done:
            self.peak_ev = max(self.peak_ev, ev)
        spread = 100 * (ba - bb) / ((ba + bb) / 2) if ba > bb > 0 else 0.0
        checks = [
            ("DEPTH_DISAPPEARED", depth_hit),
            ("HARD_STOP", basis <= p.hard),
            ("FAST_WINDOW_DUMP", last_trade and trade_1s and 100 * (last_trade / trade_1s - 1) <= p.window_dump),
            ("BID_VWAP_DROP", p.bid_vwap_drop is not None and ev_1s and 100 * (ev / ev_1s - 1) <= p.bid_vwap_drop),
            ("FAST_DROP_STOP", elapsed <= 3000 and basis <= p.fast_drop),
            ("TOTAL_PNL_PROTECT", p.guard_target is not None and self.tp1_done and stressed <= p.guard_target),
            ("TRAILING", self.trail_armed and not self.tp1_done and stressed <= self.mfe_trail - p.trail_pp),
            ("TRAILING_AFTER_TP1", p.trail_after_tp1 is not None and self.tp1_done
             and ev <= self.peak_ev * (1 - p.trail_after_tp1 / 100)),
            ("BREAK_EVEN_STOP", self.mfe_stop >= p.be_act and basis <= p.be_floor),
            ("TIME_STOP_NEGATIVE", p.time_stops and elapsed >= p.time_neg[0] and basis < p.time_neg[1]),
            ("TIME_STOP_NO_EDGE", p.time_stops and elapsed >= p.no_edge[0]
             and basis < p.no_edge[1] + (spread if p.no_edge_plus_spread else 0)),
            ("TIME_STOP_NO_MOMENTUM", p.time_stops and elapsed >= p.no_momentum_ms and not self.tp1_done
             and ev_2s and ev <= ev_2s),
            ("MAX_HOLD", elapsed >= p.max_hold_ms),
        ]
        for name, hit in checks:
            if hit:
                self.start_exit(t, name, f"netPnL {plain:+.2f}% (с резервом {stressed:+.2f}%), MFE {self.mfe_trail:+.2f}%",
                                ev)
                return

    # --- выход ---
    def start_exit(self, t: int, reason: str, detail: str, ev: float) -> None:
        self.state, self.exit_reason, self.exit_detail, self.exit_ms, self.exit_ev = "EXITING", reason, detail, t, ev
        active_tp = any(tp.left > 0 for tp in self.tps)
        # сначала отмена TP и подтверждение (ещё один круг до биржи), затем SELL
        self.exit_round, self.exit_next = 0, t + self.latency * (2 if active_tp else 1)

    def exit_step(self, t: int, book: Book) -> None:
        """Одна попытка срочной продажи остатка: LIMIT по bestBid −0,5 %, затем перестановки, затем до −20 %."""
        bb, _ = book.best()
        qty = floor_step(self.Q, self.qty_step)
        if qty <= 0:
            self.close(t, self.exit_reason)
            return
        if bb > 0 and qty * bb < MIN_NOTIONAL:
            self.close(t, self.exit_reason + "+DUST")
            return
        if bb <= 0:
            self.exit_round += 1
            self.exit_next = t + EXIT_TIMEOUT_MS
        else:
            critical = self.exit_round > EXIT_REPRICES
            self.critical_used |= critical
            floor = bb * (CRITICAL_FLOOR if critical else 1 - EXIT_OFFSET)
            sold_raw, rev_raw = vwap(book.sorted_bids(), qty, floor_step(floor, self.step), False)
            sold = floor_step(sold_raw, self.qty_step)
            rev = rev_raw * sold / sold_raw if sold_raw else 0.0
            self.R += rev * (1 - SELL_FEE)
            self.fees += rev * SELL_FEE
            self.Q -= sold
            self.exit_sold += sold
            self.exit_rev += rev
            self.exit_round += 1
            self.exit_next = t + EXIT_TIMEOUT_MS + self.latency
        if floor_step(self.Q, self.qty_step) <= 0:
            self.close(t, self.exit_reason)
        elif self.exit_round > EXIT_REPRICES + 1:
            self.close(t, self.exit_reason + "+RESIDUAL")

    def close(self, t: int, reason: str) -> None:
        self.exit_reason = reason
        pnl_usdt = self.R - self.C  # непроданный остаток (пыль) в результат не входит
        gap = None
        if self.exit_sold > 0 and self.exit_ev > 0:
            gap = 100 * (self.exit_rev / self.exit_sold / self.exit_ev - 1)
        self.result.update(exit_ms=t, reason=reason, detail=self.exit_detail, pnl_usdt=pnl_usdt,
                           pnl_pct=100 * pnl_usdt / self.C, hold_s=(t - self.entry_ms) / 1000,
                           mfe=self.mfe, mae=self.mae, tp1=self.tp1_done, tp_fills=self.tp_fills,
                           exit_rounds=self.exit_round, critical=self.critical_used, exit_gap_pct=gap,
                           left_qty=self.Q, left_usdt=self.Q * self.last_bid, fees_usdt=self.fees,
                           min_depth07_x=self.min_d07, min_depth15_x=self.min_d15)
        self.state = "DONE"

    def finish(self, t: int, reason: str) -> None:
        self.state = "DONE"
        self.result.update(reason=reason, skips=self.skip_reasons)

    # --- общий шаг ---
    def before_event(self, t: int, book: Book) -> None:
        """Отложенные действия, срок которых наступил до события t: стакан ещё в состоянии на момент их прихода."""
        if self.state == "BUY_SENT" and self.buy_at <= t:
            self.fill_buy(self.buy_at, book)
        while self.state == "EXITING" and self.exit_next <= t:
            self.exit_step(self.exit_next, book)

    def after_event(self, t: int, kind: str, book: Book, deal=None) -> None:
        if kind == "deal":
            self.on_deal(t, *deal)
        if self.state == "SCAN" and kind != "deal":
            self.check_entry(t, book)
        elif self.state == "POSITION":
            self.check_exit(t, book)


# --- прогон -------------------------------------------------------------------

def profiles_for(mode: str) -> list[Profile]:
    return [CAP_DYNAMIC[mode], CAP_NO_TIME[mode], LEGACY]


def run_listing(path: Path, args, caps: dict) -> list[dict]:
    symbol = path.name.split("_")[1]
    cap_src = caps.get(symbol) or find_cap(symbol)
    if not cap_src:
        print(f"{symbol}: нет потолка (ни 30010 в логе пробы, ни --cap) — пропуск")
        return []
    cap, src = cap_src
    open_ms, cand_ms, events, info = load(path, cap)
    if not events:
        print(f"{symbol}: потолок {cap} так и не снят в записи — пропуск")
        return []
    step = 10 ** -int(info.get("quotePrecision") or 8)
    qty_step = 10 ** -int(info.get("baseAssetPrecision") or 8)
    zones = ", ".join(info.get("conceptPlates") or [])
    opts = Opts(args.stops_basis == "stressed", args.depth_band / 100, args.depth_persist_ms, args.entry_stress,
                args.entry_basis, args.pre_buy_drop)
    book, release = Book(), None
    sims: list[Sim] = []
    for t, kind, data in events:
        for s in sims:
            if s.state != "DONE":
                s.before_event(t, book)
        if kind == "snap":
            book.snapshot(data)
        elif kind == "diff":
            book.diff(data)
        if release is None and t >= cand_ms and book.valid and book.best()[0] > cap * 1.000001:
            release = t
            sims = [Sim(mode, prof, lat, opts, release, step, qty_step)
                    for mode in args.modes for prof in profiles_for(mode) for lat in args.latency]
        for s in sims:
            if s.state != "DONE":
                s.after_event(t, kind, book, data if kind == "deal" else None)
    t_end = events[-1][0]
    for s in sims:  # запись кончилась раньше, чем ветка завершилась
        if s.state == "POSITION":
            s.start_exit(t_end, "END_OF_DATA", "", 0.0)
        while s.state == "EXITING":
            s.exit_step(t_end, book)
        if s.state != "DONE":
            s.finish(t_end, "END_OF_DATA")
    rows = []
    if not args.summary_only:
        head = (f"\n=== {symbol} [{zones}] потолок {cap} ({src}), первая сделка выше потолка "
                f"T+{(cand_ms - open_ms) / 1000:.3f} с, CAP_RELEASED T+{(release - open_ms) / 1000:.3f} с"
                if release else f"\n=== {symbol}: bid так и не поднялся выше потолка")
        print(head + f"; шаг цены {step:g}, шаг кол-ва {qty_step:g}, разрывов версий стакана: {book.gaps}")
    for s in sims:
        r = s.result
        rows.append(dict(symbol=symbol, mode=s.mode, profile=s.profile.name, latency=s.latency, **r))
        if args.summary_only:
            continue
        tag = f"  {s.mode:<9} {s.profile.name:<17} {s.latency:>3} мс:"
        if "entry_ms" not in r:
            top = sorted(r.get("skips", {}).items(), key=lambda x: -x[1])[:3]
            print(f"{tag} нет входа ({r.get('reason')}; отказы: {', '.join(f'{k} {v}' for k, v in top)})")
            continue
        drop = r.get("drop_1s_at_decision")
        gap = r.get("exit_gap_pct")
        print(f"{tag} вход +{(r['entry_ms'] - release) / 1000:.2f} с по {r['entry_px']:.6g} "
              f"({r['entry_px'] / cap:.2f}× потолок, к ask {r['slippage_pct']:+.2f} %"
              + (f", bid-VWAP за 1 с до решения {drop:+.1f} %" if drop is not None else "")
              + f") → {r['reason']} через {r['hold_s']:.1f} с: {r['pnl_pct']:+.2f} % ({r['pnl_usdt']:+.3f} USDT), "
              f"MFE {r['mfe']:+.1f} / MAE {r['mae']:+.1f} %, TP-исполнений {r['tp_fills']}, "
              f"выход: попыток {r['exit_rounds']}{', до −20 %' if r['critical'] else ''}"
              + (f", к exitVWAP {gap:+.2f} %" if gap is not None else "")
              + f", остаток {r['left_usdt']:.3f} USDT, комиссии {r['fees_usdt']:.4f}; "
              f"глубина min ×{r['min_depth07_x']:.0f} (0,7 %) / ×{r['min_depth15_x']:.0f} (1,5 %)  [{r['detail']}]")
    return rows


def summary(rows: list[dict]) -> None:
    print("\nИтого (10 USDT на вход; листинги без входа в сумму не входят):")
    print(f"  {'режим':<9} {'профиль':<17} {'мс':>3}  входов  +/−   сумма USDT  средний %      худший %"
          f"  MFE ср. %  MAE ср. %  удержание ср. с  с остатком по рынку, USDT")
    keys = sorted({(r["mode"], r["profile"], r["latency"]) for r in rows})
    for k in keys:
        rs = [r for r in rows if (r["mode"], r["profile"], r["latency"]) == k and "pnl_usdt" in r]
        if not rs:
            print(f"  {k[0]:<9} {k[1]:<17} {k[2]:>3}  0")
            continue
        total = sum(r["pnl_usdt"] for r in rs)
        wins, losses = sum(r["pnl_usdt"] > 0 for r in rs), sum(r["pnl_usdt"] <= 0 for r in rs)
        avg = 100 * total / sum(r["buy_usdt"] for r in rs)
        n = len(rs)
        print(f"  {k[0]:<9} {k[1]:<17} {k[2]:>3}  {n:>6}  {wins}/{losses}  {total:>+10.3f}  {avg:>+9.2f}"
              f"  {min(r['pnl_pct'] for r in rs):>+14.2f}  {sum(r['mfe'] for r in rs) / n:>+9.2f}"
              f"  {sum(r['mae'] for r in rs) / n:>+9.2f}  {sum(r['hold_s'] for r in rs) / n:>15.1f}"
              f"  {total + sum(r['left_usdt'] for r in rs):>+26.3f}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="+", help="записи logs/rec_<SYMBOL>_*.jsonl.gz")
    p.add_argument("--cap", action="append", default=[], help="потолок без 30010 в логе: SYMBOL=цена (можно несколько)")
    p.add_argument("--latency", default="50", help="задержка ордера в мс, через запятую (50)")
    p.add_argument("--modes", default="FAST,CONFIRMED")
    p.add_argument("--stops-basis", choices=("plain", "stressed"), default="plain",
                   help="стопы CAP_DYNAMIC по netPnL без резерва проскальзывания (plain) или с ним (stressed)")
    p.add_argument("--depth-band", type=float, default=1.5, help="DEPTH_DISAPPEARED: глубина bids в пределах N %% (1.5)")
    p.add_argument("--depth-persist-ms", type=int, default=1000,
                   help="DEPTH_DISAPPEARED: глубина ниже порога непрерывно N мс (1000)")
    p.add_argument("--entry-stress", type=float, default=0.5, help="резерв в фильтре entryStressLoss, %% (0.5)")
    p.add_argument("--entry-basis", choices=("vwap", "max"), default="vwap",
                   help="entryStressLoss от ожидаемой цены покупки (vwap) или от максимальной bestAsk +1 %% (max)")
    p.add_argument("--pre-buy-drop", action="store_true",
                   help="не покупать, если bid-VWAP за 1 с упал на порог «падения bid-VWAP» профиля (FAST −4, CONFIRMED −3)")
    p.add_argument("--summary-only", action="store_true", help="печатать только итог")
    p.add_argument("--json", help="сохранить строки результатов в JSON")
    args = p.parse_args()
    args.latency = [int(x) for x in args.latency.split(",")]
    args.modes = args.modes.split(",")
    caps = {}
    for c in args.cap:
        sym, val = c.split("=")
        caps[sym.upper()] = (float(val), "--cap")
    rows = []
    for f in args.files:
        rows += run_listing(Path(f), args, caps)
    summary(rows)
    if args.json:
        Path(args.json).write_text(json.dumps(rows, ensure_ascii=False, indent=1, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
