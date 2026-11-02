"""REPLAY стратегии CAP BREAKOUT по записям listing_recorder (ТЗ v2 + решения клиента, doc/cap_breakout_resheniya.md).

Для каждой записи замороженного листинга:
  потолок      из ответа 30010 в логе пробы (logs/probe_<SYMBOL>_*.jsonl) или --cap SYMBOL=цена;
  снятие       первая сделка выше потолка, затем bid локального стакана выше потолка;
  вход         FAST (окно 10 с, без стабилизации) и CONFIRMED (окно 30 с, фильтры держатся ≥200 мс и ≥3 версии стакана);
               фильтры §4.3: спред, depthBid0.7/1.5, depthAsk0.7, проскальзывание выхода, tickPct, entryStressLoss
               (по buyVWAP с резервом 0,5 % или — для сравнения — по bestAsk +1 %); по желанию — фильтр падения
               bid-VWAP перед покупкой; LIMIT BUY по bestAsk +1 % исполняется по asks в момент прихода ордера;
               EARLY — эксперимент клиента 02.11 (только REPLAY): окно FAST с ослабленными фильтрами EARLY_FILTERS,
               LIMIT BUY до bestAsk +5 %, позиция — то, что исполнилось (частично тоже), при неисполнении — повтор;
               цена BUY любого режима — не выше последней сделки × (1 + bidMultiplierUp) (PERCENT_PRICE_BY_SIDE);
               EARLY идёт с планом 2 / 5 / 10 USDT и с конкуренцией за asks: base — наш BUY получает их первым,
               «конкуренция 200 мс / 1 с» — объём, ушедший с уровня за это время после прихода BUY, достался другим,
               «доля 50 %» — нам половина каждого уровня; позиция — только исполненное;
               --early-sweep — тот же вход с изменением одного фактора (спред, глубина, проскальзывание, размер, задержка);
  позиция      профили CAP_DYNAMIC (свой для FAST и CONFIRMED, EARLY — как FAST), CAP_NO_TIME_STOPS (тот же без
               TIME_STOP_*) и LEGACY_STOPS (старые стопы, TP +2/+4 %, удержание 15 с); TP — LIMIT в очереди своего уровня;
               у EARLY ещё EARLY_EXITS (клиент 02.11): окно 5 / 10 с без ценовых стопов (ликвидность — только
               диагностика), затем A — продать всё, или B — trailing по пику exitVWAP (MFE +10 %, откат 15 / 20 %,
               удержание до 30 с) вместе с аварийными стопами и HARD_STOP профиля FAST;
  выход        отмена TP, затем LIMIT SELL по bestBid −0,5 %, до 3 перестановок через 400 мс, затем продажа по bids
               до −20 % (предел PERCENT_PRICE_BY_SIDE; веб-fallback не моделируется), непроданное — остаток.
Локальный стакан: REST-снимок (lastUpdateId) + поток aggre.depth с проверкой fromVersion/toVersion;
при разрыве версий стакан недействителен до следующего снимка (запись делает их раз в секунду).

Исполнение: количество округляется вниз до шага пары, ордер меньше 1 USDT не ставится (остаток — пыль);
TP исполняется сделкой выше его цены целиком, сделкой по его цене — после очереди, стоявшей на уровне при постановке;
комиссии — taker 0,08 % в USDT (tradeFee листинговых пар, commissionAsset в myTrades), TP — maker 0 %;
--fee-in-coin — прежняя модель с BUY-комиссией в купленной монете. Наш ордер не меняет стакан.

Запуск на сервере (записи лежат там):
  python3 tools/cap_replay.py logs/rec_LALAUSDT_*.jsonl.gz --latency 20,50,100,200
  python3 tools/cap_replay.py logs/rec_CHOMPUSDT_*.jsonl.gz --depth-persist-ms 2000 --summary-only
  python3 tools/cap_replay.py --all-frozen --modes EARLY --early-sweep --summary-only
  python3 tools/cap_replay.py --all-frozen --report --send     # сводка в Telegram (наблюдатель — раз в две недели)
"""

import argparse
import gzip
import json
import math
import re
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"

ORDER_USDT = 10.0
TAKER_FEE = 0.0008  # tradeFee листинговых пар; списывается в USDT (commissionAsset в myTrades), mxDeduct выключен
TP_FEE = 0.0  # maker
MIN_NOTIONAL = 1.0  # минимальная сумма ордера, USDT (30002)
# потолки листингов до проверки 30010 в пробе — цена заморозки по записи
KNOWN_CAPS = {"CHOMPUSDT": 5.0, "MTOUSDT": 0.2}
# None — фильтр выключен; min_fill — сколько % плана должно найтись в asks до лимита (и не меньше 1 USDT);
# конкуренция за asks: compete_ms — объём уровня, ушедший за N мс после прихода нашего BUY, считается доставшимся
# другим (берём минимум уровня за это время); share — какая доля оставшегося достаётся нам
FILTERS = dict(max_spread=1.2, bid07=15, bid15=30, ask07=8, max_exit_slip=1.5, max_tick=0.3, max_stress_loss=3.0,
               buy_aggressive=1.0, min_fill=100, compete_ms=0, share=1.0)
EARLY_FILTERS = dict(FILTERS, max_spread=10.0, bid07=5, bid15=10, ask07=0, max_exit_slip=3.0, max_stress_loss=None,
                     buy_aggressive=5.0, min_fill=50)
# --early-sweep: один фактор за раз от EARLY_FILTERS; usdt и latency — размер входа и задержка ордера
EARLY_SWEEP = {
    "спред 2 %": dict(max_spread=2.0), "спред 5 %": dict(max_spread=5.0), "спред 20 %": dict(max_spread=20.0),
    "спред без огр.": dict(max_spread=None),
    "bid как FAST": dict(bid07=15, bid15=30, max_exit_slip=1.5), "bid без огр.": dict(bid07=0, bid15=0, max_exit_slip=None),
    "ask 100 % плана": dict(min_fill=100), "ask как FAST": dict(ask07=8, min_fill=100), "ask ≥1 USDT": dict(min_fill=0),
    "лимит +1 %": dict(buy_aggressive=1.0), "лимит +2 %": dict(buy_aggressive=2.0), "лимит +10 %": dict(buy_aggressive=10.0),
    "лимит +20 %": dict(buy_aggressive=20.0),
    "25 USDT": dict(usdt=25.0), "50 USDT": dict(usdt=50.0), "100 USDT": dict(usdt=100.0),
    "20 мс": dict(latency=20), "100 мс": dict(latency=100), "200 мс": dict(latency=200),
    "всё без огр., +20 %": dict(max_spread=None, bid07=0, bid15=0, max_exit_slip=None, min_fill=0, buy_aggressive=20.0),
}
# EARLY всегда идёт и с конкуренцией за asks (клиент 02.11): base — верхняя оценка, наш BUY получает всё первым
EARLY_COMPETE = {"конкуренция 200 мс": dict(compete_ms=200), "конкуренция 1 с": dict(compete_ms=1000),
                 "доля 50 %": dict(share=0.5)}
EARLY_USDT = (2.0, 5.0, 10.0)  # плановый вход EARLY (клиент 02.11)
ENTRY = {"FAST": dict(window_ms=10_000, stable_ms=0, min_updates=1),
         "CONFIRMED": dict(window_ms=30_000, stable_ms=200, min_updates=3),
         "EARLY": dict(window_ms=10_000, stable_ms=0, min_updates=1)}
PROFILE_MODE = {"FAST": "FAST", "CONFIRMED": "CONFIRMED", "EARLY": "FAST"}  # чей профиль CAP_DYNAMIC у режима
HOLD_MARKS_S = (5, 10, 30, 60)  # «если держать»: netPnL всей купленной позиции по bids через N с после входа
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
    grace_ms: int = 0  # защитное окно EARLY: столько мс после покупки ценовые стопы не действуют
    time_exit: bool = False  # EARLY A: по концу окна продать всё
    peak_act: float | None = None  # EARLY B: trailing по пику exitVWAP включается при MFE (с резервом) ≥ N %
    peak_pp: float | None = None  # EARLY B: выход при откате exitVWAP на N % от пика


CAP_DYNAMIC = {
    "FAST": Profile("CAP_DYNAMIC", -6, -5, -4, -3, 5, 0.5, (3000, -0.5), (6000, 0.5), False, 10_000, 5, 4, 3,
                    ((10, 0.5),), 15, 0.5, 1.5, 45_000),
    "CONFIRMED": Profile("CAP_DYNAMIC", -5, -4, -3, -2.5, 3, 0.3, (3000, -0.3), (5000, 0.3), False, 8_000, 8, 2.5, 2,
                         ((5, 0.5),), 10, 0.3, 1.0, 60_000),
}
CAP_NO_TIME = {m: replace(p, name="CAP_NO_TIME_STOPS", time_stops=False) for m, p in CAP_DYNAMIC.items()}
LEGACY = Profile("LEGACY_STOPS", -8, -6, None, -2.5, 1.5, 0.15, (3000, -0.3), (5000, 0.3), True, 8_000, None, None,
                 None, ((2, 0.5), (4, 0.5)), None, None, 0.0, 15_000)


def early_exit(window_s: int, pullback: int | None) -> Profile:
    """Выход EARLY (клиент 02.11): окно без ценовых стопов, затем A — продать всё (pullback None) или B — trailing по
    пику exitVWAP (активация MFE +10 %, откат pullback %, удержание до 30 с); после окна в B действуют аварийные стопы
    и HARD_STOP профиля FAST, TP и временных стопов нет."""
    name = f"EARLY_TIME_{window_s}S" if pullback is None else f"EARLY_TRAIL_{window_s}S_{pullback}"
    return replace(CAP_DYNAMIC["FAST"], name=name, tp=(), trail_act=None, trail_pp=None, trail_after_tp1=None,
                   guard_target=None, time_stops=False, grace_ms=window_s * 1000, time_exit=pullback is None,
                   peak_act=None if pullback is None else 10.0, peak_pp=pullback,
                   max_hold_ms=window_s * 1000 if pullback is None else 30_000)


EARLY_EXITS = [early_exit(w, pb) for w in (5, 10) for pb in (None, 15, 20)]


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


def frozen_records() -> list[Path]:
    """Последняя запись каждого замороженного листинга (30010 в логе пробы или KNOWN_CAPS), по дате записи."""
    latest = {}
    for p in sorted(LOGS.glob("rec_*_2*.jsonl.gz")):
        latest[p.name.split("_")[1]] = p
    found = [p for sym, p in latest.items() if sym in KNOWN_CAPS or find_cap(sym)]
    return sorted(found, key=lambda p: p.name.split("_", 2)[2])


def load(path: Path, cap: float, before_ms=30_000, after_ms=170_000):
    """Два прохода: найти снятие потолка по сделкам, затем взять все события стакана и сделки вокруг него."""
    open_ms, cand_ms = None, None
    for line in iter_lines(path):
        if open_ms is None:
            if '"OPEN_DETECTED"' in line:
                e = json.loads(line)
                open_ms = e["data"].get("open_ms") or e["t"]
            continue
        if '"publicAggreDeals"' in line and line_t(line) > open_ms + 60_000:  # не текст подписки и не COUNTS
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
        elif '"publicAggreDepths"' in line:
            events.append((t, "diff", json.loads(line)["data"]["publicAggreDepths"]))
        elif '"publicAggreDeals"' in line:
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


@dataclass
class Opts:
    stops_stressed: bool
    depth_band: float  # доля от bestBid
    depth_persist_ms: int
    entry_stress: float  # %
    entry_basis: str  # vwap | max
    pre_buy_drop: bool
    fee: float  # taker
    fee_in_coin: bool  # BUY-комиссия в купленной монете (иначе — в USDT)


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
    variant: str  # вариант EARLY ("base" или метка из EARLY_SWEEP), у FAST/CONFIRMED — ""
    flt: dict  # фильтры входа
    usdt: float  # сумма входа
    profile: Profile
    latency: int
    opts: Opts
    release_ms: int
    step: float
    qty_step: float
    band_up: float  # PERCENT_PRICE_BY_SIDE bidMultiplierUp: BUY не выше цены последней сделки × (1 + band_up)
    # состояние
    state: str = "SCAN"  # SCAN → BUY_SENT → POSITION → EXITING → DONE
    stable_since: int | None = None
    stable_updates: int = 0
    seen_updates: int = -1  # book.updates при прошлой проверке: без новой версии стакана проверка не повторяется
    skip_reasons: dict = field(default_factory=dict)
    scan_px: deque = field(default_factory=deque)  # (t, exitVWAP плановой покупки) до входа
    buy_misses: int = 0  # EARLY: ордер пришёл, а asks до лимита уже нет (или их забрали другие) — повтор
    buy_at: int = 0
    fill_at: int = 0  # конкуренция: когда станет ясно, что из asks при приходе досталось нам
    avail: dict = field(default_factory=dict)  # цена → объём asks до лимита, ещё не забранный другими
    grace_liq_s: float | None = None  # EARLY: когда в защитном окне впервые пропала ликвидность (только диагностика)
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
    trades: deque = field(default_factory=deque)  # (t, цена сделки)
    evs: deque = field(default_factory=deque)  # (t, exitVWAP(Q))
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
    q0: float = 0.0  # купленное количество (за вычетом комиссии в монете)
    marks: dict = field(default_factory=dict)  # секунды после входа → netPnL % «если держать»
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
        qty = self.usdt / (ba * (1 + self.flt["buy_aggressive"] / 100))
        sold, rev = vwap(book.sorted_bids(), qty, None, False)
        if sold >= qty * 0.999:
            self.scan_px.append((t, rev / qty))

    def filters(self, t: int, book: Book):
        if not book.valid:
            return False, "book_invalid", None
        bb, ba = book.best()
        if bb <= 0 or ba <= bb:
            return False, "bad_book", None
        f, usdt, fee = self.flt, self.usdt, self.opts.fee
        mid = (bb + ba) / 2
        if f["max_spread"] is not None and 100 * (ba - bb) / mid > f["max_spread"]:
            return False, "spread", None
        bids, asks = book.sorted_bids(), book.sorted_asks()
        if depth_usdt(bids, bb * (1 - 0.007), bb) < usdt * f["bid07"]:
            return False, "depthBid0.7", None
        if depth_usdt(bids, bb * (1 - 0.015), bb) < usdt * f["bid15"]:
            return False, "depthBid1.5", None
        if depth_usdt(asks, ba, ba * (1 + 0.007)) < usdt * f["ask07"]:
            return False, "depthAsk0.7", None
        if 100 * self.step / ba >= f["max_tick"]:
            return False, "tick", None
        limit = floor_step(ba * (1 + f["buy_aggressive"] / 100), self.step)
        if self.trades:  # выше полосы биржа отклонит ордер (30087); опорная цена — последняя сделка
            limit = min(limit, floor_step(self.trades[-1][1] * (1 + self.band_up), self.step))
            if limit < ba:
                return False, "price_band", None
        qty = floor_step(usdt / limit, self.qty_step)
        if qty * limit < MIN_NOTIONAL:
            return False, "min_notional", None
        got, cost = vwap(asks, qty, limit, True)
        if got < qty * f["min_fill"] / 100 * 0.999 or cost < MIN_NOTIONAL:
            return False, "buy_depth", None
        q_net = got * (1 - fee) if self.opts.fee_in_coin else got
        sold, rev = vwap(bids, q_net, None, False)
        if sold < q_net * 0.999:
            return False, "exit_vwap", None
        exit_px = rev / q_net
        if f["max_exit_slip"] is not None and 100 * (bb - exit_px) / bb > f["max_exit_slip"]:
            return False, "exit_slippage", None
        base = cost if self.opts.entry_basis == "vwap" else got * limit
        if not self.opts.fee_in_coin:
            base *= 1 + fee
        stressed_exit = rev * (1 - fee) * (1 - self.opts.entry_stress / 100)
        if f["max_stress_loss"] is not None and 100 * (stressed_exit - base) / base < -f["max_stress_loss"]:
            return False, "entry_stress", None
        ago = self.ago(self.scan_px, t, 1000)
        drop = 100 * (exit_px / ago - 1) if ago else None
        if self.opts.pre_buy_drop and drop is not None and drop <= CAP_DYNAMIC[PROFILE_MODE[self.mode]].bid_vwap_drop:
            return False, "pre_buy_drop", None
        return True, "", (limit, qty, ba, drop)

    def arrive_buy(self, t: int, book: Book) -> None:
        """BUY дошёл до биржи: asks до лимита в этот момент — наши, если их не заберут другие за compete_ms."""
        self.avail = {p: q for p, q in book.sorted_asks() if p <= self.buy_limit}
        if self.flt["compete_ms"] <= 0:
            self.fill_buy(t, book)
            return
        self.fill_at, self.state = t + self.flt["compete_ms"], "BUY_PENDING"

    def compete(self, book: Book) -> None:
        """Объём уровня, ушедший после прихода нашего BUY, считаем доставшимся другим участникам."""
        for p in self.avail:
            self.avail[p] = min(self.avail[p], book.asks.get(p, 0.0))

    def fill_buy(self, t: int, book: Book) -> None:
        levels = [(p, q * self.flt["share"]) for p, q in sorted(self.avail.items()) if q > 0]
        got_raw, cost_raw = vwap(levels, self.buy_qty, self.buy_limit, True)
        got = floor_step(got_raw, self.qty_step)
        if got <= 0:
            self.buy_misses += 1
            if self.mode == "EARLY":  # asks до лимита разобрали — снова ищем вход в окне
                self.state, self.stable_since, self.stable_updates = "SCAN", None, 0
                return
            self.finish(t, "BUY_NOT_FILLED")
            return
        cost = cost_raw * got / got_raw
        avg = cost / got
        fee = self.opts.fee
        self.entry_ms = t
        self.C, self.Q = (cost, got * (1 - fee)) if self.opts.fee_in_coin else (cost * (1 + fee), got)
        self.q0 = self.Q
        self.fees += cost * fee
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
                           slippage_pct=100 * (avg / self.decision_ask - 1), fill_pct=100 * got / self.buy_qty,
                           buy_misses=self.buy_misses)

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
        return 100 * (self.R + self.Q * ev * (1 - self.opts.fee) * (1 - s) - self.C) / self.C

    @staticmethod
    def ago(hist: deque, t: int, ms: int, keep_ms: int = 2000):
        """Значение не позже t − ms (последнее такое) или None; записи, ненужные для окон до keep_ms, отбрасываются."""
        while len(hist) > 1 and hist[1][0] <= t - keep_ms:
            hist.popleft()
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
        in_grace = elapsed < p.grace_ms
        if ev is None:
            if in_grace:  # в защитном окне — только диагностика исчезнувшей ликвидности
                self.grace_liq_s = elapsed / 1000 if self.grace_liq_s is None else self.grace_liq_s
                return
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
        if self.tp1_done or p.grace_ms:
            self.peak_ev = max(self.peak_ev, ev)
        if p.grace_ms:
            self.check_exit_early(t, ev, plain, stressed, depth_hit, in_grace, last_trade, trade_1s, ev_1s)
            return
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

    def check_exit_early(self, t, ev, plain, stressed, depth_hit, in_grace, last_trade, trade_1s, ev_1s) -> None:
        """Профили EARLY: в окне ценовых стопов нет; затем A — продать всё, B — аварийные стопы, HARD_STOP,
        trailing по пику exitVWAP после MFE (с резервом) ≥ peak_act, maxHold."""
        p = self.profile
        if in_grace:
            if depth_hit and self.grace_liq_s is None:
                self.grace_liq_s = (t - self.entry_ms) / 1000
            return
        detail = f"netPnL {plain:+.2f}% (с резервом {stressed:+.2f}%), MFE {self.mfe_trail:+.2f}%"
        if p.time_exit:
            self.start_exit(t, f"TIME_EXIT_{p.grace_ms // 1000}S", detail, ev)
            return
        if self.mfe_trail >= p.peak_act:
            self.trail_armed = True
        checks = [
            ("DEPTH_DISAPPEARED", depth_hit),
            ("HARD_STOP", plain <= p.hard),
            ("FAST_WINDOW_DUMP", last_trade and trade_1s and 100 * (last_trade / trade_1s - 1) <= p.window_dump),
            ("BID_VWAP_DROP", ev_1s and 100 * (ev / ev_1s - 1) <= p.bid_vwap_drop),
            ("TRAILING_PEAK", self.trail_armed and ev <= self.peak_ev * (1 - p.peak_pp / 100)),
            ("MAX_HOLD", t - self.entry_ms >= p.max_hold_ms),
        ]
        for name, hit in checks:
            if hit:
                self.start_exit(t, name, detail, ev)
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
            self.R += rev * (1 - self.opts.fee)
            self.fees += rev * self.opts.fee
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
                           min_depth07_x=self.min_d07, min_depth15_x=self.min_d15, grace_liq_s=self.grace_liq_s)
        self.state = "DONE"

    def mark(self, t: int, book: Book) -> None:
        """«Если держать»: продажа всей купленной позиции по bids через HOLD_MARKS_S после входа, как бы ни вышли."""
        for sec in HOLD_MARKS_S:
            if sec not in self.marks and t >= self.entry_ms + sec * 1000:
                sold, rev = vwap(book.sorted_bids(), self.q0, None, False)
                ok = sold >= self.q0 * 0.999
                self.marks[sec] = 100 * (rev * (1 - self.opts.fee) - self.C) / self.C if ok else None

    def finish(self, t: int, reason: str) -> None:
        self.state = "DONE"
        self.result.update(reason=reason, skips=self.skip_reasons, buy_misses=self.buy_misses)

    # --- общий шаг ---
    def before_event(self, t: int, book: Book) -> None:
        """Отложенные действия, срок которых наступил до события t: стакан ещё в состоянии на момент их прихода."""
        if self.state == "BUY_SENT" and self.buy_at <= t:
            self.arrive_buy(self.buy_at, book)
        if self.state == "BUY_PENDING" and self.fill_at <= t:
            self.fill_buy(self.buy_at, book)  # позиция — с момента прихода ордера
        while self.state == "EXITING" and self.exit_next <= t:
            self.exit_step(self.exit_next, book)

    def after_event(self, t: int, kind: str, book: Book, deal=None) -> None:
        if kind == "deal":
            self.on_deal(t, *deal)
        if self.state == "BUY_PENDING" and kind != "deal":
            self.compete(book)
        elif self.state == "SCAN" and kind != "deal":
            self.check_entry(t, book)
        elif self.state == "POSITION":
            self.check_exit(t, book)


# --- прогон -------------------------------------------------------------------

def profiles_for(mode: str) -> list[Profile]:
    m = PROFILE_MODE[mode]
    return [CAP_DYNAMIC[m], CAP_NO_TIME[m], LEGACY] + (EARLY_EXITS if mode == "EARLY" else [])


def make_sims(args, opts: Opts, release: int, step: float, qty_step: float, band_up: float) -> list[Sim]:
    """Ветки: режим × вариант EARLY × профиль × задержка × сумма входа. EARLY base и варианты конкуренции —
    все профили и суммы EARLY_USDT; варианты --early-sweep — только CAP_DYNAMIC и --usdt."""
    sims = []
    for mode in args.modes:
        variants = [("", FILTERS, {})]
        if mode == "EARLY":
            base = dict(EARLY_FILTERS, **args.early)
            extra = list(EARLY_COMPETE.items()) + (list(EARLY_SWEEP.items()) if args.early_sweep else [])
            variants = [("base", base, {})] + [
                (label, dict(base, **{k: v for k, v in over.items() if k in base}), over) for label, over in extra]
        for label, flt, over in variants:
            full = label in ("", "base") or label in EARLY_COMPETE
            profiles = profiles_for(mode) if full else [CAP_DYNAMIC[PROFILE_MODE[mode]]]
            latencies = [over["latency"]] if "latency" in over else args.latency
            sizes = [over["usdt"]] if "usdt" in over else args.early_usdt if mode == "EARLY" and full else args.usdt
            sims += [Sim(mode, label, flt, usdt, prof, lat, opts, release, step, qty_step, band_up)
                     for prof in profiles for lat in latencies for usdt in sizes]
    return sims


def no_entry_reason(r: dict) -> str:
    """Чаще всего отказавший фильтр, а если отказов не было — причина конца ветки."""
    skips = r.get("skips") or {}
    return max(skips, key=skips.get) if skips else r.get("reason", "?")


def run_listing(path: Path, args, caps: dict) -> tuple[list[dict], dict]:
    """Прогон одной записи: строки результатов по веткам и сведения о листинге (потолок, снятие)."""
    symbol = path.name.split("_")[1]
    meta = dict(symbol=symbol, status="no_cap", zones="", cap=None, src=None, release_ms=None, open_ms=None,
                rec=path.name)
    cap_src = caps.get(symbol) or find_cap(symbol) or (
        (KNOWN_CAPS[symbol], "заморозка в записи") if symbol in KNOWN_CAPS else None)
    if not cap_src:
        if not args.summary_only:
            print(f"{symbol}: нет потолка (ни 30010 в логе пробы, ни --cap) — пропуск")
        return [], meta
    cap, src = cap_src
    meta.update(status="not_released", cap=cap, src=src)
    open_ms, cand_ms, events, info = load(path, cap)
    if not events:
        if not args.summary_only:
            print(f"{symbol}: потолок {cap}, но сделок выше него в записи не было — пропуск")
        return [], meta
    step = 10 ** -int(info.get("quotePrecision") or 8)
    qty_step = 10 ** -int(info.get("baseAssetPrecision") or 8)
    zones = ", ".join(info.get("conceptPlates") or [])
    band_up = next((float(f["bidMultiplierUp"]) for f in info.get("filters") or []
                    if f.get("filterType") == "PERCENT_PRICE_BY_SIDE"), 0.2)
    opts = Opts(args.stops_basis == "stressed", args.depth_band / 100, args.depth_persist_ms, args.entry_stress,
                args.entry_basis, args.pre_buy_drop, args.fee / 100, args.fee_in_coin)
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
            sims = make_sims(args, opts, release, step, qty_step, band_up)
        for s in sims:
            if s.state != "DONE":
                s.after_event(t, kind, book, data if kind == "deal" else None)
            if s.entry_ms and kind != "deal" and book.valid and len(s.marks) < len(HOLD_MARKS_S):
                s.mark(t, book)
    t_end = events[-1][0]
    for s in sims:  # запись кончилась раньше, чем ветка завершилась
        if s.state == "POSITION":
            s.start_exit(t_end, "END_OF_DATA", "", 0.0)
        while s.state == "EXITING":
            s.exit_step(t_end, book)
        if s.state != "DONE":
            s.finish(t_end, "END_OF_DATA")
    rows = []
    meta.update(zones=zones, open_ms=open_ms, release_ms=release, status="released" if release else "not_released")
    if not args.summary_only:
        head = (f"\n=== {symbol} [{zones}] потолок {cap} ({src}), первая сделка выше потолка "
                f"T+{(cand_ms - open_ms) / 1000:.3f} с, CAP_RELEASED T+{(release - open_ms) / 1000:.3f} с"
                if release else f"\n=== {symbol}: bid так и не поднялся выше потолка")
        print(head + f"; шаг цены {step:g}, шаг кол-ва {qty_step:g}, разрывов версий стакана: {book.gaps}")
    for s in sims:
        r = s.result
        if s.entry_ms:
            r["hold_marks"] = {sec: s.marks.get(sec) for sec in HOLD_MARKS_S}
        rows.append(dict(symbol=symbol, mode=s.mode, variant=s.variant, usdt=s.usdt, profile=s.profile.name,
                         latency=s.latency, **r))
        if args.summary_only:
            continue
        tag = f"  {s.mode:<9} {s.variant:<19} {s.profile.name:<17} {s.latency:>3} мс {s.usdt:>3g} USDT:"
        if "entry_ms" not in r:
            top = sorted(r.get("skips", {}).items(), key=lambda x: -x[1])[:3]
            misses = f", ордер не исполнился {r['buy_misses']} раз" if r.get("buy_misses") else ""
            print(f"{tag} нет входа ({r.get('reason')}; отказы: {', '.join(f'{k} {v}' for k, v in top)}{misses})")
            continue
        drop = r.get("drop_1s_at_decision")
        gap = r.get("exit_gap_pct")
        fill = f", исполнено {r['fill_pct']:.0f} % плана" if r["fill_pct"] < 99.9 else ""
        print(f"{tag} вход +{(r['entry_ms'] - release) / 1000:.2f} с по {r['entry_px']:.6g} "
              f"({r['entry_px'] / cap:.2f}× потолок, к ask {r['slippage_pct']:+.2f} %{fill}"
              + (f", bid-VWAP за 1 с до решения {drop:+.1f} %" if drop is not None else "")
              + f") → {r['reason']} через {r['hold_s']:.1f} с: {r['pnl_pct']:+.2f} % ({r['pnl_usdt']:+.3f} USDT), "
              f"MFE {r['mfe']:+.1f} / MAE {r['mae']:+.1f} %, TP-исполнений {r['tp_fills']}, "
              f"выход: попыток {r['exit_rounds']}{', до −20 %' if r['critical'] else ''}"
              + (f", к exitVWAP {gap:+.2f} %" if gap is not None else "")
              + f", остаток {r['left_usdt']:.3f} USDT, комиссии {r['fees_usdt']:.4f}; "
              f"глубина min ×{r['min_depth07_x']:.0f} (0,7 %) / ×{r['min_depth15_x']:.0f} (1,5 %); если держать: "
              + " ".join(f"+{sec} с {v:+.1f} %" if v is not None else f"+{sec} с —"
                         for sec, v in r["hold_marks"].items()) + f"  [{r['detail']}]")
    return rows, meta


PROFILE_LABEL = {"CAP_DYNAMIC": "CAP_DYNAMIC", "CAP_NO_TIME_STOPS": "без врем. стопов", "LEGACY_STOPS": "LEGACY",
                 **{p.name: (f"время {p.grace_ms // 1000} с" if p.time_exit else
                             f"окно {p.grace_ms // 1000} с + trailing {p.peak_pp} %") for p in EARLY_EXITS}}
EARLY_REF = "EARLY_TIME_10S"  # профиль EARLY для строк по листингам и по размеру


def base_rows(rows: list[dict], latency: int, usdt: float, variant: str = "base") -> list[dict]:
    """Ветки FAST/CONFIRMED и EARLY варианта variant с заданными задержкой и суммой входа."""
    return [r for r in rows if r["variant"] in ("", variant) and r["latency"] == latency and r["usdt"] == usdt]


def mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def hold_mean(rs: list[dict], sec: int) -> float:
    """Средний netPnL % «если держать sec секунд» — по входам, где стакан тогда выдерживал продажу."""
    return mean([v for r in rs if (v := r["hold_marks"].get(sec)) is not None])


def hold_usdt(rs: list[dict], sec: int) -> float:
    """Условный PnL удержания в USDT: вся купленная позиция продана по bids через sec секунд после входа."""
    return sum(r["buy_usdt"] * v / 100 for r in rs if (v := r["hold_marks"].get(sec)) is not None)


def prof_line(rs: list[dict], label: str) -> str:
    """Итог профиля: реализованный PnL отдельно от непроданного остатка."""
    total = sum(r["pnl_usdt"] for r in rs)
    wins = sum(r["pnl_usdt"] > 0 for r in rs)
    return (f"  {label}: {total:+.2f} USDT ({wins}/{len(rs) - wins}), худший {min(r['pnl_pct'] for r in rs):+.1f} %, "
            f"остаток {sum(r['left_usdt'] for r in rs):.2f}; MFE/MAE {mean([r['mfe'] for r in rs]):+.1f}/"
            f"{mean([r['mae'] for r in rs]):+.1f} %, удержание {mean([r['hold_s'] for r in rs]):.0f} с")


def bought_line(main: list[dict]) -> str:
    """Купленный объём и причины, по которым входа или исполнения не было (по ветке CAP_DYNAMIC каждого листинга)."""
    entered = [r for r in main if "entry_ms" in r]
    misses = sum(r.get("buy_misses") or 0 for r in main)
    skips = Counter(no_entry_reason(r) for r in main if "entry_ms" not in r)
    line = f"входов {len(entered)} из {len(main)}"
    if entered:
        line += (f", куплено {sum(r['buy_usdt'] for r in entered):.1f} USDT "
                 f"(ср. {mean([r['fill_pct'] for r in entered]):.0f} % плана)")
    if misses:
        line += f"; BUY без исполнения {misses} раз"
    if skips:
        line += "; нет входа: " + ", ".join(f"{k} {v}" for k, v in skips.most_common())
    return line


def early_compact(rs: list[dict]) -> str:
    """Итоги профилей EARLY_EXITS одной строкой: реализованный PnL, USDT."""
    parts = []
    for p in EARLY_EXITS:
        prs = [r for r in rs if r["profile"] == p.name and "pnl_usdt" in r]
        parts.append(f"{PROFILE_LABEL[p.name]} {sum(r['pnl_usdt'] for r in prs):+.2f}")
    return "; ".join(parts)


def brief(meta: dict, rows: list[dict], latency: int, usdt: float) -> str:
    """Короткий итог одного листинга для Telegram (наблюдатель шлёт его после записи замороженного листинга)."""
    head = f"📊 REPLAY CAP BREAKOUT — {meta['symbol']}" + (f" [{meta['zones']}]" if meta["zones"] else "")
    if meta["status"] == "no_cap":
        return f"{head}\nПотолка нет (30010 не было) — листинг не замороженный"
    if meta["status"] == "not_released":
        return (f"{head}\nПотолок {meta['cap']} ({meta['src']}), подтверждённого снятия нет: сделок выше потолка "
                "в записи не было — торгового входа нет")
    lines = [head, f"Потолок {meta['cap']} ({meta['src']}), снят на T+{(meta['release_ms'] - meta['open_ms']) / 1000:.0f} с"]
    base = base_rows(rows, latency, usdt)
    for mode in ENTRY:
        rs = [r for r in base if r["mode"] == mode]
        if not rs:
            continue
        r0 = rs[0]
        label = mode + (f" (эксперимент, план {usdt:g} USDT, наш BUY первым)" if mode == "EARLY" else "")
        if "entry_ms" not in r0:
            misses = f", BUY без исполнения {r0['buy_misses']} раз" if r0.get("buy_misses") else ""
            lines.append(f"{label}: нет входа ({no_entry_reason(r0)}{misses})")
        else:
            lines.append(f"{label}: вход через {(r0['entry_ms'] - meta['release_ms']) / 1000:.1f} с "
                         f"по {r0['entry_px'] / meta['cap']:.2f}× потолка, куплено {r0['buy_usdt']:.2f} USDT")
            for r in rs:
                if r["profile"] in PROFILE_LABEL and (mode != "EARLY" or r["profile"] == "CAP_DYNAMIC"):
                    left = f", остаток {r['left_usdt']:.2f} USDT" if r["left_usdt"] >= 0.01 else ""
                    lines.append(f"  {PROFILE_LABEL[r['profile']]}: {r['pnl_pct']:+.2f} % "
                                 f"({r['reason']}, {r['hold_s']:.0f} с{left})")
            if mode == "EARLY":
                lines.append("  выходы EARLY, USDT: " + early_compact(rs))
            lines.append("  если держать: " + ", ".join(f"{sec} с {v:+.1f} %" for sec, v in r0["hold_marks"].items()
                                                        if v is not None))
        if mode == "EARLY":
            for variant in EARLY_COMPETE:
                cr = [r for r in base_rows(rows, latency, usdt, variant) if r["mode"] == mode
                      and r["profile"] == EARLY_REF]
                if cr:
                    c0 = cr[0]
                    got = (f"куплено {c0['buy_usdt']:.2f} USDT, время 10 с {c0['pnl_usdt']:+.2f} USDT"
                           if "entry_ms" in c0 else f"нет входа ({no_entry_reason(c0)})")
                    lines.append(f"  {variant}: {got}")
    lines.append(f"{usdt:g} USDT на вход, задержка {latency} мс; модель упрощённая — см. doc/cap_breakout_resheniya.md")
    return "\n".join(lines)


def report(rows: list[dict], metas: list[dict], latency: int, usdt: float, fee: float, days: int) -> list[str]:
    """Сводка REPLAY по всем замороженным листингам — три сообщения Telegram: FAST и CONFIRMED, EARLY, по листингам."""
    short = lambda s: s.removesuffix("USDT")  # noqa: E731
    since = datetime.now().timestamp() - days * 86_400
    new = [m["symbol"] for m in metas
           if datetime.strptime("_".join(m["rec"].split("_")[2:4])[:15], "%Y%m%d_%H%M%S").timestamp() >= since]
    released = [m for m in metas if m["status"] == "released"]
    unreleased = [short(m["symbol"]) for m in metas if m["status"] != "released"]
    head = [f"📊 CAP BREAKOUT — сводный REPLAY на {datetime.now():%d.%m.%Y}",
            f"Замороженных листингов: {len(metas)} — со снятием и пробоем потолка {len(released)}"
            + (f", без подтверждённого снятия и без входа {len(unreleased)} ({', '.join(unreleased)})"
               if unreleased else "") + f"; новые за {days} дн.: {', '.join(map(short, new)) or 'нет'}",
            f"{usdt:g} USDT на вход, задержка {latency} мс, taker {100 * fee:g} % в USDT. Итог — реализованный PnL; "
            "остаток — непроданное по bid, в итог не входит; «если держать» — условный PnL удержания."]
    base = base_rows(rows, latency, usdt)
    for mode in ("FAST", "CONFIRMED"):
        rs_mode = [r for r in base if r["mode"] == mode]
        if not rs_mode:
            continue
        main = [r for r in rs_mode if r["profile"] == "CAP_DYNAMIC"]
        entered = [r for r in main if "entry_ms" in r]
        head.append(f"\n{mode}: {bought_line(main)}")
        for prof, label in PROFILE_LABEL.items():
            rs = [r for r in rs_mode if r["profile"] == prof and "pnl_usdt" in r]
            if rs:
                head.append(prof_line(rs, label))
        if entered:
            gaps = [r["exit_gap_pct"] for r in rs_mode if r.get("exit_gap_pct") is not None]
            head.append(f"  проскальзывание: вход {mean([r['slippage_pct'] for r in entered]):+.2f} % к ask решения"
                        + (f", выход {mean(gaps):+.2f} % к exitVWAP сигнала" if gaps else ""))
            head.append("  если держать всю позицию: " + ", ".join(
                f"{sec} с {hold_usdt(entered, sec):+.2f} USDT ({hold_mean(entered, sec):+.1f} %)" for sec in (10, 60)))
    early = [f"EARLY — эксперимент, только REPLAY; план {usdt:g} USDT, задержка {latency} мс"]
    rs_mode = [r for r in base if r["mode"] == "EARLY"]
    if rs_mode:
        main = [r for r in rs_mode if r["profile"] == "CAP_DYNAMIC"]
        entered = [r for r in main if "entry_ms" in r]
        early.append(f"Наш BUY получает asks первым (верхняя оценка): {bought_line(main)}")
        for prof, label in PROFILE_LABEL.items():
            rs = [r for r in rs_mode if r["profile"] == prof and "pnl_usdt" in r]
            if rs:
                early.append(prof_line(rs, label))
        if entered:
            early.append("  если держать всю позицию: " + ", ".join(
                f"{sec} с {hold_usdt(entered, sec):+.2f} USDT" for sec in (10, 60)))
        for variant in EARLY_COMPETE:
            vr = [r for r in base_rows(rows, latency, usdt, variant) if r["mode"] == "EARLY"]
            vmain = [r for r in vr if r["profile"] == "CAP_DYNAMIC"]
            if vmain:
                early.append(f"\n{variant}: {bought_line(vmain)}")
                early.append("  выходы, USDT: " + early_compact(vr))
        sizes = sorted({r["usdt"] for r in rows if r["mode"] == "EARLY" and r["variant"] == "base"})
        parts = []
        for size in sizes:
            sr = [r for r in base_rows(rows, latency, size) if r["mode"] == "EARLY" and r["profile"] == EARLY_REF]
            got = [r for r in sr if "entry_ms" in r]
            parts.append(f"{size:g} USDT — входов {len(got)}, куплено {sum(r['buy_usdt'] for r in got):.1f}, "
                         f"итог {sum(r['pnl_usdt'] for r in got):+.2f}")
        early.append(f"\nПо размеру плана ({PROFILE_LABEL[EARLY_REF]}, наш BUY первым): " + "; ".join(parts))
    per = [f"По листингам — % позиции: FAST и CONFIRMED с CAP_DYNAMIC, EARLY с выходом «{PROFILE_LABEL[EARLY_REF]}» "
           "(наш BUY первым); — нет входа:"]
    for m in metas[-20:]:
        if m["status"] != "released":
            per.append(f"  {short(m['symbol'])}: без подтверждённого снятия потолка — торгового входа нет")
            continue
        parts = []
        for mode in ENTRY:
            prof = EARLY_REF if mode == "EARLY" else "CAP_DYNAMIC"
            r = next((r for r in base if r["symbol"] == m["symbol"] and r["mode"] == mode and r["profile"] == prof),
                     None)
            if r is not None:
                parts.append(f"{mode} {r['pnl_pct']:+.1f}" if "pnl_pct" in r else f"{mode} — ({no_entry_reason(r)})")
        per.append(f"  {short(m['symbol'])}: " + ", ".join(parts))
    return ["\n".join(head), "\n".join(early), "\n".join(per)]


def summary(rows: list[dict]) -> None:
    print("\nИтого (листинги без входа в сумму не входят; остаток — непроданное по bid, в сумму не входит):")
    print(f"  {'режим':<9} {'вариант':<19} {'профиль':<19} {'мс':>3} {'USDT':>4}  входов  +/−   сумма USDT  средний %"
          f"  худший %  MFE ср. %  MAE ср. %  удерж. с  к ask %  куплено USDT  исполн. %  остаток USDT  "
          + "  ".join(f"держать {sec} с" for sec in HOLD_MARKS_S))
    order = {m: i for i, m in enumerate(ENTRY)}
    keys = sorted({(r["mode"], r["variant"], r["profile"], r["latency"], r["usdt"]) for r in rows},
                  key=lambda k: (order[k[0]], k[1] != "base", k[1], k[2], k[3], k[4]))
    for k in keys:
        tag = f"  {k[0]:<9} {k[1]:<19} {k[2]:<19} {k[3]:>3} {k[4]:>4g}"
        rs = [r for r in rows if (r["mode"], r["variant"], r["profile"], r["latency"], r["usdt"]) == k
              and "pnl_usdt" in r]
        if not rs:
            print(f"{tag}  {0:>6}")
            continue
        total = sum(r["pnl_usdt"] for r in rs)
        wins, losses = sum(r["pnl_usdt"] > 0 for r in rs), sum(r["pnl_usdt"] <= 0 for r in rs)
        bought = sum(r["buy_usdt"] for r in rs)
        print(f"{tag}  {len(rs):>6}  {wins}/{losses}  {total:>+10.3f}  {100 * total / bought:>+9.2f}"
              f"  {min(r['pnl_pct'] for r in rs):>+8.2f}  {mean([r['mfe'] for r in rs]):>+9.2f}"
              f"  {mean([r['mae'] for r in rs]):>+9.2f}  {mean([r['hold_s'] for r in rs]):>8.1f}"
              f"  {mean([r['slippage_pct'] for r in rs]):>+7.2f}  {bought:>12.2f}  {mean([r['fill_pct'] for r in rs]):>9.0f}"
              f"  {sum(r['left_usdt'] for r in rs):>12.3f}  "
              + "  ".join(f"{hold_mean(rs, sec):>+{10 + len(str(sec))}.2f}" for sec in HOLD_MARKS_S))


def parse_early(text: str) -> dict:
    """--early max_spread=20,min_fill=0,ask07=off → изменения EARLY_FILTERS (off — фильтр выключен)."""
    out = {}
    for item in filter(None, text.split(",")):
        key, val = item.split("=")
        if key not in EARLY_FILTERS:
            raise SystemExit(f"--early: нет фильтра {key}; есть {', '.join(EARLY_FILTERS)}")
        out[key] = None if val == "off" else float(val)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="*", help="записи logs/rec_<SYMBOL>_*.jsonl.gz")
    p.add_argument("--all-frozen", action="store_true", help="все замороженные листинги из logs/ (последняя запись пары)")
    p.add_argument("--cap", action="append", default=[], help="потолок без 30010 в логе: SYMBOL=цена (можно несколько)")
    p.add_argument("--latency", default="50", help="задержка ордера в мс, через запятую (50)")
    p.add_argument("--usdt", default=f"{ORDER_USDT:g}", help=f"сумма входа в USDT, через запятую ({ORDER_USDT:g})")
    p.add_argument("--early-usdt", default=",".join(f"{x:g}" for x in EARLY_USDT),
                   help=f"плановый вход EARLY в USDT, через запятую ({','.join(f'{x:g}' for x in EARLY_USDT)})")
    p.add_argument("--modes", default="FAST,CONFIRMED,EARLY")
    p.add_argument("--early", type=parse_early, default={},
                   help="изменить фильтры EARLY: max_spread=20,min_fill=0,... (off — выключить фильтр)")
    p.add_argument("--early-sweep", action="store_true",
                   help="EARLY с изменением одного фактора за раз (EARLY_SWEEP), только профиль CAP_DYNAMIC")
    p.add_argument("--fee", type=float, default=100 * TAKER_FEE, help=f"комиссия taker, %% ({100 * TAKER_FEE:g})")
    p.add_argument("--fee-in-coin", action="store_true", help="BUY-комиссия в купленной монете (прежняя модель)")
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
    p.add_argument("--brief", action="store_true", help="только короткий итог по каждому листингу (для Telegram)")
    p.add_argument("--report", action="store_true", help="сводка по всем листингам (для Telegram)")
    p.add_argument("--report-days", type=int, default=14, help="«новые» листинги в сводке — за N дней (14)")
    p.add_argument("--send", action="store_true", help="отправить --brief / --report в Telegram (TG_CHAT_ID)")
    p.add_argument("--json", help="сохранить строки результатов в JSON")
    args = p.parse_args()
    args.latency = [int(x) for x in args.latency.split(",")]
    args.usdt = [float(x) for x in args.usdt.split(",")]
    args.early_usdt = [float(x) for x in args.early_usdt.split(",")]
    args.modes = args.modes.split(",")
    caps = {}
    for c in args.cap:
        sym, val = c.split("=")
        caps[sym.upper()] = (float(val), "--cap")
    files = [Path(f) for f in args.files] + (frozen_records() if args.all_frozen else [])
    if not files:
        p.error("нужны записи или --all-frozen")
    args.summary_only |= args.brief or args.report
    started = time.monotonic()
    rows, metas, texts = [], [], []
    for f in files:
        listing_rows, meta = run_listing(f, args, caps)
        rows += listing_rows
        metas.append(meta)
        if args.brief:
            texts.append(brief(meta, listing_rows, args.latency[0], args.usdt[0]))
    if args.report:
        texts += report(rows, metas, args.latency[0], args.usdt[0], args.fee / 100, args.report_days)
    if texts:
        print("\n\n".join(texts))
    else:
        summary(rows)
    print(f"\n({len(files)} записей, {time.monotonic() - started:.0f} с)", file=sys.stderr)
    if args.json:
        Path(args.json).write_text(json.dumps(rows, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    if args.send and texts:
        from notify import notify
        from probe_listing import load_dotenv
        load_dotenv(ROOT / ".env")
        for text in texts:
            notify(text)


if __name__ == "__main__":
    main()
