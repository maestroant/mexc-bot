"""Стратегия CAP BREAKOUT (ТЗ v2 + решения клиента, doc/cap_breakout_resheniya.md): параметры, профили выхода,
ветка симуляции Sim (вход, позиция, выход, исполнение по стакану) и движок CapEngine одного листинга.
Один и тот же движок гоняют REPLAY (tools/cap_replay.py, по записи) и PAPER (tools/paper_cap.py, по живому потоку)."""

import re
from collections import deque
from itertools import product
from dataclasses import dataclass, field, replace
from pathlib import Path

from .book import Book, ceil_step, depth_usdt, floor_step, vwap

LOGS = Path(__file__).resolve().parent.parent.parent / "logs"

ORDER_USDT = 10.0
TAKER_FEE = 0.0008  # tradeFee листинговых пар; списывается в USDT (commissionAsset в myTrades), mxDeduct выключен
MAKER_FEE = 0.0  # TP — maker
MIN_NOTIONAL = 1.0  # минимальная сумма ордера, USDT (30002)
# потолки листингов до проверки 30010 в пробе — цена заморозки по записи
KNOWN_CAPS = {"CHOMPUSDT": 5.0, "MTOUSDT": 0.2}
# None — фильтр выключен; min_fill — сколько % плана должно найтись в asks до лимита (и не меньше 1 USDT);
# конкуренция за asks: compete_ms — объём уровня, ушедший за N мс после прихода нашего BUY, считается доставшимся
# другим (берём минимум уровня за это время); share — какая доля оставшегося достаётся нам
# основной стратегии (MAIN) — ещё активность и свежесть стакана, окно входа и ожидание исполнения BUY
FILTERS = dict(max_spread=1.2, bid07=15, bid15=30, ask07=8, max_exit_slip=1.5, max_tick=0.3, max_stress_loss=3.0,
               buy_aggressive=1.0, min_fill=100, compete_ms=0, share=1.0, min_ups=None, min_best_ups=None,
               max_book_age=None, window_ms=None, buy_rest_ms=0, max_chase=None, chase_ref="ask", mom_ms=0,
               mom_strict=False)
# ТЗ v2.1 (основная стратегия): фильтры исходного ТЗ без entryStressLoss CAP BREAKOUT; окно 8 с от открытия,
# прогрев 500 мс, LIMIT BUY ждёт исполнения до 1200 мс (buyFillTimeoutMs), частичное — остаток отменяется
MAIN_FILTERS = dict(FILTERS, max_stress_loss=None, min_ups=6, min_best_ups=3, max_book_age=500, buy_rest_ms=1200)
MAIN_VARIANTS = {"окно 30 с": dict(window_ms=30_000), "конкуренция 1 с": dict(compete_ms=1000)}
# MAIN_EARLY (клиент 03–04.11, только REPLAY и PAPER): ранний вход на листингах без потолка — сетка комбинаций.
# Окно W с от T0 (официальное время открытия); рост bestAsk ≤ C % от первого действительного ask двустороннего стакана
# (основная опора) или от первой сделки (метка «t», диагностика: первая сделка бывает случайным принтом — EMYA 0,03 при
# рынке 0,055); импульс за M мс: середина выросла и bestBid не упал (метка «s» — строгий: bestBid вырос хотя бы на шаг
# цены, т. е. рост середины не только от расширения спреда); M0 — без проверки; истории на окно нет — импульс не
# подтверждён; depthBid0.7 ≥ D × план и depthBid1.5 ≥ 2D × план; стакан не старше A мс; план S USDT; выход MAIN_V21,
# MAIN_TP_TRAIL или MAIN_TP_TRAIL_NT. Постоянные условия: спред ≤ 3 %, весь план в asks до bestAsk +1 %, проскальзывание
# выхода ≤ 1,5 %, стакан синхронизирован; активность в первые секунды не проверяется. Только данные до момента решения.
MAIN_EARLY_FILTERS = dict(MAIN_FILTERS, max_spread=3.0, ask07=0, min_ups=None, min_best_ups=None)
MAIN_EARLY_GRID = dict(window_s=(1, 2, 3, 5), chase=(20, 40, 60), chase_ref=("ask", "trade"),
                       momentum=((0, False), (100, False), (100, True), (250, False), (250, True), (500, False),
                                 (500, True)),
                       depth_x=(2, 5, 15), book_age=(100, 250, 500), usdt=(2.0, 5.0, 10.0))
MAIN_EARLY_PAPER = dict(window_s=(2, 5), chase=(40,), chase_ref=("ask",),
                        momentum=((0, False), (250, False), (250, True)), depth_x=(5,), book_age=(250,),
                        usdt=(10.0,))  # для PAPER — малая часть сетки
SPLIT_MARGIN = 0.10  # запас к minNotional при делении позиции на TP: цена может уйти, комиссия, округление по qtyStep

# --sweep main_replay: один фактор за раз от MAIN_FILTERS, только профиль MAIN_V21
MAIN_SWEEP = {
    "спред 2 %": dict(max_spread=2.0), "спред 3 %": dict(max_spread=3.0),
    "bid 5× / 10×": dict(bid07=5, bid15=10), "bid 2× / 4×": dict(bid07=2, bid15=4),
    "bid без огр.": dict(bid07=0, bid15=0, max_exit_slip=None), "ask без огр.": dict(ask07=0),
    "активность без огр.": dict(min_ups=None, max_book_age=None),
    "спред 3 % + bid 5×/10×": dict(max_spread=3.0, bid07=5, bid15=10),
    "спред 3 % + bid 5×/10×, окно 30 с": dict(max_spread=3.0, bid07=5, bid15=10, window_ms=30_000),
}
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
         "EARLY": dict(window_ms=10_000, stable_ms=0, min_updates=1),
         "MAIN": dict(window_ms=8_000, stable_ms=0, min_updates=1, warmup_ms=500),
         "MAIN_EARLY": dict(window_ms=5_000, stable_ms=0, min_updates=1)}
PROFILE_MODE = {"FAST": "FAST", "CONFIRMED": "CONFIRMED", "EARLY": "FAST", "MAIN": "FAST",
                "MAIN_EARLY": "FAST"}  # чей CAP_DYNAMIC у режима
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
    time_stops_after_tp1: bool = True  # False — после фактического исполнения TP1 временные стопы остатка выключены
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
# основная стратегия (ТЗ v2.1, контрольный BASELINE): стопы исходного ТЗ (как LEGACY_STOPS), FAST_WINDOW_DUMP —
# по exitVWAP за 1 с (п. 4.9), DEPTH_DISAPPEARED — только когда exitVWAP не считается (п. 4.2); комиссии — фактические
MAIN_V21 = replace(LEGACY, name="MAIN_V21", window_dump=-1e9, bid_vwap_drop=-6.0)
MAIN_NO_TIME = replace(MAIN_V21, name="MAIN_NO_TIME_STOPS", time_stops=False)
# MAIN_EARLY, альтернативный выход (клиент 03.11): TP1 +2 % на половину, остаток — trailing 5 % от пика exitVWAP
# остатка и защита общего netPnL +0,3 %, удержание до 30 с; стопы до TP1 — как у MAIN_V21
MAIN_TP_TRAIL = replace(MAIN_V21, name="MAIN_TP_TRAIL", tp=((2, 0.5),), trail_after_tp1=5.0, guard_target=0.3,
                        max_hold_ms=30_000)
# то же, но после исполнения TP1 TIME_STOP_* для остатка выключены (клиент 04.11); HARD_STOP, FAST_WINDOW_DUMP,
# FAST_DROP_STOP, DEPTH_DISAPPEARED, trailing, защита общего netPnL и maxHold действуют
MAIN_TP_TRAIL_NT = replace(MAIN_TP_TRAIL, name="MAIN_TP_TRAIL_NT", time_stops_after_tp1=False)


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


# --- параметры прогона ------------------------------------------------------------

@dataclass
class Opts:
    stops_stressed: bool = False
    depth_band: float = 0.015  # доля от bestBid
    depth_persist_ms: int = 1000
    entry_stress: float = 0.5  # %
    entry_basis: str = "vwap"  # vwap | max
    pre_buy_drop: bool = False
    fee: float = TAKER_FEE  # taker
    fee_in_coin: bool = False  # BUY-комиссия в купленной монете (иначе — в USDT)
    maker_fee: float = MAKER_FEE
    fee_from_info: bool = True  # брать taker/maker инструмента из exchangeInfo (takerCommission/makerCommission)


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
    price_cap: float | None = None  # MAIN на замороженном листинге: BUY не выше потолка (иначе 30010)
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
    rest_until: int = 0  # MAIN: LIMIT BUY без исполнения ждёт до этого времени (buyFillTimeoutMs)
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
        if t > self.release_ms + (self.flt["window_ms"] or cfg["window_ms"]):
            self.finish(t, "SKIP_WINDOW_END")
            return
        if book.updates == self.seen_updates or t < self.release_ms + cfg.get("warmup_ms", 0):
            return
        self.seen_updates = book.updates
        if self.mode != "MAIN_EARLY":  # фильтр падения перед BUY в MAIN_EARLY не участвует, а стоит дорого на сетке
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
        # состояние рынка в момент решения (признаки v2.1 — только в результат, на вход не влияют)
        bb, ba = book.best()
        d_bid, d_ask = book.depth("bids", 0.007), book.depth("asks", 0.007)
        first = book.first_trade
        self.result.update(decision_ms=t, decision_spread_pct=200 * (ba - bb) / (ba + bb), decision_bid07_usdt=d_bid,
                           book_imbalance07=(d_bid - d_ask) / max(d_bid + d_ask, 1e-9),
                           rise_from_first_pct=100 * (ba / first - 1) if first else None,
                           rise_from_first_ask_pct=100 * (ba / book.first_ask - 1) if book.first_ask else None,
                           ups_at_decision=book.activity(t)[0])
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
        if f["max_chase"] is not None:  # рост от первой сделки листинга или от первого действительного ask
            ref = book.first_trade if f["chase_ref"] == "trade" else book.first_ask
            if not ref:
                return False, "no_first_price", None
            if ba > ref * (1 + f["max_chase"] / 100):
                return False, "chase", None
        if f["mom_ms"]:  # импульс: середина выросла, bestBid не упал (строгий: вырос на шаг цены) за mom_ms
            past = book.px_ago(t, f["mom_ms"])
            if past is None:
                return False, "momentum_not_ready", None
            bid_ok = bb >= past[1] + self.step * 0.999 if f["mom_strict"] else bb >= past[1]
            if not (bb + ba > past[1] + past[2] and bid_ok):
                return False, "no_momentum", None
        if f["max_spread"] is not None and 100 * (ba - bb) / mid > f["max_spread"]:
            return False, "spread", None
        bids, asks = book.sorted_bids(), book.sorted_asks()
        if book.depth("bids", 0.007) < usdt * f["bid07"]:
            return False, "depthBid0.7", None
        if book.depth("bids", 0.015) < usdt * f["bid15"]:
            return False, "depthBid1.5", None
        if f["ask07"] and book.depth("asks", 0.007) < usdt * f["ask07"]:
            return False, "depthAsk0.7", None
        if 100 * self.step / ba >= f["max_tick"]:
            return False, "tick", None
        if f["min_ups"] is not None:
            ups, best_ups = book.activity(t)
            if ups < f["min_ups"] or best_ups < f["min_best_ups"]:
                return False, "book_not_active", None
        if f["max_book_age"] is not None and t - book.last_t > f["max_book_age"]:
            return False, "stale_book", None
        limit = floor_step(ba * (1 + f["buy_aggressive"] / 100), self.step)
        if self.price_cap is not None:  # замороженный листинг: выше потолка биржа не примет (30010)
            limit = min(limit, floor_step(self.price_cap, self.step))
            if limit < ba:
                return False, "price_cap", None
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
            if self.flt["buy_rest_ms"] and self.state != "BUY_RESTING":  # MAIN: LIMIT BUY стоит до buyFillTimeoutMs
                self.state, self.rest_until = "BUY_RESTING", self.buy_at + self.flt["buy_rest_ms"]
                return
            if self.state == "BUY_RESTING":
                return
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
        for price, qty in self.tp_plan(avg):
            queue = sum(q for p, q in book.asks.items() if abs(p - price) < self.step * 0.01)
            self.tps.append(TP(price, qty, qty, queue))
        self.state = "POSITION"
        self.result.update(entry_ms=t, entry_px=avg, buy_usdt=cost, decision_ask=self.decision_ask,
                           slippage_pct=100 * (avg / self.decision_ask - 1), fill_pct=100 * got / self.buy_qty,
                           buy_misses=self.buy_misses)

    def tp_plan(self, avg: float) -> list[tuple[float, float]]:
        """TP профиля по фактической позиции. Если при делении хоть одна часть — TP или остаток под trailing — не
        проходит minNotional с запасом SPLIT_MARGIN (цена, комиссия, qtyStep), ставится один TP первого уровня
        на весь объём (клиент 04.11, решение D2 по ТЗ)."""
        tps, n = self.profile.tp, len(self.profile.tp)
        if not tps:
            return []
        parts, placed = [], 0.0
        for i, (pct, share) in enumerate(tps):
            price = ceil_step(avg * (1 + pct / 100), self.step)
            rest = n > 1 and i == n - 1  # последний из нескольких TP — на весь остаток
            qty = floor_step(self.Q - placed if rest else self.Q * share, self.qty_step)
            parts.append((price, qty))
            placed += qty
        need = MIN_NOTIONAL * (1 + SPLIT_MARGIN)
        left = floor_step(self.Q - placed, self.qty_step)  # остаток вне TP (частичный TP + trailing)
        if all(q * p >= need for p, q in parts) and (left <= 0 or left * avg * (1 - SPLIT_MARGIN) >= need):
            return parts
        whole = floor_step(self.Q, self.qty_step)
        self.result["single_tp"] = True
        return [(parts[0][0], whole)] if whole * parts[0][0] >= MIN_NOTIONAL else []

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
                self.R += fill * tp.price * (1 - self.opts.maker_fee)
                self.fees += fill * tp.price * self.opts.maker_fee
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
        time_stops = p.time_stops and (p.time_stops_after_tp1 or not self.tp1_done)
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
            ("TIME_STOP_NEGATIVE", time_stops and elapsed >= p.time_neg[0] and basis < p.time_neg[1]),
            ("TIME_STOP_NO_EDGE", time_stops and elapsed >= p.no_edge[0]
             and basis < p.no_edge[1] + (spread if p.no_edge_plus_spread else 0)),
            ("TIME_STOP_NO_MOMENTUM", time_stops and elapsed >= p.no_momentum_ms and not self.tp1_done
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
        if self.state == "BUY_RESTING" and self.rest_until <= t:
            self.buy_misses += 1
            self.finish(self.rest_until, "BUY_NOT_FILLED_FAST")
        if self.state == "BUY_PENDING" and self.fill_at <= t:
            self.fill_buy(self.buy_at, book)  # позиция — с момента прихода ордера
        while self.state == "EXITING" and self.exit_next <= t:
            self.exit_step(self.exit_next, book)

    def after_event(self, t: int, kind: str, book: Book, deal=None) -> None:
        if kind == "deal":
            self.on_deal(t, *deal)
        if self.state == "BUY_PENDING" and kind != "deal":
            self.compete(book)
        elif self.state == "BUY_RESTING" and kind != "deal":  # новые asks до лимита сразу встречают наш BUY
            self.avail = {p: q for p, q in book.sorted_asks() if p <= self.buy_limit}
            if self.avail:
                self.fill_buy(t, book)
        elif self.state == "SCAN" and kind != "deal":
            self.check_entry(t, book)
        elif self.state == "POSITION":
            self.check_exit(t, book)


# --- ветки и движок -------------------------------------------------------------

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


def make_main_sims(cfg, opts: Opts, open_ms: int, step: float, qty_step: float, band_up: float,
                   price_cap: float | None) -> list[Sim]:
    """Ветки основной стратегии: base и MAIN_VARIANTS × профили MAIN × задержка × сумма входа; окно — от открытия."""
    sims = []
    sweep = list(MAIN_SWEEP.items()) if getattr(cfg, "sweep", False) else []
    for label, over in [("base", {})] + list(MAIN_VARIANTS.items()) + sweep:
        flt = dict(MAIN_FILTERS, **over)
        profiles = (MAIN_V21,) if label in MAIN_SWEEP else (MAIN_V21, MAIN_NO_TIME)
        sims += [Sim("MAIN", label, flt, usdt, prof, lat, opts, open_ms, step, qty_step, band_up, price_cap)
                 for prof in profiles for lat in cfg.latency for usdt in cfg.usdt]
    grid = getattr(cfg, "main_early", None)  # MAIN_EARLY_GRID / MAIN_EARLY_PAPER или None
    if grid and price_cap is None:  # MAIN_EARLY — только листинги без потолка
        for w, c, ref, (m, strict), d, age in product(grid["window_s"], grid["chase"], grid["chase_ref"],
                                                       grid["momentum"], grid["depth_x"], grid["book_age"]):
            flt = dict(MAIN_EARLY_FILTERS, window_ms=w * 1000, max_chase=c, chase_ref=ref, mom_ms=m, mom_strict=strict,
                       bid07=d, bid15=2 * d, max_book_age=age)
            label = f"W{w} C{c}{'t' if ref == 'trade' else ''} M{m}{'s' if strict else ''} D{d} A{age}"
            sims += [Sim("MAIN_EARLY", label, flt, usdt, prof, lat, opts, open_ms, step, qty_step, band_up, price_cap)
                     for prof in (MAIN_V21, MAIN_TP_TRAIL, MAIN_TP_TRAIL_NT) for lat in cfg.latency
                     for usdt in grid["usdt"]]
    return sims


def no_entry_reason(r: dict) -> str:
    """Чаще всего отказавший фильтр, а если отказов не было — причина конца ветки."""
    skips = r.get("skips") or {}
    return max(skips, key=skips.get) if skips else r.get("reason", "?")


def with_fees(opts: Opts, info: dict) -> Opts:
    """Фактические taker/maker инструмента из exchangeInfo (takerCommission/makerCommission), если opts это велит."""
    if not opts.fee_from_info or info.get("takerCommission") in (None, ""):
        return opts
    return replace(opts, fee=float(info["takerCommission"]), maker_fee=float(info.get("makerCommission") or 0))


def market_params(info: dict) -> tuple[float, float, float, str]:
    """Из exchangeInfo пары: шаг цены, шаг количества, bidMultiplierUp полосы, зоны."""
    step = 10 ** -int(info.get("quotePrecision") or 8)
    qty_step = 10 ** -int(info.get("baseAssetPrecision") or 8)
    band_up = next((float(f["bidMultiplierUp"]) for f in info.get("filters") or []
                    if f.get("filterType") == "PERCENT_PRICE_BY_SIDE"), 0.2)
    return step, qty_step, band_up, ", ".join(info.get("conceptPlates") or [])


class CapEngine:
    """CAP BREAKOUT по одному листингу — общий движок REPLAY и PAPER.

    События (t, kind, data): snap — REST-снимок стакана, diff — aggre.depth, deal — (цена, кол-во, сторона).
    Снятие потолка: первая сделка выше потолка позже T+60 с (cand), затем bid локального стакана выше потолка.
    В момент снятия создаются ветки (make_sims); дальше каждое событие проходит через все ветки."""

    def __init__(self, cfg, opts: Opts, cap: float, open_ms: int, info: dict, cand_ms: int | None = None):
        self.cfg, self.opts, self.cap, self.open_ms = cfg, opts, cap, open_ms
        self.info = info
        self.cand_ms = cand_ms  # REPLAY знает его заранее; PAPER находит по потоку
        self.book = Book()
        self.release: int | None = None
        self.sims: list[Sim] = []
        self.last_t = 0

    def on_event(self, t: int, kind: str, data) -> None:
        self.last_t = t
        book = self.book
        for s in self.sims:
            if s.state != "DONE":
                s.before_event(t, book)
        before = book.updates
        if kind == "snap":
            book.snapshot(data)
        elif kind == "diff":
            book.diff(data)
        book.stamp(t, before)
        if self.release is None:
            self.try_start(t, kind, data)
        for s in self.sims:
            if s.state != "DONE":
                s.after_event(t, kind, book, data if kind == "deal" else None)
            if s.entry_ms and kind != "deal" and book.valid and len(s.marks) < len(HOLD_MARKS_S):
                s.mark(t, book)

    def try_start(self, t: int, kind: str, data) -> None:
        """Снятие потолка: первая сделка выше него позже T+60 с, затем bid локального стакана выше потолка."""
        book = self.book
        if kind == "deal" and self.cand_ms is None and t > self.open_ms + 60_000 and data[0] > self.cap * 1.000001:
            self.cand_ms = t
        if self.cand_ms is not None and t >= self.cand_ms and book.valid and book.best()[0] > self.cap * 1.000001:
            self.release = t
            step, qty_step, band_up, _ = market_params(self.info)
            self.sims = make_sims(self.cfg, with_fees(self.opts, self.info), t, step, qty_step, band_up)

    @property
    def done(self) -> bool:
        """Все ветки завершены и отметки «если держать» сняты."""
        return self.release is not None and all(
            s.state == "DONE" and (not s.entry_ms or len(s.marks) == len(HOLD_MARKS_S)) for s in self.sims)

    def finish(self, t_end: int | None = None) -> None:
        """Поток кончился раньше, чем ветка завершилась: выйти по последнему стакану."""
        t_end = t_end or self.last_t
        for s in self.sims:
            if s.state == "POSITION":
                s.start_exit(t_end, "END_OF_DATA", "", 0.0)
            while s.state == "EXITING":
                s.exit_step(t_end, self.book)
            if s.state != "DONE":
                s.finish(t_end, "END_OF_DATA")

    def rows(self, symbol: str) -> list[dict]:
        out = []
        for s in self.sims:
            r = s.result
            if s.entry_ms:
                r["hold_marks"] = {sec: s.marks.get(sec) for sec in HOLD_MARKS_S}
            out.append(dict(symbol=symbol, mode=s.mode, variant=s.variant, usdt=s.usdt, profile=s.profile.name,
                            latency=s.latency, **r))
        return out


class MainEngine(CapEngine):
    """Основная стратегия (BASELINE и MAIN_EARLY) по одному листингу. Временная модель одна для REPLAY и PAPER:
    T0 — официальное время открытия (firstOpenTime из exchangeInfo); если его нет — первое из «первый действительный
    двусторонний стакан» и «первая сделка». Ветки стартуют с T0, как только стакан действителен; окно входа и прогрев
    считаются от T0. Отдельно сохраняются first_book_ms, first_trade_ms и first_exec — первый момент, когда BUY плана
    по bestAsk +1 % (не выше полосы биржи) исполним целиком по asks. price_cap — потолок замороженного листинга."""

    EXEC_USDT = (2.0, 5.0, 10.0)
    PRE_MS = 2000  # часы сервера и биржи расходятся на сотни мс: события за 2 с до T0 ещё относятся к открытию

    def __init__(self, cfg, opts: Opts, open_ms: int | None, info: dict, price_cap: float | None = None):
        super().__init__(cfg, opts, 0.0, open_ms, info, open_ms)
        self.price_cap = price_cap
        self.first_book_ms: int | None = None
        self.first_trade_ms: int | None = None
        self.first_exec: dict[float, tuple[int, float]] = {}  # план USDT → (время, bestAsk)
        self.last_trade_px: float | None = None

    def exchange_open_ms(self) -> int | None:
        v = (self.info or {}).get("firstOpenTime")
        return int(v) if v else None

    def try_start(self, t: int, kind: str, data) -> None:
        if self.open_ms is not None and t >= self.open_ms and self.book.valid:
            self.release = t
            step, qty_step, band_up, _ = market_params(self.info)
            self.sims = make_main_sims(self.cfg, with_fees(self.opts, self.info), self.open_ms, step, qty_step,
                                       band_up, self.price_cap)

    def near_open(self, t: int) -> bool:
        return self.open_ms is None or t >= self.open_ms - self.PRE_MS

    def on_event(self, t: int, kind: str, data) -> None:
        book = self.book
        if self.open_ms is None and self.exchange_open_ms():
            self.open_ms = self.cand_ms = self.exchange_open_ms()
        if kind == "deal":
            self.last_trade_px = data[0]
            if book.first_trade is None and self.near_open(t):
                book.first_trade, self.first_trade_ms = data[0], t  # опоры для ограничения роста и отчёта
                if self.open_ms is None:
                    self.open_ms = self.cand_ms = t  # официального времени нет — открытие по первой сделке
        super().on_event(t, kind, data)
        if not book.valid or not self.near_open(t):
            return
        bb, ba = book.best()
        if not ba > bb > 0:
            return
        if book.first_ask is None:
            book.first_ask, self.first_book_ms = ba, t
            if self.open_ms is None:
                self.open_ms = self.cand_ms = t  # или по первому действительному двустороннему стакану
        if len(self.first_exec) < len(self.EXEC_USDT):
            self.track_exec(t, ba)

    def track_exec(self, t: int, ba: float) -> None:
        """Первый момент, когда BUY плана по bestAsk +1 % исполним целиком по asks (без учёта других участников)."""
        step, qty_step, band_up, _ = market_params(self.info)
        limit = floor_step(ba * 1.01, step)
        if self.last_trade_px:
            limit = min(limit, floor_step(self.last_trade_px * (1 + band_up), step))
        for usdt in self.EXEC_USDT:
            if usdt in self.first_exec or limit < ba:
                continue
            qty = floor_step(usdt / limit, qty_step)
            got, _ = vwap(self.book.sorted_asks(), qty, limit, True)
            if qty > 0 and got >= qty * 0.999:
                self.first_exec[usdt] = (t, ba)

    def times(self) -> dict:
        """Отметки времени листинга относительно T0, мс."""
        rel = lambda v: None if v is None or self.open_ms is None else v - self.open_ms  # noqa: E731
        return dict(t0_ms=self.open_ms, exchange_open_ms=self.exchange_open_ms(),
                    first_book_rel_ms=rel(self.first_book_ms), first_ask=self.book.first_ask,
                    first_trade_rel_ms=rel(self.first_trade_ms), first_trade=self.book.first_trade,
                    first_exec={f"{u:g}": dict(rel_ms=rel(v[0]), ask=v[1]) for u, v in self.first_exec.items()})
