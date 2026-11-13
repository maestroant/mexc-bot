"""Офлайн-сценарии реальных проб (решение клиента 11.11, п. 3–4): биржа-заглушка вместо API, ни одного реального
ордера. Каждый сценарий задаёт ответы биржи на подачу ордеров и их статусы; проверяются позиция, продажи только
купленного, отсутствие конфликтующих SELL, блокировка проб и журнал.
  python tools/probe_scenarios.py            # все сценарии, итог PASS/FAIL"""

import json
import subprocess
import sys
import tempfile
import threading
import time
from argparse import Namespace
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probe_listing as pl  # noqa: E402
import probe_risk as risk  # noqa: E402
import probe_supervisor as sup  # noqa: E402

FEE = Decimal("0.0008")  # комиссия MEXC в USDT (myTrades: commissionAsset USDT)
BID, ASK = Decimal("0.99"), Decimal("1.00")


class Clock:
    def __init__(self):
        self.t = 1_800_000_000.0

    def time(self):
        return self.t

    def monotonic(self):
        return self.t

    def perf_counter(self):
        return self.t

    def sleep(self, s):
        self.t += max(s, 0.001)


class Exchange:
    """Пара XUSDT. script — по одному описанию на каждую подачу ордера, по порядку:
    accept: True | "neterr" | код ошибки; final: окончательный статус; frac: доля исполнения;
    stuck: статус NEW навсегда (и после отмены); found: при "neterr" — найден ли ордер по clientOrderId."""

    def __init__(self, script: list[dict], usdt: str = "100", base: str = "0", quote_asset: str = "USDT"):
        self.script = list(script)
        self.bal = {quote_asset: Decimal(usdt), "X": Decimal(base)}
        self.quote_asset = quote_asset
        self.orders: dict[str, dict] = {}
        self.by_cid: dict[str, str] = {}
        self.posts: list[dict] = []
        self.n = 0
        self.fee_asset = "USDT"  # валюта комиссии в myTrades
        self.prices = {"MXUSDT": "2.5"}  # /api/v3/ticker/price
        self.down = False  # REST биржи недоступен целиком
        self.post_code = None  # неясный код ответа на подачу при принятом ордере (проверка сверки по clientOrderId)
        self.hidden_get: set = set()  # clientOrderId, которых GET /order пока не видит (запоздалое появление)
        self.hidden_trades: set = set()  # … и которых пока нет в истории сделок
        self.trades_down = False  # история сделок недоступна

    has_keys = True
    time_offset_ms = 0

    def resp(self, body, http=200, error=None):
        return pl.Resp(http, body, 1.0, error)

    def call(self, method, path, params=None, signed=False):
        params = dict(params or {})
        if self.down:
            return self.resp(None, None, "URLError: connection refused")
        if path == "/api/v3/ticker/price":
            px = self.prices.get(params.get("symbol"))
            return self.resp({"symbol": params.get("symbol"), "price": px}) if px else \
                self.resp({"code": -1121, "msg": "invalid symbol"}, 400)
        if path == "/api/v3/depth":
            if getattr(self, "depth_down", False):
                return self.resp(None, None, "TimeoutError: depth")
            return self.resp({"bids": [[str(BID), "1000"]], "asks": [[str(ASK), "1000"]]})
        if path == "/api/v3/account":
            return self.resp({"balances": [{"asset": k, "free": str(v)} for k, v in self.bal.items()]})
        if path == "/api/v3/exchangeInfo":
            return self.resp({"symbols": [dict(INFO)]})
        if path == "/api/v3/myTrades" and self.trades_down:
            return self.resp(None, None, "TimeoutError: myTrades")
        if path == "/api/v3/myTrades" and "orderId" not in params:  # история сделок пары
            return self.resp([{"orderId": o["orderId"], "clientOrderId": o.get("cid"), "qty": str(o["qty"]),
                               "quoteQty": str(o["quote"]), "commission": "0", "commissionAsset": "USDT"}
                              for o in self.orders.values() if o["qty"] > 0 and o.get("cid") not in self.hidden_trades])
        if path == "/api/v3/myTrades":
            o = self.orders.get(params["orderId"])
            if not o or o["qty"] <= 0:
                return self.resp([])
            fee = o["quote"] * FEE if self.fee_asset == "USDT" else Decimal("0.002")
            return self.resp([{"qty": str(o["qty"]), "quoteQty": str(o["quote"]), "price": str(o["px"]),
                               "commission": str(fee), "commissionAsset": self.fee_asset}])
        if path == "/api/v3/order" and method == "POST":
            self.posts.append(params)
            spec = self.script.pop(0) if self.script else {"accept": 30000}
            acc = spec.get("accept", True)
            if acc not in (True, "neterr"):
                return self.resp({"code": acc, "msg": "rejected"}, 400)
            self.n += 1
            oid = f"o{self.n}"
            side, typ = params["side"], params["type"]
            px = ASK if side == "BUY" else (Decimal(params["price"]) if typ == "LIMIT" and side == "SELL" else BID)
            if side == "BUY":
                want = (Decimal(params["quoteOrderQty"]) / px).quantize(Decimal("0.01")) if "quoteOrderQty" in params \
                    else Decimal(params["quantity"])
            else:
                want = Decimal(params["quantity"])
            qty = (want * Decimal(str(spec.get("frac", 1)))).quantize(Decimal("0.01"))
            if spec.get("open"):  # заявка встала в стакан и не исполнилась; отмена — CANCELED без исполнения
                self.orders[oid] = {"orderId": oid, "side": side, "px": px, "qty": Decimal(0), "quote": Decimal(0),
                                    "stuck": False, "open": True, "final": "NEW", "cid": params["newClientOrderId"]}
                self.by_cid[params["newClientOrderId"]] = oid
                return self.resp({"orderId": oid})
            o = {"orderId": oid, "side": side, "px": px, "qty": qty, "quote": qty * px, "stuck": spec.get("stuck"),
                 "final": spec.get("final", "FILLED" if spec.get("frac", 1) == 1 else "PARTIALLY_CANCELED")}
            if not o["stuck"]:  # исполнение отражается в балансе
                if side == "BUY":
                    self.bal["X"] += qty
                    self.bal[self.quote_asset] -= o["quote"] * (1 + FEE)
                else:
                    self.bal["X"] -= qty
                    self.bal[self.quote_asset] += o["quote"] * (1 - FEE)
            o["cid"] = params["newClientOrderId"]
            self.orders[oid] = o
            self.by_cid[params["newClientOrderId"]] = oid
            if acc == "neterr":
                o["hidden"] = not spec.get("found", True)
                if o["hidden"]:
                    del self.orders[oid]
                return self.resp(None, None, "TimeoutError: timed out")
            if self.post_code:  # ордер принят, но ответ — неясный код
                code, self.post_code = self.post_code, None
                return self.resp({"code": code, "msg": "unknown"}, 400)
            return self.resp({"orderId": oid})
        if path == "/api/v3/order" and method == "GET":
            if params.get("origClientOrderId") in self.hidden_get:
                return self.resp({"code": -2013, "msg": "Order does not exist"}, 400)
            oid = params.get("orderId") or self.by_cid.get(params.get("origClientOrderId"))
            o = self.orders.get(oid)
            if not o:
                return self.resp({"code": -2013, "msg": "Order does not exist"}, 400)
            if o["stuck"] or o.get("open"):
                ex_q = o["qty"] if o.get("open") else Decimal(0)
                return self.resp({"orderId": oid, "status": "PARTIALLY_FILLED" if ex_q > 0 else "NEW",
                                  "executedQty": str(ex_q), "cummulativeQuoteQty": str(ex_q * o["px"])})
            return self.resp({"orderId": oid, "status": o["final"], "executedQty": str(o["qty"]),
                              "cummulativeQuoteQty": str(o["quote"])})
        if path == "/api/v3/order" and method == "DELETE":
            o = self.orders.get(params["orderId"])
            if o and o.get("open"):  # отмена: исполненная часть остаётся
                o.update(open=False, final="PARTIALLY_CANCELED" if o["qty"] > 0 else "CANCELED", quote=o["qty"] * o["px"])
            return self.resp({"orderId": params["orderId"]})
        if path == "/api/v3/openOrders":
            return self.resp([{"orderId": o["orderId"], "clientOrderId": o.get("cid", ""), "status": "NEW"}
                              for o in self.orders.values() if o.get("open") or o.get("stuck")])
        return self.resp({})

    def leftover_order(self, cid: str, side: str = "BUY", executed: str = "0", filled: bool = False) -> str:
        """Ордер, оставшийся после падения прошлого процесса: открытый (executed — уже исполненная часть) или
        исполненный (filled). Баланс уже отражает исполненное."""
        self.n += 1
        oid = f"o{self.n}"
        q = Decimal(executed)
        px = ASK if side == "BUY" else BID
        self.orders[oid] = {"orderId": oid, "side": side, "px": px, "qty": q, "quote": q * px, "stuck": False,
                            "open": not filled, "final": "FILLED" if filled else "NEW", "cid": cid}
        self.by_cid[cid] = oid
        return oid


INFO = {"symbol": "XUSDT", "status": "1", "baseAsset": "X", "quoteAsset": "USDT", "quotePrecision": 4,
        "baseAssetPrecision": 2, "quoteAmountPrecision": "1", "isSpotTradingAllowed": True, "filters": []}


LAST: dict = {}


def make(ex: Exchange, tmp: Path, api_ok=True, info=None) -> pl.Probe:
    LAST.update(ex=ex, tmp=tmp)
    args = Namespace(symbol="XUSDT", market_test_usdt=5.0, market_test_every=0.0, market_test_max_spread=10.0,
                     market_test_far_pct=50.0, real_order=True, usdt=2.0, offset_pct=10.0, cap_test=False,
                     duration=60, max_wait=1, real_order_every=2.0, real_order_every_after=30.0)
    pr = pl.Probe(args, ex, pl.now_ms(), tmp / f"probe_XUSDT_2026{len(list(tmp.glob('probe_*')))}.jsonl")
    pr.risk_path = tmp / "probe_risk.json"
    if not (tmp / "no_watch").exists():
        with risk.locked(pr.risk_path) as st:
            if not st["operator_watch"]:
                st["operator_watch"] = {"since": pl.now_ms(), "until": pl.now_ms() + 3_600_000, "who": "тест"}
    pr.market.info = dict(info or INFO)
    pr.market.bids, pr.market.asks = [[str(BID), "1000"]], [[str(ASK), "1000"]]
    pr.api_ok_mono = pl.time.monotonic() if api_ok else None
    return pr


def state(tmp: Path) -> dict:
    with risk.locked(tmp / "probe_risk.json") as st:
        return json.loads(json.dumps(st))


def sells(ex: Exchange) -> list[dict]:
    return [p for p in ex.posts if p["side"] == "SELL"]


def sold_qty(ex: Exchange) -> Decimal:
    return sum((o["qty"] for o in ex.orders.values() if o["side"] == "SELL" and not o["stuck"]), Decimal(0))


SCENARIOS = {}


def scenario(fn):
    SCENARIOS[fn.__name__] = fn
    return fn


@scenario
def buy_filled_sell_filled(tmp):
    ex = Exchange([{}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    st = state(tmp)
    return (not st["blocked"] and not st["positions"] and len(sells(ex)) == 1 and ex.bal["X"] == 0
            and st["ledger"][-1]["pnl_usdt"] < 0, "BUY и MARKET SELL исполнены, позиция закрыта, PnL с комиссиями")


@scenario
def buy_canceled_zero(tmp):
    ex = Exchange([{"final": "CANCELED", "frac": 0}])
    pr = make(ex, tmp)
    pr.check_market_test()
    st = state(tmp)
    return (not sells(ex) and not st["positions"] and not st["blocked"] and st["ledger"][-1]["bought"] == "0",
            "BUY отменён с executedQty = 0: позиции нет, SELL не ставится")


@scenario
def buy_partial(tmp):
    ex = Exchange([{"frac": 0.5}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    bought = ex.orders["o1"]["qty"]
    return (sells(ex) and Decimal(sells(ex)[0]["quantity"]) == bought and ex.bal["X"] == 0,
            "BUY исполнен частично: продаётся только купленное")


@scenario
def sell_partial_then_fallback(tmp):
    ex = Exchange([{}, {"frac": 0.5}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    st = state(tmp)
    return (len(sells(ex)) == 2 and sells(ex)[1]["type"] == "LIMIT" and ex.bal["X"] == 0 and not st["blocked"],
            "MARKET SELL частично: остаток продан LIMIT по bid −2 %, позиция закрыта")


@scenario
def sell_canceled_zero(tmp):
    ex = Exchange([{}, {"final": "CANCELED", "frac": 0}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    st = state(tmp)
    return (len(sells(ex)) == 2 and ex.bal["X"] == 0 and not st["blocked"],
            "MARKET SELL отменён без исполнения: позиция открыта до LIMIT-выхода, затем закрыта")


@scenario
def sell_rejected_30083(tmp):
    ex = Exchange([{}, {"accept": 30083}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    return (ex.bal["X"] == 0 and sells(ex)[-1]["type"] == "LIMIT", "MARKET SELL не принят (30083): LIMIT-выход")


@scenario
def exit_fails_leftover_blocks(tmp):
    ex = Exchange([{}, {"accept": 30083}] + [{"final": "CANCELED", "frac": 0}] * 3)
    pr = make(ex, tmp)
    pr.check_market_test()
    st = state(tmp)
    return (st["blocked"] and "XUSDT" in st["positions"] and ex.bal["X"] > 0 and len(sells(ex)) == 4,
            "выход не удался: позиция не закрыта, пробные BUY заблокированы, уведомление")


@scenario
def buy_status_unknown(tmp):
    ex = Exchange([{"stuck": True}])
    pr = make(ex, tmp)
    pr.check_market_test()
    st = state(tmp)
    return (st["blocked"] and not sells(ex), "статус BUY неизвестен: отмена, блокировка, продаж нет")


@scenario
def sell_status_unknown_no_conflicting_sell(tmp):
    ex = Exchange([{}, {"stuck": True}])
    pr = make(ex, tmp)
    pr.check_market_test()
    st = state(tmp)
    return (st["blocked"] and len(sells(ex)) == 1, "статус SELL неизвестен: повторных SELL нет, блокировка")


@scenario
def blocked_no_new_buy(tmp):
    risk.block("YUSDT", "тест", path=tmp / "probe_risk.json")
    ex = Exchange([{}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    pr.check_real_order()
    return (not ex.posts, "блокировка: ни MARKET BUY, ни пробного LIMIT BUY")


@scenario
def no_probe_before_open(tmp):
    ex = Exchange([{}, {}])
    pr = make(ex, tmp)
    pr.open_ms = None
    pr.check_real_order()
    pr.check_market_test()
    return (not ex.posts, "до открытия торгов ни пробы доступности API, ни MARKET BUY")


@scenario
def api_not_confirmed(tmp):
    ex = Exchange([{}, {}])
    pr = make(ex, tmp, api_ok=False)
    pr.check_market_test()
    return (not ex.posts and "API" in (pr.states["market_test"].last or ""), "API не подтверждён: BUY не подаётся")


@scenario
def no_quote_balance(tmp):
    ex = Exchange([{}], usdt="0", quote_asset="USD1")
    pr = make(ex, tmp, info={**INFO, "quoteAsset": "USD1", "symbol": "XUSD1"})
    pr.check_market_test()
    pr.check_market_test()
    return (not ex.posts and pr.market_test_done, "нет котируемой валюты (USD1): проба по паре окончена, без 30004")


@scenario
def one_market_attempt(tmp):
    ex = Exchange([{"accept": 30010}] * 5)
    pr = make(ex, tmp)
    for _ in range(6):
        pr.check_market_test()
    st = state(tmp)
    return (len(ex.posts) == 1 and pr.market_test_done and not st["positions"],
            "MARKET_EXECUTION_PROBE: одна попытка BUY на пару, отказ — раунд окончен")


@scenario
def api_probe_tech_rejects_stop(tmp):
    ex = Exchange([{"accept": 10007}] * 10)
    pr = make(ex, tmp, api_ok=False)
    for _ in range(10):
        pr.last_real_attempt = -1e9
        pr.check_real_order()
    return (len(ex.posts) == 3, "проба доступности API: не больше 3 технических отказов (10007) на пару")


@scenario
def no_quote_no_real_orders(tmp):
    ex = Exchange([{}] * 3, usdt="0", quote_asset="USD1")
    pr = make(ex, tmp, info={**INFO, "quoteAsset": "USD1", "symbol": "XUSD1"})
    pr.check_real_order()
    pr.check_cap_maker()
    pr.check_market_test()
    return (not ex.posts, "нет котируемой валюты: ни пробы API, ни LIMIT_MAKER, ни MARKET — без 30004")


@scenario
def risk_budget_daily(tmp):
    with risk.locked(tmp / "probe_risk.json") as st:
        st["ledger"].append({"ts": pl.now_ms(), "symbol": "YUSDT", "kind": "MARKET_EXECUTION_PROBE", "pnl_usdt": -0.6})
    ex = Exchange([{}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    return (not ex.posts and "риск-бюджет" in (pr.states["market_test"].last or ""),
            "убыток за сутки 0,6 + неблагоприятный сценарий 0,5 > 1 USDT: проба не начинается")


@scenario
def risk_budget_rolling_week(tmp):
    with risk.locked(tmp / "probe_risk.json") as st:
        st["ledger"].append({"ts": pl.now_ms() - 3 * 86_400_000, "symbol": "YUSDT", "pnl_usdt": -2.6})
        st["ledger"].append({"ts": pl.now_ms() - 8 * 86_400_000, "symbol": "ZUSDT", "pnl_usdt": -2.9})
    ex = Exchange([{}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    blocked_now = not ex.posts
    with risk.locked(tmp / "probe_risk.json") as st:
        st["ledger"] = [r for r in st["ledger"] if r["symbol"] != "YUSDT"]  # остался только убыток 8 суток назад
    pr2 = make(ex, tmp)
    pr2.check_market_test()
    return (blocked_now and len(ex.posts) == 2, "7 × 24 ч скользящие: убыток 3 дня назад считается, 8 дней назад — нет")


@scenario
def corrupt_state_blocks(tmp):
    (tmp / "probe_risk.json").write_text("{не json", encoding="utf-8")
    ex = Exchange([{}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    st = state(tmp)
    return (not ex.posts and st["blocked"] and list(tmp.glob("probe_risk.corrupt*")),
            "состояние нечитаемо: аварийная блокировка, файл сохранён, ордеров нет")


@scenario
def restart_reconcile_sells_position(tmp):
    risk.open_position("XUSDT", {"stage": "OPEN_UNRESOLVED", "left_qty": "5", "usdt": "5"}, path=tmp / "probe_risk.json")
    risk.block("XUSDT", "непроданный остаток", path=tmp / "probe_risk.json")
    ex = Exchange([{}], base="5")
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    st = state(tmp)
    return (ex.bal["X"] == 0 and not st["positions"] and st["blocked"],
            "после перезапуска незакрытая позиция продаётся при сверке, блокировка остаётся до оператора")


@scenario
def crash_before_ack_found_filled(tmp):
    """crash-before-ack: резерв и регистрация есть, ответа биржи процесс не дождался; ордер исполнен."""
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.register_order("market1", path=rp, symbol="XUSDT", side="BUY", round_id="R1", status="SENDING")
    ex = Exchange([{}], base="5")
    ex.leftover_order("market1", "BUY", executed="5", filled=True)
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    st = state(tmp)
    return (ex.bal["X"] == 0 and not st["positions"] and len(st["ledger"]) == 1,
            "crash-before-ack: BUY найден по clientOrderId (исполнен) → позиция подтверждена и продана, раунд закрыт")


@scenario
def crash_before_ack_not_placed(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.register_order("market1", path=rp, symbol="XUSDT", side="BUY", round_id="R1", status="SENDING")
    ex = Exchange([])
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    first = state(tmp)
    pl.time.sleep(40)
    pr.reconcile(force=True)
    second = state(tmp)
    pl.time.sleep(35)
    pr.reconcile(force=True)
    st = state(tmp)
    return (first["positions"]["XUSDT"]["stage"] == "RECONCILIATION_REQUIRED" and first["blocked"]
            and "XUSDT" in second["positions"] and not st["positions"] and not ex.posts
            and st["ledger"][-1]["note"].startswith("NO_ORDER_CONFIRMED"),
            "crash-before-ack: один «не найден» — RECONCILIATION_REQUIRED и блокировка; через 40 с ещё рано; "
            "через 75 с (3 отрицательные сверки ордера и сделок) — NO_ORDER_CONFIRMED")


@scenario
def crash_before_ack_late_appearance(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.register_order("market1", path=rp, symbol="XUSDT", side="BUY", round_id="R1", status="SENDING")
    ex = Exchange([{}], base="5")
    ex.leftover_order("market1", "BUY", executed="5", filled=True)
    ex.hidden_get.add("market1")
    ex.hidden_trades.add("market1")
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    early = state(tmp)
    pl.time.sleep(30)
    ex.hidden_get.clear()
    ex.hidden_trades.clear()  # ордер и сделка появились в истории позже
    pr.reconcile(force=True)
    st = state(tmp)
    return (early["positions"]["XUSDT"]["stage"] == "RECONCILIATION_REQUIRED" and ex.bal["X"] == 0
            and not st["positions"] and st["blocked"],
            "ордер появился в истории позже: сначала RECONCILIATION_REQUIRED, затем найден, позиция продана, "
            "аварийная блокировка остаётся до оператора")


@scenario
def fill_without_create_confirmation(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.register_order("market1", path=rp, symbol="XUSDT", side="BUY", round_id="R1", status="SENDING")
    ex = Exchange([{}], base="5")
    ex.leftover_order("market1", "BUY", executed="5", filled=True)
    ex.hidden_get.add("market1")  # создание не подтверждается, но сделка в истории есть
    pr = make(ex, tmp)
    pr.find_buy("market1", pl.now_ms())  # прямой вызов: найдено по сделкам
    pr.reconcile(force=True)
    st = state(tmp)
    return (ex.bal["X"] == 0 and not st["positions"],
            "исполнение без подтверждения создания: ордер найден по истории сделок (clientOrderId), продано")


@scenario
def incomplete_trade_history(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.register_order("market1", path=rp, symbol="XUSDT", side="BUY", round_id="R1", status="SENDING")
    ex = Exchange([])
    ex.trades_down = True
    pr = make(ex, tmp)
    for _ in range(4):
        pr.reconcile(force=True)
        pl.time.sleep(40)
    st = state(tmp)
    return ("XUSDT" in st["positions"] and st["blocked"] and not st["ledger"],
            "история сделок недоступна: «ордера нет» не подтверждается никогда, раунд не закрыт, блокировка")


@scenario
def hard_kill_between_reserve_and_send(tmp):
    rp = tmp / "probe_risk.json"
    code = ("import os, sys; sys.path.insert(0, %r); import probe_risk as r; from pathlib import Path; "
            "p = Path(%r); r.reserve('XUSDT', 5.0, 'MARKET_EXECUTION_PROBE', 'R1', path=p); "
            "r.register_order('market1', path=p, symbol='XUSDT', side='BUY', round_id='R1', status='SENDING'); "
            "os._exit(9)") % (str(Path(pl.__file__).parent), str(rp))
    rc = subprocess.run([sys.executable, "-c", code], capture_output=True).returncode
    ex = Exchange([])
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    st = state(tmp)
    return (rc == 9 and st["positions"]["XUSDT"]["stage"] == "RECONCILIATION_REQUIRED" and st["blocked"]
            and not ex.posts,
            "процесс убит (os._exit) после резерва и регистрации, до отправки: состояние цело, резерв не освобождён, "
            "RECONCILIATION_REQUIRED и блокировка")


@scenario
def market_requires_operator_watch(tmp):
    (tmp / "no_watch").write_text("1")
    ex = Exchange([{}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    no_watch = not ex.posts
    risk.watch(1, "оператор", path=tmp / "probe_risk.json")
    pr.last_market_attempt = -1e9
    pr.check_market_test()
    return (no_watch and len(ex.posts) == 2, "C-04: без подтверждённого наблюдения оператора MARKET не начинается, "
            "с наблюдением — раунд проходит")


@scenario
def operator_watch_expired(tmp):
    (tmp / "no_watch").write_text("1")
    risk.watch(0.01, "оператор", path=tmp / "probe_risk.json")
    pl.time.sleep(60)
    ex = Exchange([{}, {}])
    pr = make(ex, tmp)
    pr.check_market_test()
    return (not ex.posts and "истекло" in (pr.states["market_test"].last or ""),
            "наблюдение истекло: MARKET не начинается")


@scenario
def supervisor_resumes_orphan_position(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.update_position("XUSDT", path=rp, stage="OPEN_UNRESOLVED", qty="5", left_qty="5")
    risk.block("XUSDT", "непроданный остаток", path=rp)
    ex = Exchange([{}], base="5")
    LAST.update(ex=ex, tmp=tmp)
    out = sup.run_once(ex, rp)
    st = state(tmp)
    return (ex.bal["X"] == 0 and not st["positions"] and st["blocked"] and st["supervisor"].get("last_ok")
            and out["checked"],
            "процесс пробы завершился с незакрытой позицией: супервизор продал остаток, пульс записан, "
            "аварийная блокировка ждёт оператора")


@scenario
def supervisor_skips_live_symbol(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.update_position("XUSDT", path=rp, stage="POSITION", qty="5", left_qty="5")
    ex = Exchange([{}], base="5")
    LAST.update(ex=ex, tmp=tmp)
    with risk.symbol_lock("XUSDT", root=tmp) as mine:
        out = sup.run_once(ex, rp)
    return (mine and out["skipped_live"] == ["XUSDT"] and not ex.posts,
            "пару ведёт живой процесс пробы: супервизор её не трогает (нет конфликтующих SELL)")


@scenario
def supervisor_api_down(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.update_position("XUSDT", path=rp, stage="POSITION", qty="5", left_qty="5")
    ex = Exchange([{}], base="5")
    ex.down = True
    LAST.update(ex=ex, tmp=tmp)
    out = sup.run_once(ex, rp)
    pl.time.sleep(16 * 60)
    stale = risk.supervisor_stale(path=rp)
    st = state(tmp)
    return (out["errors"] and st["supervisor"]["errors"] == 1 and stale and "XUSDT" in st["positions"] and not ex.posts,
            "API недоступен при восстановлении: ошибка в пульсе, позиция сохранена, через 16 мин — тревога о "
            "неработающем супервизоре")


def late_setup(tmp, status="NO_ORDER_CONFIRMED", age_ms=0):
    rp = tmp / "probe_risk.json"
    risk.register_order("market_late1", path=rp, symbol="XUSDT", check="market_buy", side="BUY", type="MARKET",
                        round_id="R1", status=status)
    if age_ms:
        with risk.locked(rp) as st:
            st["orders"]["market_late1"]["ts"] -= age_ms
    ex = Exchange([{}])
    LAST.update(ex=ex, tmp=tmp)
    return rp, ex


@scenario
def late_appearance_after_no_order(tmp):
    rp, ex = late_setup(tmp)
    out1 = sup.run_once(ex, rp)  # первая поздняя сверка: ордера по-прежнему нет
    ex.leftover_order("market_late1", filled=True, executed="5")  # исполнение появилось позже
    ex.bal["X"] += Decimal("5")
    pl.time.sleep(31 * 60)
    out2 = sup.run_once(ex, rp)
    st = state(tmp)
    o = st["orders"]["market_late1"]
    return (out1["late"] == [("market_late1", "absent")] and out2["late"] == [("market_late1", "LATE_APPEARANCE")]
            and o["status"] == "LATE_APPEARANCE" and o["late_was"] == "NO_ORDER_CONFIRMED" and st["blocked"]
            and not ex.posts,
            "после NO_ORDER_CONFIRMED исполнение появилось позже: пассивная сверка — LATE_APPEARANCE, блокировка, "
            "повторная классификация; ни одного ордера")


@scenario
def late_balance_growth_blocks(tmp):
    rp, ex = late_setup(tmp, status="NOT_PLACED_RECONCILED")
    sup.run_once(ex, rp)
    ex.bal["X"] += Decimal("3")  # ордера и сделок не видно, но остаток вырос без пробной позиции
    pl.time.sleep(31 * 60)
    out = sup.run_once(ex, rp)
    st = state(tmp)
    return (bool(out["late"]) and out["late"][-1][1] == "LATE_APPEARANCE" and st["blocked"] and not ex.posts,
            "остаток базовой валюты вырос после «ордера нет» без пробной позиции — LATE_APPEARANCE и блокировка")


@scenario
def late_absent_no_block_and_window(tmp):
    rp, ex = late_setup(tmp)
    a = sup.run_once(ex, rp)["late"]
    b = sup.run_once(ex, rp)["late"]  # не чаще раза в 30 мин
    pl.time.sleep(25 * 3600)  # окно 24 ч прошло — сверка прекращается
    c = sup.run_once(ex, rp)["late"]
    st = state(tmp)
    return (a == [("market_late1", "absent")] and b == [] and c == [] and not st["blocked"]
            and st["orders"]["market_late1"]["late_checks"] == 1,
            "ордера по-прежнему нет: без блокировки; сверка не чаще 30 мин и только 24 ч после регистрации")


@scenario
def late_watch_skips_live_symbol(tmp):
    rp, ex = late_setup(tmp)
    with risk.symbol_lock("XUSDT", root=tmp) as mine:
        out = sup.run_once(ex, rp)
    return (mine and out["late"] == [] and "XUSDT" in out["skipped_live"],
            "пару ведёт живой процесс пробы — поздняя сверка её пропускает")


@scenario
def fee_revaluation_keeps_original(tmp):
    ex = Exchange([{}, {}])
    ex.fee_asset = "MX"
    ex.prices = {}
    pr = make(ex, tmp)
    pr.check_market_test()
    rid = state(tmp)["ledger"][-1]["round_id"]
    v = risk.revalue_fee(rid, "MX", 2.5, "ручная котировка MXUSDT на 12.11 16:00 UTC", "котировка была недоступна",
                         path=tmp / "probe_risk.json")
    r = state(tmp)["ledger"][-1]
    return (v["previous"]["pnl_status"] == "PNL_INCOMPLETE/UNVALUED_FEE" and r["pnl_status"].startswith("NET_PNL")
            and r["revaluations"] and r["fee_reserve_usdt"] == 0.0,
            "дооценка комиссии: исходная версия расчёта, источник и причина сохранены, резерв снят")


@scenario
def crash_before_register(tmp):
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=tmp / "probe_risk.json")
    ex = Exchange([], base="5")
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    st = state(tmp)
    return (not sells(ex) and ex.bal["X"] == 5 and not st["positions"],
            "резерв без регистрации ордера (ордер не отправлялся): позиция снята, чужие монеты не продаются")


@scenario
def crash_after_buy(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.update_position("XUSDT", path=rp, stage="POSITION", qty="5", left_qty="5")
    ex = Exchange([{}], base="5")
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    st = state(tmp)
    return (ex.bal["X"] == 0 and not st["positions"],
            "crash-after-buy: подтверждённое количество продано сверкой, раунд закрыт")


@scenario
def restart_during_exit(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.update_position("XUSDT", path=rp, stage="POSITION", qty="5", left_qty="5")
    risk.register_order("fallba1", path=rp, symbol="XUSDT", side="SELL", round_id="R1", status="ACCEPTED")
    ex = Exchange([{}], base="3")  # 2 из 5 уже проданы открытым LIMIT SELL до падения
    ex.leftover_order("fallba1", "SELL", executed="2")
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    st = state(tmp)
    s_ = sells(ex)
    return (len(s_) == 1 and Decimal(s_[0]["quantity"]) == 3 and ex.bal["X"] == 0 and not st["positions"],
            "restart-during-exit: свой открытый SELL отменён с подтверждением, учтены 2 проданных, допродано 3")


@scenario
def reconcile_idempotent(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.update_position("XUSDT", path=rp, stage="POSITION", qty="5", left_qty="5")
    ex = Exchange([{}, {}], base="5")
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    pr.reconcile(force=True)
    risk.close_position("XUSDT", {"round_id": "R1", "pnl_usdt": 0.0}, path=rp)  # повтор закрытия того же раунда
    st = state(tmp)
    return (len(st["ledger"]) == 1 and len(sells(ex)) == 1, "повторная сверка и повторное закрытие не дублируют журнал")


@scenario
def foreign_order_untouched(tmp):
    ex = Exchange([])
    ex.leftover_order("web_manual_123")
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    st = state(tmp)
    return (ex.orders["o1"]["open"] and not st["blocked"], "чужой ордер (не пробы) не отменяется")


@scenario
def unregistered_probe_like_order(tmp):
    ex = Exchange([])
    ex.leftover_order("market1800000000000")
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    st = state(tmp)
    return (ex.orders["o1"]["open"] and st["blocked"],
            "ордер с префиксом пробы, но без регистрации: принадлежность неясна — не отменён, блокировка")


@scenario
def owned_open_order_cancelled(tmp):
    risk.register_order("market18", path=tmp / "probe_risk.json", symbol="XUSDT", side="BUY", status="ACCEPTED")
    ex = Exchange([])
    ex.leftover_order("market18")
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    st = state(tmp)
    return (ex.orders["o1"]["final"] == "CANCELED" and not st["blocked"],
            "свой открытый ордер без исполнения: отменён с подтверждением, без блокировки")


@scenario
def parallel_probe(tmp):
    rp = tmp / "probe_risk.json"
    results, barrier = [], threading.Barrier(8)

    def go(sym):
        barrier.wait()
        results.append(risk.reserve(sym, 5.0, "MARKET_EXECUTION_PROBE", sym, path=rp))
    threads = [threading.Thread(target=go, args=(f"S{i}USDT",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    st = state(tmp)
    return (sum(r is None for r in results) == 1 and len(st["positions"]) == 1,
            "parallel-probe: 8 одновременных кандидатов — резерв получил ровно один (атомарно под блокировкой файла)")


@scenario
def duplicate_client_order_id(tmp):
    pl.time.t = 1_800_000_000.0  # все в одну миллисекунду
    cids = {pl.new_cid("market_buy") for _ in range(100)}
    ex = Exchange([{}, {}])
    ex.post_code = 700099  # ордер принят, ответ — неясный код
    pr = make(ex, tmp)
    pr.check_market_test()
    st = state(tmp)
    return (len(cids) == 100 and ex.bal["X"] == 0 and not st["blocked"],
            "duplicate-clientOrderId: 100 идентификаторов в одну мс уникальны; неясный ответ — сверка по clientOrderId")


@scenario
def exchange_rest_unavailable(tmp):
    rp = tmp / "probe_risk.json"
    risk.reserve("XUSDT", 5.0, "MARKET_EXECUTION_PROBE", "R1", path=rp)
    risk.update_position("XUSDT", path=rp, stage="POSITION", qty="5", left_qty="5")
    ex = Exchange([{}, {}], base="5")
    ex.down = True
    pr = make(ex, tmp)
    pr.reconcile(force=True)
    pr2 = make(Exchange([{}], usdt="100"), tmp)
    pr2.symbol = "YUSDT"
    pr2.args.symbol = "YUSDT"
    pr2.client.down = True
    pr2.check_market_test()
    st = state(tmp)
    return (not ex.posts and not pr2.client.posts and "XUSDT" in st["positions"],
            "exchange-REST-unavailable: сверка отложена, ни продаж, ни покупок; позиция сохранена")


@scenario
def api_probe_stops_after_confirmed(tmp):
    ex = Exchange([{"open": True}] * 10)
    pr = make(ex, tmp, api_ok=False)
    for _ in range(10):
        pr.last_real_attempt = -1e9
        pr.check_real_order()
    return (len(ex.posts) == 1 and ex.orders["o1"]["final"] == "CANCELED" and pr.api_ok_mono is not None,
            "проба API: принятая заявка отменена с подтверждением — API подтверждён, новых подач нет (3 — предел, "
            "не обязанность)")


@scenario
def api_probe_three_rejects_max(tmp):
    ex = Exchange([{"accept": 10007}] * 10)
    pr = make(ex, tmp, api_ok=False)
    for _ in range(10):
        pr.last_real_attempt = -1e9
        pr.check_real_order()
    return (len(ex.posts) == 3, "проба API: при отказах 10007 — не больше 3 подач на пару")


@scenario
def api_probe_30004_stops(tmp):
    ex = Exchange([{"accept": 30004}] * 5)
    pr = make(ex, tmp, api_ok=False)
    pr.check_real_order()
    ex.bal["USDT"] = Decimal("1")  # недостаток баланса подтверждается повторной проверкой
    for _ in range(4):
        pr.last_real_attempt = -1e9
        pr.check_real_order()
    return (len(ex.posts) == 1, "30004 при подтверждённом недостатке баланса: дальнейших подач по паре нет")


def cap_probe(ex: Exchange, tmp: Path, listed: bool = True) -> pl.Probe:
    pr = make(ex, tmp)
    pr.args.cap_test_pct = 10.0
    pr.self_listed = listed
    return pr


def cap_round(pr: pl.Probe, n: int, rise: Decimal = Decimal(0)) -> None:
    """n слотов CAP-пробы; rise — рост цены перед каждым слотом (доля)."""
    for _ in range(n):
        if rise:
            px = Decimal(pr.market.bids[0][0]) * (1 + rise)
            pr.market.bids = [[str(px.quantize(Decimal("0.0001"))), "1000"]]
        pr.check_cap_maker()


@scenario
def cap_g1_waits_self_symbols(tmp):
    ex = Exchange([{"open": True}] * 3)
    pr = cap_probe(ex, tmp, listed=False)
    cap_round(pr, 3)
    before = len(ex.posts)
    pr.self_listed = True
    cap_round(pr, 1)
    return (before == 0 and len(ex.posts) == 1, "G1: пока пары нет в selfSymbols — CAP-проба не подаётся; появилась — "
                                                "подача")


@scenario
def cap_g2_skips_uninformative(tmp):
    ex = Exchange([{"open": True}] * 5)
    pr = cap_probe(ex, tmp)
    cap_round(pr, 3)  # цена не меняется: после первой принятой — пропуски
    flat = len(ex.posts)
    cap_round(pr, 1, rise=Decimal("0.2"))  # цена выросла на 20 % — проба информативна
    return (flat == 1 and len(ex.posts) == 2, "G2: проба не выше принятой × 1,1 не подаётся; после роста цены — подаётся")


@scenario
def cap_stop_on_first_30010(tmp):
    ex = Exchange([{"accept": 30010}] + [{"open": True}] * 5)
    pr = cap_probe(ex, tmp)
    cap_round(pr, 5, rise=Decimal("0.2"))
    return (len(ex.posts) == 1 and "CAP_FOUND" in (pr.states["cap_maker"].last or ""),
            "первая 30010 — CAP_FOUND, активные CAP-пробы прекращаются")


@scenario
def cap_three_accepted_stop(tmp):
    ex = Exchange([{"open": True}] * 10)
    pr = cap_probe(ex, tmp)
    cap_round(pr, 10, rise=Decimal("0.2"))
    return (len(ex.posts) == 3 and all(o["final"] == "CANCELED" for o in ex.orders.values())
            and "NO_CAP_OBSERVED_IN_PROBES" in (pr.states["cap_maker"].last or ""),
            "без 30010 — не больше 3 принятых и подтверждённо отменённых LIMIT_MAKER, затем стоп")


@scenario
def cap_six_submits_with_rejects(tmp):
    ex = Exchange([{"accept": 30087}] * 10)  # отказ по ценовому правилу — не технический, не 30010
    pr = cap_probe(ex, tmp)
    cap_round(pr, 10, rise=Decimal("0.2"))
    return (len(ex.posts) == 6, "жёсткий предел — 6 подач CAP-пробы на пару с учётом отказов")


@scenario
def cap_unresolved_blocks_next(tmp):
    ex = Exchange([{"open": True}] * 3)
    pr = cap_probe(ex, tmp)
    risk.register_order("cap_ma_crash1", path=pr.risk_path, symbol="XUSDT", check="cap_maker", side="BUY",
                        type="LIMIT_MAKER", status="SENDING", price="1")  # процесс упал до ответа на подачу
    cap_round(pr, 3, rise=Decimal("0.2"))
    return (not ex.posts and "не сверен" in (pr.states["cap_maker"].last or ""),
            "подача CAP-пробы с неизвестным исходом: следующая — только после сверки")


@scenario
def cap_restart_keeps_counts(tmp):
    ex = Exchange([{"open": True}] * 10)
    pr = cap_probe(ex, tmp)
    cap_round(pr, 3, rise=Decimal("0.2"))
    pr2 = cap_probe(ex, tmp)  # перезапуск процесса пробы: новый объект, тот же реестр
    pr2.market.bids = pr.market.bids
    cap_round(pr2, 5, rise=Decimal("0.2"))
    return (len(ex.posts) == 3, "перезапуск пробы не обнуляет счётчики 3/6: после 3 принятых новых подач нет")


@scenario
def cap_pure_36_when_gates_off(tmp):
    ex = Exchange([{"open": True}] * 10)
    pr = cap_probe(ex, tmp, listed=False)
    old = pl.CAP_GATE_SELF_SYMBOLS, pl.CAP_GATE_STEP
    pl.CAP_GATE_SELF_SYMBOLS, pl.CAP_GATE_STEP = False, None
    try:
        cap_round(pr, 10)
    finally:
        pl.CAP_GATE_SELF_SYMBOLS, pl.CAP_GATE_STEP = old
    return (len(ex.posts) == 3, "G1/G2 выключены (откат): чистое правило 3/6 — 3 принятых при неизменной цене")


@scenario
def api_probe_restart_no_repeat(tmp):
    ex = Exchange([{"open": True}] * 5)
    pr = make(ex, tmp, api_ok=False)
    pr.check_real_order()
    pr2 = make(ex, tmp, api_ok=False)  # перезапуск
    for _ in range(3):
        pr2.last_real_attempt = -1e9
        pr2.check_real_order()
    return (len(ex.posts) == 1 and pr2.api_ok_mono is not None,
            "перезапуск: подтверждённая (принята и отменена) проба API не повторяется, подтверждение — из реестра")


@scenario
def third_currency_fee_valued(tmp):
    ex = Exchange([{}, {}])
    ex.fee_asset = "MX"
    pr = make(ex, tmp)
    pr.check_market_test()
    r = state(tmp)["ledger"][-1]
    return (r["pnl_status"] == "NET_PNL_COMPLETE" and Decimal(r["fee_other_usdt"]) == Decimal("0.0100"),
            "комиссия в MX оценена по котировке MXUSDT сразу после исполнения: PnL полный")


@scenario
def third_currency_fee_unvalued(tmp):
    ex = Exchange([{}, {}])
    ex.fee_asset = "MX"
    ex.prices = {}
    pr = make(ex, tmp)
    pr.check_market_test()
    r = state(tmp)["ledger"][-1]
    return (r["pnl_status"] == "PNL_INCOMPLETE/UNVALUED_FEE" and r["unvalued_fee"] and
            r["pnl_usdt"] < r["pnl_net_known_usdt"],
            "котировки MX нет: PNL_INCOMPLETE/UNVALUED_FEE, в бюджет — консервативный резерв")


@scenario
def limit_breach_recorded(tmp):
    rp = tmp / "probe_risk.json"
    with risk.locked(rp) as st:
        st["ledger"].append({"ts": pl.now_ms(), "symbol": "YUSDT", "pnl_usdt": -0.9, "round_id": "R0"})
    risk.close_position("XUSDT", {"round_id": "R1", "pnl_usdt": -0.3}, path=rp)  # гэп: раунд потерял больше резерва
    st = state(tmp)
    return (len(st["breaches"]) == 1 and st["breaches"][0]["window"] == "сутки UTC" and st["blocked"]
            and "LIMIT_BREACH" in st["blocked"]["reason"],
            "превышение лимита после раунда (гэп) — LIMIT_BREACH: запись хранится, новые входы заблокированы")


def limit_probe(ex: Exchange, tmp: Path, enabled: bool = True) -> pl.Probe:
    pl.LIMIT_RESEARCH_ENABLED = enabled
    pr = make(ex, tmp)
    pr.args.limit_research = True
    pr.args.real_buy_mode = "limit"  # предрегистрировано до T0: на этом листинге реальный BUY — только LIMIT
    return pr


@scenario
def limit_disabled_by_default(tmp):
    ex = Exchange([{}, {}])
    pr = limit_probe(ex, tmp, enabled=False)
    pr.check_limit_research()
    return (not ex.posts, "LIMIT-исследование: реальный режим выключен (NOT ACTIVATED) — ордеров нет даже с флагом")


@scenario
def limit_filled_then_sold(tmp):
    ex = Exchange([{}, {}])
    pr = limit_probe(ex, tmp)
    pr.check_limit_research()
    buy = ex.posts[0]
    st = state(tmp)
    return (buy["type"] == "LIMIT" and Decimal(buy["price"]) == Decimal("1.01") and ex.bal["X"] == 0
            and st["ledger"][-1]["kind"] == "LIMIT_EXECUTION_RESEARCH" and "limit_vs_model" in pr.states,
            "LIMIT BUY по bestAsk + 1 %: исполнен, продано купленное, факт рядом с моделью PAPER")


@scenario
def limit_partial_timeout(tmp):
    ex = Exchange([{"frac": 0.4}, {}])
    pr = limit_probe(ex, tmp)
    pr.check_limit_research()
    bought = ex.orders["o1"]["qty"]
    return (Decimal(sells(ex)[0]["quantity"]) == bought and ex.bal["X"] == 0,
            "LIMIT BUY частично за buyFillTimeoutMs: остаток отменён, продано только купленное")


@scenario
def limit_not_filled(tmp):
    ex = Exchange([{"open": True}])
    pr = limit_probe(ex, tmp)
    pr.check_limit_research()
    st = state(tmp)
    return (ex.orders["o1"]["final"] == "CANCELED" and not sells(ex) and not st["positions"] and not st["blocked"],
            "LIMIT BUY не исполнился: отмена подтверждена, позиции нет, блокировки нет")


@scenario
def limit_only_if_preregistered(tmp):
    ex = Exchange([{}, {}])
    pr = limit_probe(ex, tmp)
    pr.args.real_buy_mode = "market"
    pr.check_limit_research()
    return (not ex.posts, "LIMIT не выполняется, если на листинг предрегистрирован режим MARKET")


@scenario
def one_real_buy_mode_per_pair(tmp):
    with risk.locked(tmp / "probe_risk.json") as st:
        st["ledger"].append({"ts": pl.now_ms(), "symbol": "XUSDT", "kind": "MARKET_EXECUTION_PROBE",
                             "pnl_usdt": 0.0, "round_id": "R0"})
    ex = Exchange([{}, {}])
    pr = limit_probe(ex, tmp)
    pr.check_limit_research()
    return (not ex.posts, "по паре уже был реальный MARKET-раунд: LIMIT-раунд не выполняется (один режим BUY)")


@scenario
def limit_stale_book(tmp):
    ex = Exchange([{}, {}])
    pr = limit_probe(ex, tmp)
    ex.depth_down = True
    pr.book_ts = pl.now_ms() - 5000
    pr.check_limit_research()
    return (not ex.posts, "стакан старше 300 мс: LIMIT BUY не подаётся")


def main() -> None:
    clock = Clock()
    pl.time = clock  # время, sleep и таймауты ожидания — по часам сценария
    risk.time = clock
    pl.notify = lambda *a, **k: None
    fails = 0
    report = []
    for name, fn in SCENARIOS.items():
        pl.LIMIT_RESEARCH_ENABLED = False  # сценарий включает реальный LIMIT-режим только сам, на заглушке
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
            ok, what = fn(Path(d))
            ex, tmp = LAST.get("ex"), LAST.get("tmp")
            actual = {}
            if ex is not None and tmp == Path(d):
                st = state(tmp)
                actual = {"posts": [f"{x['side']} {x['type']}" for x in ex.posts], "base_left": str(ex.bal.get("X")),
                          "blocked": bool(st["blocked"]), "positions": list(st["positions"]),
                          "ledger": len(st["ledger"]), "breaches": len(st["breaches"])}
            LAST.clear()
        fails += not ok
        report.append({"scenario": name, "expected": what, "actual": actual, "result": "PASS" if ok else "FAIL"})
        print(f"{'PASS' if ok else 'FAIL'}  {name}: {what}")
    out = Path(__file__).resolve().parent.parent / "logs" / f"probe_scenarios_{time.strftime('%Y%m%d')}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nсценариев {len(SCENARIOS)}, не прошли {fails}; протокол {out}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
