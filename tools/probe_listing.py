"""Проба: работает ли официальный MEXC Spot API v3 на новом листинге.

Скрипт стартует за --lead секунд до открытия пары и каждые --interval секунд
опрашивает официальный API, записывая, что именно отвечает биржа:

  exchange_info  GET  /api/v3/exchangeInfo?symbol=   есть ли пара, isSpotTradingAllowed
  default_syms   GET  /api/v3/defaultSymbols          входит ли пара в белый список API
  depth          GET  /api/v3/depth                   отдаются ли рыночные данные
  self_syms      GET  /api/v3/selfSymbols      (ключ) белый список для этого аккаунта
  order_test     POST /api/v3/order/test       (ключ) примет ли биржа ордер (без исполнения)
  real_order     POST /api/v3/order            (ключ, --real-order) настоящий LIMIT BUY
                 ниже рынка на --offset-pct и немедленная отмена; до первого успеха —
                 раз в --real-order-every с, после — раз в --real-order-every-after с,
                 чтобы видеть, стабильно ли API принимает ордера (серверы MEXC
                 включают пару не одновременно). В итоге — таблица ответов по минутам.
                 Если пробный ордер хоть раз исполнился, попытки прекращаются.
  market_*       (ключ, --market-test) один раз после открытия, при спреде ≤ --market-test-max-spread:
                 MARKET BUY на --market-test-usdt, LIMIT SELL половины на --market-test-far-pct ниже
                 bid (пропустит ли защита цены PERCENT_PRICE_BY_SIDE), MARKET SELL остатка; если
                 MARKET SELL отклонён — запасной выход LIMIT-ами по bid −2 %. Пишется проскальзывание и PnL.
  cap_*, preopen_* (--cap-test) потолок цены на открытии, со status=1 до T + --cap-window с:
                 public_prices  ticker/price, avgPrice, свеча 1m, ticker/24hr — когда появляется openPrice
                                (базовая цена листинга; у замороженных листингов потолок был ровно ×10);
                 cap_order_test order/test BUY выше вероятного потолка — отвечает ли тестовый эндпоинт 30010;
                 с --real-order, раз в --cap-order-every с, ордер на минимум пары и сразу отмена:
                 preopen_low    до открытия LIMIT BUY вдвое ниже нижнего ask — принимает ли биржа ордера заранее;
                 preopen_cap    до открытия LIMIT BUY выше видимых asks — проверяется ли потолок заранее
                                (оба не ближе минуты к firstOpenTime);
                 cap_maker      после открытия LIMIT_MAKER BUY выше потолка — 30010 с точным потолком.
                 Потолок из каждого 30010 (_extend.price) и openPrice попадают в итог.

В консоль печатаются только смены состояния каждой проверки, всё остальное
пишется в logs/probe_<SYMBOL>_<время>.jsonl.

Ключи: переменные окружения MEXC_API_KEY / MEXC_API_SECRET или файл .env
в корне проекта. Права ключа: чтение аккаунта + спот-торговля.

С --open auto время старта не нужно: проба ждёт status=1 (опрашивая только exchangeInfo),
затем опрашивает всё раз в --interval, а T=0 — первый двусторонний стакан.

Примеры:
  python tools/probe_listing.py PALMAIUSDT --open now --duration 10
  python tools/probe_listing.py MTOUSDT --open auto --duration 3600 --real-order
  python tools/probe_listing.py XYZ/USDT --open "2026-10-06 10:00" --real-order
Время без часового пояса считается UTC (так пишет анонсы MEXC).
"""

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from pathlib import Path

from notify import notify

BASE_URL = "https://api.mexc.com"
ROOT = Path(__file__).resolve().parent.parent


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class Resp:
    http: int | None
    body: object
    latency_ms: float
    error: str | None = None

    @property
    def ok(self) -> bool:
        if self.error or self.http != 200:
            return False
        return not (isinstance(self.body, dict) and self.body.get("code") not in (None, 0, 200))

    def label(self) -> str:
        if self.error:
            return f"NET {self.error}"
        if isinstance(self.body, dict) and "code" in self.body and self.body["code"] not in (0, 200):
            return f"ERR {self.body['code']} {self.body.get('msg')}"
        return f"HTTP {self.http}"


class Client:
    def __init__(self, key: str | None, secret: str | None, timeout: float):
        self.key = key
        self.secret = secret
        self.timeout = timeout
        self.time_offset_ms = 0

    @property
    def has_keys(self) -> bool:
        return bool(self.key and self.secret)

    def sync_time(self) -> None:
        r = self.call("GET", "/api/v3/time")
        if r.ok:
            # serverTime соответствует середине запроса: локальное «после ответа» − RTT/2
            self.time_offset_ms = r.body["serverTime"] - now_ms() + int(r.latency_ms / 2)

    def call(self, method: str, path: str, params: dict | None = None, signed: bool = False) -> Resp:
        params = dict(params or {})
        headers = {"Content-Type": "application/json"}
        if signed:
            params["timestamp"] = now_ms() + self.time_offset_ms
            params["recvWindow"] = 5000
            query = urllib.parse.urlencode(params)
            signature = hmac.new(self.secret.encode(), query.encode(), hashlib.sha256).hexdigest()
            query += "&signature=" + signature
            headers["X-MEXC-APIKEY"] = self.key
        else:
            query = urllib.parse.urlencode(params)
        url = BASE_URL + path + ("?" + query if query else "")
        req = urllib.request.Request(url, method=method, headers=headers)

        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                status, raw = r.status, r.read()
        except urllib.error.HTTPError as e:
            status, raw = e.code, e.read()
        except Exception as e:  # таймаут, DNS, обрыв соединения
            return Resp(None, None, (time.perf_counter() - t0) * 1000, f"{type(e).__name__}: {e}")
        latency = (time.perf_counter() - t0) * 1000
        try:
            body = json.loads(raw)
        except ValueError:
            body = raw.decode(errors="replace")[:300]
        return Resp(status, body, latency)


def decimals_of(values: list[str]) -> int:
    """Минимальное число знаков, которым записаны значения (нижняя граница точности)."""
    result = 0
    for v in values:
        if "." in v:
            result = max(result, len(v.split(".")[1].rstrip("0")))
    return result


@dataclass
class Market:
    info: dict | None = None  # запись пары из exchangeInfo
    bids: list = field(default_factory=list)
    asks: list = field(default_factory=list)


def build_buy_params(symbol: str, market: Market, usdt: float, offset_pct: float) -> dict | None:
    """LIMIT BUY на offset_pct ниже bestBid, объём ~usdt. None, если нет bid."""
    if not market.bids:
        return None
    best_bid = Decimal(market.bids[0][0])
    if market.info:
        price_dec = int(market.info.get("quotePrecision", 8))
        qty_dec = int(market.info.get("baseAssetPrecision", 8))
        min_amount = float(market.info.get("quoteAmountPrecision") or 0)
    else:
        levels = market.bids + market.asks
        price_dec = decimals_of([lvl[0] for lvl in levels])
        qty_dec = decimals_of([lvl[1] for lvl in levels])
        min_amount = 0.0
    amount = Decimal(str(max(usdt, min_amount * 1.05)))
    price = (best_bid * (1 - Decimal(str(offset_pct)) / 100)).quantize(Decimal(1).scaleb(-price_dec), ROUND_DOWN)
    if price <= 0:
        return None
    qty = (amount / price).quantize(Decimal(1).scaleb(-qty_dec), ROUND_UP)
    return {"symbol": symbol, "side": "BUY", "type": "LIMIT", "quantity": str(qty), "price": str(price)}


@dataclass
class CheckState:
    last: str | None = None
    first_positive_ts: int | None = None
    history: list = field(default_factory=list)  # (ts, state)


class Probe:
    def __init__(self, args, client: Client, open_ms: int | None, log_path: Path):
        self.args = args
        self.client = client
        self.symbol = args.symbol
        self.open_ms = open_ms  # None в режиме --open auto, пока открытие не обнаружено
        self.auto = open_ms is None
        self.started_ms = now_ms()
        self.online = False  # status == 1 в exchangeInfo
        self.absent_since: int | None = None  # с какого момента пары нет в exchangeInfo
        self.market = Market()
        self.states: dict[str, CheckState] = {}
        self.real_order_accepted = False  # хотя бы один ордер принят
        self.real_order_halted = False  # пробный ордер исполнился — больше не пробуем
        self.last_real_attempt = 0.0
        self.order_stats: dict[int, dict[str, int]] = {}  # минута -> {"OK"/код ошибки: число}
        self.market_test_done = False  # MARKET-тест делается один раз
        self.last_market_attempt = 0.0
        self.market_stats: dict[int, dict[str, int]] = {}  # ответы на MARKET BUY по минутам
        self.first_open_ms: int | None = None  # firstOpenTime из exchangeInfo (последнее непустое)
        self.last_price = Decimal(0)  # ticker/price
        self.open_price: tuple[int, str] | None = None  # (когда впервые, openPrice из ticker/24hr)
        self.caps: list[tuple[int, str, str]] = []  # (время, проверка, _extend.price из ответа 30010)
        self.last_cap_check = 0.0
        self.last_cap_order = 0.0
        self.log_path = log_path
        self.log = log_path.open("a", encoding="utf-8")

    def rel(self) -> int | None:
        return None if self.open_ms is None else now_ms() - self.open_ms

    def fmt(self, ts: int) -> str:
        if self.open_ms is None:
            return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%H:%M:%S UTC")
        return f"T{(ts - self.open_ms) / 1000:+8.1f}s"

    def record(self, check: str, r: Resp | None, state: str, positive: bool, extra: dict | None = None) -> None:
        ts = now_ms()
        entry = {"ts": ts, "rel_ms": self.rel(), "check": check, "state": state, "positive": positive}
        if r is not None:
            entry.update(http=r.http, latency_ms=round(r.latency_ms, 1))
        if extra:
            entry.update(extra)
        self.log.write(json.dumps(entry, ensure_ascii=False) + "\n")
        self.log.flush()

        st = self.states.setdefault(check, CheckState())
        if state != st.last:
            print(f"[{self.fmt(ts)}] {check:<13} {state}", flush=True)
            st.last = state
            st.history.append((ts, state))
        if positive and st.first_positive_ts is None:
            st.first_positive_ts = ts

    def detect_open(self, reason: str) -> None:
        self.open_ms = now_ms()
        self.record("open_detected", None, reason, True, {"open_ms": self.open_ms})
        notify(f"🟢 {self.symbol}: старт торгов обнаружен ({reason}), проба идёт {self.args.duration:.0f} с")

    # --- проверки -------------------------------------------------------------

    def check_exchange_info(self) -> None:
        r = self.client.call("GET", "/api/v3/exchangeInfo", {"symbol": self.symbol})
        symbols = r.body.get("symbols") if r.ok and isinstance(r.body, dict) else None
        if symbols:
            s = symbols[0]
            self.market.info = s
            self.online = s.get("status") == "1"
            self.first_open_ms = s.get("firstOpenTime") or self.first_open_ms  # пропадает, когда status мигает
            state = (f"listed status={s.get('status')} apiAllowed={s.get('isSpotTradingAllowed')} "
                     f"side={s.get('tradeSideType')} types={','.join(s.get('orderTypes', []))}")
            self.record("exchange_info", r, state, s.get("isSpotTradingAllowed") is True, {"symbol_info": s})
        else:
            state = "absent" if r.ok else r.label()
            self.record("exchange_info", r, state, False, {"body": r.body})
        if r.ok and not symbols:
            self.absent_since = self.absent_since or now_ms()
        elif symbols:
            self.absent_since = None

    def check_symbol_list(self, check: str, path: str, signed: bool) -> None:
        r = self.client.call("GET", path, signed=signed)
        if r.ok and isinstance(r.body, dict) and isinstance(r.body.get("data"), list):
            listed = self.symbol in r.body["data"]
            self.record(check, r, f"in_list={listed}", listed, {"list_size": len(r.body["data"])})
        else:
            self.record(check, r, r.label(), False, {"body": r.body})

    def check_depth(self) -> None:
        r = self.client.call("GET", "/api/v3/depth", {"symbol": self.symbol, "limit": 20})
        if r.ok and isinstance(r.body, dict):
            self.market.bids = r.body.get("bids") or []
            self.market.asks = r.body.get("asks") or []
            if self.market.bids and self.market.asks:
                state = "ok"
            else:
                state = f"one-sided bids={len(self.market.bids)} asks={len(self.market.asks)}"
            best = {"bid": self.market.bids[0] if self.market.bids else None,
                    "ask": self.market.asks[0] if self.market.asks else None}
            self.record("depth", r, state, state == "ok", best)
            # у приостановленной старой пары (status=2) стакан тоже двусторонний — это не открытие;
            # до firstOpenTime bid может быть нашей же заявкой preopen_* — тоже не открытие
            if state == "ok" and self.open_ms is None and self.online and self.after_scheduled_open():
                self.detect_open("two_sided_book")
        else:
            self.record("depth", r, r.label(), False, {"body": r.body})

    def check_order_test(self) -> None:
        params = build_buy_params(self.symbol, self.market, self.args.usdt, self.args.offset_pct)
        if params is None:
            self.record("order_test", None, "skipped: no bid", False)
            return
        r = self.client.call("POST", "/api/v3/order/test", params, signed=True)
        state = "ACCEPTED" if r.ok else r.label()
        self.record("order_test", r, state, r.ok, {"params": params, "body": r.body})

    def count_order(self, outcome: str, stats: dict | None = None) -> None:
        stats = self.order_stats if stats is None else stats
        ts = now_ms()
        minute = (ts - self.open_ms) // 60000 if self.open_ms is not None else ts // 60000
        bucket = stats.setdefault(minute, {})
        bucket[outcome] = bucket.get(outcome, 0) + 1

    def check_real_order(self) -> None:
        every = self.args.real_order_every_after if self.real_order_accepted else self.args.real_order_every
        if self.real_order_halted or time.monotonic() - self.last_real_attempt < every:
            return
        params = build_buy_params(self.symbol, self.market, self.args.usdt, self.args.offset_pct)
        if params is None:
            self.record("real_order", None, "skipped: no bid", False)
            return
        self.last_real_attempt = time.monotonic()
        params["newClientOrderId"] = f"probe{now_ms()}"
        r = self.client.call("POST", "/api/v3/order", params, signed=True)
        order_id = r.body.get("orderId") if r.ok and isinstance(r.body, dict) else None
        if not order_id:
            code = r.body.get("code") if isinstance(r.body, dict) else None
            self.count_order(str(code) if code is not None else r.label())
            self.record("real_order", r, r.label(), False, {"params": params, "body": r.body})
            return

        self.count_order("OK")
        self.record("real_order", r, "ACCEPTED", True, {"params": params, "body": r.body})
        self.real_order_accepted = True
        self.cancel_order(order_id)

    def order_stats_lines(self, stats: dict | None = None) -> list[str]:
        """Таблица ответов на пробные ордера по минутам: OK — принят, 10007 — пара закрыта для API,
        прочие коды (30004 и т.п.) — проверку пары прошёл, отказ по другой причине."""
        stats = self.order_stats if stats is None else stats
        lines = []
        total: dict[str, int] = {}
        for minute in sorted(stats):
            bucket = stats[minute]
            for k, v in bucket.items():
                total[k] = total.get(k, 0) + v
            label = (f"T+{minute:>3} мин" if self.open_ms is not None
                     else datetime.fromtimestamp(minute * 60, timezone.utc).strftime("%H:%M UTC"))
            lines.append(f"  {label}  " + ", ".join(f"{k}: {v}" for k, v in sorted(bucket.items())))
        if total:
            n = sum(total.values())
            passed = n - total.get("10007", 0)
            lines.append(f"  всего {n}: прошли проверку пары {passed} ({passed / n * 100:.0f}%), "
                         + ", ".join(f"{k}: {v}" for k, v in sorted(total.items())))
        return lines

    def cancel_order(self, order_id: str) -> None:
        for attempt in range(1, 4):
            r = self.client.call("DELETE", "/api/v3/order", {"symbol": self.symbol, "orderId": order_id}, signed=True)
            self.record("real_cancel", r, "CANCELED" if r.ok else r.label(), r.ok, {"attempt": attempt, "body": r.body})
            if r.ok:
                break
            time.sleep(0.3)
        q = self.client.call("GET", "/api/v3/order", {"symbol": self.symbol, "orderId": order_id}, signed=True)
        executed = q.body.get("executedQty") if q.ok and isinstance(q.body, dict) else None
        status = q.body.get("status") if q.ok and isinstance(q.body, dict) else q.label()
        self.record("real_final", q, f"status={status} executedQty={executed}", False, {"body": q.body})
        if executed not in (None, "0", "0.0") and Decimal(str(executed)) > 0:
            self.real_order_halted = True
            msg = (f"!!! Ордер {order_id} частично исполнен: куплено {executed} {self.symbol}. Продайте вручную. "
                   f"Пробные ордера по {self.symbol} остановлены.")
            print(msg, flush=True)
            notify(msg)
        if status not in ("CANCELED", "PARTIALLY_CANCELED", "FILLED"):
            msg = f"!!! Не удалось подтвердить отмену ордера {order_id} ({self.symbol}) — проверьте открытые ордера."
            print(msg, flush=True)
            notify(msg)

    # --- MARKET-тест: покупка и продажа по рынку через API ----------------------

    def precisions(self) -> tuple[int, int, Decimal]:
        info = self.market.info or {}
        return (int(info.get("quotePrecision", 8)), int(info.get("baseAssetPrecision", 8)),
                Decimal(str(info.get("quoteAmountPrecision") or 1)))

    def wait_order(self, order_id: str, timeout_s: float = 3.0) -> dict:
        """Ждёт финального статуса ордера (или таймаута) и возвращает последний ответ GET /api/v3/order."""
        deadline = time.monotonic() + timeout_s
        last: dict = {}
        while True:
            q = self.client.call("GET", "/api/v3/order", {"symbol": self.symbol, "orderId": order_id}, signed=True)
            if q.ok and isinstance(q.body, dict):
                last = q.body
                if last.get("status") in ("FILLED", "CANCELED", "PARTIALLY_CANCELED"):
                    return last
            if time.monotonic() > deadline:
                return last
            time.sleep(0.2)

    def base_free(self, asset: str) -> Decimal:
        r = self.client.call("GET", "/api/v3/account", signed=True)
        if r.ok and isinstance(r.body, dict):
            for b in r.body.get("balances", []):
                if b.get("asset") == asset:
                    return Decimal(str(b.get("free") or 0))
        return Decimal(0)

    def place(self, check: str, params: dict) -> str | None:
        """Ставит ордер, пишет результат в check, возвращает orderId или None."""
        params = {**params, "symbol": self.symbol, "newClientOrderId": f"{check[:6]}{now_ms()}"}
        r = self.client.call("POST", "/api/v3/order", params, signed=True)
        self.note_cap(check, r)
        order_id = r.body.get("orderId") if r.ok and isinstance(r.body, dict) else None
        self.record(check, r, "ACCEPTED" if order_id else r.label(), bool(order_id),
                    {"params": params, "body": r.body,
                     "bid": self.market.bids[0][0] if self.market.bids else None,
                     "ask": self.market.asks[0][0] if self.market.asks else None})
        return order_id

    def record_fill(self, check: str, fill: dict, ref_price: Decimal, side: str) -> Decimal:
        """Пишет итог исполнения; возвращает сумму в USDT. Проскальзывание — против bestAsk/bestBid в момент отправки."""
        qty = Decimal(str(fill.get("executedQty") or 0))
        quote = Decimal(str(fill.get("cummulativeQuoteQty") or fill.get("cumulativeQuoteQty") or 0))
        avg = (quote / qty).quantize(Decimal("1e-8")).normalize() if qty else None
        slip = None
        if avg and ref_price:
            slip = float((avg / ref_price - 1) * 100) * (1 if side == "BUY" else -1)  # >0 — хуже референса
        state = f"status={fill.get('status')} qty={qty} usdt={quote:.4f} avg={avg} " + \
                (f"slippage={slip:+.2f}%" if slip is not None else "")
        self.record(check, None, state, qty > 0, {"body": fill, "ref_price": str(ref_price), "slippage_pct": slip})
        return quote

    def refresh_book(self) -> tuple[Decimal, Decimal]:
        self.check_depth()
        bid = Decimal(self.market.bids[0][0]) if self.market.bids else Decimal(0)
        ask = Decimal(self.market.asks[0][0]) if self.market.asks else Decimal(0)
        return bid, ask

    def check_market_test(self) -> None:
        a = self.args
        if self.market_test_done or self.open_ms is None:
            return
        if time.monotonic() - self.last_market_attempt < a.market_test_every:
            return
        if not (self.market.bids and self.market.asks):
            return
        bid, ask = Decimal(self.market.bids[0][0]), Decimal(self.market.asks[0][0])
        spread = float((ask - bid) / ((ask + bid) / 2) * 100)
        bid_depth = sum(Decimal(p) * Decimal(q) for p, q in self.market.bids if Decimal(p) >= bid * Decimal("0.9"))
        usdt = Decimal(str(a.market_test_usdt))
        if spread > a.market_test_max_spread or bid_depth < 2 * usdt:
            # тест стоит денег на спреде и проскальзывании — ждём нормального рынка
            self.record("market_test", None, "waiting: spread/depth", False,
                        {"spread_pct": round(spread, 2), "bid_depth_10pct": float(bid_depth)})
            return
        self.last_market_attempt = time.monotonic()

        buy_id = self.place("market_buy", {"side": "BUY", "type": "MARKET", "quoteOrderQty": str(usdt)})
        if not buy_id:
            r_state = self.states["market_buy"].last or "?"
            code = r_state.split()[1] if r_state.startswith("ERR ") else r_state
            self.count_order(code, self.market_stats)
            return
        self.count_order("OK", self.market_stats)
        self.market_test_done = True
        spent = self.record_fill("market_buy_fill", self.wait_order(buy_id), ask, "BUY")
        received = self.market_exit()
        base = (self.market.info or {}).get("baseAsset", "")
        bid_now, _ = self.refresh_book()
        leftover = self.base_free(base)
        pnl = received - spent
        state = f"spent={spent:.4f} received={received:.4f} pnl={pnl:+.4f} USDT leftover={leftover} {base}"
        self.record("market_roundtrip", None, state, True,
                    {"spent": str(spent), "received": str(received), "pnl": str(pnl), "leftover": str(leftover)})
        notify(f"🧪 MARKET-тест {self.symbol}: "
               + "; ".join(f"{c}: {self.states[c].last}" for c in
                           ("market_buy", "far_limit_sell", "market_sell", "fallback_sell") if c in self.states)
               + f"\nИтог: {state}")
        _, _, min_amount = self.precisions()
        if leftover * bid_now >= min_amount:
            notify(f"!!! {self.symbol}: после MARKET-теста осталось {leftover} {base} — продайте вручную.")

    def market_exit(self) -> Decimal:
        """Продаёт купленное: половину LIMIT-ом далеко ниже рынка (проверка защиты цены), остаток MARKET-ом,
        при отказе — запасной выход LIMIT-ами чуть ниже bid. Возвращает полученные USDT."""
        a = self.args
        price_dec, qty_dec, min_amount = self.precisions()
        base = (self.market.info or {}).get("baseAsset", "")
        step = Decimal(1).scaleb(-qty_dec)
        received = Decimal(0)

        free = Decimal(0)
        for _ in range(5):  # баланс может обновиться не мгновенно
            free = self.base_free(base)
            if free > 0:
                break
            time.sleep(0.2)

        # 1. LIMIT SELL половины на far_pct ниже bid — так ставит цену веб-форма критической продажи
        # Минимальная сумма ордера (1 USDT) считается по цене лимитного ордера, поэтому половина по заниженной
        # цене должна стоить больше минимума (иначе 30002) — для этого MARKET BUY берётся с запасом (5 USDT).
        bid, _ = self.refresh_book()
        half = (free / 2).quantize(step, ROUND_DOWN)
        far_price = (bid * (1 - Decimal(str(a.market_test_far_pct)) / 100)).quantize(
            Decimal(1).scaleb(-price_dec), ROUND_DOWN)
        if bid and half * far_price < min_amount * Decimal("1.05"):
            self.record("far_limit_sell", None,
                        f"skipped: {half} x {far_price} меньше минимума {min_amount} USDT", False)
        elif bid:
            far_id = self.place("far_limit_sell", {"side": "SELL", "type": "LIMIT", "quantity": str(half),
                                                   "price": str(far_price)})
            if far_id:
                fill = self.wait_order(far_id)
                received += self.record_fill("far_limit_sell_fill", fill, bid, "SELL")
                if fill.get("status") not in ("FILLED", "CANCELED", "PARTIALLY_CANCELED"):
                    self.client.call("DELETE", "/api/v3/order", {"symbol": self.symbol, "orderId": far_id}, signed=True)

        # 2. MARKET SELL всего остатка — главный вопрос теста
        bid, _ = self.refresh_book()
        qty = self.base_free(base).quantize(step, ROUND_DOWN)
        if bid and qty * bid >= min_amount:
            sell_id = self.place("market_sell", {"side": "SELL", "type": "MARKET", "quantity": str(qty)})
            if sell_id:
                received += self.record_fill("market_sell_fill", self.wait_order(sell_id), bid, "SELL")
        else:
            self.record("market_sell", None, f"skipped: остаток {qty} меньше минимума", False)

        # 3. Запасной выход, если что-то осталось (MARKET SELL отклонён или исполнился частично)
        for attempt in range(1, 4):
            bid, _ = self.refresh_book()
            qty = self.base_free(base).quantize(step, ROUND_DOWN)
            if not bid or qty * bid < min_amount:
                break
            price = (bid * Decimal("0.98")).quantize(Decimal(1).scaleb(-price_dec), ROUND_DOWN)
            fb_id = self.place("fallback_sell", {"side": "SELL", "type": "LIMIT", "quantity": str(qty),
                                                 "price": str(price)})
            if not fb_id:
                time.sleep(1)
                continue
            fill = self.wait_order(fb_id, timeout_s=2.0)
            received += self.record_fill("fallback_sell_fill", fill, bid, "SELL")
            if fill.get("status") not in ("FILLED", "CANCELED", "PARTIALLY_CANCELED"):
                self.client.call("DELETE", "/api/v3/order", {"symbol": self.symbol, "orderId": fb_id}, signed=True)
        return received

    # --- потолок цены на открытии (--cap-test) ----------------------------------

    def after_scheduled_open(self) -> bool:
        """Наступило ли время старта из exchangeInfo (firstOpenTime); если его нет — считаем, что да."""
        return not self.first_open_ms or now_ms() + self.client.time_offset_ms >= self.first_open_ms - 2000

    def note_cap(self, check: str, r: Resp) -> None:
        """Запоминает потолок из ответа 30010: {"code": 30010, "_extend": {"price": "0.025USDT"}}."""
        if isinstance(r.body, dict) and r.body.get("code") == 30010:
            self.caps.append((now_ms(), check, str((r.body.get("_extend") or {}).get("price", ""))))

    def check_public_prices(self) -> None:
        """Публичные цены пары: появляется ли openPrice (ticker/24hr) и другие цены до открытия."""
        sym = {"symbol": self.symbol}
        calls = (("price", "/api/v3/ticker/price", sym), ("avg", "/api/v3/avgPrice", sym),
                 ("kline", "/api/v3/klines", {**sym, "interval": "1m", "limit": 1}),
                 ("ticker24h", "/api/v3/ticker/24hr", sym))
        bodies, parts, open_price = {}, [], None
        for name, path, params in calls:
            r = self.client.call("GET", path, params)
            bodies[name] = r.body
            if not r.ok:
                parts.append(f"{name}={r.label()}")
            elif name == "kline":
                parts.append(f"kline_open={r.body[-1][1] if isinstance(r.body, list) and r.body else 'нет'}")
            elif name == "ticker24h" and isinstance(r.body, dict):
                open_price = r.body.get("openPrice")
                parts.append(f"openPrice={open_price}")
            elif isinstance(r.body, dict):
                price = Decimal(str(r.body.get("price") or 0))
                if name == "price" and price > 0:
                    self.last_price = price
                parts.append(f"{name}={'есть' if price > 0 else '0'}")  # само значение — в логе
        has_open = bool(open_price) and Decimal(str(open_price)) > 0
        if has_open and self.open_price is None:
            self.open_price = (now_ms(), str(open_price))
        self.record("public_prices", None, " ".join(parts), has_open, {"bodies": bodies})

    def cap_ref_price(self) -> Decimal | None:
        """Цена, выше которой ставится проверочный BUY. Во время заморозки лучший bid и последняя цена стоят
        на потолке; до открытия bid-ов нет — берётся верх видимых asks (у RT, MTO, HDLR, CHOMP он был не ниже потолка)."""
        if self.market.bids:
            return max(Decimal(self.market.bids[0][0]), self.last_price)
        if self.market.asks:
            return max(Decimal(p) for p, _ in self.market.asks)
        return None

    def cap_buy_params(self, order_type: str, price: Decimal | None) -> dict | None:
        """BUY на минимальную сумму пары (+10 %) по цене price, округлённой вниз до точности пары."""
        if not price:
            return None
        price_dec, qty_dec, min_amount = self.precisions()
        price = price.quantize(Decimal(1).scaleb(-price_dec), ROUND_DOWN)
        if price <= 0:
            return None
        qty = (min_amount * Decimal("1.1") / price).quantize(Decimal(1).scaleb(-qty_dec), ROUND_UP)
        return {"side": "BUY", "type": order_type, "quantity": str(qty), "price": str(price)}

    def above_cap_price(self) -> Decimal | None:
        ref = self.cap_ref_price()
        return ref * (1 + Decimal(str(self.args.cap_test_pct)) / 100) if ref else None

    def check_cap_order_test(self) -> None:
        """order/test BUY выше вероятного потолка: проверяет ли тестовый эндпоинт потолок (30010). Без риска."""
        params = self.cap_buy_params("LIMIT", self.above_cap_price())
        if params is None:
            self.record("cap_order_test", None, "skipped: no book", False)
            return
        params["symbol"] = self.symbol
        r = self.client.call("POST", "/api/v3/order/test", params, signed=True)
        self.note_cap("cap_order_test", r)
        is_cap = isinstance(r.body, dict) and r.body.get("code") == 30010
        self.record("cap_order_test", r, "ACCEPTED" if r.ok else r.label(), is_cap, {"params": params, "body": r.body})

    def check_preopen_orders(self) -> None:
        """До открытия: принимает ли биржа ордера (LIMIT BUY вдвое ниже нижнего ask) и проверяет ли уже потолок
        (LIMIT BUY выше видимых asks). Принятый ордер сразу отменяется. Ближе минуты к firstOpenTime не ставим."""
        if not self.market.asks or not self.first_open_ms:
            return
        if self.first_open_ms - (now_ms() + self.client.time_offset_ms) < 60_000:
            return
        low = self.cap_buy_params("LIMIT", Decimal(self.market.asks[0][0]) / 2)
        high = self.cap_buy_params("LIMIT", self.above_cap_price())
        for check, params in (("preopen_low", low), ("preopen_cap", high)):
            if params and (order_id := self.place(check, params)):
                self.cancel_order(order_id)

    def check_cap_maker(self) -> None:
        """После открытия: LIMIT_MAKER BUY выше вероятного потолка. «Только мейкер» не покупает по рынку:
        во время заморозки ждём 30010 с точным потолком, без неё — отказ, потому что ордер исполнился бы сразу.
        Если ордер всё же встал в стакан (широкий спред), он сразу отменяется."""
        params = self.cap_buy_params("LIMIT_MAKER", self.above_cap_price())
        if params and (order_id := self.place("cap_maker", params)):
            self.cancel_order(order_id)

    def check_cap(self) -> None:
        """Проверки потолка: со status=1 до открытия и --cap-window секунд после."""
        rel = self.rel()
        if rel is not None and rel > self.args.cap_window * 1000:
            return
        now = time.monotonic()
        if now - self.last_cap_check >= self.args.cap_every:
            self.last_cap_check = now
            self.check_public_prices()
            if self.client.has_keys:
                self.check_cap_order_test()
        if (self.args.real_order and not self.real_order_halted
                and now - self.last_cap_order >= self.args.cap_order_every):
            self.last_cap_order = now
            if self.open_ms is None:
                self.check_preopen_orders()
            else:
                self.check_cap_maker()

    def cap_lines(self) -> list[str]:
        """Итог по потолку: openPrice и каждое значение потолка из 30010 — когда видели и в каких проверках."""
        lines = []
        if self.open_price:
            lines.append(f"openPrice {self.open_price[1]} (впервые {self.fmt(self.open_price[0]).strip()})")
        seen: dict[str, list[tuple[int, str]]] = {}
        for ts, check, raw in self.caps:
            seen.setdefault(raw, []).append((ts, check))
        for raw, items in seen.items():
            m = re.match(r"[0-9.]+", raw)
            ratio = ""
            if m and self.open_price and Decimal(self.open_price[1]) > 0:
                ratio = f" = {Decimal(m.group()) / Decimal(self.open_price[1]):.2f} × openPrice"
            checks = ", ".join(sorted({c for _, c in items}))
            lines.append(f"потолок {raw}{ratio}: {self.fmt(items[0][0]).strip()} … {self.fmt(items[-1][0]).strip()}, "
                         f"30010 ×{len(items)} ({checks})")
        if self.args.cap_test and not seen:
            lines.append("30010 не было")
        return lines

    # --- цикл -----------------------------------------------------------------

    def tick(self) -> None:
        self.check_exchange_info()
        self.check_symbol_list("default_syms", "/api/v3/defaultSymbols", signed=False)
        self.check_depth()
        if self.client.has_keys:
            self.check_symbol_list("self_syms", "/api/v3/selfSymbols", signed=True)
            self.check_order_test()
            if self.args.real_order:
                self.check_real_order()
            if self.args.market_test:
                self.check_market_test()
        if self.args.cap_test:
            self.check_cap()

    def run(self) -> None:
        last_heartbeat = 0.0
        while True:
            now = now_ms()
            if self.open_ms is not None and now >= self.open_ms + self.args.duration * 1000:
                break
            if self.open_ms is None and now - self.started_ms > self.args.max_wait * 3600 * 1000:
                print(f"Открытие не обнаружено за {self.args.max_wait} ч — завершаю.", flush=True)
                break
            if self.absent_since and now - self.absent_since > self.args.absent_stop * 60 * 1000:
                print(f"Пары нет в exchangeInfo дольше {self.args.absent_stop} мин (листинг отменён?) — завершаю.",
                      flush=True)
                break
            started = time.monotonic()
            if self.auto and not self.online and self.open_ms is None:
                # пара ещё закрыта: опрашиваем только exchangeInfo и редко
                self.check_exchange_info()
                interval = 0 if self.online else self.args.auto_poll
            else:
                self.tick()
                rel = self.rel()
                fast = rel is None or rel < self.args.fast_window * 1000
                interval = self.args.interval if fast else self.args.slow_interval
            if started - last_heartbeat >= 30:
                print(f"[{self.fmt(now_ms())}] ...опрос идёт", flush=True)
                last_heartbeat = started
            time.sleep(max(0.0, interval - (time.monotonic() - started)))

    def summary(self) -> None:
        lines = []
        for check, st in self.states.items():
            first = "никогда" if st.first_positive_ts is None else self.fmt(st.first_positive_ts).strip()
            lines.append(f"{check:<13} первый успех: {first:<12} последнее: {st.last}")
        stats = self.order_stats_lines()
        market_stats = self.order_stats_lines(self.market_stats)
        cap = self.cap_lines()
        print("\n=== Итог (T — время относительно открытия) ===\n" + "\n".join(lines))
        if cap:
            print("\nПотолок цены:\n" + "\n".join(cap))
        if stats:
            print("\nПробные ордера по минутам (OK — принят, 10007 — пара закрыта для API):\n" + "\n".join(stats))
        if market_stats:
            print("\nПопытки MARKET BUY по минутам:\n" + "\n".join(market_stats))
        print("\nСмены состояний:")
        for check, st in self.states.items():
            for ts, state in st.history:
                print(f"  {check:<13} {self.fmt(ts)}  {state}")
        self.log.write(json.dumps({"summary": {
            "open_ms": self.open_ms,
            "checks": {c: {"first_positive_ts": s.first_positive_ts, "history": s.history}
                       for c, s in self.states.items()},
            "order_stats": self.order_stats,
            "market_stats": self.market_stats,
            "caps": self.caps,
            "open_price": self.open_price,
        }}, ensure_ascii=False) + "\n")
        self.log.close()
        notify(f"📋 Проба {self.symbol} завершена\n" + "\n".join(lines)
               + ("\nПотолок цены:\n" + "\n".join(cap) if cap else "")
               + ("\nОрдера по минутам:\n" + "\n".join(stats[-15:]) if stats else "")
               + f"\nЛог: {self.log_path.name}")


def parse_open(value: str) -> int | None:
    if value == "auto":
        return None
    if value == "now":
        return now_ms()
    if value.isdigit():
        return int(value)
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("symbol", help="пара, например XYZUSDT или XYZ/USDT")
    p.add_argument("--open", required=True,
                   help='время открытия: "auto" (ждать status=1 и двусторонний стакан), "now", '
                        'ISO ("2026-10-06 10:00", UTC по умолчанию) или epoch ms')
    p.add_argument("--lead", type=float, default=60, help="начать опрос за N секунд до открытия (60)")
    p.add_argument("--duration", type=float, default=900, help="опрашивать N секунд после открытия (900)")
    p.add_argument("--interval", type=float, default=1.0, help="интервал опроса в первые --fast-window секунд (1.0)")
    p.add_argument("--fast-window", type=float, default=300, help="длительность частого опроса после открытия (300)")
    p.add_argument("--slow-interval", type=float, default=5.0, help="интервал опроса после --fast-window (5.0)")
    p.add_argument("--auto-poll", type=float, default=10.0,
                   help="--open auto: интервал опроса exchangeInfo, пока пара закрыта, с (10)")
    p.add_argument("--max-wait", type=float, default=72, help="--open auto: сколько часов ждать открытия (72)")
    p.add_argument("--absent-stop", type=float, default=30,
                   help="завершить, если пары нет в exchangeInfo дольше N минут (листинг отменён) (30)")
    p.add_argument("--real-order", action="store_true", help="ставить настоящий LIMIT BUY ниже рынка и сразу отменять")
    p.add_argument("--real-order-every", type=float, default=2.0,
                   help="до первого принятого ордера: не чаще одной попытки за N секунд (2.0)")
    p.add_argument("--real-order-every-after", type=float, default=30.0,
                   help="после первого принятого: продолжать проверку раз в N секунд (30)")
    p.add_argument("--market-test", action="store_true",
                   help="один раз после открытия: MARKET BUY, LIMIT SELL половины далеко ниже рынка, MARKET SELL остатка")
    p.add_argument("--market-test-usdt", type=float, default=5.0,
                   help="сумма MARKET BUY в USDT (5: половина по цене −50%% должна быть больше минимума 1 USDT)")
    p.add_argument("--market-test-every", type=float, default=30.0,
                   help="повтор MARKET BUY, пока API отклоняет, раз в N секунд (30)")
    p.add_argument("--market-test-max-spread", type=float, default=10.0,
                   help="делать тест только при спреде не больше N %% (10)")
    p.add_argument("--market-test-far-pct", type=float, default=50.0,
                   help="LIMIT SELL половины на N %% ниже bestBid — проверка защиты цены (50)")
    p.add_argument("--cap-test", action="store_true",
                   help="проверки потолка цены на открытии (cap_*, preopen_*; реальные ордера — только с --real-order)")
    p.add_argument("--cap-window", type=float, default=700, help="проверять потолок N секунд после открытия (700)")
    p.add_argument("--cap-every", type=float, default=10.0, help="публичные цены и order/test раз в N секунд (10)")
    p.add_argument("--cap-order-every", type=float, default=30.0,
                   help="реальные ордера проверки потолка раз в N секунд (30)")
    p.add_argument("--cap-test-pct", type=float, default=10.0,
                   help="BUY «выше потолка» на N %% выше лучшего bid / последней цены (10; защита цены биржи — 20)")
    p.add_argument("--usdt", type=float, default=2.0, help="объём пробного ордера в USDT (2.0, не меньше минимума пары)")
    p.add_argument("--offset-pct", type=float, default=10.0, help="на сколько %% ниже bestBid ставить пробный BUY (10)")
    p.add_argument("--timeout", type=float, default=5.0, help="таймаут HTTP-запроса, с (5)")
    args = p.parse_args()
    args.symbol = args.symbol.upper().replace("/", "").replace("_", "").replace("-", "")

    load_dotenv(ROOT / ".env")
    client = Client(os.environ.get("MEXC_API_KEY"), os.environ.get("MEXC_API_SECRET"), args.timeout)
    if not client.has_keys:
        print("Ключей нет (MEXC_API_KEY / MEXC_API_SECRET): проверяю только публичные эндпоинты.")
        if args.real_order or args.market_test:
            sys.exit("--real-order и --market-test требуют ключей.")
    client.sync_time()

    open_ms = parse_open(args.open)
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"probe_{args.symbol}_{datetime.now():%Y%m%d_%H%M%S}.jsonl"

    if open_ms is None:
        print(f"{args.symbol}: время открытия определяется автоматически (ждём до {args.max_wait} ч), лог {log_path}")
    else:
        open_str = datetime.fromtimestamp(open_ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"{args.symbol}: открытие {open_str}, лог {log_path}")
    print(f"Смещение часов относительно биржи: {client.time_offset_ms} ms")
    if open_ms is not None:
        wait = (open_ms - int(args.lead * 1000) - now_ms()) / 1000
        if wait > 0:
            print(f"Жду {wait:.0f} с до начала опроса...", flush=True)
            time.sleep(wait)

    probe = Probe(args, client, open_ms, log_path)
    try:
        probe.run()
    except KeyboardInterrupt:
        print("\nОстановлено вручную.")
    finally:
        probe.summary()


if __name__ == "__main__":
    main()
