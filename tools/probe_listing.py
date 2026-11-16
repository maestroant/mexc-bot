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
  market_*       (ключ, --market-test) MARKET_EXECUTION_PROBE, один раз на пару, после открытия торгового API
                 (последняя проба доступности принята не позже 60 с назад), при спреде ≤ --market-test-max-spread,
                 при свободной котируемой валюте и в лимитах tools/probe_risk.py: MARKET BUY на --market-test-usdt,
                 MARKET SELL фактически купленного, при отказе / отмене / частичном исполнении — до трёх LIMIT по
                 bid −2 %. Не больше 3 отказов MARKET BUY. Неопределённый статус, непроданный остаток, ошибка выхода
                 → блокировка новых пробных BUY до сверки (logs/probe_risk.json). LIMIT SELL половины на −50 %
                 (прежняя проверка полосы цены) приостановлен — ждёт согласования клиента (решение 11.11).
  cap_*, preopen_* (--cap-test) потолок цены на открытии, со status=1 до T + --cap-window с:
                 public_prices  ticker/price, avgPrice, свеча 1m, ticker/24hr — когда появляется openPrice
                                (базовая цена листинга; у замороженных листингов потолок был ровно ×10);
                 cap_order_test order/test BUY выше вероятного потолка — отвечает ли тестовый эндпоинт 30010;
                 с --real-order, раз в --cap-order-every с, ордер на минимум пары и сразу отмена:
                 preopen_low, preopen_cap — ордера до открытия торгов приостановлены (решение клиента 11.11:
                                CAP-пробу не отправлять до фактического открытия);
                 cap_maker      после открытия LIMIT_MAKER BUY выше потолка — 30010 с точным потолком.
                 Потолок из каждого 30010 (_extend.price) и openPrice попадают в итог.

В консоль печатаются только смены состояния каждой проверки, всё остальное
пишется в logs/probe_<SYMBOL>_<время>.jsonl. У каждой записи об ордере — режим (mode): API_AVAILABILITY_PROBE,
CAP_LIMIT_MAKER_PROBE, MARKET_EXECUTION_PROBE, PROTECTIVE_EXIT; у подачи — класс ответа (outcome), время запроса и
ответа, цены bid/ask и время REST-стакана, ограничения пары, clientOrderId; у исполнения — история статусов,
executedQty, сумма, средняя цена, комиссии по myTrades. Неожиданное исполнение пробного LIMIT — защитный выход
фактически купленного и блокировка проб до сверки.

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
import itertools
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
import probe_risk as risk

BASE_URL = "https://api.mexc.com"
TERMINAL = ("FILLED", "CANCELED", "PARTIALLY_CANCELED", "REJECTED", "EXPIRED")
MAX_MARKET_ATTEMPTS = 1  # MARKET_EXECUTION_PROBE: одна попытка BUY на пару (решение клиента 12.11)
MAX_TECH_REJECTS = 3  # API_AVAILABILITY_PROBE и LIMIT_MAKER: до трёх технических отказов (10007, 30004 и т. п.) на пару
MAX_API_ORDERS = 3  # API_AVAILABILITY_PROBE: всего реальных подач на пару (решение клиента 12.11; было 10)
# CAP_LIMIT_MAKER_PROBE (решение клиента 13.11, раздел 4): стоп на первой 30010 (CAP_FOUND); без 30010 — не больше
# CAP_MAX_ACCEPTED принятых и подтверждённо отменённых заявок; жёсткий предел CAP_MAX_SUBMITS подач с учётом отказов и
# неоднозначных ответов. Подача с неизвестным исходом не разрешает следующую до сверки. Счётчики — из долговечного
# реестра ордеров (logs/probe_risk.json), перезапуск процесса их не обнуляет. Окно — от firstOpenTime пары.
CAP_MAX_ACCEPTED = 3
CAP_MAX_SUBMITS = 6
# Read-only условия перед подачей (только убирают подачи, пределы не меняют). Решение клиента 13.11 (пакет A–E):
# 3/6 — APPROVED; G1 — CONTROLLED PILOT; G2 — CONTROLLED RESEARCH PILOT, не общая политика; коэффициент не подбирать.
# Каждая подавленная подача пишется в журнал (cap_suppressed) с причиной, ценами, временем данных и версией; случаи,
# где подавленная проба могла бы получить 30010, считаются как CAP_MAY_HAVE_BEEN_MISSED. Исторический прогон —
# tools/cap_probe_limit_study.py (in-sample). Откат к чистому 3/6: CAP_GATE_SELF_SYMBOLS = False, CAP_GATE_STEP = None.
CAP_GATE_SELF_SYMBOLS = True  # G1: не подавать, пока пары ни разу не было в selfSymbols (API пары не открыт → 10007)
CAP_GATE_STEP = Decimal("0.10")  # G2: подавать, только если цена пробы ≥ макс. принятой цены пробы × (1 + шаг)
PROBE_WINDOW_BEFORE_MS = 10 * 60_000  # подачи пары, учитываемые в счётчиках: с firstOpenTime − 10 мин
# LIMIT_EXECUTION_RESEARCH (решение клиента 12.11, C3): реальный LIMIT BUY по правилам стратегии. NOT ACTIVATED:
# реализация и офлайн-сценарии есть, реальный режим выключен здесь и включается только отдельным решением клиента
# (вместе с флагом --limit-research). Источник стакана для реального режима — синхронизированный стакан (REST-снимок +
# WS diff, как в PAPER); пока его в пробе нет, допускается только свежий REST-снимок для офлайн-проверки логики.
LIMIT_RESEARCH_ENABLED = False
# Один реальный режим BUY на листинг, предрегистрируется до T0 (--real-buy-mode market | limit, решение 12.11, C3.5):
# второй режим по той же паре не выполняется, альтернатива моделируется в PAPER.
LIMIT_AGGRESSIVE = Decimal("0.01")  # bestAsk + 1 %, как MAIN (buy_aggressive)
LIMIT_FILL_TIMEOUT_S = 1.2  # buyFillTimeoutMs стратегии
LIMIT_BOOK_MAX_AGE_MS = 300  # стакан на момент решения не старше
NEG_CHECKS_MIN = 2  # crash-before-ack: «ордера нет» — только после стольких отрицательных сверок ордера и сделок
NEG_WINDOW_MS = 60_000  # … не раньше чем через столько после регистрации ордера и с разносом проверок ≥ 30 с
RECONCILE_EVERY_S = 60  # сверка и защита незакрытой пробной позиции пары
PROBE_CIDS = ("market", "real_o", "cap_ma", "fallba", "protec", "reconc", "limit_")  # префиксы clientOrderId проб
MODES = {"order_test": "API_AVAILABILITY_PROBE", "real_order": "API_AVAILABILITY_PROBE",
         "real_cancel": "API_AVAILABILITY_PROBE", "real_final": "API_AVAILABILITY_PROBE",
         "cap_order_test": "CAP_LIMIT_MAKER_PROBE", "cap_maker": "CAP_LIMIT_MAKER_PROBE",
         "preopen_low": "CAP_LIMIT_MAKER_PROBE", "preopen_cap": "CAP_LIMIT_MAKER_PROBE",
         "market": "MARKET_EXECUTION_PROBE", "far_limit_sell": "MARKET_EXECUTION_PROBE",
         "fallback_sell": "MARKET_EXECUTION_PROBE", "protect": "PROTECTIVE_EXIT", "protective_exit": "PROTECTIVE_EXIT",
         "limit": "LIMIT_EXECUTION_RESEARCH", "reconcile": "PROTECTIVE_EXIT"}


_CID_SEQ = itertools.count()


def new_cid(check: str) -> str:
    """clientOrderId пробы: префикс вида (принадлежность), время и счётчик процесса — уникален и в одну мс."""
    return f"{check[:6]}{now_ms()}{next(_CID_SEQ) % 100:02d}"


def mode_of(check: str) -> str | None:
    for k, v in MODES.items():
        if check == k or check.startswith(k + "_") or check.startswith(k):
            return v
    return None
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


def code_label() -> str:
    """Метка выкладки (файл VERSION) — для журнала подавленных проб."""
    try:
        return (ROOT / "VERSION").read_text(encoding="utf-8").strip()
    except OSError:
        return "?"


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
        self.risk_path = risk.STATE
        self.market_attempts = 0
        self.api_orders = 0
        self.tech_rejects: dict[str, int] = {}  # проверка → технических отказов
        self.self_listed = False  # G1: пара хотя бы раз была в selfSymbols (API для неё открыт этому аккаунту)
        self.self_first: dict | None = None  # G1: когда пара впервые увидена в selfSymbols (время, T0+, источник)
        self.self_last_ok_ts: int | None = None  # время последнего успешного ответа selfSymbols (свежесть)
        self.cap_missed: list[dict] = []  # CAP_MAY_HAVE_BEEN_MISSED: подавленные G2 пробы выше доказанной цены
        self.api_restored = False  # подтверждение API восстановлено из реестра после перезапуска
        self.last_reconcile = 0.0
        self.limit_done = False  # LIMIT_EXECUTION_RESEARCH — один раунд на пару
        self.round_id: str | None = None
        self.api_ok_mono: float | None = None  # когда проба доступности API последний раз принята
        self.quote_free: Decimal | None = None
        self.book_ts: int | None = None  # время последнего REST-стакана
        self.suspended_noted: set = set()

    def rel(self) -> int | None:
        return None if self.open_ms is None else now_ms() - self.open_ms

    def fmt(self, ts: int) -> str:
        if self.open_ms is None:
            return datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%H:%M:%S UTC")
        return f"T{(ts - self.open_ms) / 1000:+8.1f}s"

    def record(self, check: str, r: Resp | None, state: str, positive: bool, extra: dict | None = None) -> None:
        ts = now_ms()
        entry = {"ts": ts, "rel_ms": self.rel(), "check": check, "state": state, "positive": positive}
        if mode_of(check):
            entry["mode"] = mode_of(check)
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
            if check == "self_syms":
                self.self_last_ok_ts = now_ms()
                if listed and not self.self_listed:
                    self.self_first = {"ts": self.self_last_ok_ts, "rel_ms": self.rel(),
                                       "source": "GET /api/v3/selfSymbols (подписанный REST)"}
                    self.record("cap_g1_seen", r, "G1: пара впервые в selfSymbols", True, self.self_first)
                self.self_listed = self.self_listed or listed
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
            self.book_ts = now_ms()
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

    def window_orders(self, check: str) -> list[dict]:
        """Подачи пробы check по этой паре в окне текущего листинга — из долговечного реестра, по времени."""
        ref = self.first_open_ms or self.open_ms
        if ref is None:
            return []
        since = ref - PROBE_WINDOW_BEFORE_MS
        rows = [o for _, o in risk.orders_of(self.symbol, path=self.risk_path)
                if o.get("check") == check and int(o.get("ts") or 0) >= since]
        return sorted(rows, key=lambda o: int(o.get("ts") or 0))

    def restore_api_probe(self) -> None:
        """После перезапуска: число подач пробы доступности и её подтверждение — из реестра. Последняя подача принята
        и отменена с подтверждением (CANCELED) — API считается подтверждённым, новая проба не подаётся."""
        if self.api_restored or self.open_ms is None:
            return
        self.api_restored = True
        rows = self.window_orders("real_order")
        self.api_orders = max(self.api_orders, len(rows))
        if rows and rows[-1].get("orderId") and rows[-1].get("status") == "CANCELED" and self.api_ok_mono is None:
            self.api_ok_mono = time.monotonic()
            self.real_order_accepted = True

    def check_real_order(self) -> None:
        every = self.args.real_order_every_after if self.real_order_accepted else self.args.real_order_every
        if self.real_order_halted or time.monotonic() - self.last_real_attempt < every:
            return
        self.restore_api_probe()
        reason = self.real_buy_gate("real_order")
        if reason is None and self.api_ok_mono is not None:
            reason = "stop: API подтверждён принятой и отменённой заявкой — больше не подаём"
        if reason is None and self.api_orders >= MAX_API_ORDERS:
            reason = f"stop: подач пробы доступности {self.api_orders} — предел {MAX_API_ORDERS}"
        if reason:
            self.note_once("real_order", reason)
            return
        params = build_buy_params(self.symbol, self.market, self.args.usdt, self.args.offset_pct)
        if params is None:
            self.record("real_order", None, "skipped: no bid", False)
            return
        self.last_real_attempt = time.monotonic()
        params.pop("symbol", None)
        self.api_orders += 1
        order_id = self.place("real_order", params)
        if not order_id:
            r_state = self.states["real_order"].last or "?"
            self.count_order(r_state.split()[1] if r_state.startswith("ERR ") else r_state)
            self.note_reject("real_order")
            if "10007" in r_state:
                self.api_ok_mono = None  # API снова закрыт для пары
            return
        self.count_order("OK")
        self.real_order_accepted = True
        self.cancel_order(order_id, "real_order")  # окончательная судьба заявки подтверждается до признания API открытым
        if not risk.is_blocked(self.risk_path):
            self.api_ok_mono = time.monotonic()

    def real_buy_gate(self, check: str) -> str | None:
        """Общие условия любого реального пробного BUY (решение клиента 12.11, C4/C5): после открытия торгов; без
        аварийной блокировки и незакрытых пробных позиций; не больше MAX_TECH_REJECTS технических отказов; при
        наличии котируемой валюты."""
        if self.open_ms is None:
            return "waiting: до открытия торгов реальные пробные ордера не ставятся"
        problem = risk.state_problem(self.symbol, path=self.risk_path)
        if problem:
            return f"skipped: {problem} — новые реальные BUY не ставятся до сверки"
        if self.tech_rejects.get(check, 0) >= MAX_TECH_REJECTS:
            return f"stop: технических отказов {self.tech_rejects[check]} — больше не подаём"
        quote = (self.market.info or {}).get("quoteAsset", "USDT")
        if self.quote_free is None:
            self.quote_free = self.balance(quote)
        need = Decimal(str(self.args.market_test_usdt if check in ("market_buy", "limit_buy") else self.args.usdt))
        if self.quote_free < need:
            return f"stop: свободно {self.quote_free} {quote} меньше {need} — реальные ордера не отправляются"
        return None

    def note_reject(self, check: str) -> None:
        """Технический отказ. Сеть / тайм-аут — не отказ: подача сверяется по clientOrderId в place().
        30004 — проверить баланс: недостаток подтверждён — дальнейшие попытки по паре прекращаются (real_buy_gate)."""
        last = self.states[check].last or ""
        if "30004" in last:
            self.quote_free = None  # перечитать баланс котируемой валюты при следующей проверке
        if any(c in last for c in ("10007", "30004", "30005")):
            self.tech_rejects[check] = self.tech_rejects.get(check, 0) + 1

    def note_once(self, check: str, state: str) -> None:
        if (check, state) not in self.suspended_noted:
            self.suspended_noted.add((check, state))
            self.record(check, None, state, False)

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

    def cancel_order(self, order_id: str, source: str = "real") -> None:
        """Отмена пробного LIMIT-ордера и подтверждение его окончательного статуса. Неожиданное исполнение —
        защитный выход фактически купленного количества и блокировка новых пробных BUY до сверки."""
        for attempt in range(1, 4):
            r = self.client.call("DELETE", "/api/v3/order", {"symbol": self.symbol, "orderId": order_id}, signed=True)
            self.record("real_cancel", r, "CANCELED" if r.ok else r.label(), r.ok,
                        {"attempt": attempt, "body": r.body, "orderId": order_id, "source": source})
            if r.ok:
                break
            time.sleep(0.3)
        fin = self.settle(order_id, "real_final", None, "BUY", timeout_s=2.0, cancel=False)
        executed, terminal = fin["qty"], fin["terminal"]
        if executed > 0:
            self.real_order_halted = True
            msg = (f"!!! {self.symbol}: пробный LIMIT-ордер {order_id} ({source}) исполнился на {executed} — "
                   f"защитный выход, новые пробные BUY заблокированы до сверки.")
            print(msg, flush=True)
            notify(msg)
            self.protective_exit(executed, fin, source)
        elif not terminal:
            reason = f"не подтверждена отмена пробного ордера {order_id} ({source}): статус {fin['status']}"
            risk.block(self.symbol, reason, path=self.risk_path, orderId=order_id)
            msg = f"!!! {self.symbol}: {reason} — проверьте открытые ордера; пробные BUY заблокированы."
            print(msg, flush=True)
            notify(msg)

    # --- реальные пробы: журнал, статусы, исполнения (решение клиента 11.11, п. 3) ---------------

    def precisions(self) -> tuple[int, int, Decimal]:
        info = self.market.info or {}
        return (int(info.get("quotePrecision", 8)), int(info.get("baseAssetPrecision", 8)),
                Decimal(str(info.get("quoteAmountPrecision") or 1)))

    def filters(self) -> dict:
        """Биржевые ограничения пары на момент ордера: шаг цены и количества, минимальная сумма, полоса цены."""
        info = self.market.info or {}
        price_dec, qty_dec, min_amount = self.precisions()
        band = {k: f.get(k) for f in info.get("filters") or [] if f.get("filterType") == "PERCENT_PRICE_BY_SIDE"
                for k in ("bidMultiplierUp", "bidMultiplierDown", "askMultiplierUp", "askMultiplierDown")}
        return {"priceStep": str(Decimal(1).scaleb(-price_dec)), "qtyStep": str(Decimal(1).scaleb(-qty_dec)),
                "minNotional": str(min_amount), "maxQuoteAmount": info.get("maxQuoteAmount"), "priceBand": band,
                "apiAllowed": info.get("isSpotTradingAllowed"), "status": info.get("status")}

    @staticmethod
    def outcome(r: Resp) -> str:
        """Класс ответа на подачу ордера: принят, API закрыт, ценовое правило, количество/номинал/остаток, неясно."""
        if r.error:
            return "AMBIGUOUS"
        if r.ok:
            return "ACCEPTED"
        code = r.body.get("code") if isinstance(r.body, dict) else None
        if code == 10007:
            return "API_CLOSED"
        if code in (30010, 30087, 30083, 30088):
            return "PRICE_RULE"
        if code in (30002, 30004, 30005, 30029):
            return "QTY_NOTIONAL_BALANCE"
        return f"AMBIGUOUS_{code}"

    def wait_order(self, order_id: str, timeout_s: float = 3.0) -> tuple[dict, list]:
        """Опрос GET /api/v3/order до окончательного статуса или таймаута: (последний ответ, история статусов)."""
        deadline = time.monotonic() + timeout_s
        last: dict = {}
        history: list = []
        while True:
            q = self.client.call("GET", "/api/v3/order", {"symbol": self.symbol, "orderId": order_id}, signed=True)
            if q.ok and isinstance(q.body, dict):
                last = q.body
                point = [now_ms(), last.get("status"), last.get("executedQty")]
                if not history or history[-1][1:] != point[1:]:
                    history.append(point)
                if last.get("status") in TERMINAL:
                    return last, history
            elif not history or history[-1][1] != q.label():
                history.append([now_ms(), q.label(), None])
            if time.monotonic() > deadline:
                return last, history
            time.sleep(0.2)

    def trades(self, order_id: str) -> tuple[Decimal, Decimal, dict, list]:
        """Сделки ордера (GET /api/v3/myTrades): количество, сумма, комиссии по валютам, сами сделки."""
        r = self.client.call("GET", "/api/v3/myTrades", {"symbol": self.symbol, "orderId": order_id}, signed=True)
        rows = r.body if r.ok and isinstance(r.body, list) else []
        qty = sum((Decimal(str(t.get("qty") or 0)) for t in rows), Decimal(0))
        quote = sum((Decimal(str(t.get("quoteQty") or 0)) for t in rows), Decimal(0))
        fees: dict[str, Decimal] = {}
        for t in rows:
            asset = t.get("commissionAsset") or "?"
            fees[asset] = fees.get(asset, Decimal(0)) + Decimal(str(t.get("commission") or 0))
        return qty, quote, fees, (rows if r.ok else [{"error": r.label()}])

    def settle(self, order_id: str, check: str, ref_price: Decimal | None, side: str, timeout_s: float = 3.0,
               cancel: bool = True) -> dict:
        """Окончательный результат ордера: ждём статус; не дождались — отмена и ещё ожидание; всё ещё нет —
        UNKNOWN. Исполнение — по executedQty окончательного (или последнего известного) ответа, комиссии — по
        myTrades. Принятие ордера исполнением не считается, отменённый с executedQty = 0 — не исполнен."""
        final, history = self.wait_order(order_id, timeout_s)
        if final.get("status") not in TERMINAL and cancel:
            r = self.client.call("DELETE", "/api/v3/order", {"symbol": self.symbol, "orderId": order_id}, signed=True)
            history.append([now_ms(), "CANCEL_SENT " + ("OK" if r.ok else r.label()), None])
            final2, history2 = self.wait_order(order_id, 2.0)
            final, history = final2 or final, history + history2
        terminal = final.get("status") in TERMINAL
        qty = Decimal(str(final.get("executedQty") or 0))
        quote = Decimal(str(final.get("cummulativeQuoteQty") or final.get("cumulativeQuoteQty") or 0))
        t_qty, t_quote, fees, rows = self.trades(order_id) if qty > 0 else (Decimal(0), Decimal(0), {}, [])
        quote_asset = (self.market.info or {}).get("quoteAsset", "USDT")
        base_asset = (self.market.info or {}).get("baseAsset", "")
        fee_quote = fees.get(quote_asset, Decimal(0))
        fee_base = fees.get(base_asset, Decimal(0))
        avg = (quote / qty).quantize(Decimal("1e-10")).normalize() if qty else None
        slip = None
        if avg and ref_price:
            slip = float((avg / ref_price - 1) * 100) * (1 if side == "BUY" else -1)  # > 0 — хуже референса
        status = final.get("status") if terminal else "UNKNOWN"
        state = (f"status={status} qty={qty} {quote_asset.lower()}={quote:.4f} avg={avg} fee={dict(fees)} "
                 + (f"slippage={slip:+.2f}%" if slip is not None else ""))
        self.record(check, None, state, qty > 0, {
            "orderId": order_id, "body": final, "status_history": history, "terminal": terminal,
            "ref_price": str(ref_price) if ref_price else None, "slippage_pct": slip, "avg_price": str(avg),
            "trades": rows, "trades_qty": str(t_qty), "trades_quote": str(t_quote),
            "commission": {k: str(v) for k, v in fees.items()}})
        risk.register_order_by_id(order_id, path=self.risk_path, status=status, executed=str(qty))
        # комиссия в третьей валюте (например, MX): в котируемую по котировке, доступной сразу после исполнения;
        # котировки нет — UNVALUED_FEE: величина и валюта отдельно, PnL неполный, в бюджет — консервативный резерв
        fee_other_quote, unvalued, valuation = Decimal(0), {}, []
        for asset, amount in fees.items():
            if asset in (quote_asset, base_asset) or amount <= 0:
                continue
            tr = self.client.call("GET", "/api/v3/ticker/price", {"symbol": f"{asset}{quote_asset}"})
            px = Decimal(str(tr.body.get("price") or 0)) if tr.ok and isinstance(tr.body, dict) else Decimal(0)
            fee_ts = max((int(t.get("time") or 0) for t in rows if isinstance(t, dict)), default=None)
            if px > 0:  # котировка получена после исполнения: оценка расхода, не знание на момент решения
                fee_other_quote += amount * px
                valuation.append({"asset": asset, "amount": str(amount), "price": str(px), "value": str(amount * px),
                                  "source": f"/api/v3/ticker/price {asset}{quote_asset}", "fee_ts": fee_ts,
                                  "quote_ts": now_ms()})
            else:
                unvalued[asset] = str(amount)
        if valuation or unvalued:
            self.record(f"{check}_fee_other", None, f"комиссия в третьей валюте: оценено {fee_other_quote} "
                        f"{quote_asset}" + (f", не оценено {unvalued} — UNVALUED_FEE" if unvalued else ""),
                        not unvalued, {"orderId": order_id, "valuation": valuation, "unvalued": unvalued})
        return {"status": status, "terminal": terminal, "qty": qty, "quote": quote, "fee_quote": fee_quote,
                "fee_base": fee_base, "avg": avg, "fee_other_quote": fee_other_quote, "unvalued": unvalued}

    def base_free(self, asset: str) -> Decimal:
        return self.balance(asset)

    def balance(self, asset: str) -> Decimal:
        r = self.client.call("GET", "/api/v3/account", signed=True)
        if r.ok and isinstance(r.body, dict):
            for b in r.body.get("balances", []):
                if b.get("asset") == asset:
                    return Decimal(str(b.get("free") or 0))
        return Decimal(0)

    def place(self, check: str, params: dict) -> str | None:
        """Ставит ордер и пишет полный журнал подачи; возвращает orderId или None. Если ответа нет (сеть),
        ордер ищется по clientOrderId; не нашли и не смогли проверить — UNKNOWN и блокировка проб."""
        cid = new_cid(check)
        params = {**params, "symbol": self.symbol, "newClientOrderId": cid}
        # принадлежность пробе — в общем состоянии до отправки: сверка после падения отменяет только такие ордера
        risk.register_order(cid, path=self.risk_path, symbol=self.symbol, check=check, side=params.get("side"),
                            type=params.get("type"), round_id=self.round_id, status="SENDING",
                            price=params.get("price"))
        req_ts = now_ms()
        r = self.client.call("POST", "/api/v3/order", params, signed=True)
        self.note_cap(check, r)
        order_id = r.body.get("orderId") if r.ok and isinstance(r.body, dict) else None
        extra = {"params": params, "body": r.body, "clientOrderId": cid, "req_ts": req_ts, "resp_ts": now_ms(),
                 "outcome": self.outcome(r), "filters": self.filters(),
                 "bid": self.market.bids[0][0] if self.market.bids else None,
                 "ask": self.market.asks[0][0] if self.market.asks else None,
                 "book_ts": self.book_ts, "book_src": "REST depth top-20 (не синхронизированный стакан)"}
        if extra["outcome"].startswith("AMBIGUOUS"):  # нет ответа или неясный код: принят ли — сверка по clientOrderId
            q = self.client.call("GET", "/api/v3/order", {"symbol": self.symbol, "origClientOrderId": cid}, signed=True)
            if q.ok and isinstance(q.body, dict) and q.body.get("orderId"):
                order_id = q.body["orderId"]
                extra.update(reconciled=q.body, outcome="ACCEPTED_RECONCILED")
            elif q.http is not None and not q.error and isinstance(q.body, dict) and q.body.get("code"):
                extra.update(reconciled=q.body, outcome="NOT_PLACED_RECONCILED")
            else:
                extra.update(reconciled=q.label(), outcome="UNKNOWN")
                risk.block(self.symbol, f"{check}: нет ответа на подачу ордера и не удалось проверить clientOrderId",
                           path=self.risk_path, clientOrderId=cid)
                notify(f"!!! {self.symbol}: {check} — статус подачи неизвестен ({cid}); пробные BUY заблокированы.")
        risk.register_order(cid, path=self.risk_path, orderId=order_id,
                            code=r.body.get("code") if isinstance(r.body, dict) else None,
                            status="ACCEPTED" if order_id else extra["outcome"])
        self.record(check, r, "ACCEPTED" if order_id else r.label(), bool(order_id), extra)
        return order_id

    def refresh_book(self) -> tuple[Decimal, Decimal]:
        self.check_depth()
        bid = Decimal(self.market.bids[0][0]) if self.market.bids else Decimal(0)
        ask = Decimal(self.market.asks[0][0]) if self.market.asks else Decimal(0)
        return bid, ask

    # --- MARKET_EXECUTION_PROBE: один пробный MARKET BUY и продажа купленного -------------------

    def market_gate(self) -> tuple[str | None, bool]:
        """(почему MARKET BUY сейчас нельзя, окончательно ли). Условия клиента 11.11, п. 3.1."""
        a = self.args
        if self.market_attempts >= MAX_MARKET_ATTEMPTS:
            return f"stop: попыток MARKET BUY {self.market_attempts} — одна на пару", True
        if self.earlier_market_probe():
            return "stop: MARKET-проба по паре уже была в прошлом запуске", True
        reason = self.real_buy_gate("market_buy")
        if reason:
            return reason, reason.startswith("stop")
        reason = risk.gate(self.symbol, a.market_test_usdt, path=self.risk_path)
        if reason:
            return reason, False
        reason = risk.watch_ok(path=self.risk_path)  # временное правило C-04: позиция — только под наблюдением
        if reason:
            return f"waiting: {reason}", False
        if a.real_order:  # API открыт = проба API_AVAILABILITY (LIMIT BUY ниже рынка) принята и отменена
            if self.api_ok_mono is None:
                return "waiting: торговый API не подтверждён пробой доступности", False
        elif (self.market.info or {}).get("isSpotTradingAllowed") is not True:
            return "waiting: isSpotTradingAllowed не true", False
        if not (self.market.bids and self.market.asks):
            return "waiting: нет двустороннего стакана", False
        bid, ask = Decimal(self.market.bids[0][0]), Decimal(self.market.asks[0][0])
        spread = float((ask - bid) / ((ask + bid) / 2) * 100)
        bid_depth = sum(Decimal(p) * Decimal(q) for p, q in self.market.bids if Decimal(p) >= bid * Decimal("0.9"))
        if spread > a.market_test_max_spread or bid_depth < 2 * Decimal(str(a.market_test_usdt)):
            # тест стоит денег на спреде и проскальзывании — ждём нормального рынка
            return f"waiting: spread {spread:.2f} % / bids 10 % {float(bid_depth):.2f}", False
        return None, False

    def earlier_market_probe(self) -> bool:
        """Принятый MARKET BUY по этой паре в прошлых логах пробы (перезапуск процесса — не повод для второй)."""
        for p in sorted(self.log_path.parent.glob(f"probe_{self.symbol}_2*.jsonl")):
            if p == self.log_path:
                continue
            with p.open(encoding="utf-8") as f:
                if any('"check": "market_buy"' in ln and '"state": "ACCEPTED"' in ln for ln in f):
                    return True
        return False

    def check_market_test(self) -> None:
        a = self.args
        if self.market_test_done or self.open_ms is None:
            return
        if time.monotonic() - self.last_market_attempt < a.market_test_every:
            return
        reason, final = self.market_gate()
        if reason:
            self.record("market_test", None, reason, False)
            if final:
                self.market_test_done = True
            return
        self.last_market_attempt = time.monotonic()
        ask = Decimal(self.market.asks[0][0])
        usdt = Decimal(str(a.market_test_usdt))
        self.round_id = f"{self.symbol}-M-{now_ms()}"
        reason = risk.reserve(self.symbol, float(usdt), "MARKET_EXECUTION_PROBE", self.round_id, path=self.risk_path)
        if reason:  # атомарная проверка: другой процесс успел занять лимит
            self.record("market_test", None, reason, False)
            return
        buy_id = self.place("market_buy", {"side": "BUY", "type": "MARKET", "quoteOrderQty": str(usdt)})
        if not buy_id:
            code = (self.states["market_buy"].last or "?")
            self.count_order(code.split()[1] if code.startswith("ERR ") else code, self.market_stats)
            self.market_attempts += 1
            self.market_test_done = True
            if not risk.is_blocked(self.risk_path):  # не принят — позиции нет; попытка пары израсходована
                risk.close_position(self.symbol, {"kind": "MARKET_EXECUTION_PROBE", "round_id": self.round_id,
                                                  "bought": "0", "pnl_usdt": 0.0, "pnl_status": "NET_PNL_COMPLETE",
                                                  "note": f"BUY не принят: {code}"}, path=self.risk_path)
            return
        self.count_order("OK", self.market_stats)
        self.market_attempts += 1
        self.market_test_done = True
        buy = self.settle(buy_id, "market_buy_fill", ask, "BUY")
        bought = buy["qty"] - buy["fee_base"]  # комиссия BUY на MEXC — в USDT; если в монете — меньше купленного
        if not buy["terminal"]:
            risk.block(self.symbol, f"MARKET BUY {buy_id}: статус не подтверждён", path=self.risk_path, orderId=buy_id)
        if bought <= 0:
            self.finish_round(buy, None, Decimal(0), "BUY без исполнения — позиции нет" if buy["terminal"]
                              else "статус BUY не подтверждён — позиция не определена, нужна сверка")
            return
        risk.update_position(self.symbol, path=self.risk_path, stage="POSITION", qty=str(bought), buy_id=buy_id)
        ex = self.exit_position(bought, "")
        self.finish_round(buy, ex, bought, "")

    def check_limit_research(self) -> None:
        """Реальный LIMIT BUY по правилам стратегии: после подтверждённого API, отдельным раундом (не вместе с
        MARKET по той же паре — общая одна открытая позиция), цена bestAsk + 1 % с учётом полосы биржи, ожидание
        buyFillTimeoutMs, затем отмена с подтверждением; исполнение — по фактическим сделкам; продажа только
        купленного; ожидаемое исполнение модели PAPER на том же снимке пишется рядом для сравнения."""
        a = self.args
        if not (LIMIT_RESEARCH_ENABLED and getattr(a, "limit_research", False)) or self.limit_done:
            return
        if getattr(a, "real_buy_mode", "market") != "limit" or self.open_ms is None:  # предрегистрировано до T0
            return
        reason = self.real_buy_gate("limit_buy")
        if reason is None:
            reason = risk.gate(self.symbol, a.market_test_usdt, path=self.risk_path, kind="LIMIT_EXECUTION_RESEARCH")
        if reason is None and self.api_ok_mono is None:
            reason = "waiting: торговый API не подтверждён пробой доступности"
        if reason is None:
            reason = risk.watch_ok(path=self.risk_path)
        if reason:
            self.note_once("limit_test", reason)
            if reason.startswith(("stop", "limit: по паре уже был")):
                self.limit_done = True
            return
        t_book = now_ms()
        bid, ask = self.refresh_book()
        age = now_ms() - (self.book_ts or 0)
        if not (bid and ask) or age > LIMIT_BOOK_MAX_AGE_MS:
            self.note_once("limit_test", f"waiting: стакан не свежий ({age} мс) или односторонний")
            return
        price_dec, qty_dec, min_amount = self.precisions()
        pstep, qstep = Decimal(1).scaleb(-price_dec), Decimal(1).scaleb(-qty_dec)
        limit = (ask * (1 + LIMIT_AGGRESSIVE)).quantize(pstep, ROUND_DOWN)
        band = next((f for f in (self.market.info or {}).get("filters") or []
                     if f.get("filterType") == "PERCENT_PRICE_BY_SIDE"), {})
        if self.last_price and band.get("bidMultiplierUp"):
            limit = min(limit, (self.last_price * (1 + Decimal(str(band["bidMultiplierUp"])))).quantize(pstep, ROUND_DOWN))
        usdt = Decimal(str(a.market_test_usdt))
        qty = (usdt / limit).quantize(qstep, ROUND_DOWN)
        if limit < ask or qty * limit < min_amount:
            self.note_once("limit_test", f"skipped: цена {limit} ниже ask {ask} (полоса) или сумма меньше минимума")
            self.limit_done = True
            return
        exp_qty, exp_cost = Decimal(0), Decimal(0)  # модель PAPER на том же снимке: asks до цены лимита
        for px, q in self.market.asks:
            px, q = Decimal(px), Decimal(q)
            if px > limit or exp_qty >= qty:
                break
            take = min(q, qty - exp_qty)
            exp_qty, exp_cost = exp_qty + take, exp_cost + take * px
        self.round_id = f"{self.symbol}-L-{now_ms()}"
        reason = risk.reserve(self.symbol, float(usdt), "LIMIT_EXECUTION_RESEARCH", self.round_id, path=self.risk_path)
        if reason:
            self.note_once("limit_test", reason)
            return
        self.limit_done = True
        model = {"book_age_ms": age, "book_t": t_book, "bid": str(bid), "ask": str(ask), "limit": str(limit),
                 "qty": str(qty), "model_qty": str(exp_qty),
                 "model_avg": str((exp_cost / exp_qty).quantize(Decimal("1e-10"))) if exp_qty else None}
        self.record("limit_test", None, f"LIMIT BUY {qty} по {limit} (ask {ask}); модель: {exp_qty}", False, model)
        buy_id = self.place("limit_buy", {"side": "BUY", "type": "LIMIT", "quantity": str(qty), "price": str(limit)})
        if not buy_id:
            self.note_reject("limit_buy")
            if not risk.is_blocked(self.risk_path):
                risk.close_position(self.symbol, {"kind": "LIMIT_EXECUTION_RESEARCH", "round_id": self.round_id,
                                                  "bought": "0", "pnl_usdt": 0.0, "pnl_status": "NET_PNL_COMPLETE",
                                                  "note": f"LIMIT BUY не принят: {self.states['limit_buy'].last}"},
                                    path=self.risk_path)
            return
        buy = self.settle(buy_id, "limit_buy_fill", ask, "BUY", timeout_s=LIMIT_FILL_TIMEOUT_S)
        bought = buy["qty"] - buy["fee_base"]
        self.record("limit_vs_model", None, f"факт {buy['qty']} по {buy['avg']} против модели {exp_qty} по "
                    f"{model['model_avg']}", True, {**model, "fact_qty": str(buy["qty"]), "fact_avg": str(buy["avg"]),
                                                    "status": buy["status"]})
        if not buy["terminal"]:
            risk.block(self.symbol, f"LIMIT BUY {buy_id}: статус не подтверждён", path=self.risk_path, orderId=buy_id)
        if bought <= 0:
            self.finish_round(buy, None, Decimal(0), "LIMIT BUY без исполнения — позиции нет" if buy["terminal"]
                              else "статус LIMIT BUY не подтверждён — нужна сверка", kind="LIMIT_EXECUTION_RESEARCH")
            return
        risk.update_position(self.symbol, path=self.risk_path, stage="POSITION", qty=str(bought), buy_id=buy_id)
        ex = self.exit_position(bought, "limit_")
        self.finish_round(buy, ex, bought, "", kind="LIMIT_EXECUTION_RESEARCH")

    def finish_round(self, buy: dict, ex: dict | None, bought: Decimal, note: str,
                     kind: str = "MARKET_EXECUTION_PROBE") -> None:
        """Итог раунда: реализованный PnL (с комиссиями) и непроданный остаток отдельно; закрытие или блокировка."""
        base = (self.market.info or {}).get("baseAsset", "")
        _, _, min_amount = self.precisions()
        sold = ex["sold"] if ex else Decimal(0)
        cost = buy["quote"] + buy["fee_quote"]
        cost_sold = cost * sold / bought if bought else Decimal(0)
        received = (ex["quote"] - ex["fee_quote"]) if ex else Decimal(0)
        left = bought - sold
        bid = ex["bid"] if ex else Decimal(0)
        left_value = left * bid
        position_after = self.base_free(base) if base else None
        fee_other = buy.get("fee_other_quote", Decimal(0)) + (ex["fee_other_quote"] if ex else Decimal(0))
        unvalued = {**buy.get("unvalued", {}), **((ex or {}).get("unvalued") or {})}
        pnl = received - cost_sold - fee_other
        reserve = (buy["quote"] + (ex["quote"] if ex else Decimal(0))) * Decimal(str(risk.UNVALUED_FEE_RESERVE_PCT)) \
            if unvalued else Decimal(0)
        pnl_status = "PNL_INCOMPLETE/UNVALUED_FEE" if unvalued else "NET_PNL_COMPLETE"
        rec = {"kind": kind, "round_id": self.round_id, "buy_status": buy["status"], "bought": str(bought),
               "sold": str(sold), "spent_usdt": str(cost), "received_usdt": str(received),
               "fee_other_usdt": str(fee_other), "unvalued_fee": unvalued, "pnl_status": pnl_status,
               "fee_reserve_usdt": float(reserve),  # допущение риск-бюджета, не уплаченная комиссия
               "pnl_net_known_usdt": float(pnl), "pnl_usdt": float(pnl - reserve),  # в бюджет — с резервом
               "left_qty": str(left), "left_value_usdt": float(left_value),
               "position_after": str(position_after), "note": note}
        state = (f"bought={bought} sold={sold} realized_pnl={pnl:+.4f} USDT ({pnl_status}) "
                 f"left={left} {base} (~{left_value:.4f} USDT) position_after={position_after}")
        self.record("market_roundtrip" if kind == "MARKET_EXECUTION_PROBE" else "limit_roundtrip", None, state, True,
                    rec)
        unsold = left_value >= min_amount
        unknown = (ex or {}).get("unknown") or not buy["terminal"]
        if unsold or unknown:
            why = "неопределённый статус ордера" if unknown else f"непроданный остаток {left} {base}"
            risk.update_position(self.symbol, path=self.risk_path, stage="OPEN_UNRESOLVED", **rec)
            risk.block(self.symbol, why, path=self.risk_path)
            notify(f"!!! {self.symbol}: MARKET-проба — {why}. Позиция не закрыта, новые пробные BUY заблокированы "
                   f"до сверки (python tools/probe_risk.py --clear ...).\n{state}")
        else:
            risk.close_position(self.symbol, {**rec, "dust_qty": str(left)}, path=self.risk_path)
        notify(f"🧪 MARKET-проба {self.symbol}: "
               + "; ".join(f"{c}: {self.states[c].last}" for c in
                           ("market_buy", "market_buy_fill", "market_sell_fill", "fallback_sell_fill")
                           if c in self.states) + f"\nИтог: {state}")

    def exit_position(self, bought: Decimal, prefix: str) -> dict:
        """Продаёт только фактически купленное и доступное (min(купленное − проданное, свободный остаток)):
        MARKET SELL, затем до трёх LIMIT по bid −2 % с отменой и подтверждением. При неопределённом статусе
        любой продажи новых SELL не ставит (конфликтующие продажи запрещены до сверки).
        LIMIT SELL половины на −50 % (прежний far_limit_sell) приостановлен: не входит в утверждённый MARKET-раунд."""
        price_dec, qty_dec, min_amount = self.precisions()
        base = (self.market.info or {}).get("baseAsset", "")
        step = Decimal(1).scaleb(-qty_dec)
        out = {"sold": Decimal(0), "quote": Decimal(0), "fee_quote": Decimal(0), "unknown": False, "bid": Decimal(0),
               "fee_other_quote": Decimal(0), "unvalued": {}}

        def sell_qty() -> tuple[Decimal, Decimal]:
            bid, _ = self.refresh_book()
            out["bid"] = bid
            free = Decimal(0)
            for _ in range(5):  # баланс может обновиться не мгновенно
                free = self.base_free(base)
                if free > 0:
                    break
                time.sleep(0.2)
            return bid, min(bought - out["sold"], free).quantize(step, ROUND_DOWN)

        def add(res: dict) -> None:
            out["sold"] += res["qty"]
            out["quote"] += res["quote"]
            out["fee_quote"] += res["fee_quote"]
            out["fee_other_quote"] += res.get("fee_other_quote", Decimal(0))
            out["unvalued"].update(res.get("unvalued") or {})
            out["unknown"] |= not res["terminal"]
            # прогресс выхода — в общем состоянии: после падения продаётся только непроданное
            risk.update_position(self.symbol, path=self.risk_path, left_qty=str(bought - out["sold"]),
                                 left_value_usdt=None)

        bid, qty = sell_qty()
        if bid and qty * bid >= min_amount:
            sid = self.place(f"{prefix}market_sell", {"side": "SELL", "type": "MARKET", "quantity": str(qty)})
            if sid:
                add(self.settle(sid, f"{prefix}market_sell_fill", bid, "SELL"))
        else:
            self.record(f"{prefix}market_sell", None, f"skipped: остаток {qty} меньше минимума", False)

        for _ in range(3):  # запасной выход: MARKET SELL отклонён, отменён или исполнен частично
            if out["unknown"]:
                break
            bid, qty = sell_qty()
            if not bid or qty * bid < min_amount:
                break
            price = (bid * Decimal("0.98")).quantize(Decimal(1).scaleb(-price_dec), ROUND_DOWN)
            fb_id = self.place(f"{prefix}fallback_sell", {"side": "SELL", "type": "LIMIT", "quantity": str(qty),
                                                          "price": str(price)})
            if not fb_id:
                time.sleep(1)
                continue
            add(self.settle(fb_id, f"{prefix}fallback_sell_fill", bid, "SELL", timeout_s=2.0))
        if not out["bid"]:
            out["bid"], _ = self.refresh_book()
        if out["unknown"]:
            risk.block(self.symbol, "статус продажи не подтверждён", path=self.risk_path)
        return out

    def reconcile(self, force: bool = False) -> None:
        """Сверка и защита (решения клиента 12.11): при старте и раз в RECONCILE_EVERY_S с. Повторный запуск
        результат не дублирует (раунд закрывается один раз по round_id).
        1) Открытые ордера пары: отменяются только зарегистрированные пробой (clientOrderId в общем состоянии), с
           подтверждением; исполнение отменённого BUY своего раунда — количество позиции, чужого (проба API /
           LIMIT_MAKER) — защитный выход; исполнение отменённого SELL уменьшает остаток. Ордер с префиксом пробы, но
           без регистрации — неясная принадлежность: не трогаем, блокировка. Прочие ордера не трогаем.
        2) Позиция BUY_SENT (процесс упал после резерва): BUY раунда ищется по clientOrderId — исполнен → количество
           позиции; не исполнен / не подан → раунд закрыт без позиции; проверить нельзя → блокировка.
        3) Незакрытая позиция с подтверждённым количеством — продажа только непроданного остатка, не больше
           свободного, тем же защищённым выходом, независимо от блокировки новых BUY."""
        if not self.client.has_keys or not self.market.info:
            return
        if not force and time.monotonic() - self.last_reconcile < RECONCILE_EVERY_S:
            return
        self.last_reconcile = time.monotonic()
        r = self.client.call("GET", "/api/v3/openOrders", {"symbol": self.symbol}, signed=True)
        if not r.ok or not isinstance(r.body, list):  # без списка открытых ордеров продавать нельзя — конфликт
            self.record("reconcile", r, f"openOrders не получен ({r.label()}) — сверка отложена", False)
            return
        pos = risk.position(self.symbol, path=self.risk_path)
        for o in r.body:
            cid = str(o.get("clientOrderId") or "")
            own = risk.owned_order(cid, path=self.risk_path)
            if own:
                self.record("reconcile", None, f"открытый пробный ордер {o.get('orderId')} ({cid}) — отмена", False,
                            {"order": o, "owned": own})
                self.cancel_owned(o["orderId"], own, pos)
                pos = risk.position(self.symbol, path=self.risk_path)
            elif cid.startswith(PROBE_CIDS):
                risk.block(self.symbol, f"открытый ордер {o.get('orderId')} ({cid}) с префиксом пробы без регистрации — "
                           "принадлежность неясна, не отменён", path=self.risk_path)
                notify(f"!!! {self.symbol}: ордер {cid} похож на пробный, но не зарегистрирован — не отменён, "
                       f"пробные BUY заблокированы до сверки.")
        if not pos:
            return
        if pos.get("stage") in ("BUY_SENT", "RECONCILIATION_REQUIRED"):
            if not self.resolve_buy_sent(pos):
                return
            pos = risk.position(self.symbol, path=self.risk_path)
            if not pos:
                return
        qty = Decimal(str(pos.get("left_qty") or pos.get("qty") or 0))
        if qty <= 0:
            risk.block(self.symbol, f"пробная позиция без подтверждённого количества ({pos.get('stage')})",
                       path=self.risk_path)
            self.note_once("reconcile", f"позиция {pos.get('stage')} без количества — блокировка, нужна ручная сверка")
            return
        self.record("reconcile", None, f"незакрытая пробная позиция {qty} ({pos.get('stage')}) — защитный выход",
                    False, {"position": pos})
        self.round_id = pos.get("round_id")
        ex = self.exit_position(qty, "reconcile_")
        left = qty - ex["sold"]
        _, _, min_amount = self.precisions()
        rec = {"kind": pos.get("kind") or "RECONCILE_EXIT", "round_id": pos.get("round_id"), "sold": str(ex["sold"]),
               "left_qty": str(left), "left_value_usdt": float(left * ex["bid"]),
               "received_usdt": str(ex["quote"] - ex["fee_quote"]), "closed_by": "reconcile"}
        if left * ex["bid"] < min_amount and not ex["unknown"]:
            risk.close_position(self.symbol, {**rec, "pnl_usdt": 0.0, "pnl_status": "PNL_INCOMPLETE/RECONCILED",
                                              "note": "закрыта сверкой; PnL раунда — по журналу пробы"},
                                path=self.risk_path)
            notify(f"{self.symbol}: незакрытая пробная позиция продана при сверке ({ex['sold']}).")
        else:
            risk.update_position(self.symbol, path=self.risk_path, stage="OPEN_UNRESOLVED", **rec)
            risk.block(self.symbol, f"сверка не закрыла позицию: остаток {left}", path=self.risk_path)

    def cancel_owned(self, order_id: str, own: dict, pos: dict | None) -> None:
        """Отмена своего открытого ордера после падения, подтверждение статуса и учёт исполненной части."""
        self.client.call("DELETE", "/api/v3/order", {"symbol": self.symbol, "orderId": order_id}, signed=True)
        fin = self.settle(order_id, "reconcile_final", None, own.get("side") or "BUY", timeout_s=2.0, cancel=False)
        if not fin["terminal"]:
            risk.block(self.symbol, f"после падения не подтверждена отмена ордера {order_id}", path=self.risk_path)
            return
        mine = pos and own.get("round_id") and own.get("round_id") == pos.get("round_id")
        if own.get("side") == "SELL" and fin["qty"] > 0 and pos:
            left = Decimal(str(pos.get("left_qty") or pos.get("qty") or 0)) - fin["qty"]
            risk.update_position(self.symbol, path=self.risk_path, left_qty=str(max(left, Decimal(0))))
        elif own.get("side") == "BUY" and fin["qty"] > 0:
            if mine:
                q = fin["qty"] - fin["fee_base"]
                risk.update_position(self.symbol, path=self.risk_path, stage="POSITION", qty=str(q), left_qty=str(q),
                                     spent_usdt=str(fin["quote"] + fin["fee_quote"]))
            else:
                self.protective_exit(fin["qty"], fin, "reconcile")

    def find_buy(self, cid: str, since_ms: int) -> tuple[str, dict | None]:
        """Поиск BUY раунда после падения по двум источникам: ордер по clientOrderId и сделки пары с момента
        регистрации. ('found', {orderId}) / ('absent', ответ) — оба источника отвечают и ни один его не знает /
        ('error', описание) — хотя бы один источник не ответил или ответил неоднозначно."""
        q = self.client.call("GET", "/api/v3/order", {"symbol": self.symbol, "origClientOrderId": cid}, signed=True)
        if q.ok and isinstance(q.body, dict) and q.body.get("orderId"):
            return "found", q.body
        t = self.client.call("GET", "/api/v3/myTrades", {"symbol": self.symbol, "startTime": since_ms - 5000},
                             signed=True)
        if t.ok and isinstance(t.body, list):
            hit = next((x for x in t.body if isinstance(x, dict) and x.get("clientOrderId") == cid), None)
            if hit:  # исполнение без подтверждения создания: ордер есть
                return "found", {"orderId": hit.get("orderId")}
        order_absent = q.http is not None and not q.error and isinstance(q.body, dict) and q.body.get("code") is not None
        if order_absent and t.ok and isinstance(t.body, list):
            return "absent", {"order": q.body, "trades": len(t.body)}
        return "error", {"order": q.label(), "trades": t.label() if not t.ok else "ok"}

    def resolve_buy_sent(self, pos: dict) -> bool:
        """Позиция BUY_SENT / RECONCILIATION_REQUIRED после падения (C-05). True — количество позиции подтверждено.
        «Ордера нет» (NO_ORDER_CONFIRMED) — только после NEG_CHECKS_MIN отрицательных сверок ордера и сделок, не
        раньше NEG_WINDOW_MS после регистрации и с разносом проверок ≥ 30 с; до этого — RECONCILIATION_REQUIRED,
        аварийная блокировка новых реальных BUY и уведомление."""
        rid = pos.get("round_id")
        orders = [(cid, o) for cid, o in risk.orders_of(self.symbol, path=self.risk_path)
                  if o.get("round_id") == rid and o.get("side") == "BUY"]
        if not orders:  # резерв был, регистрации нет — ордер не отправлялся (регистрация идёт до отправки)
            risk.close_position(self.symbol, {"kind": pos.get("kind"), "round_id": rid, "bought": "0", "pnl_usdt": 0.0,
                                              "pnl_status": "NET_PNL_COMPLETE",
                                              "note": "после перезапуска: BUY не регистрировался и не отправлялся"},
                                path=self.risk_path)
            return False
        cid, own = orders[-1]
        order_id = own.get("orderId")
        if not order_id:
            res, info = self.find_buy(cid, int(own.get("ts") or 0))
            if res == "found":
                order_id = info["orderId"]
                risk.register_order(cid, path=self.risk_path, orderId=order_id, status="ACCEPTED_RECONCILED")
            else:
                checks = list(pos.get("neg_checks") or []) + [{"ts": now_ms(), "clientOrderId": cid, "result": res,
                                                                "detail": info}]
                risk.update_position(self.symbol, path=self.risk_path, stage="RECONCILIATION_REQUIRED",
                                     neg_checks=checks)
                neg = [c for c in checks if c["result"] == "absent"]
                if (res == "absent" and len(neg) >= NEG_CHECKS_MIN and now_ms() - int(own.get("ts") or 0) >= NEG_WINDOW_MS
                        and neg[-1]["ts"] - neg[0]["ts"] >= 30_000):
                    risk.register_order(cid, path=self.risk_path, status="NO_ORDER_CONFIRMED")
                    risk.close_position(self.symbol, {"kind": pos.get("kind"), "round_id": rid, "bought": "0",
                                                      "pnl_usdt": 0.0, "pnl_status": "NET_PNL_COMPLETE",
                                                      "note": f"NO_ORDER_CONFIRMED: {len(neg)} отрицательных сверки "
                                                              f"ордера и сделок за {(neg[-1]['ts'] - neg[0]['ts']) // 1000} с",
                                                      "neg_checks": checks}, path=self.risk_path)
                    return False
                risk.block(self.symbol, f"RECONCILIATION_REQUIRED: BUY {cid} не подтверждён и не опровергнут ({res})",
                           path=self.risk_path)
                self.note_once("reconcile", f"BUY {cid}: {res} — RECONCILIATION_REQUIRED, повторная сверка")
                return False
        fin = self.settle(order_id, "reconcile_buy", None, "BUY", timeout_s=2.0)
        if not fin["terminal"]:
            risk.block(self.symbol, f"после перезапуска статус BUY {order_id} не подтверждён", path=self.risk_path)
            return False
        q = fin["qty"] - fin["fee_base"]
        if q <= 0:
            risk.close_position(self.symbol, {"kind": pos.get("kind"), "round_id": rid, "bought": "0", "pnl_usdt": 0.0,
                                              "pnl_status": "NET_PNL_COMPLETE",
                                              "note": f"после перезапуска: BUY {fin['status']} без исполнения"},
                                path=self.risk_path)
            return False
        risk.update_position(self.symbol, path=self.risk_path, stage="POSITION", qty=str(q), left_qty=str(q),
                             spent_usdt=str(fin["quote"] + fin["fee_quote"]))
        return True

    def protective_exit(self, executed: Decimal, fin: dict, source: str) -> None:
        """Неожиданное исполнение пробного LIMIT BUY: продать фактически купленное, заблокировать пробы до сверки."""
        risk.open_position(self.symbol, {"stage": "UNEXPECTED_FILL", "source": source, "qty": str(executed)},
                           path=self.risk_path)
        ex = self.exit_position(executed - fin["fee_base"], "protect_")
        left = executed - fin["fee_base"] - ex["sold"]
        cost = fin["quote"] + fin["fee_quote"]
        rec = {"kind": "PROTECTIVE_EXIT", "source": source, "bought": str(executed), "sold": str(ex["sold"]),
               "pnl_usdt": float(ex["quote"] - ex["fee_quote"] - cost * ex["sold"] / executed),
               "left_qty": str(left), "left_value_usdt": float(left * ex["bid"])}
        self.record("protective_exit", None, f"sold={ex['sold']} left={left}", True, rec)
        _, _, min_amount = self.precisions()
        if left * ex["bid"] < min_amount and not ex["unknown"]:
            risk.close_position(self.symbol, rec, path=self.risk_path)
        else:
            risk.update_position(self.symbol, path=self.risk_path, stage="OPEN_UNRESOLVED", **rec)
        risk.block(self.symbol, f"неожиданное исполнение пробного LIMIT ({source})", path=self.risk_path, **rec)
        notify(f"!!! {self.symbol}: защитный выход после исполнения пробного LIMIT ({source}): "
               f"продано {ex['sold']}, остаток {left}. Пробные BUY заблокированы до сверки.")

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
        self.note_once("preopen_cap", "suspended: реальные ордера до открытия торгов не ставятся (решение 11.11)")

    def cap_probe_history(self) -> dict:
        """Подачи CAP-пробы пары в окне листинга (реестр): число подач, принятых, техотказов, 30010, неразрешённые,
        максимальная принятая цена и её время — граница «потолка нет ниже X»."""
        rows = self.window_orders("cap_maker")
        acc = [o for o in rows if o.get("orderId")]
        best = max(acc, key=lambda o: Decimal(str(o.get("price") or 0)), default=None)
        return {"submits": len(rows), "accepted": len(acc),
                "tech": sum(1 for o in rows if o.get("code") in (10007, 30004, 30005)),
                "cap": next((o for o in rows if o.get("code") == 30010), None),
                "unresolved": [o for o in rows if not o.get("orderId") and (
                    o.get("status") in ("SENDING", "UNKNOWN") or str(o.get("status", "")).startswith("AMBIGUOUS"))],
                "max_acc": Decimal(str(best["price"])) if best and best.get("price") else None,
                "max_acc_ts": int(best["ts"]) if best else None}

    def cap_probe_gate(self, params: dict | None) -> str | None:
        """Правило 13.11 (3/6) и read-only условия G1/G2 — до подачи; None — подавать можно."""
        h = self.cap_probe_history()
        if h["cap"]:
            return "stop: CAP_FOUND — 30010 получена, активные CAP-пробы по паре прекращены"
        if h["accepted"] >= CAP_MAX_ACCEPTED:
            return (f"stop: NO_CAP_OBSERVED_IN_PROBES — принятых проб {h['accepted']} без 30010 (предел "
                    f"{CAP_MAX_ACCEPTED}); дальше только read-only наблюдение")
        if h["submits"] >= CAP_MAX_SUBMITS:
            return f"stop: подач CAP-пробы {h['submits']} — жёсткий предел {CAP_MAX_SUBMITS} на пару"
        if h["tech"] >= MAX_TECH_REJECTS:
            return f"stop: технических отказов CAP-пробы {h['tech']} — больше не подаём"
        if h["unresolved"]:
            return "waiting: исход прошлой подачи CAP-пробы не сверен — следующая подача только после сверки"
        if CAP_GATE_SELF_SYMBOLS and not self.self_listed:
            if self.self_last_ok_ts is None or now_ms() - self.self_last_ok_ts > 10_000:
                return self.suppress("G1", params, h, "selfSymbols недоступен или устарел — состояние пары неизвестно "
                                                      "(не «пары нет»); подача не выполняется")
            return self.suppress("G1", params, h, "пары ещё не было в selfSymbols (API для пары не открыт)")
        if params is None:
            return "skipped: no book"
        if CAP_GATE_STEP is not None and h["max_acc"] is not None \
                and Decimal(params["price"]) < h["max_acc"] * (1 + CAP_GATE_STEP):
            return self.suppress("G2", params, h, f"цена пробы ниже максимальной принятой {h['max_acc']} × "
                                                  f"{1 + CAP_GATE_STEP}")
        return None

    def suppress(self, gate: str, params: dict | None, h: dict, why: str) -> str:
        """Пилот G1/G2 (клиент 13.11): каждая подавленная подача — в журнал с причиной, ценами, временем данных и
        версией. G2 выше уже принятой цены — CAP_MAY_HAVE_BEEN_MISSED: реальная заявка могла бы получить 30010 в
        интервале (макс. принятая, цена пробы]; пассивный расчёт не равен ответу биржи на неотправленный ордер."""
        px = Decimal(params["price"]) if params else None
        missed = bool(gate == "G2" and px is not None and h["max_acc"] is not None and px > h["max_acc"])
        extra = {"gate": gate, "why": why, "probe_price": str(px) if px is not None else None,
                 "max_accepted_price": str(h["max_acc"]) if h["max_acc"] is not None else None,
                 "bid": self.market.bids[0][0] if self.market.bids else None, "last_price": str(self.last_price),
                 "book_ts": self.book_ts, "self_symbols_last_ok_ts": self.self_last_ok_ts,
                 "self_symbols_first": self.self_first, "submits": h["submits"], "accepted": h["accepted"],
                 "cap_may_have_been_missed": missed, "version": code_label()}
        if missed:
            self.cap_missed.append({"ts": now_ms(), "rel_ms": self.rel(), "band": [str(h["max_acc"]), str(px)]})
        self.record("cap_suppressed", None, f"{gate}: {why}", False, extra)
        return f"skipped: {gate} (пилот) — {why}"

    def check_cap_maker(self) -> None:
        """После открытия: LIMIT_MAKER BUY выше вероятного потолка. «Только мейкер» не покупает по рынку:
        во время заморозки ждём 30010 с точным потолком, без неё — отказ, потому что ордер исполнился бы сразу.
        Если ордер всё же встал в стакан (широкий спред), он сразу отменяется. Ограничения — cap_probe_gate."""
        reason = self.real_buy_gate("cap_maker")
        if reason:
            self.note_once("cap_maker", reason)
            return
        params = self.cap_buy_params("LIMIT_MAKER", self.above_cap_price())
        reason = self.cap_probe_gate(params)
        if reason:
            self.note_once("cap_maker", reason)
            return
        order_id = self.place("cap_maker", params)
        if order_id:
            self.cancel_order(order_id, "cap_maker")
        else:
            self.note_reject("cap_maker")

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
            h = self.cap_probe_history()
            sent = [f"{o.get('price')} ({self.fmt(int(o['ts'])).strip()})" for o in self.window_orders("cap_maker")
                    if o.get("orderId")]
            if h["accepted"]:
                lines.append(f"NO_CAP_OBSERVED: для отправленных цен {', '.join(sent)} 30010 не получен (подач "
                             f"{h['submits']}). Это наблюдение только по этим ценам и моментам, не CAP_ABSENT.")
            else:
                lines.append(f"CAP_UNKNOWN: 30010 не было, принятых CAP-проб нет (подач {h['submits']})")
        if self.cap_missed:
            lines.append(f"CAP_MAY_HAVE_BEEN_MISSED: G2 подавил {len(self.cap_missed)} проб выше принятой цены — "
                         + "; ".join(f"T{m['rel_ms'] / 1000:+.0f} с ({m['band'][0]}; {m['band'][1]}]"
                                     for m in self.cap_missed[:5] if m["rel_ms"] is not None))
        return lines

    # --- цикл -----------------------------------------------------------------

    def tick(self) -> None:
        self.check_exchange_info()
        self.check_symbol_list("default_syms", "/api/v3/defaultSymbols", signed=False)
        self.check_depth()
        if self.client.has_keys and (self.args.real_order or self.args.market_test):
            self.reconcile()
        if self.client.has_keys:
            self.check_symbol_list("self_syms", "/api/v3/selfSymbols", signed=True)
            self.check_order_test()
            if self.args.real_order:
                self.check_real_order()
            if self.args.market_test and getattr(self.args, "real_buy_mode", "market") == "market":
                self.check_market_test()
            self.check_limit_research()
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
    p.add_argument("--real-buy-mode", choices=("market", "limit"), default="market",
                   help="единственный реальный режим BUY на этот листинг, фиксируется до T0 (market)")
    p.add_argument("--limit-research", action="store_true",
                   help="LIMIT_EXECUTION_RESEARCH: реальный LIMIT BUY по правилам стратегии (NOT ACTIVATED: нужен ещё "
                        "LIMIT_RESEARCH_ENABLED = True по решению клиента)")
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
    stale = risk.supervisor_stale()
    if stale:
        print(f"!!! {stale}", flush=True)
    # пока процесс жив, пара — его: супервизор восстановления (probe_supervisor.py) её не трогает
    with risk.symbol_lock(args.symbol) as mine:
        if not mine and (args.real_order or args.market_test):
            print(f"{args.symbol}: пару уже ведёт другой процесс пробы или супервизор — реальные ордера выключены.")
            args.real_order = args.market_test = False
        try:
            probe.run()
        except KeyboardInterrupt:
            print("\nОстановлено вручную.")
        finally:
            probe.summary()


if __name__ == "__main__":
    main()
