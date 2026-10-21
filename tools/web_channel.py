"""Веб-канал ордеров MEXC (вариант B): ордера через функцию самого сайта в залогиненном браузере.

Нужен для листингов, где пара закрыта для официального API (10007 / apiAllowed=false).
Playwright держит страницу пары с залогиненной сессией. Скрипт достаёт из рантайма Turbopack
обёртку запросов сайта `request` (ту же, что вызывает кнопка Buy/Sell) и ставит ордера через неё.
Подпись (x-mxc-nonce / x-mxc-sign), сдвиг серверного времени и, если MEXC их вернёт,
p0/k0/mhash сайт добавляет сам. Подробности разбора — doc/zhurnal_rabot.md, «20.10 — разведка веб-API».

Эндпоинты сайта:
  POST   /api/platform/spot/order/place        LIMIT_ORDER (price + quantity)
  POST   /api/platform/spot/v4/order/place     MARKET_ORDER (price + amount в USDT | quantity в монете);
                                               биржа исполняет не дальше price ×1.05 (BUY) / ×0.95 (SELL)
  DELETE /api/platform/spot/order/cancel/v2?orderId=…
  GET    /api/platform/spot/market-v2/web/symbolsV2              cd/mcd/ps/qs/ot всех пар
  GET    /api/platform/asset/api/asset/spot/currency/v3?currency=X баланс (заодно проверка логина)
Id веб-ордера (C02__…) виден и в официальном API: /api/v3/order, /api/v3/myTrades.

Команды:
  .venv/Scripts/python tools/web_channel.py login              окно браузера для входа (логин и 2FA вводит человек)
  .venv/Scripts/python tools/web_channel.py import-cookies F   или: cookies mexc.com из JSON-экспорта своего браузера
                                                               (файл удаляется после загрузки)
  .venv/Scripts/python tools/web_channel.py check MXUSDT       самопроверка без ордеров
  .venv/Scripts/python tools/web_channel.py limit-test MXUSDT  РЕАЛЬНЫЙ LIMIT BUY ниже рынка и отмена, замер задержки
Профиль браузера с сессией — .browser/mexc (в .gitignore). На сервере запускать под Xvfb (xvfb-run).
"""

import argparse
import asyncio
import json
import re
import time
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from pathlib import Path

from playwright.async_api import BrowserContext, Page, async_playwright
from playwright.async_api import Error as PlaywrightError

ROOT = Path(__file__).resolve().parent.parent
PROFILE_DIR = ROOT / ".browser" / "mexc"
SITE = "https://www.mexc.com"

PLACE_LIMIT_URL = "/api/platform/spot/order/place"
PLACE_MARKET_URL = "/api/platform/spot/v4/order/place"
CANCEL_URL = "/api/platform/spot/order/cancel/v2"
SYMBOLS_URL = "/api/platform/spot/market-v2/web/symbolsV2"
BALANCE_URL = "/api/platform/asset/api/asset/spot/currency/v3"
ORDER_HEADERS = {"content-type": "application/json", "platform": "WEB"}
DOLOS_SCENE = 27  # needDolos у кнопки ордера: при наличии window.fp сайт добавит p0/k0/mhash
# облегчённая страница: график TradingView, аналитика, push-SDK, капча, рекламный виджет, картинки, шрифты, видео
LITE_BLOCK_RE = re.compile(
    r"charting_library|/sensors/|mePushSdk|gt4\.js|remote-activity-adv-widget"
    r"|\.(png|jpe?g|gif|webp|svg|ico|woff2?|ttf|otf|mp4|webm)(\?|$)", re.IGNORECASE)

# Находит обёртку `request`, которую вызывает кнопка ордера. Turbopack не отдаёт модули наружу,
# поэтому регистрируем свой модуль и запускаем его как runtime-модуль: фабрика получает контекст
# рантайма, где ctx.c — кэш модулей, а среди полей есть Map фабрик. Номера модулей меняются при
# пересборке сайта, поэтому всё ищем по коду:
#   1) фабрика модуля кнопки (в коде есть order/place и needDolos) → какой модуль она импортирует
#      как X и вызывает (0,X.request)(…) — оттуда и берём request;
#   2) запасной путь — в кэше функции request с withSignature и needDolos, самая полная из них
#      (на сайте две версии: 798942 у кнопки и более старая в общей библиотеке 384399).
INJECT_JS = r"""
async () => {
  if (window.__mxweb && window.__mxweb.request) return window.__mxweb.info;
  const tp = globalThis.TURBOPACK;
  if (!tp || typeof tp.push !== 'function') throw new Error('рантайм TURBOPACK не найден');
  const ctx = await new Promise((resolve, reject) => {
    const id = 'mxweb_' + Date.now() + '_' + Math.random().toString(36).slice(2);
    const timer = setTimeout(() => reject(new Error('свой модуль в TURBOPACK не запустился')), 5000);
    tp.push([id + '.js', id, function (c) { clearTimeout(timer); resolve(c); }]);
    tp.push([id + '_rt.js', { otherChunks: [], runtimeModuleIds: [id] }]);
  });
  const fsrc = f => Function.prototype.toString.call(f);
  const isRequest = f => typeof f === 'function' && fsrc(f).includes('withSignature') && fsrc(f).includes('needDolos');
  const exportsOf = id => (ctx.c[id] ? ctx.c[id].exports : ctx.i(id));
  const keys = [...Object.getOwnPropertyNames(Object.getPrototypeOf(ctx)), ...Object.keys(ctx)];
  const factories = keys.map(k => ctx[k]).find(v => v instanceof Map && v.size > 100);
  if (factories) {
    for (const f of factories.values()) {
      if (typeof f !== 'function') continue;
      const src = fsrc(f);
      if (!src.includes('/api/platform/spot/order/place') || !src.includes('needDolos')) continue;
      const imports = {};
      for (const m of src.matchAll(/([\w$]+)=[\w$]+\.i\((\d+)\)/g)) imports[m[1]] = m[2];
      for (const m of src.matchAll(/\(0,([\w$]+)\.request\)\(/g)) {
        const id = imports[m[1]];
        const fn = id && exportsOf(id).request;
        if (isRequest(fn)) {
          window.__mxweb = { request: fn, info: { moduleId: id, via: 'order-module' } };
          return window.__mxweb.info;
        }
      }
    }
  }
  const found = Object.entries(ctx.c || {}).map(([id, m]) => [id, m && m.exports && m.exports.request])
    .filter(([, f]) => isRequest(f)).sort((a, b) => fsrc(b[1]).length - fsrc(a[1]).length);
  if (!found.length) throw new Error('функция request сайта не найдена');
  window.__mxweb = { request: found[0][1], info: { moduleId: found[0][0], via: 'cache-longest', candidates: found.length } };
  return window.__mxweb.info;
}
"""

CALL_JS = r"""
async ([url, opts]) => {
  if (!window.__mxweb || !window.__mxweb.request) return { missing: true };
  const t0 = performance.now();
  try {
    const r = await window.__mxweb.request(url, opts);
    return { r, ms: performance.now() - t0 };
  } catch (e) {
    return { error: String(e && e.message || e), status: e && e.status, ms: performance.now() - t0 };
  }
}
"""


@dataclass
class WebResp:
    code: int | None
    data: object
    msg: str | None
    page_ms: float  # от вызова request в странице до ответа (сеть + подпись)
    total_ms: float  # от Python и обратно, с накладными расходами Playwright
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.code in (0, 200)

    def label(self) -> str:
        if self.error:
            return f"NET {self.error}"
        return "OK" if self.ok else f"ERR {self.code} {self.msg}"


@dataclass
class SymbolInfo:
    symbol: str  # как в официальном API: MXUSDT
    base: str
    quote: str
    symbol_id: str
    cd: str  # currencyId базовой монеты
    mcd: str  # marketCurrencyId котируемой
    price_scale: int
    qty_scale: int
    open_time_ms: int | None


def dec_str(x, scale: int | None = None) -> str:
    """Число в строку без экспоненты; со scale — с округлением вниз до scale знаков."""
    d = Decimal(str(x))
    if scale is not None:
        d = d.quantize(Decimal(1).scaleb(-scale), rounding=ROUND_DOWN)
    return format(d, "f")


class WebChannel:
    def __init__(self, profile_dir: Path = PROFILE_DIR, headless: bool = False, lite: bool = True):
        self.profile_dir = profile_dir
        self.headless = headless
        self.lite = lite
        self._front: Page | None = None
        self._pw = None
        self.ctx: BrowserContext | None = None
        self.page: Page | None = None
        self.symbols: dict[str, SymbolInfo] = {}
        self.inject_info: dict | None = None

    async def launch(self) -> None:
        """Запускает браузер с профилем (cookies сессии сохраняются между запусками)."""
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self._pw = await async_playwright().start()
        self.ctx = await self._pw.chromium.launch_persistent_context(
            str(self.profile_dir), headless=self.headless, viewport={"width": 1400, "height": 900},
            args=["--deny-permission-prompts"])  # сайт просит разрешить уведомления — отказ без попапа
        self.page = self.ctx.pages[0] if self.ctx.pages else await self.ctx.new_page()
        if self.lite:
            await self.make_lite()

    async def make_lite(self) -> None:
        """Страница без рыночных обновлений: торговая страница MEXC иначе занимает ~70 % CPU сервера
        (стакан, график, лента), и вызовы request ждут в очереди браузера по секунде и больше.
        Ордерам это не нужно: request — обычный HTTP, рыночные данные у бота свои (официальный WS).
        WebSocket страницы подменяются пустышкой (соединение «открыто», данных нет), график, картинки,
        шрифты и аналитика не загружаются. Для входа (капча, форма) — WebChannel(lite=False)."""
        # перехватываем только блокируемое (по адресу): запросы API и ордера идут мимо Python без задержки
        await self.ctx.route(LITE_BLOCK_RE, lambda r: r.abort())
        await self.ctx.route_web_socket(re.compile(r"^wss?://"), lambda ws: None)

    async def start(self, page_symbol: str = "MX_USDT", ready_timeout: float = 30) -> None:
        """Запускает браузер, открывает страницу пары и находит request сайта."""
        await self.launch()
        await self.open_pair(page_symbol, ready_timeout)

    async def has_session_cookie(self) -> bool:
        # u_id — токен сессии; им же сайт подписывает ордера (x-mxc-sign)
        return any(c["name"] == "u_id" and c["value"] for c in await self.ctx.cookies(SITE))

    async def open_pair(self, page_symbol: str, ready_timeout: float = 30) -> None:
        await self.page.goto(f"{SITE}/exchange/{page_symbol}", wait_until="domcontentloaded")
        await self.inject(ready_timeout)
        if self.lite and not self.headless:
            # пустая вкладка поверх торговой: невидимую страницу браузер не перерисовывает
            # (на сервере CPU ~70 % → ~18 %), а request из неё работает как обычно
            if self._front is None:
                self._front = await self.ctx.new_page()
            await self._front.bring_to_front()

    async def inject(self, timeout: float = 30) -> dict:
        # чанки сайта догружаются после domcontentloaded — пробуем, пока request не появится в кэше
        deadline = time.monotonic() + timeout
        while True:
            try:
                self.inject_info = await self.page.evaluate(INJECT_JS)
                return self.inject_info
            except Exception:
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(0.5)

    async def close(self) -> None:
        if self.ctx:
            await self.ctx.close()
        if self._pw:
            await self._pw.stop()

    async def call(self, url: str, method: str = "GET", body: dict | None = None, **opts) -> WebResp:
        req = {"method": method, "showErrorMsg": False, **opts}
        if body is not None:
            req["body"] = body
        t0 = time.perf_counter()
        try:
            res = await self.page.evaluate(CALL_JS, [url, req])
        except PlaywrightError as e:  # страница ушла на другой адрес прямо во время вызова
            res = {"missing": True, "nav_error": str(e)}
        if res.get("missing"):  # страница перезагрузилась — находим request заново
            await self.inject()
            res = await self.page.evaluate(CALL_JS, [url, req])
        total = (time.perf_counter() - t0) * 1000
        if "error" in res:
            return WebResp(res.get("status"), None, None, res.get("ms", 0), total, res["error"])
        r = res.get("r") or {}
        return WebResp(r.get("code"), r.get("data"), r.get("msg") or r.get("message"), res["ms"], total)

    # --- данные ---

    async def load_symbols(self) -> int:
        r = await self.call(SYMBOLS_URL)
        if not r.ok or not isinstance(r.data, dict):
            raise RuntimeError(f"symbolsV2: {r.label()}")
        for quote, items in (r.data.get("symbols") or {}).items():
            for s in items:
                sym = f"{s['vn']}{quote}"
                self.symbols[sym] = SymbolInfo(sym, s["vn"], quote, s["id"], s["cd"], s["mcd"],
                                               int(s.get("ps") or 0), int(s.get("qs") or 0), s.get("ot"))
        return len(self.symbols)

    async def symbol(self, symbol: str) -> SymbolInfo:
        if symbol not in self.symbols:
            await self.load_symbols()  # новая пара могла появиться после прошлой загрузки
        if symbol not in self.symbols:
            raise KeyError(f"{symbol}: нет в symbolsV2")
        return self.symbols[symbol]

    async def balance(self, currency: str) -> WebResp:
        return await self.call(f"{BALANCE_URL}?currency={currency}")

    async def is_logged_in(self) -> bool:
        r = await self.balance("USDT")
        return r.ok and isinstance(r.data, dict) and "available" in r.data

    # --- ордера ---

    async def place_limit(self, symbol: str, side: str, price, quantity) -> WebResp:
        s = await self.symbol(symbol)
        body = {"currencyId": s.cd, "marketCurrencyId": s.mcd, "tradeType": side.upper(),
                "price": dec_str(price, s.price_scale), "quantity": dec_str(quantity, s.qty_scale),
                "orderType": "LIMIT_ORDER", "orderSource": "WEB"}
        return await self.call(PLACE_LIMIT_URL, "POST", body, headers=ORDER_HEADERS, needDolos=DOLOS_SCENE)

    async def place_market(self, symbol: str, side: str, price, amount=None, quantity=None) -> WebResp:
        """MARKET: amount — сумма в котируемой валюте, quantity — количество монет (одно из двух).
        price — свежий ask (BUY) / bid (SELL): биржа исполнит не дальше ±5 % от него."""
        if (amount is None) == (quantity is None):
            raise ValueError("нужно ровно одно из amount / quantity")
        s = await self.symbol(symbol)
        body = {"currencyId": s.cd, "marketCurrencyId": s.mcd, "tradeType": side.upper(),
                "price": dec_str(price, s.price_scale), "orderType": "MARKET_ORDER", "orderSource": "WEB"}
        if amount is not None:
            body["amount"] = dec_str(amount)
        else:
            body["quantity"] = dec_str(quantity, s.qty_scale)
        return await self.call(PLACE_MARKET_URL, "POST", body, headers=ORDER_HEADERS, needDolos=DOLOS_SCENE)

    async def cancel(self, order_id: str) -> WebResp:
        return await self.call(f"{CANCEL_URL}?orderId={order_id}", "DELETE")


# --- команды ---

def book_ticker(symbol: str) -> tuple[float, float]:
    """Лучшие bid/ask из официального публичного API (работает и для пар, закрытых для API-торговли)."""
    from probe_listing import Client
    r = Client(None, None, 10).call("GET", "/api/v3/ticker/bookTicker", {"symbol": symbol})
    if not r.ok:
        raise RuntimeError(f"bookTicker {symbol}: {r.label()}")
    return float(r.body["bidPrice"]), float(r.body["askPrice"])


async def cmd_login(args) -> None:
    # Пока человек входит, страницу не трогаем: форма входа — другие страницы и редиректы,
    # а вызовы JS на уходящей странице падают. Ждём cookie сессии, потом проверяем вход запросом.
    wc = WebChannel(headless=False, lite=False)  # форме входа нужны капча и картинки
    await wc.launch()
    try:
        if await wc.has_session_cookie():
            print("В профиле уже есть сессия — проверяю её.")
        else:
            await wc.page.goto(f"{SITE}/exchange/{args.page}", wait_until="domcontentloaded")
            print("Войдите в аккаунт MEXC в открывшемся окне (логин, пароль и 2FA вводите сами).")
            print("Ждём входа (проверка cookie раз в 2 с), Ctrl+C — выход. Окно не закрывайте.")
            while not await wc.has_session_cookie():
                await asyncio.sleep(2)
            print("Cookie сессии появилась, проверяю вход...")
            await asyncio.sleep(3)  # даём сайту закончить редиректы после входа
        await wc.open_pair(args.page)
        if await wc.is_logged_in():
            r = await wc.balance("USDT")
            print(f"Сессия активна (USDT available={r.data.get('available')}), профиль сохранён в {wc.profile_dir}")
        else:
            print("Cookie есть, но сайт отвечает «не залогинен» — попробуйте войти ещё раз.")
    finally:
        await wc.close()


SAME_SITE = {"no_restriction": "None", "none": "None", "lax": "Lax", "strict": "Strict"}


def load_cookie_file(path: Path) -> list[dict]:
    """Cookies mexc.com из JSON-экспорта (Cookie-Editor и подобные: список объектов,
    или storage_state Playwright: {"cookies": [...]}) в формат add_cookies."""
    data = json.loads(path.read_text(encoding="utf-8"))
    items = data.get("cookies", []) if isinstance(data, dict) else data
    out = []
    for c in items:
        domain = c.get("domain", "")
        if not domain.lstrip(".").endswith("mexc.com"):
            continue
        cookie = {"name": c["name"], "value": c["value"], "domain": domain, "path": c.get("path") or "/",
                  "httpOnly": bool(c.get("httpOnly")), "secure": bool(c.get("secure"))}
        expires = c.get("expirationDate", c.get("expires"))
        if expires and not c.get("session"):
            cookie["expires"] = float(expires)
        same_site = SAME_SITE.get(str(c.get("sameSite", "")).lower())
        if same_site:
            cookie["sameSite"] = same_site
        out.append(cookie)
    return out


async def cmd_import_cookies(args) -> None:
    path = Path(args.file)
    cookies = load_cookie_file(path)
    if not any(c["name"] == "u_id" for c in cookies):
        raise SystemExit(f"в {path} нет cookie u_id (токен сессии mexc.com) — экспорт сделан не со страницы MEXC "
                         "или не залогиненного браузера")
    wc = WebChannel(headless=args.headless, lite=not args.full)
    await wc.launch()
    try:
        await wc.ctx.add_cookies(cookies)
        await wc.open_pair(args.page)
        ok = await wc.is_logged_in()
        print(f"загружено cookies mexc.com: {len(cookies)}; залогинен: {ok}")
        if ok:
            r = await wc.balance("USDT")
            print(f"USDT available={r.data.get('available')}; профиль сохранён в {wc.profile_dir}")
    finally:
        await wc.close()
    if not args.keep:
        path.unlink()  # файл равен паролю от аккаунта — не оставляем его на диске
        print(f"файл {path} удалён")


async def cmd_check(args) -> None:
    wc = WebChannel(headless=args.headless, lite=not args.full)
    t0 = time.perf_counter()
    await wc.start(args.page)
    print(f"страница и request сайта: {time.perf_counter() - t0:.1f} с, {wc.inject_info}")
    s = await wc.symbol(args.symbol)
    print(f"{s.symbol}: cd={s.cd} mcd={s.mcd} id={s.symbol_id} ps={s.price_scale} qs={s.qty_scale} ot={s.open_time_ms}")
    for cur in (s.quote, s.base):
        r = await wc.balance(cur)
        avail = r.data.get("available") if isinstance(r.data, dict) else None
        print(f"баланс {cur}: {r.label()} available={avail}  ({r.page_ms:.0f} мс в странице, {r.total_ms:.0f} мс всего)")
    print("залогинен:", await wc.is_logged_in())
    await wc.close()


async def cmd_limit_test(args) -> None:
    wc = WebChannel(headless=args.headless, lite=not args.full)
    await wc.start(args.page)
    s = await wc.symbol(args.symbol)
    # первые секунды страница догружается и запросы идут по 1–3 с — ордер ставим на прогретой странице
    print(f"прогрев страницы {args.warmup:.0f} с...", flush=True)
    await asyncio.sleep(args.warmup)
    w = await wc.call(f"/api/platform/spot/market-v2/web/symbol/ticker?symbol={s.base}_{s.quote}")
    print(f"контрольный запрос: {w.label()} {w.total_ms:.0f} мс")
    bid, _ = book_ticker(args.symbol)
    price = bid * (1 - args.below / 100)
    qty = args.usdt / price
    print(f"{s.symbol}: bid {bid}, LIMIT BUY {dec_str(qty, s.qty_scale)} по {dec_str(price, s.price_scale)}")
    r = await wc.place_limit(args.symbol, "BUY", price, qty)
    print(f"place:  {r.label()} data={r.data}  {r.page_ms:.0f} мс в странице, {r.total_ms:.0f} мс всего")
    if r.ok and r.data:
        c = await wc.cancel(r.data)
        print(f"cancel: {c.label()}  {c.page_ms:.0f} мс в странице, {c.total_ms:.0f} мс всего")
    await wc.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--page", default="MX_USDT", help="какую страницу пары держать открытой (MX_USDT)")
    p.add_argument("--headless", action="store_true", help="без окна (Akamai может принять за бота)")
    p.add_argument("--full", action="store_true", help="полная страница: с графиком и потоками данных (по умолчанию облегчённая)")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("login", help="окно для входа в аккаунт")
    ic = sub.add_parser("import-cookies", help="загрузить cookies mexc.com из JSON-экспорта браузера")
    ic.add_argument("file")
    ic.add_argument("--keep", action="store_true", help="не удалять файл после загрузки")
    c = sub.add_parser("check", help="самопроверка без ордеров")
    c.add_argument("symbol")
    t = sub.add_parser("limit-test", help="реальный LIMIT BUY ниже рынка и сразу отмена")
    t.add_argument("symbol")
    t.add_argument("--usdt", type=float, default=2, help="сумма ордера в USDT (2)")
    t.add_argument("--below", type=float, default=10, help="на сколько %% ниже bid ставить (10)")
    t.add_argument("--warmup", type=float, default=20, help="сколько секунд прогревать страницу перед ордером (20)")
    args = p.parse_args()
    handler = {"login": cmd_login, "import-cookies": cmd_import_cookies, "check": cmd_check,
               "limit-test": cmd_limit_test}[args.cmd]
    asyncio.run(handler(args))


if __name__ == "__main__":
    main()
