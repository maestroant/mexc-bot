"""Наблюдатель за новыми листингами MEXC: уведомления в Telegram, проба API и запись рынка.

Раз в --interval секунд берёт полный /api/v3/exchangeInfo и передаёт его ListingTracker (tools/listings.py):
будущие пары (status != 1), время старта (firstOpenTime), перенос и снятие листинга.
О каждом событии по парам с котировкой из --notify-quotes пишет в Telegram (получатели — TG_LISTING_CHAT_ID,
если не задан — TG_CHAT_ID). Для каждой отслеживаемой пары запускает отдельными процессами:
  tools/probe_listing.py SYMBOL --open auto ...   — проверки API и пробный ордер
  tools/listing_recorder.py SYMBOL ...            — запись всех WS-потоков и REST-снимков
  tools/paper.py SYMBOL ...                       — PAPER на живом потоке (без ордеров): BASELINE, MAIN_EARLY, CAP BREAKOUT
Процессы запускаются в своей сессии и переживают перезапуск наблюдателя.
Если для пары процесс уже работает (например, запущенный вручную), второй не запускается.
Когда проба и запись пары закончились, а в логе пробы есть 30010 (листинг был заморожен потолком цены),
наблюдатель прогоняет tools/cap_replay.py --brief по записи и шлёт итог REPLAY CAP BREAKOUT в Telegram.
Раз в --report-days дней (14) — сводный REPLAY по всем замороженным листингам (cap_replay.py --report) в Telegram.
REPLAY идёт с nice 19, чтобы не отнимать процессор у проб и записей идущих листингов.

Состояние — logs/watcher_state.json, вывод процессов — logs/<probe|rec>_<SYMBOL>_console.txt.
Запуск: .venv/bin/python tools/listing_watcher.py   (обычно как systemd-служба mexc-listing-watcher)
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from cap_replay import find_cap
from listings import ListingTracker, format_event
from notify import notify
from probe_listing import ROOT, Client, load_dotenv, now_ms

HERE = Path(__file__).resolve().parent
LOGS = ROOT / "logs"
STATE_PATH = LOGS / "watcher_state.json"


def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def is_running(script: str, symbol: str) -> bool:
    r = subprocess.run(["pgrep", "-f", f"{script} {symbol}( |$)"], capture_output=True)
    return r.returncode == 0


class Watcher:
    def __init__(self, args):
        self.args = args
        self.client = Client(None, None, 15.0)
        self.state = self.load_state()
        self.state.pop("known", None)  # новые пары теперь определяет ListingTracker по firstOpenTime
        self.tracker = ListingTracker(self.state.setdefault("listings", {}), self.state.setdefault("ignored", {}),
                                      remind_ms=int(args.remind_min * 60_000))
        self.children: list[subprocess.Popen] = []

    def load_state(self) -> dict:
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"handled": {}}

    def save_state(self) -> None:
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(STATE_PATH)

    def spawn(self, kind: str, script: str, symbol: str, extra: list[str]) -> None:
        if is_running(script, symbol):
            log(f"{symbol}: {script} уже работает — не запускаю второй")
            return
        out = (LOGS / f"{kind}_{symbol}_console.txt").open("a", encoding="utf-8")
        cmd = [sys.executable, "-u", str(HERE / script), symbol, *extra]
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT,
                                stdin=subprocess.DEVNULL, start_new_session=True)
        self.children.append(proc)
        log(f"{symbol}: запущен {script} (pid {proc.pid})")

    def launch(self, symbol: str, info: dict) -> None:
        a = self.args
        probe_args = ["--open", "auto", "--duration", str(a.duration), "--max-wait", str(a.max_wait)]
        if a.real_order:
            probe_args += ["--real-order", "--real-order-every", str(a.real_order_every)]
        if a.market_test:
            probe_args += ["--market-test"]
        if a.cap_test:
            probe_args += ["--cap-test"]
        self.spawn("probe", "probe_listing.py", symbol, probe_args)
        self.spawn("rec", "listing_recorder.py", symbol,
                   ["--duration", str(a.duration), "--max-wait", str(a.max_wait)])
        if a.paper:
            self.spawn("paper", "paper.py", symbol, ["--max-wait", str(a.max_wait)])
        self.state["handled"][symbol] = {"ts": now_ms(), "status": info.get("status"),
                                         "firstOpenTime": info.get("firstOpenTime"), "zones": info.get("conceptPlates"),
                                         "replay": "pending"}
        self.save_state()

    def check_replays(self) -> None:
        """Проба и запись пары закончились, в логе пробы есть 30010 — REPLAY CAP BREAKOUT по записи, итог в Telegram."""
        for sym, h in self.state["handled"].items():
            if h.get("replay") != "pending":
                continue
            if is_running("probe_listing.py", sym) or is_running("listing_recorder.py", sym):
                continue
            h["replay"] = "done"
            recs = sorted(LOGS.glob(f"rec_{sym}_2*.jsonl.gz"))
            if not recs or not find_cap(sym):
                continue  # листинг не был заморожен
            cmd = ["nice", "-n", "19", sys.executable, str(HERE / "cap_replay.py"), str(recs[-1]), "--brief"]
            r = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=600)
            text = r.stdout.strip()
            if r.returncode != 0 or not text:
                log(f"{sym}: REPLAY CAP BREAKOUT не удался: {r.stderr.strip()[-300:]}")
                continue
            log(f"{sym}: REPLAY CAP BREAKOUT — {' | '.join(text.splitlines()[1:])}")
            notify(text)

    def check_report(self) -> None:
        """Раз в --report-days дней — сводный REPLAY CAP BREAKOUT по всем замороженным листингам; процесс сам шлёт его."""
        last = self.state.setdefault("cap_report_ms", now_ms())  # первая сводка — через срок после включения
        if now_ms() - last < self.args.report_days * 86_400_000:
            return
        self.state["cap_report_ms"] = now_ms()
        out = (LOGS / "cap_report_console.txt").open("a", encoding="utf-8")
        cmd = ["nice", "-n", "19", sys.executable, "-u", str(HERE / "cap_replay.py"), "--all-frozen", "--report",
               "--send", "--json", str(LOGS / f"cap_report_{datetime.now():%Y%m%d}.json")]
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                start_new_session=True)
        log(f"сводный REPLAY CAP BREAKOUT запущен (pid {proc.pid})")

    def has_trades(self, symbol: str) -> bool:
        r = self.client.call("GET", "/api/v3/trades", {"symbol": symbol, "limit": 1})
        found = r.ok and isinstance(r.body, list) and len(r.body) > 0
        if found:
            # status≠1 у пары с историей сделок — приостановка или делистинг, а не новый листинг
            log(f"{symbol}: status≠1, но сделки уже были — не листинг, пропускаю")
        return found

    def active_count(self) -> int:
        self.children = [p for p in self.children if p.poll() is None]  # заодно убираем завершённые
        return len(self.children)

    def scan(self) -> None:
        r = self.client.call("GET", "/api/v3/exchangeInfo")
        if not r.ok or not isinstance(r.body, dict):
            log(f"exchangeInfo: {r.label()}")
            return
        symbols = {s["symbol"]: s for s in r.body.get("symbols", [])}
        # пара исчезла (листинг отменён/перенесён, делистинг) — если её процессы завершены, забываем её,
        # чтобы при повторном появлении запустить пробу заново
        for sym in [s for s in self.state["handled"] if s not in symbols]:
            if not is_running("probe_listing.py", sym) and not is_running("listing_recorder.py", sym):
                del self.state["handled"][sym]
                log(f"{sym}: пары больше нет в exchangeInfo — снята с учёта")
        now = now_ms()
        for ev in self.tracker.update(symbols, now, self.has_trades):
            text = format_event(ev, now)
            log(f"{ev.symbol}: {text.splitlines()[0]}")
            if ev.quote in self.args.notify_quotes:
                notify(text, to="TG_LISTING_CHAT_ID")
        for sym in self.tracker.tracked:
            if sym in self.state["handled"] or sym not in symbols:
                continue
            if self.active_count() >= self.args.max_active * 3:
                log(f"{sym}: лимит одновременных процессов, отложено")
                break
            self.launch(sym, symbols[sym])
        self.check_replays()
        self.check_report()
        self.save_state()

    def run(self) -> None:
        log(f"Наблюдатель запущен: опрос раз в {self.args.interval} с, реальные ордера: {self.args.real_order}")
        notify("👀 Наблюдатель листингов MEXC запущен")
        while True:
            try:
                self.scan()
            except Exception as e:  # сбой одного опроса не должен останавливать службу
                log(f"ошибка опроса: {type(e).__name__}: {e}")
            time.sleep(self.args.interval)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--interval", type=float, default=60, help="опрос exchangeInfo раз в N секунд (60)")
    p.add_argument("--duration", type=float, default=3600, help="секунд пробы и записи после открытия (3600)")
    p.add_argument("--max-wait", type=float, default=72, help="сколько часов ждать открытия пары (72)")
    p.add_argument("--max-active", type=int, default=6, help="сколько пар обрабатывать одновременно (6)")
    p.add_argument("--notify-quotes", type=lambda s: s.upper().split(","), default=["USDT"],
                   help="о парах с какими котировками писать в Telegram, через запятую (USDT)")
    p.add_argument("--remind-min", type=float, default=5, help="напоминание в Telegram за N минут до старта, 0 — нет (5)")
    p.add_argument("--real-order", action="store_true", help="пробный реальный LIMIT BUY с отменой в каждой пробе")
    p.add_argument("--real-order-every", type=float, default=10, help="не чаще одной попытки ордера за N секунд (10)")
    p.add_argument("--market-test", action="store_true",
                   help="в каждой пробе: MARKET BUY/SELL на 5 USDT (проверка рыночных ордеров через API)")
    p.add_argument("--cap-test", action="store_true", help="в каждой пробе: проверки потолка цены на открытии")
    p.add_argument("--no-paper", dest="paper", action="store_false",
                   help="не запускать PAPER (tools/paper.py) для каждой пары")
    p.add_argument("--report-days", type=float, default=14,
                   help="сводный REPLAY CAP BREAKOUT в Telegram раз в N дней (14)")
    args = p.parse_args()
    LOGS.mkdir(exist_ok=True)
    load_dotenv(ROOT / ".env")
    Watcher(args).run()


if __name__ == "__main__":
    main()
