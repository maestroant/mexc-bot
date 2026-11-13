"""Независимый супервизор восстановления реальных проб (решение клиента 12.11 вечер, C-04).

Работает отдельно от процессов листингов (по расписанию раз в несколько минут). За один проход:
  1. Берёт из долговечного реестра logs/probe_risk.json все пары с незавершёнными пробными раундами (позиции) и с
     собственными ордерами пробы в незавершённом статусе.
  2. Пару, которую сейчас ведёт живой процесс пробы (держит блокировку пары), пропускает — сверяет тот процесс.
  3. Для остальных — та же сверка, что в пробе (probe_listing.Probe.reconcile): отмена только зарегистрированных
     пробой ордеров с подтверждением; BUY после падения — по ордерам и сделкам, «ордера нет» только после повторной
     отрицательной сверки; продажа только подтверждённого непроданного количества, при UNKNOWN — без SELL;
     аварийная блокировка остаётся до оператора.
  4. Пассивная поздняя сверка (решение клиента 13.11, п. 5): ордера, закрытые как NO_ORDER_CONFIRMED или
     NOT_PLACED_RECONCILED, LATE_WATCH_MS после регистрации раз в LATE_EVERY_MS read-only проверяются снова — ордер по
     clientOrderId, сделки пары с момента регистрации, остаток базовой валюты. Нашёлся ордер/сделка или остаток вырос —
     LATE_APPEARANCE: аварийная блокировка, повторная классификация ордера, уведомление. Ордеров не ставит.
  5. Пишет пульс (supervisor.last_run / last_ok / errors) в состояние. Ошибка сверки — Telegram; три ошибки подряд —
     повторное предупреждение; аварийная блокировка — напоминание не чаще раза в 6 часов.
Проверка самого супервизора: p0_autoreport и каждый процесс пробы при старте смотрят на возраст пульса.
  python tools/probe_supervisor.py            # один проход
"""

import os
import sys
import time
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probe_listing as pl  # noqa: E402
import probe_risk as risk  # noqa: E402
from notify import notify  # noqa: E402

REMIND_BLOCK_MS = 6 * 3_600_000
LATE_WATCH_MS = 24 * 3_600_000  # сколько после «ордера нет» продолжается пассивная сверка
LATE_EVERY_MS = 30 * 60_000  # не чаще
LATE_STATUSES = ("NO_ORDER_CONFIRMED", "NOT_PLACED_RECONCILED")


def pending_symbols(path: Path) -> list[str]:
    with risk.locked(path) as st:
        syms = set(st["positions"])
        final = {"FILLED", "CANCELED", "PARTIALLY_CANCELED", "REJECTED", "EXPIRED", "NO_ORDER_CONFIRMED",
                 "NOT_PLACED_RECONCILED"}
        syms |= {o.get("symbol") for o in st["orders"].values()
                 if o.get("symbol") and o.get("orderId") and o.get("status") not in final
                 and not str(o.get("status", "")).startswith("ERR")}
        return sorted(s for s in syms if s)


def probe_for(client, symbol: str, path: Path) -> "pl.Probe":
    args = Namespace(symbol=symbol, market_test_usdt=5.0, market_test_every=30.0, market_test_max_spread=10.0,
                     market_test_far_pct=0.0, real_order=False, market_test=False, usdt=2.0, offset_pct=10.0,
                     cap_test=False, duration=0, max_wait=0, real_order_every=2.0, real_order_every_after=30.0)
    log = path.parent / f"probe_supervisor_{symbol}_{time.strftime('%Y%m%d')}.jsonl"
    pr = pl.Probe(args, client, pl.now_ms(), log)
    pr.risk_path = path
    pr.check_exchange_info()
    if not pr.market.info:
        raise RuntimeError(f"{symbol}: exchangeInfo не получен")
    return pr


def supervise_symbol(client, symbol: str, path: Path) -> str:
    pr = probe_for(client, symbol, path)
    pr.check_depth()
    pr.reconcile(force=True)
    return pr.states.get("reconcile").last if pr.states.get("reconcile") else "сверка без действий"


def late_candidates(path: Path) -> list[tuple[str, dict]]:
    now = pl.now_ms()
    with risk.locked(path) as st:
        return [(cid, dict(o)) for cid, o in st["orders"].items()
                if o.get("status") in LATE_STATUSES and o.get("symbol")
                and now - int(o.get("ts") or 0) <= LATE_WATCH_MS
                and now - int(o.get("late_last") or 0) >= LATE_EVERY_MS]


def late_check(client, cid: str, o: dict, path: Path) -> str:
    """Одна пассивная сверка ордера, закрытого как «нет ордера». Только чтение."""
    sym = o["symbol"]
    pr = probe_for(client, sym, path)
    res, info = pr.find_buy(cid, int(o.get("ts") or 0))
    base = (pr.market.info or {}).get("baseAsset")
    bal = pr.balance(base) if base else None
    with risk.locked(path) as st:
        cur = st["orders"].get(cid, {})
        base0 = cur.get("late_base0")
        cur.update(late_last=pl.now_ms(), late_checks=int(cur.get("late_checks") or 0) + 1, late_result=res,
                   late_base0=base0 if base0 is not None else (str(bal) if bal is not None else None))
        has_position = sym in st["positions"]
    grew = bal is not None and base0 is not None and bal > pl.Decimal(str(base0)) and not has_position
    if res == "found" or grew:
        what = (f"ордер/сделка найдены: {info}" if res == "found"
                else f"остаток {base} вырос {base0} → {bal} без пробной позиции")
        risk.register_order(cid, path=path, status="LATE_APPEARANCE", late_was=o.get("status"), late_info=str(info)[:300])
        risk.block(sym, f"LATE_APPEARANCE: {cid} был закрыт как {o.get('status')}, поздняя сверка — {what}",
                   path=path, clientOrderId=cid)
        notify(f"!!! {sym}: позднее появление {cid} (был {o.get('status')}): {what}. Пробные BUY заблокированы, "
               f"нужна сверка оператором.")
        return "LATE_APPEARANCE"
    return res


def run_once(client, path: Path = risk.STATE) -> dict:
    out = {"checked": [], "skipped_live": [], "errors": [], "late": []}
    for sym in pending_symbols(path):
        with risk.symbol_lock(sym, root=path.parent) as mine:
            if not mine:
                out["skipped_live"].append(sym)
                continue
            try:
                out["checked"].append((sym, supervise_symbol(client, sym, path)))
            except Exception as e:  # noqa: BLE001 — любая ошибка сверки — в пульс и уведомление
                out["errors"].append(f"{sym}: {type(e).__name__}: {e}")
    for cid, o in late_candidates(path):
        with risk.symbol_lock(o["symbol"], root=path.parent) as mine:
            if not mine:
                out["skipped_live"].append(o["symbol"])
                continue
            try:
                out["late"].append((cid, late_check(client, cid, o, path)))
            except Exception as e:  # noqa: BLE001
                out["errors"].append(f"{o['symbol']} поздняя сверка {cid}: {type(e).__name__}: {e}")
    sv = risk.supervisor_beat(not out["errors"], "; ".join(out["errors"]), path=path)
    if out["errors"]:
        notify("!!! Супервизор проб: сверка не выполнена — " + "; ".join(out["errors"])
               + (f" (ошибок подряд {sv['errors']})" if sv.get("errors", 0) >= 3 else ""))
    with risk.locked(path) as st:
        b = st["blocked"]
        if b and pl.now_ms() - int(sv.get("last_block_remind") or 0) > REMIND_BLOCK_MS:
            st["supervisor"]["last_block_remind"] = pl.now_ms()
            notify(f"⚠️ Пробы заблокированы с {time.strftime('%d.%m %H:%M', time.gmtime(b['since'] / 1000))} UTC: "
                   f"{b.get('reason')}. Открытые пробные позиции: {list(st['positions']) or 'нет'}. "
                   f"Снять после сверки: python tools/probe_risk.py --clear \"...\"")
    return out


def main() -> None:
    pl.load_dotenv(pl.ROOT / ".env")
    client = pl.Client(os.environ.get("MEXC_API_KEY"), os.environ.get("MEXC_API_SECRET"), 10.0)
    if not client.has_keys:
        risk.supervisor_beat(False, "нет ключей API")
        sys.exit("нет ключей API")
    client.sync_time()
    out = run_once(client)
    print(time.strftime("%Y-%m-%d %H:%M:%S"), out, flush=True)


if __name__ == "__main__":
    main()
