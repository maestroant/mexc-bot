"""Лимиты и блокировка реальных пробных ордеров (решение клиента 11.11: CONTROLLED REAL PROBES — CONTINUE WITH SAFETY
GATES). Общее состояние всех процессов пробы — logs/probe_risk.json (под блокировкой файла):

  blocked       — новые пробные BUY запрещены (неизвестный статус ордера, непроданный остаток, ошибка критического
                  выхода, неожиданное исполнение LIMIT-пробы); снимает только оператор после сверки:
                  python tools/probe_risk.py --clear "сверено: ..."
  positions     — открытые пробные позиции по парам (до подтверждённого закрытия);
  ledger        — итог каждого раунда: реализованный PnL с комиссиями и непроданный остаток отдельно.

Лимиты (утверждены клиентом 12.11 как верхние ограничения): одна открытая пробная позиция на все пары, пробный BUY не
больше MAX_PROBE_USDT, один раунд каждого вида на пару, реализованный убыток проб не больше DAILY_LOSS_USDT за сутки UTC и
WEEKLY_LOSS_USDT за скользящие 7 × 24 ч (прибыль убыток не гасит). Риск-бюджет: новая проба не начинается, если её
неблагоприятный сценарий (ADVERSE_PCT от суммы: проскальзывание входа и выхода, комиссии, защитный выход — худший раунд
истории −5,9 %) вместе с уже реализованным убытком и неблагоприятным сценарием открытых позиций превысит дневной или
недельный лимит. Лимиты не гарантируют максимальный убыток: проскальзывание и недоступность выхода могут их превысить.

Резервирование атомарно (reserve: проверка лимитов и запись позиции BUY_SENT — под одной блокировкой файла), поэтому
два одновременных кандидата не пройдут проверку одного и того же свободного лимита. Ордера проб регистрируются в
состоянии до отправки (orders: clientOrderId → пара, вид, сторона, orderId, статус) — автоматическая сверка отменяет только
их. Раунд закрывается один раз (round_id), повторная сверка не дублирует журнал. Превышение лимита после закрытия раунда
(гэп, проскальзывание) — запись LIMIT_BREACH, она не обнуляется сменой суток.

Жизненный цикл резерва раунда (решение клиента 12.11 вечер, C-03): reserve → BUY_SENT (занят; ордер регистрируется до
отправки) → POSITION (списан на купленное) → закрыт записью в ledger (освобождён) после подтверждённой продажи или
подтверждённого отсутствия ордера (NO_ORDER_CONFIRMED: повторная отрицательная сверка по ордерам и сделкам в окне);
при неясности — RECONCILIATION_REQUIRED (занят) и аварийная блокировка. Отсутствие процесса резерв не освобождает.
Запись состояния — временный файл, fsync, атомарная замена, fsync каталога (переживает падение процесса и ОС).

Временное правило C-04: новая реальная пробная позиция (MARKET-раунд) — только при подтверждённом наблюдении оператора
(`--watch ЧАСЫ "кто"`), пока независимое восстановление (tools/probe_supervisor.py) не принято клиентом.

Два вида запрета: blocked — аварийный (снимает оператор после сверки), лимит — штатный (снимается сам: сутки, 7 × 24 ч,
закрытие позиции). Состояние нечитаемо — аварийная блокировка (fail-closed), файл сохраняется для разбора."""

import argparse
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "logs" / "probe_risk.json"
MAX_PROBE_USDT = 5.0
MAX_OPEN_POSITIONS = 1
DAILY_LOSS_USDT = 1.0
WEEKLY_LOSS_USDT = 3.0
ADVERSE_PCT = 0.10  # стресс-резерв для допуска новой пробы, доля суммы (не граница фактического убытка)
BUY_KINDS = ("MARKET_EXECUTION_PROBE", "LIMIT_EXECUTION_RESEARCH")
UNVALUED_FEE_RESERVE_PCT = 0.005  # резерв за неоценённую комиссию в третьей валюте, доля суммы сделки


@contextmanager
def locked(path: Path = STATE):
    """Состояние под исключительной блокировкой (несколько процессов пробы одновременно)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = path.with_suffix(".lock")
    fd = os.open(lock, os.O_CREAT | os.O_RDWR)
    try:
        try:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX)
        except ImportError:  # Windows: msvcrt, первый байт файла блокировки
            import msvcrt
            while True:
                try:
                    msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
                    break
                except OSError:
                    time.sleep(0.01)
        try:
            state = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
            if not isinstance(state, dict):
                raise ValueError("не объект")
        except ValueError as e:  # состояние не восстановить достоверно — блокировка и уведомление
            bad = path.with_suffix(f".corrupt{now_ms()}")
            os.replace(path, bad)
            state = {"blocked": {"since": now_ms(), "symbol": "*", "reason": f"состояние проб нечитаемо ({e}), "
                                 f"сохранено в {bad.name}: сверить ордера и остатки вручную"}}
            try:
                from notify import notify
                notify(f"!!! Состояние проб нечитаемо — все пробные BUY заблокированы. Файл {bad.name}.")
            except Exception:  # noqa: BLE001 — уведомление не должно мешать блокировке
                pass
        state.setdefault("blocked", None)
        state.setdefault("positions", {})
        state.setdefault("ledger", [])
        state.setdefault("orders", {})
        state.setdefault("breaches", [])
        state.setdefault("operator_watch", None)
        state.setdefault("supervisor", {})
        yield state
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps(state, ensure_ascii=False, indent=1))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        try:  # каталог — чтобы замена файла пережила падение ОС (Linux)
            dfd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except (OSError, PermissionError):
            pass
    finally:
        try:
            import msvcrt
            os.lseek(fd, 0, 0)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except (ImportError, OSError):
            pass
        os.close(fd)


def now_ms() -> int:
    return int(time.time() * 1000)


def loss_since(state: dict, since_ms: int) -> float:
    return -sum(min(0.0, r.get("pnl_usdt") or 0.0) for r in state["ledger"] if r["ts"] >= since_ms)


def gate(symbol: str, usdt: float, path: Path = STATE, kind: str = "MARKET_EXECUTION_PROBE") -> str | None:
    """Почему пробный BUY с позицией сейчас нельзя (None — можно). Только проверка; резерв — reserve()."""
    with locked(path) as st:
        return check(st, symbol, usdt, kind)


def check(st: dict, symbol: str, usdt: float, kind: str) -> str | None:
    if st["blocked"]:
        b = st["blocked"]
        return f"blocked: {b.get('reason')} ({b.get('symbol')})"
    if usdt > MAX_PROBE_USDT:
        return f"limit: проба {usdt} USDT больше {MAX_PROBE_USDT}"
    if symbol in st["positions"]:
        return "limit: по паре уже есть пробная позиция"
    if len(st["positions"]) >= MAX_OPEN_POSITIONS:
        return f"limit: открытых пробных позиций {len(st['positions'])} ({', '.join(st['positions'])})"
    done = [r.get("kind") for r in st["ledger"] if r["symbol"] == symbol and r.get("kind") in BUY_KINDS]
    if done:  # один реальный режим BUY на листинг (решение клиента 12.11, C3.5)
        return f"limit: по паре уже был реальный BUY-раунд ({done[0]})"
    adverse = usdt * ADVERSE_PCT
    open_adverse = sum(position_risk(p) for p in st["positions"].values())
    day = now_ms() - now_ms() % 86_400_000
    for label, since, limit in (("сутки UTC", day, DAILY_LOSS_USDT),
                                ("7 × 24 ч", now_ms() - 7 * 86_400_000, WEEKLY_LOSS_USDT)):
        used = loss_since(st, since)
        if used >= limit:
            return f"limit: убыток проб за {label} {used:.2f} ≥ {limit} USDT"
        if used + open_adverse + adverse > limit:
            return (f"limit: риск-бюджет за {label}: убыток {used:.2f} + открытые {open_adverse:.2f} + "
                    f"неблагоприятный сценарий пробы {adverse:.2f} > {limit} USDT")
    return None


def position_risk(p: dict) -> float:
    """Риск открытой позиции для бюджета: стресс-резерв суммы или известный нереализованный убыток (стоимость минус
    оценка остатка по bid), что больше; остаток не оценить — вся сумма."""
    usdt = float(p.get("usdt") or MAX_PROBE_USDT)
    risk = usdt * ADVERSE_PCT
    if p.get("left_qty") and p.get("left_value_usdt") is None:
        return usdt
    if p.get("spent_usdt") and p.get("left_value_usdt") is not None:
        risk = max(risk, float(p["spent_usdt"]) - float(p.get("received_usdt") or 0) - float(p["left_value_usdt"]))
    return risk


def reserve(symbol: str, usdt: float, kind: str, round_id: str, path: Path = STATE) -> str | None:
    """Атомарно: проверка всех лимитов и запись позиции BUY_SENT. None — зарезервировано."""
    with locked(path) as st:
        reason = check(st, symbol, usdt, kind)
        if reason is None:
            st["positions"][symbol] = {"since": now_ms(), "stage": "BUY_SENT", "usdt": str(usdt), "kind": kind,
                                       "round_id": round_id}
        return reason


def register_order(cid: str, path: Path = STATE, **info) -> None:
    """Пробный ордер — в состоянии до отправки (принадлежность пробе для сверки), затем orderId и статус."""
    with locked(path) as st:
        st["orders"].setdefault(cid, {"ts": now_ms()}).update(info)


def register_order_by_id(order_id, path: Path = STATE, **info) -> None:
    """Статус своего ордера по orderId (после ожидания исполнения)."""
    if not order_id:
        return
    with locked(path) as st:
        for o in st["orders"].values():
            if o.get("orderId") == order_id:
                o.update(info)


def orders_of(symbol: str, path: Path = STATE) -> list[tuple[str, dict]]:
    with locked(path) as st:
        return [(cid, dict(o)) for cid, o in st["orders"].items() if o.get("symbol") == symbol]


def owned_order(cid: str, path: Path = STATE) -> dict | None:
    with locked(path) as st:
        return st["orders"].get(cid)


def state_problem(symbol: str | None = None, path: Path = STATE) -> str | None:
    """Аварийная блокировка или незакрытая позиция (по паре или любая) — новые реальные BUY нельзя."""
    with locked(path) as st:
        if st["blocked"]:
            return f"blocked: {st['blocked'].get('reason')} ({st['blocked'].get('symbol')})"
        bad = [s for s, p in st["positions"].items() if symbol is None or s == symbol or
               p.get("stage") in ("OPEN_UNRESOLVED", "UNEXPECTED_FILL", "BUY_SENT",
                                                         "RECONCILIATION_REQUIRED")]
        if bad:
            return f"открытые пробные позиции: {', '.join(bad)}"
    return None


def position(symbol: str, path: Path = STATE) -> dict | None:
    with locked(path) as st:
        return st["positions"].get(symbol)


def open_position(symbol: str, info: dict, path: Path = STATE) -> None:
    with locked(path) as st:
        st["positions"][symbol] = {"since": now_ms(), **info}


def update_position(symbol: str, path: Path = STATE, **info) -> None:
    with locked(path) as st:
        st["positions"].setdefault(symbol, {"since": now_ms()}).update(info)


def close_position(symbol: str, record: dict, path: Path = STATE) -> None:
    """Позиция закрыта (продано подтверждённо, остаток — пыль ниже минимума): в журнал раундов — один раз на round_id.
    Убыток за сутки или 7 × 24 ч сверх лимита после закрытия — LIMIT_BREACH (хранится, сменой суток не обнуляется)."""
    with locked(path) as st:
        pos = st["positions"].pop(symbol, None) or {}
        rid = record.get("round_id") or pos.get("round_id")
        if rid and any(r.get("round_id") == rid for r in st["ledger"]):
            return
        st["ledger"].append({"ts": now_ms(), "symbol": symbol, "round_id": rid, **record})
        day = now_ms() - now_ms() % 86_400_000
        for label, since, limit in (("сутки UTC", day, DAILY_LOSS_USDT),
                                    ("7 × 24 ч", now_ms() - 7 * 86_400_000, WEEKLY_LOSS_USDT)):
            used = loss_since(st, since)
            if used > limit:
                br = {"ts": now_ms(), "symbol": symbol, "round_id": rid, "window": label,
                      "loss_usdt": round(used, 4), "limit_usdt": limit,
                      "why": "раунд закрылся с убытком сверх остатка лимита (гэп / проскальзывание / выход)"}
                st["breaches"].append(br)
                if not st["blocked"]:  # до выяснения причины новые пробные входы закрыты (C-06)
                    st["blocked"] = {"since": now_ms(), "symbol": symbol, "reason": f"LIMIT_BREACH: {label} "
                                     f"{used:.2f} > {limit} USDT, раунд {rid}"}


def watch(hours: float, who: str, path: Path = STATE) -> dict:
    """Оператор подтверждает наблюдение за пробами и готовность вмешаться на hours часов (C-04)."""
    with locked(path) as st:
        st["operator_watch"] = {"since": now_ms(), "until": now_ms() + int(hours * 3_600_000), "who": who}
        st.setdefault("events", []).append({"ts": now_ms(), "watch": st["operator_watch"]})
        return st["operator_watch"]


def unwatch(path: Path = STATE) -> None:
    with locked(path) as st:
        st["operator_watch"] = None


def watch_ok(path: Path = STATE) -> str | None:
    """None — наблюдение подтверждено и не истекло; иначе причина."""
    with locked(path) as st:
        w = st["operator_watch"]
        if not w:
            return "нет подтверждённого наблюдения оператора (probe_risk.py --watch)"
        if w["until"] < now_ms():
            return f"наблюдение оператора истекло ({w.get('who')})"
    return None


def supervisor_beat(ok: bool, note: str = "", path: Path = STATE) -> dict:
    with locked(path) as st:
        sv = st["supervisor"]
        sv["last_run"] = now_ms()
        if ok:
            sv.update(last_ok=now_ms(), errors=0, last_error=None)
        else:
            sv.update(errors=int(sv.get("errors") or 0) + 1, last_error=note)
        return dict(sv)


SUPERVISOR_STALE_MS = 15 * 60_000


def supervisor_stale(path: Path = STATE, notify_every_ms: int = 3_600_000) -> str | None:
    """Супервизор восстановления не отчитывался дольше SUPERVISOR_STALE_MS при незакрытых пробных позициях —
    описание (и Telegram не чаще раза в час), иначе None."""
    with locked(path) as st:
        if not st["positions"]:
            return None
        sv = st["supervisor"]
        age = now_ms() - int(sv.get("last_ok") or 0)
        if age <= SUPERVISOR_STALE_MS:
            return None
        msg = f"супервизор проб не отчитывался {age // 60_000} мин при незакрытых позициях {list(st['positions'])}"
        if now_ms() - int(sv.get("last_stale_notify") or 0) > notify_every_ms:
            sv["last_stale_notify"] = now_ms()
            try:
                from notify import notify
                notify("!!! " + msg)
            except Exception:  # noqa: BLE001
                pass
        return msg


def revalue_fee(round_id: str, asset: str, price: float, source: str, reason: str, path: Path = STATE) -> dict:
    """Дооценка ранее не оценённой комиссии: исходная версия расчёта сохраняется, новая — с источником и причиной."""
    with locked(path) as st:
        r = next((x for x in st["ledger"] if x.get("round_id") == round_id), None)
        if r is None:
            raise KeyError(round_id)
        amount = float((r.get("unvalued_fee") or {}).get(asset) or 0)
        version = {"ts": now_ms(), "asset": asset, "amount": amount, "price": price, "value_usdt": amount * price,
                   "source": source, "reason": reason,
                   "previous": {k: r.get(k) for k in ("pnl_status", "pnl_net_known_usdt", "pnl_usdt",
                                                     "fee_reserve_usdt")}}
        r.setdefault("revaluations", []).append(version)
        left = {k: v for k, v in (r.get("unvalued_fee") or {}).items() if k != asset}
        r["unvalued_fee"] = left
        r["pnl_net_known_usdt"] = float(r.get("pnl_net_known_usdt") or 0) - amount * price
        if not left:
            r["pnl_status"] = "NET_PNL_COMPLETE (дооценено)"
            r["fee_reserve_usdt"] = 0.0
            r["pnl_usdt"] = r["pnl_net_known_usdt"]
        return version


@contextmanager
def symbol_lock(symbol: str, blocking: bool = False, root: Path | None = None):
    """Пара занята живым процессом пробы: супервизор восстановления её не трогает. Да — yield True."""
    lockp = (root or STATE.parent) / f"probe_{symbol}.plock"
    lockp.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lockp, os.O_CREAT | os.O_RDWR)
    got = False
    try:
        try:
            import fcntl
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
                got = True
            except BlockingIOError:
                got = False
        except ImportError:
            import msvcrt
            try:
                msvcrt.locking(fd, msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK, 1)
                got = True
            except OSError:
                got = False
        yield got
    finally:
        if got:
            try:
                import msvcrt
                os.lseek(fd, 0, 0)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except (ImportError, OSError):
                pass
        os.close(fd)


def drop_position(symbol: str, path: Path = STATE) -> None:
    """Ордер не принят — позиции нет (без записи в журнал раундов)."""
    with locked(path) as st:
        st["positions"].pop(symbol, None)


def is_blocked(path: Path = STATE) -> bool:
    with locked(path) as st:
        return bool(st["blocked"])


def block(symbol: str, reason: str, path: Path = STATE, **info) -> None:
    with locked(path) as st:
        if not st["blocked"]:
            st["blocked"] = {"since": now_ms(), "symbol": symbol, "reason": reason, **info}
        st.setdefault("events", []).append({"ts": now_ms(), "symbol": symbol, "reason": reason, **info})


def clear(note: str, path: Path = STATE) -> dict:
    with locked(path) as st:
        was = st["blocked"]
        st["blocked"] = None
        st.setdefault("events", []).append({"ts": now_ms(), "cleared": was, "note": note})
        return {"cleared": was, "open_positions": st["positions"]}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--clear", metavar="ЗАМЕТКА", help="снять блокировку после сверки ордеров и остатков")
    p.add_argument("--drop-position", metavar="SYMBOL", help="убрать пару из открытых после ручной сверки")
    p.add_argument("--watch", nargs=2, metavar=("ЧАСЫ", "КТО"),
                   help="подтвердить наблюдение оператора за пробами на N часов (без него MARKET-раунд не начинается)")
    p.add_argument("--unwatch", action="store_true", help="снять подтверждение наблюдения")
    p.add_argument("--revalue", nargs=5, metavar=("ROUND_ID", "ASSET", "PRICE", "SOURCE", "REASON"),
                   help="дооценить комиссию в третьей валюте, сохранив исходную версию")
    a = p.parse_args()
    if a.watch:
        print("наблюдение:", watch(float(a.watch[0]), a.watch[1]))
    if a.unwatch:
        unwatch()
    if a.revalue:
        print(revalue_fee(a.revalue[0], a.revalue[1], float(a.revalue[2]), a.revalue[3], a.revalue[4]))
    if a.drop_position:
        with locked() as st:
            print("убрана позиция:", st["positions"].pop(a.drop_position.upper(), None))
    if a.clear:
        print(json.dumps(clear(a.clear), ensure_ascii=False, indent=1))
    with locked() as st:
        print(json.dumps({k: st[k] for k in ("blocked", "positions", "breaches", "operator_watch", "supervisor")},
                         ensure_ascii=False, indent=1))
        print(f"раундов {len(st['ledger'])}, убыток за сутки {loss_since(st, now_ms() - now_ms() % 86_400_000):.3f} USDT")


if __name__ == "__main__":
    main()
