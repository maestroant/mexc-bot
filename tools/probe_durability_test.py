"""Долговечность состояния проб на РЕАЛЬНОМ файловом хранилище и РЕАЛЬНЫХ процессах (решение клиента 13.11, п. 5).

В отличие от tools/probe_scenarios.py (биржа-заглушка, часы сценария, один процесс) здесь — настоящие процессы Linux,
SIGKILL, flock ядра и файловая система сервера. Ни одного запроса к бирже. Только Linux (fcntl, SIGKILL, rlimit).
  K1 kill -9 во время записи: процесс непрерывно пишет состояние (регистрация ордеров, резерв/закрытие позиции);
     его убивают SIGKILL в случайный момент 40 раз подряд на одном файле — после каждого убийства файл читается,
     не повреждён, число записей не уменьшается, «нечитаемых» копий нет (повторные перезапуски).
  K2 параллельные писатели: 4 процесса × 150 изменений счётчика под блокировкой — ни одно изменение не потеряно.
  K3 супервизор и проба одновременно: пока процесс пробы держит блокировку пары, второй процесс её не получает;
     после SIGKILL держателя ядро снимает блокировку — супервизор получает пару (нет «вечной» блокировки).
  K4 неполная запись: обрезанный файл состояния — чтение не продолжает работу на мусоре: аварийная блокировка всех
     пробных BUY, повреждённый файл сохранён для разбора.
  K5 отказ хранилища (каталог только для чтения): регистрация ордера падает с ошибкой, файл состояния не изменён,
     ордер на биржу (заглушку) не отправлен — регистрация идёт до отправки.
  K6 отказ записи на середине (предел размера файла, EFBIG): запись временного файла обрывается, исходный файл
     состояния цел, ордер не отправлен.
  python3 tools/probe_durability_test.py        # протокол: logs/probe_durability_<дата>.json
"""

import json
import multiprocessing as mp
import os
import platform
import random
import resource
import signal
import stat
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probe_risk as risk  # noqa: E402


def writer(path: str, start: int) -> None:
    p = Path(path)
    i = start
    while True:
        i += 1
        risk.register_order(f"dur{os.getpid()}_{i}", path=p, symbol="XUSDT", check="cap_maker", status="SENDING")
        if risk.reserve("XUSDT", 1.0, "CAP_LIMIT_MAKER_PROBE", f"R{i}", path=p) is None:
            risk.close_position("XUSDT", {"kind": "CAP_LIMIT_MAKER_PROBE", "round_id": f"R{i}", "bought": "0",
                                          "pnl_usdt": 0.0, "pnl_status": "NET_PNL_COMPLETE"}, path=p)


def read_raw(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def k1_kill9(tmp: Path) -> tuple[bool, str]:
    p = tmp / "probe_risk.json"
    risk.register_order("seed", path=p, symbol="XUSDT", status="FILLED")
    last_n, bad = 1, []
    for k in range(40):
        proc = mp.Process(target=writer, args=(str(p), k * 100_000))
        proc.start()
        time.sleep(random.uniform(0.005, 0.08))
        os.kill(proc.pid, signal.SIGKILL)
        proc.join()
        try:
            st = read_raw(p)
            n = len(st["orders"])
            if n < last_n:
                bad.append(f"убийство {k}: записей стало меньше {last_n} → {n}")
            last_n = n
            if st["positions"] and st["positions"]["XUSDT"]["stage"] != "BUY_SENT":
                bad.append(f"убийство {k}: позиция в неожиданной стадии {st['positions']}")
        except Exception as e:  # noqa: BLE001
            bad.append(f"убийство {k}: файл не читается: {e}")
        with risk.locked(p) as st:  # «перезапуск»: следующий процесс сам видит незакрытый резерв
            st["positions"].pop("XUSDT", None)
    corrupt = list(tmp.glob("*.corrupt*"))
    ok = not bad and not corrupt and last_n > 1
    return ok, (f"40 SIGKILL во время записи: файл всегда читается, записей {last_n}, повреждённых копий "
                f"{len(corrupt)}" + (f"; ошибки: {bad[:3]}" if bad else ""))


def bump(path: str, n: int) -> None:
    for _ in range(n):
        with risk.locked(Path(path)) as st:
            o = st["orders"].setdefault("counter", {"symbol": "XUSDT", "n": 0})
            o["n"] += 1


def k2_parallel(tmp: Path) -> tuple[bool, str]:
    p = tmp / "probe_risk.json"
    procs = [mp.Process(target=bump, args=(str(p), 150)) for _ in range(4)]
    for x in procs:
        x.start()
    for x in procs:
        x.join()
    n = read_raw(p)["orders"]["counter"]["n"]
    return n == 600, f"4 процесса × 150 изменений под блокировкой: итог {n} из 600"


def hold_lock(root: str, ready) -> None:
    with risk.symbol_lock("XUSDT", blocking=True, root=Path(root)):
        ready.set()
        time.sleep(60)


def try_lock(root: str, q) -> None:
    with risk.symbol_lock("XUSDT", root=Path(root)) as mine:
        q.put(mine)


def k3_lock(tmp: Path) -> tuple[bool, str]:
    ready, q = mp.Event(), mp.Queue()
    holder = mp.Process(target=hold_lock, args=(str(tmp), ready))
    holder.start()
    ready.wait(10)
    p1 = mp.Process(target=try_lock, args=(str(tmp), q))
    p1.start()
    p1.join()
    while_held = q.get(timeout=5)
    os.kill(holder.pid, signal.SIGKILL)
    holder.join()
    p2 = mp.Process(target=try_lock, args=(str(tmp), q))
    p2.start()
    p2.join()
    after_kill = q.get(timeout=5)
    return (while_held is False and after_kill is True,
            f"пара занята живой пробой: супервизор получил блокировку = {while_held}; после SIGKILL пробы = {after_kill}")


def k4_truncated(tmp: Path) -> tuple[bool, str]:
    p = tmp / "probe_risk.json"
    risk.register_order("a", path=p, symbol="XUSDT", status="FILLED")
    raw = p.read_text(encoding="utf-8")
    p.write_text(raw[: len(raw) // 2], encoding="utf-8")
    import notify as nt
    nt.notify = lambda *a, **k: None
    with risk.locked(p) as st:
        blocked = st["blocked"]
    corrupt = list(tmp.glob("*.corrupt*"))
    gate = risk.state_problem("XUSDT", path=p)
    return (bool(blocked) and len(corrupt) == 1 and bool(gate),
            f"обрезанный файл: блокировка всех BUY = {bool(blocked)}, отказ проверки перед BUY = {bool(gate)}, "
            f"повреждённый файл сохранён: {[c.name for c in corrupt]}")


def stub_place(p: Path, fail) -> tuple[bool, int]:
    """Подача пробного ордера через probe_listing.place на бирже-заглушке: проба готовится, затем fail() включает
    отказ хранилища, затем подача. (упала ли регистрация, число POST)."""
    import probe_scenarios as ps
    ex = ps.Exchange([{"open": True}])
    pr = ps.make(ex, p.parent)
    pr.risk_path = p
    fail()
    try:
        pr.place("cap_maker", {"side": "BUY", "type": "LIMIT_MAKER", "quantity": "1", "price": "1"})
        failed = False
    except OSError:
        failed = True
    return failed, sum(1 for x in ex.posts)


def k5_readonly(tmp: Path) -> tuple[bool, str]:
    d = tmp / "ro"
    d.mkdir()
    p = d / "probe_risk.json"
    risk.register_order("seed", path=p, symbol="XUSDT", status="FILLED")
    snap = {}

    def fail():
        snap["before"] = p.read_bytes()
        os.chmod(d, stat.S_IRUSR | stat.S_IXUSR)
    try:
        failed, posts = stub_place(p, fail)
    finally:
        os.chmod(d, stat.S_IRWXU)
    after, before = p.read_bytes(), snap.get("before")
    return (failed and posts == 0 and before == after,
            f"каталог только для чтения: регистрация упала = {failed}, POST на биржу {posts}, файл не изменён = "
            f"{before == after}")


def efbig_child(path: str, q) -> None:
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
    p = Path(path)
    snap = {}

    def fail():
        snap["before"] = p.read_bytes()
        resource.setrlimit(resource.RLIMIT_FSIZE, (p.stat().st_size // 2, resource.RLIM_INFINITY))
    try:
        failed, posts = stub_place(p, fail)
        q.put((failed, posts, snap.get("before")))
    except Exception as e:  # noqa: BLE001
        q.put((f"исключение {type(e).__name__}: {e}", -1, None))


def k6_efbig(tmp: Path) -> tuple[bool, str]:
    p = tmp / "probe_risk.json"
    for i in range(30):
        risk.register_order(f"seed{i}", path=p, symbol="XUSDT", status="FILLED")
    q = mp.Queue()
    c = mp.Process(target=efbig_child, args=(str(p), q))
    c.start()
    c.join()
    failed, posts, before = q.get(timeout=5)
    after = p.read_bytes()
    return (failed is True and posts == 0 and before == after and bool(json.loads(after)),
            f"запись оборвалась на середине (EFBIG): ошибка = {failed}, POST {posts}, исходный файл цел = "
            f"{before == after}")


TESTS = {"K1_kill9_during_write_x40": k1_kill9, "K2_parallel_writers": k2_parallel,
         "K3_symbol_lock_vs_supervisor_and_sigkill": k3_lock, "K4_truncated_state_fail_closed": k4_truncated,
         "K5_readonly_storage_no_order": k5_readonly, "K6_write_fails_midway_no_order": k6_efbig}


def fs_of(path: Path) -> str:
    best = ("?", "")
    for line in Path("/proc/mounts").read_text().splitlines():
        dev, mnt, fs = line.split()[:3]
        if str(path).startswith(mnt) and len(mnt) >= len(best[1]):
            best = (fs, mnt)
    return f"{best[0]} ({best[1]})"


def main() -> None:
    if not hasattr(signal, "SIGKILL") or sys.platform == "win32":
        sys.exit("только Linux")
    import probe_listing as pl
    pl.notify = lambda *a, **k: None
    logs = Path(__file__).resolve().parent.parent / "logs"
    base = logs / "durability_tmp"  # на том же хранилище, что и logs/probe_risk.json
    base.mkdir(exist_ok=True)
    report = {"host": platform.node(), "kernel": platform.release(), "fs": fs_of(base.resolve()),
              "python": sys.version.split()[0], "ts": time.strftime("%Y-%m-%d %H:%M:%S"), "tests": []}
    fails = 0
    for name, fn in TESTS.items():
        with tempfile.TemporaryDirectory(dir=base) as d:
            try:
                ok, what = fn(Path(d))
            except Exception as e:  # noqa: BLE001
                ok, what = False, f"исключение {type(e).__name__}: {e}"
        fails += not ok
        report["tests"].append({"test": name, "result": "PASS" if ok else "FAIL", "detail": what,
                                "storage": "REAL_FS", "processes": "REAL"})
        print(f"{'PASS' if ok else 'FAIL'}  {name}: {what}", flush=True)
    out = logs / f"probe_durability_{time.strftime('%Y%m%d')}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nхранилище {report['fs']}, ядро {report['kernel']}; тестов {len(TESTS)}, не прошли {fails}; протокол {out}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    mp.set_start_method("fork")
    main()
