"""Обязательный регрессионный набор (решение клиента 10.11): запускать после каждого изменения ядра.

Проверяет на окнах открытия записей (T0 − 90 с … T0 + 135 с):
  1. все контролируемые тесты tools/p0_stress.py — решений на недействительном стакане нет, будущих данных нет,
     REST-снимок + WS diff собирают тот же стакан, восстановление после потери diff, задержка / повтор / порядок
     кадров, PAPER = REPLAY на одинаковом потоке;
  2. неизменность результатов: строки основной стратегии (путь REPLAY и путь PAPER, набор PAPER) по каждой записи
     совпадают с эталоном tools/regress_golden.json (сравнивается отпечаток строк);
  3. то же для CAP BREAKOUT (tools/cap_replay.py --all-frozen) по полным записям замороженных листингов, если задана
     папка --cap-dir (в ней rec_*.jsonl.gz и probe_*.jsonl со строками 30010);
  4. причинность CAP BREAKOUT (решение клиента 11.11) на тех же записях: снятие потолка — не раньше обработки первой
     сделки выше потолка; обрыв потока на любом из моментов около неё и снятия не меняет того, что уже решено
     (prefix invariance); путь PAPER (движок создан на T0 + 1 с, как после обнаружения 30010 пробой) = REPLAY.
Запись, которой нет в эталоне, — предупреждение, не ошибка. Код выхода 1 — что-то не совпало.
  python3 tools/p0_regress.py extract --src logs --dst logs/p0win
  python3 tools/p0_regress.py check --win logs/p0win --cap-dir logs
  python3 tools/p0_regress.py save --win logs/p0win --cap-dir logs --reason "что изменилось и почему"
Эталон переписывается только командой save с причиной — при осознанном изменении результатов.
"""

import argparse
import contextlib
import gzip
import hashlib
import io
import json
import sys
import tempfile
import time
from pathlib import Path

GOLDEN = Path(__file__).resolve().parent / "regress_golden.json"
WIN_BEFORE_MS, WIN_AFTER_MS = 90_000, 135_000


def digest(rows: list) -> str:
    return hashlib.sha256(json.dumps(rows, sort_keys=True, default=str, ensure_ascii=False).encode()).hexdigest()[:16]


def latest_records(folder: Path) -> list[Path]:
    latest = {}
    for p in sorted(folder.glob("rec_*USDT_2*.jsonl.gz")):
        latest[p.name.split("_")[1]] = p
    return sorted(latest.values())


def extract(src: Path, dst: Path) -> None:
    """Окна открытия: строки с t в [T0 − 90 с, T0 + 135 с + 15 с], все exchangeInfo и OPEN_DETECTED, порядок тот же."""
    dst.mkdir(parents=True, exist_ok=True)
    for p in latest_records(src):
        first_open = detected = None
        with gzip.open(p, "rt", encoding="utf-8") as f:
            try:
                for line in f:
                    if '"ch":"exchangeInfo"' in line and '"firstOpenTime"' in line:
                        syms = (json.loads(line)["data"] or {}).get("symbols") or []
                        if syms and syms[0].get("firstOpenTime"):
                            first_open = int(syms[0]["firstOpenTime"])
                    elif '"OPEN_DETECTED"' in line:
                        e = json.loads(line)
                        detected = e["data"].get("open_ms") or e["t"]
                        break
            except EOFError:
                pass
        if detected is None:
            print(f"{p.name}: открытия нет")
            continue
        t0 = first_open or detected
        lo, hi = min(t0, detected) - WIN_BEFORE_MS, max(t0, detected) + WIN_AFTER_MS + 15_000
        n = 0
        with gzip.open(p, "rt", encoding="utf-8") as f, gzip.open(dst / p.name, "wt", encoding="utf-8") as out:
            try:
                for line in f:
                    if not line.endswith("\n"):
                        break
                    t = int(line[5:18])
                    if t > hi:
                        break
                    if lo <= t or '"ch":"exchangeInfo"' in line or '"OPEN_DETECTED"' in line:
                        out.write(line)
                        n += 1
            except EOFError:
                pass
        print(f"{p.name}: строк {n}", flush=True)


def main_rows(win: Path) -> dict:
    """Отпечатки строк основной стратегии по записям: путь REPLAY и путь PAPER (набор PAPER)."""
    import p0_stress as ps
    import paper_vs_replay as pvr
    out = {}
    for p in ps.records(win):
        rec = ps.Rec(p)
        if rec.t0 is None:
            continue
        _, rep = pvr.replay_path(p, ps.CFG)
        pap = ps.Run(rec, rec.entries).rows.values()
        rep = sorted(rep, key=lambda r: str(ps.key(r)))
        pap = sorted(pap, key=lambda r: str(ps.key(r)))
        out[rec.sym] = dict(replay=digest(rep), paper=digest(pap), rows=len(rep) + len(pap))
    return out


def cap_rows(cap_dir: Path) -> dict:
    """Отпечатки строк CAP BREAKOUT по листингам (tools/cap_replay.py --all-frozen на полных записях папки)."""
    import cap_replay
    import core.cap_breakout as cb
    for mod in (cb, cap_replay):
        mod.LOGS = cap_dir
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "cap.json"
        argv = sys.argv
        sys.argv = ["cap_replay.py", "--all-frozen", "--summary-only", "--json", str(path)]
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                cap_replay.main()
        finally:
            sys.argv = argv
        rows = json.loads(path.read_text(encoding="utf-8"))
    by = {}
    for r in rows:
        by.setdefault(r["symbol"], []).append(r)
    return {s: dict(cap=digest(sorted(rs, key=lambda r: json.dumps(r, sort_keys=True, default=str))), rows=len(rs))
            for s, rs in sorted(by.items())}


CAP_KEYS = ("decision_ms", "entry_ms", "entry_px", "reason", "pnl_usdt", "exit_ms")


def cap_causal(cap_dir: Path) -> tuple[list[str], list[str]]:
    """Причинность CAP BREAKOUT на полных записях папки: (ошибки, строки отчёта)."""
    from argparse import Namespace

    import cap_replay
    import core.cap_breakout as cb
    from core.cap_breakout import EARLY_USDT, ORDER_USDT, CapEngine, Opts
    for mod in (cb, cap_replay):
        mod.LOGS = cap_dir
    cfg = Namespace(modes=["FAST", "CONFIRMED", "EARLY"], early={}, early_sweep=False, latency=[50],
                    usdt=[ORDER_USDT], early_usdt=list(EARLY_USDT))  # как PAPER
    errors, lines = [], []
    for path in cap_replay.frozen_records():
        sym = path.name.split("_")[1]
        found = cb.find_cap(sym) or ((cb.KNOWN_CAPS[sym], "") if sym in cb.KNOWN_CAPS else None)
        if not found:
            continue
        cap = found[0]
        idx: dict = {}
        snaps: dict = {}  # момент обрыва → состояние веток после последнего события не позже него
        cuts: list = []

        def sim_state(eng):
            return [(x.state, x.result.get("decision_ms"), x.result.get("entry_ms"), x.result.get("entry_px"),
                     x.buy_misses, round(x.Q, 12), x.tp_fills) for x in eng.sims]

        def mark(i, t, kind, eng):
            if eng.cand_ms is not None and "cand" not in idx:
                idx["cand"] = (i, t, kind)
            if eng.release is not None and "release" not in idx:
                idx["release"] = (i, t, kind)
            for cut in cuts:
                if t <= cut:
                    snaps[cut] = (eng.release, sim_state(eng))

        full, t0 = cap_replay.run_causal(path, cfg, Opts(), cap, on_event=mark)
        if full is None or full.release is None:
            lines.append(f"  {sym}: снятия нет — проверки причинности не нужны")
            continue
        bad = []
        c, r = idx["cand"], idx["release"]
        if not (r[0] >= c[0] and r[1] >= c[1] and c[2] == "deal"):
            bad.append(f"снятие (событие {r[0]}, {r[2]}) раньше первой сделки выше потолка (событие {c[0]})")
        cuts[:] = [c[1] - 1000, c[1], r[1], r[1] + 1000, r[1] + 5000, r[1] + 30_000]
        snaps.clear()
        cap_replay.run_causal(path, cfg, Opts(), cap, on_event=mark)  # второй проход — снимки на моментах обрыва
        for cut in cuts:
            part, _ = cap_replay.run_causal(path, cfg, Opts(), cap, cut_ms=cut)
            if snaps.get(cut) != (part.release, sim_state(part)):
                bad.append(f"обрыв на T0{cut - t0:+d}: состояние веток отличается от полного прогона на тот же момент")
        # путь PAPER: движок создаётся на T0 + 1 с (после 30010 пробы), дальше тот же поток
        eng = CapEngine(cfg, Opts(), cap, t0, {})
        for item in cap_replay.stream(path, t0):
            if item[0] == "info":
                if eng.release is None:
                    eng.info = item[2]
                continue
            t, kind, data = item
            if t < t0 + 1000:
                continue
            if (eng.release is not None and t > eng.release + cap_replay.CAP_AFTER_MS) or (
                    eng.release is None and t > t0 + cap_replay.RELEASE_WAIT_MS):
                break
            eng.on_event(t, kind, data)
            last = t
        if eng.release is not None:
            eng.finish(min(eng.release + cap_replay.CAP_AFTER_MS, last))
        rows_p = {(x["mode"], x["variant"], x["profile"], x["usdt"]): tuple(x.get(k) for k in CAP_KEYS)
                  for x in eng.rows(sym)}
        rows_r = {(x["mode"], x["variant"], x["profile"], x["usdt"]): tuple(x.get(k) for k in CAP_KEYS)
                  for x in full.rows(sym)}
        diff = sum(rows_p.get(k) != rows_r.get(k) for k in set(rows_p) | set(rows_r))
        if diff:
            bad.append(f"путь PAPER против REPLAY: расхождений {diff} из {len(rows_r)}")
        lines.append(f"  {sym}: первая сделка выше потолка T0{c[1] - t0:+d} мс (событие {c[0]}), снятие T0"
                     f"{r[1] - t0:+d} мс (событие {r[0]}, {r[2]}); обрывов {len(cuts)}; PAPER = REPLAY: "
                     f"{len(rows_r) - diff}/{len(rows_r)}" + (f"; ОШИБКИ: {bad[:3]}" if bad else ""))
        errors += [f"CAP {sym}: {b}" for b in bad]
    return errors, lines


def stress(win: Path) -> list[str]:
    """Все тесты p0_stress по всем окнам; список непрошедших «запись: тест»."""
    import p0_stress as ps
    bad = []
    for p in ps.records(win):
        res = ps.run_record(p, set(ps.TESTS))
        bad += [f"{p.name.split('_')[1]}: {n}" for n, r in res.items() if not n.startswith("_") and not r["pass"]]
        print(f"  {p.name.split('_')[1]}: тестов {max(len(res) - 1, 0)}", flush=True)
    return bad


def compare(name: str, now: dict, gold: dict) -> tuple[list[str], list[str]]:
    errors, warnings = [], []
    for sym, v in now.items():
        g = gold.get(sym)
        if g is None:
            warnings.append(f"{name} {sym}: нет в эталоне (новая запись)")
        elif {k: v[k] for k in v if k != "rows"} != {k: g[k] for k in g if k != "rows"}:
            errors.append(f"{name} {sym}: строки изменились ({g.get('rows')} → {v.get('rows')} строк)")
    for sym in gold:
        if sym not in now:  # 13.11: проверка без данных — не «пройдена» (раньше было предупреждение)
            errors.append(f"{name} {sym}: есть в эталоне, нет в данных — проверьте --win / --cap-dir")
    return errors, warnings


def code_label() -> str:
    try:
        from paper import code_version
        return code_version()
    except Exception as e:  # noqa: BLE001 — локально может не быть websockets; версия тогда неизвестна
        return f"unknown ({type(e).__name__})"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", choices=("extract", "check", "save"))
    p.add_argument("--src", default="logs", help="extract: папка с полными записями")
    p.add_argument("--dst", default="logs/p0win", help="extract: куда положить окна")
    p.add_argument("--win", default="logs/p0win", help="check/save: папка с окнами")
    p.add_argument("--cap-dir", help="check/save: папка с полными записями и логами проб для CAP BREAKOUT")
    p.add_argument("--golden", default=str(GOLDEN))
    p.add_argument("--reason", help="save: почему эталон меняется")
    p.add_argument("--skip-stress", action="store_true", help="check: только неизменность результатов")
    a = p.parse_args()
    if a.action == "extract":
        extract(Path(a.src), Path(a.dst))
        return
    started = time.time()
    win = Path(a.win)
    main_now = main_rows(win)
    cap_now = cap_rows(Path(a.cap_dir)) if a.cap_dir else {}
    if a.action == "check" and not a.cap_dir:
        raise SystemExit("check: нужна --cap-dir (причинность и эталон CAP BREAKOUT обязательны, решение клиента 12.11)")
    golden = Path(a.golden)
    if a.action == "save":
        if not a.reason:
            raise SystemExit("save: нужна --reason")
        old = json.loads(golden.read_text(encoding="utf-8")) if golden.exists() else {}
        history = old.get("history", []) + [dict(at=time.strftime("%Y-%m-%d %H:%M"), code=code_label(), reason=a.reason)]
        golden.write_text(json.dumps(dict(main=main_now, cap=cap_now or old.get("cap", {}), history=history),
                                     ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"эталон записан: {golden} (основная {len(main_now)} записей, CAP {len(cap_now or old.get('cap', {}))})")
        return
    gold = json.loads(golden.read_text(encoding="utf-8"))
    errors, warnings = compare("основная", main_now, gold["main"])
    if cap_now:
        e, w = compare("CAP", cap_now, gold.get("cap", {}))
        errors, warnings = errors + e, warnings + w
    if not a.skip_stress:
        print("контролируемые тесты p0_stress:")
        errors += [f"тест не прошёл — {x}" for x in stress(win)]
    if a.cap_dir:
        print("причинность CAP BREAKOUT:")
        e, lines = cap_causal(Path(a.cap_dir))
        print("\n".join(lines))
        errors += e
    for w in warnings:
        print(f"предупреждение: {w}")
    for e in errors:
        print(f"ОШИБКА: {e}")
    print(f"\nрегрессия: основная {len(main_now)} записей, CAP {len(cap_now)} листингов, тесты "
          f"{'пропущены' if a.skip_stress else 'выполнены'}; {'НЕ ПРОЙДЕНА' if errors else 'пройдена'} "
          f"({time.time() - started:.0f} с)")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
