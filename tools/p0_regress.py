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


ACCOUNT: dict = {"main": {"found": 0, "run": 0, "skipped": {}, "errors": {}, "knowledge": {}},
                 "cap": {"found": 0, "run": 0, "skipped": {}, "errors": {}, "no_release": {}}}


def file_sha(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def manifest(win: Path, cap_dir: Path | None) -> dict:
    """Входы проверки: окна основной стратегии и полные записи CAP — имя → SHA-256 (16 знаков)."""
    import p0_stress as ps
    out = {"main": {p.name: file_sha(p) for p in ps.records(win)}}
    if cap_dir is not None:
        import cap_replay
        import core.cap_breakout as cb
        for mod in (cb, cap_replay):
            mod.LOGS = cap_dir
        out["cap"] = {p.name: file_sha(p) for p in cap_replay.frozen_records()}
    return out


def main_rows(win: Path) -> dict:
    """Отпечатки строк основной стратегии по записям: путь REPLAY и путь PAPER (набор PAPER). Каждая найденная запись
    учитывается: запущена, пропущена с причиной (нет открытия) или ошибка (повреждена) — без молчаливого пропуска."""
    import p0_stress as ps
    import paper_vs_replay as pvr
    out, acc = {}, ACCOUNT["main"]
    recs = ps.records(win)
    acc["found"] = len(recs)
    for p in recs:
        sym = p.name.split("_")[1]
        try:  # окна вырезает extract и пишет целиком: обрезанный gzip — повреждение (core.feed читает его молча)
            with gzip.open(p, "rb") as f:
                while f.read(1 << 20):
                    pass
        except (EOFError, OSError) as e:
            acc["errors"][sym] = f"{p.name}: gzip не дочитан до конца ({type(e).__name__}: {e})"
            continue
        try:
            rec = ps.Rec(p)
            if rec.t0 is None:
                acc["skipped"][sym] = "в окне нет открытия"
                continue
            _, rep = pvr.replay_path(p, ps.CFG)
            run = ps.Run(rec, rec.entries)
            pap = run.rows.values()
            # время знания exchangeInfo (решение 16.11, п. 6.3): REPLAY получает параметры инструмента заранее — они
            # обязаны совпасть с тем, что путь PAPER имел к старту веток, и T0 — тоже; иначе это заглядывание REPLAY
            pinfo = run.eng.info or {}
            diff = [k for k in ("firstOpenTime",) + ps.INFO_KEYS
                    if str(rec.info_replay.get(k)) != str(pinfo.get(k))]
            if run.eng.open_ms != rec.t0 or diff:
                acc["knowledge"][sym] = (f"T0 REPLAY {rec.t0} / PAPER {run.eng.open_ms}; параметры, известные REPLAY "
                                         f"заранее, отличаются: {diff or 'нет'}")
        except Exception as e:  # noqa: BLE001 — повреждённая запись — ошибка проверки, не пропуск
            acc["errors"][sym] = f"{p.name}: {type(e).__name__}: {e}"
            continue
        rep = sorted(rep, key=lambda r: str(ps.key(r)))
        pap = sorted(pap, key=lambda r: str(ps.key(r)))
        out[rec.sym] = dict(replay=digest(rep), paper=digest(pap), rows=len(rep) + len(pap))
    acc["run"] = len(out)
    return out


def cap_rows(cap_dir: Path) -> dict:
    """Отпечатки строк CAP BREAKOUT по листингам (tools/cap_replay.py --all-frozen на полных записях папки)."""
    import cap_replay
    import core.cap_breakout as cb
    for mod in (cb, cap_replay):
        mod.LOGS = cap_dir
    if not cap_replay.frozen_records():  # пустая папка — 0 листингов; сравнение с эталоном покажет «нет в данных»
        ACCOUNT["cap"]["found"] = 0
        return {}
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
    ACCOUNT["cap"]["found"] = len(cap_replay.frozen_records())
    by = {}
    for r in rows:
        by.setdefault(r["symbol"], []).append(r)
    out = {s: dict(cap=digest(sorted(rs, key=lambda r: json.dumps(r, sort_keys=True, default=str))), rows=len(rs))
           for s, rs in sorted(by.items())}
    ACCOUNT["cap"]["run"] = len(out)
    for f in cap_replay.frozen_records():
        sym = f.name.split("_")[1]
        if sym not in out:  # движок CAP не открыл ни одной ветки — отрицательный оракул, не пропуск (решение 16.11, п. 7)
            ACCOUNT["cap"]["no_release"][sym] = no_release_oracle(f)
    return out


def no_release_oracle(path: Path) -> dict:
    """EXPECTED_NO_RELEASE_VERIFIED (решение клиента 16.11, п. 7): у замороженного листинга без строк CAP проверяется, что
    причинный прогон (cap_replay.run_causal, конфигурация PAPER) по записанному потоку (1) не подтвердил снятие потолка,
    (2) не открыл ни одного входа, которому нужно подтверждённое снятие, (3) даёт исход по правилу PAPER
    NO_RELEASE_OBSERVED_WITHIN_WINDOW, и фиксирует фактическую границу окна (T0 + 30 мин или конец записи, что раньше).
    Это не PASS полного теста снятия: снятия в записи нет, проверено только отсутствие ложного снятия и ложного входа."""
    from argparse import Namespace

    import cap_replay
    import core.cap_breakout as cb
    from core.cap_breakout import EARLY_USDT, ORDER_USDT, Opts
    sym = path.name.split("_")[1]
    found = cb.find_cap(sym) or ((cb.KNOWN_CAPS[sym], "") if sym in cb.KNOWN_CAPS else None)
    cfg = Namespace(modes=["FAST", "CONFIRMED", "EARLY"], early={}, early_sweep=False, latency=[50],
                    usdt=[ORDER_USDT], early_usdt=list(EARLY_USDT))
    from core.feed import iter_lines, line_t
    eng, open_ms = cap_replay.run_causal(path, cfg, Opts(), found[0])
    if eng is None:
        return dict(ok=False, why="в записи нет открытия")
    rec_end = None
    for line in iter_lines(path):
        rec_end = line_t(line)
    entries = sum(1 for s in eng.sims if getattr(s, "entry_ms", None))
    status = "RELEASE_OBSERVED" if eng.release is not None else "NO_RELEASE_OBSERVED_WITHIN_WINDOW"
    until = min(open_ms + cap_replay.RELEASE_WAIT_MS, rec_end or open_ms)
    ok = eng.release is None and entries == 0 and status == "NO_RELEASE_OBSERVED_WITHIN_WINDOW"
    return dict(ok=ok, status=status, release_ms=eng.release, entries=entries, sims=len(eng.sims), cap=found[0],
                t0=open_ms, window_until_rel_s=round((until - open_ms) / 1000, 1),
                window_full_30min=bool(rec_end and rec_end >= open_ms + cap_replay.RELEASE_WAIT_MS),
                why=None if ok else f"снятие {eng.release}, входов {entries}")


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
        try:
            with gzip.open(p, "rb") as f:
                while f.read(1 << 20):
                    pass
            res = ps.run_record(p, set(ps.TESTS))
        except Exception as e:  # noqa: BLE001 — повреждённая запись: тест не пройден, не пропущен
            bad.append(f"{p.name.split('_')[1]}: запись не прочитана — gzip не дочитан или повреждён ({type(e).__name__}: {e})")
            continue
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
    golden = Path(a.golden)
    if a.action == "check" and not a.cap_dir:
        raise SystemExit("ОШИБКА: check без --cap-dir — причинность и эталон CAP BREAKOUT обязательны (решение 12.11)")
    if a.action == "check" and not golden.is_file():
        raise SystemExit(f"ОШИБКА: эталон {golden} недоступен — проверка невозможна")
    for d in [win] + ([Path(a.cap_dir)] if a.cap_dir else []):
        if not d.is_dir():
            raise SystemExit(f"ОШИБКА: папка {d} не существует")
    main_now = main_rows(win)
    cap_now = cap_rows(Path(a.cap_dir)) if a.cap_dir else {}
    inputs = manifest(win, Path(a.cap_dir) if a.cap_dir else None)
    if a.action == "save":
        if not a.reason:
            raise SystemExit("save: нужна --reason")
        old = json.loads(golden.read_text(encoding="utf-8")) if golden.exists() else {}
        history = old.get("history", []) + [dict(at=time.strftime("%Y-%m-%d %H:%M"), code=code_label(), reason=a.reason)]
        if ACCOUNT["main"]["errors"] or ACCOUNT["cap"]["errors"]:
            raise SystemExit(f"save: есть ошибки входных данных — эталон не записан: {ACCOUNT}")
        bad_nr = {s: r for s, r in ACCOUNT["cap"]["no_release"].items() if not r["ok"]}
        if bad_nr:
            raise SystemExit(f"save: оракул отсутствия снятия не прошёл — эталон не записан: {bad_nr}")
        nr = sorted(ACCOUNT["cap"]["no_release"]) if cap_now else old.get("cap_expected_no_release", [])
        golden.write_text(json.dumps(dict(main=main_now, cap=cap_now or old.get("cap", {}), cap_expected_no_release=nr,
                                          inputs=inputs, history=history), ensure_ascii=False, indent=1),
                          encoding="utf-8")
        print(f"эталон записан: {golden} (основная {len(main_now)} записей, CAP {len(cap_now or old.get('cap', {}))})")
        return
    try:
        gold = json.loads(golden.read_text(encoding="utf-8"))
        gold["main"], gold["cap"]
    except (ValueError, KeyError) as e:
        raise SystemExit(f"ОШИБКА: эталон {golden} повреждён ({type(e).__name__}: {e})")
    errors, warnings = compare("основная", main_now, gold["main"])
    e, w = compare("CAP", cap_now, gold.get("cap", {}))  # всегда: пустая папка CAP — ошибка, не тихий пропуск
    errors, warnings = errors + e, warnings + w
    for sym, why in ACCOUNT["main"]["knowledge"].items():
        errors.append(f"основная {sym}: время знания exchangeInfo — {why}")
    exp_nr, nr = set(gold.get("cap_expected_no_release") or []), ACCOUNT["cap"]["no_release"]
    for sym, r in nr.items():  # решение 16.11, п. 7: EXPECTED_NO_RELEASE_VERIFIED — не PASS и не тихий пропуск
        if not r["ok"]:
            errors.append(f"CAP {sym}: оракул отсутствия снятия не прошёл — {r['why']}")
        elif sym not in exp_nr:
            errors.append(f"CAP {sym}: строк CAP нет, а в эталоне листинг не объявлен ожидаемым без снятия "
                          "(cap_expected_no_release) — набор меняется только через save с причиной")
    if a.cap_dir and cap_now:
        for sym in sorted(exp_nr - set(nr)):
            errors.append(f"CAP {sym}: в эталоне объявлен ожидаемым без снятия, но оракул не выполнен (есть строки CAP "
                          "или нет записи)")
    for part in ("main", "cap"):
        for sym, why in ACCOUNT[part]["errors"].items():
            errors.append(f"{part} {sym}: запись не прочитана — {why}")
        if ACCOUNT[part]["run"] == 0:
            errors.append(f"{part}: 0 сравнений — проверка ничего не проверила")
    gin = gold.get("inputs")
    if gin is None:
        warnings.append("в эталоне нет манифеста входов (SHA-256) — сверка версий входов невозможна до save")
    else:
        for part in ("main", "cap"):
            for name, h in (gin.get(part) or {}).items():
                now_h = (inputs.get(part) or {}).get(name)
                if now_h is None:
                    errors.append(f"вход {part} {name}: есть в манифесте эталона, нет в данных")
                elif now_h != h:
                    errors.append(f"вход {part} {name}: SHA-256 {now_h} ≠ эталона {h} — входные данные изменились")
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
    for part, title in (("main", "основная"), ("cap", "CAP")):
        acc = ACCOUNT[part]
        gold_part = gold["main"] if part == "main" else gold.get("cap", {})
        now_part = main_now if part == "main" else cap_now
        bad = {x.split()[1].rstrip(":") for x in errors if x.startswith(("основная " if part == "main" else "CAP ",
                                                                          f"{part} "))}
        passed = sum(1 for s_ in now_part if s_ in gold_part and s_ not in bad)
        print(f"{title}: найдено {acc['found']}, запущено {acc['run']}, пропущено {len(acc['skipped'])}"
              + (f" ({acc['skipped']})" if acc["skipped"] else "")
              + f", прошло {passed}, не прошло {len(bad)}, ошибок чтения {len(acc['errors'])}"
              + ("" if part == "main" else
                 f", EXPECTED_NO_RELEASE_VERIFIED {sum(r['ok'] for r in nr.values())}"
                 + "".join(f"; {s}: {r['status']}, снятие {r['release_ms']}, входов {r['entries']}, окно до T0+"
                           f"{r['window_until_rel_s']} с{' (полные 30 мин)' if r['window_full_30min'] else ' (конец записи)'}"
                           for s, r in sorted(nr.items()) if r['ok'])))
    head = "?"
    try:
        import subprocess
        head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                              cwd=Path(__file__).resolve().parent).stdout.strip() or "нет git"
    except Exception:  # noqa: BLE001
        pass
    print(f"код {code_label()}; git {head}; эталон {golden.name}, последняя запись: "
          f"{(gold.get('history') or [{}])[-1].get('at')}; входов в манифесте "
          f"{sum(len(v) for v in (gold.get('inputs') or {}).values())}")
    print(f"\nрегрессия: основная {len(main_now)} записей, CAP {len(cap_now)} листингов, тесты "
          f"{'пропущены' if a.skip_stress else 'выполнены'}; {'НЕ ПРОЙДЕНА' if errors else 'пройдена'} "
          f"({time.time() - started:.0f} с)")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
