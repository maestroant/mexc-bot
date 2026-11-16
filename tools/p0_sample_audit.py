"""Аудит основной оценочной выборки после исключения VLX (решение клиента 13.11, пакет A–E, п. 2).

1. Знаменатели по каждому алгоритму до и после исключения листингов с меткой core/data_marks:
   записей, другие основания исключения, ветвей, входов (оценочных сделок), суммарный PnL.
   BASELINE — MAIN / base / MAIN_V21 при задержке 50 мс; MAIN_EARLY — вся сетка; CAP BREAKOUT — замороженные листинги.
2. Автоматическая проверка метки во всех шести отчётных путях — не только вывод строки, но и что листинг не попадает
   в агрегаты (итоги групп, сетку MAIN_EARLY, итог CAP, итог PAPER):
   M1 main_replay  M2 paper_report  M3 cap_replay  M4 paper_vs_replay  M5 p0_three_way  M6 listing_report.
  python3 tools/p0_sample_audit.py          # протокол logs/p0_sample_audit_<дата>.json, код выхода 1 при FAIL
"""

import contextlib
import io
import json
import subprocess
import sys
import time
from argparse import Namespace
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import main_replay as mr  # noqa: E402
from core.cap_breakout import MAIN_EARLY_GRID  # noqa: E402
from core.data_marks import MARKS, excluded  # noqa: E402

LOGS = HERE.parent / "logs"


def capture(fn, *a, **k) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        fn(*a, **k)
    return buf.getvalue()


def stats(rows: list[dict]) -> dict:
    got = [r for r in rows if "pnl_usdt" in r]
    return {"branches": len(rows), "entries": len(got), "pnl_usdt": round(sum(r["pnl_usdt"] for r in got), 4)}


def denominators() -> tuple[dict, list[dict], list[dict]]:
    args = Namespace(latency=[50], usdt=[mr.ORDER_USDT], fee=None, sweep=False, main_early=MAIN_EARLY_GRID)
    rows, metas = [], []
    for f in mr.usdt_records():
        r, m = mr.run_listing(f, args)
        rows += r
        metas.append(m)
    evaluated = [m for m in metas if m["group"] in ("no_cap", "cap", "unconfirmed")]
    other = {}
    for m in metas:
        if m["group"] not in ("no_cap", "cap", "unconfirmed"):
            key = "API закрыт в начальном окне" if m["group"] == "api" else (m["status"] or "?")
            other[key] = other.get(key, 0) + 1
    out = {"records_total": len(metas), "excluded_other_reasons": other, "excluded_by_mark":
           sorted(m["symbol"] for m in metas if m["group"] == "unconfirmed")}
    algos = {
        "BASELINE (MAIN base, MAIN_V21, 50 мс)": lambda r: r["mode"] == "MAIN" and r["variant"] == "base"
        and r["profile"] == "MAIN_V21" and r["latency"] == 50 and r["usdt"] == mr.ORDER_USDT,
        "MAIN_EARLY (вся сетка, только листинги без потолка)": lambda r: r["mode"] == "MAIN_EARLY" and r["latency"] == 50,
    }
    for name, pick in algos.items():
        pool = {m["symbol"] for m in evaluated if (m["group"] != "cap" or "BASELINE" in name)}
        before = [r for r in rows if r["symbol"] in pool and pick(r)]
        after = [r for r in before if not excluded(r["symbol"])]
        out[name] = {"before": {"records": len(pool), **stats(before)},
                     "after": {"records": len({s for s in pool if not excluded(s)}), **stats(after)}}
    import cap_replay
    frozen = {p.name.split("_")[1] for p in cap_replay.frozen_records()}
    out["CAP BREAKOUT (замороженные)"] = {"records": len(frozen), "marked_in_pool": sorted(frozen & set(MARKS)),
                                          "note": "VLX — не замороженный листинг: в выборке CAP его не было"}
    return out, rows, metas


def checks(rows: list[dict], metas: list[dict]) -> list[dict]:
    res = []

    def add(name, ok, detail):
        res.append({"check": name, "result": "PASS" if ok else "FAIL", "detail": detail})
        print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}", flush=True)

    vlx = [m for m in metas if m["symbol"] == "VLXUSDT"]
    summ = capture(mr.print_baseline_summary, rows, metas, 50, mr.ORDER_USDT)
    early = capture(mr.print_early, rows, metas, 50, 15)
    base = capture(mr.print_baseline, rows, metas, 50, mr.ORDER_USDT)
    in_unconf = base.split("вне основной выборки")[-1] if "вне основной выборки" in base else ""
    no_cap_n = sum(1 for m in metas if m["group"] == "no_cap")
    add("M1 main_replay", bool(vlx) and vlx[0]["group"] == "unconfirmed" and "VLX" not in summ and "VLX" not in early
        and "VLX" in in_unconf and f"Без потолка ({no_cap_n})" in summ,
        f"группа VLX = {vlx[0]['group'] if vlx else 'нет записи'}; в итогах групп VLX нет; «Без потолка ({no_cap_n})» "
        f"без VLX; в сетке MAIN_EARLY VLX нет; VLX показан отдельной группой")

    import paper_report as pr
    items = pr.summaries(60)
    out = capture(lambda: (sys.argv.__setitem__(slice(None), ["paper_report.py", "--days", "60"]), pr.main()))
    has_vlx_log = any(s["symbol"] == "VLXUSDT" for s in items)
    head = out.splitlines()[1] if len(out.splitlines()) > 1 else ""
    body = "\n".join(out.splitlines()[2:])
    add("M2 paper_report", has_vlx_log and "DATA_AVAILABILITY_UNCONFIRMED" in out
        and f"листингов {len(items) - 1}" in head and " VLX " not in body,
        f"лог PAPER VLX есть = {has_vlx_log}; метка выведена; число листингов в итоге {len(items) - 1} (без VLX); "
        f"строк VLX в итогах нет")

    import cap_replay as cr
    fake = [{"symbol": "VLXUSDT", "x": 1}, {"symbol": "HDLRUSDT", "x": 2}]
    kept = []
    txt = capture(lambda: kept.extend(cr.main_sample(fake)))
    add("M3 cap_replay", [r["symbol"] for r in kept] == ["HDLRUSDT"] and "DATA_AVAILABILITY_UNCONFIRMED" in txt,
        "итог CAP (main_sample) отбрасывает строки VLX и печатает метку; --json и регрессия — без фильтра")

    rec = sorted(LOGS.glob("rec_VLXUSDT_2*.jsonl.gz"))
    import paper_vs_replay as pvr
    from core.cap_breakout import MAIN_EARLY_PAPER, ORDER_USDT
    cfg = Namespace(latency=[50], usdt=[ORDER_USDT], sweep=False, main_early=MAIN_EARLY_PAPER)
    o = capture(pvr.compare, rec[-1], cfg) if rec else ""
    add("M4 paper_vs_replay", "DATA_AVAILABILITY_UNCONFIRMED" in o, "сравнение по записи VLX начинается с метки")

    o = subprocess.run([sys.executable, str(HERE / "p0_three_way.py"), "VLXUSDT"], capture_output=True, text=True,
                       cwd=HERE.parent).stdout
    add("M5 p0_three_way", "DATA_AVAILABILITY_UNCONFIRMED" in o, "отчёт трёх соединений по VLX выводит метку")

    o = subprocess.run([sys.executable, str(HERE / "listing_report.py"), "VLXUSDT"], capture_output=True, text=True,
                       cwd=HERE.parent).stdout
    add("M6 listing_report", "| VLXUSDT (DATA_AVAILABILITY_UNCONFIRMED) |" in o, "строка VLX в таблице листингов с меткой")
    return res


def main() -> None:
    t0 = time.monotonic()
    den, rows, metas = denominators()
    print(json.dumps(den, ensure_ascii=False, indent=1))
    res = checks(rows, metas)
    out = LOGS / f"p0_sample_audit_{time.strftime('%Y%m%d')}.json"
    out.write_text(json.dumps({"denominators": den, "mark_checks": res}, ensure_ascii=False, indent=1), encoding="utf-8")
    fails = sum(r["result"] == "FAIL" for r in res)
    print(f"\nпроверок метки {len(res)}, не прошли {fails}; протокол {out} ({time.monotonic() - t0:.0f} с)")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
