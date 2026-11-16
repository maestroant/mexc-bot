"""До/после варианта (b) — один расчёт глубины bids на состояние стакана (решение клиента 13.11, A–E, п. 5.2).

Один и тот же журнал прогоняется путём PAPER основной стратегии (p0_stress.Run, конфигурация PAPER) дважды — с core.cap_breakout.DEPTH_MEMO выключенным и включённым:
  * строки результатов (вход, выход, причины, PnL, MFE/MAE, глубины) — побитово равны (сравнение JSON-отпечатков);
  * время обработки событий p50/p95/p99/max и сумма, число вызовов depth_usdt.
Время — на этом сервере в момент прогона; одна машина, тот же процесс, по очереди, по 3 повтора с чередованием порядка — берётся лучший.
  python3 tools/depth_memo_bench.py logs/paperrec_BTCUSDT_*.jsonl.gz [...]
"""

import hashlib
import json
import sys
import time
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import core.cap_breakout as cb  # noqa: E402
from core.cap_breakout import MAIN_EARLY_PAPER, ORDER_USDT, MainEngine, Opts  # noqa: E402
from paper_vs_replay import load_open  # noqa: E402


def q(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else 0.0


def run(path: Path, memo: bool) -> dict:
    """Путь PAPER (p0_stress.Run: кадры журнала в порядке получения, как живой PAPER) — время каждого кадра."""
    import p0_stress as ps
    cb.DEPTH_MEMO = memo
    cb._MEMO.update(lv=None, vals={})
    calls = [0]
    orig = cb.depth_usdt

    def counted(*a):
        calls[0] += 1
        return orig(*a)
    cb.depth_usdt = counted
    times = []
    orig_on = cb.MainEngine.on_event

    def timed(self, *a, **k):
        c = time.perf_counter()
        try:
            return orig_on(self, *a, **k)
        finally:
            times.append((time.perf_counter() - c) * 1000)
    cb.MainEngine.on_event = timed
    try:
        rec = ps.Rec(path)
        rows = list(ps.Run(rec, rec.entries).rows.values())
    finally:
        cb.depth_usdt = orig
        cb.MainEngine.on_event = orig_on
        cb.DEPTH_MEMO = False
    dig = hashlib.sha256(json.dumps(sorted(rows, key=lambda r: json.dumps(r, sort_keys=True, default=str)),
                                    sort_keys=True, default=str).encode()).hexdigest()[:16]
    return {"rows": len(rows), "entries": sum("entry_ms" in r for r in rows), "digest": dig, "depth_calls": calls[0],
            "events": len(times), "sum_ms": round(sum(times), 1), "p50": round(q(times, .5), 3),
            "p95": round(q(times, .95), 3), "p99": round(q(times, .99), 3), "max": round(max(times), 2)}


def main() -> None:
    out = []
    for f in sys.argv[1:]:
        p = Path(f)
        best = {}
        runs = {False: [], True: []}
        for memo in (False, True, True, False, False, True):  # чередование — без преимущества прогрева
            runs[memo].append(run(p, memo))
        for memo in (False, True):
            best[memo] = min(runs[memo], key=lambda r: r["sum_ms"])
            best[memo]["digests_same_across_repeats"] = len({r["digest"] for r in runs[memo]}) == 1
        same = best[False]["digest"] == best[True]["digest"] and best[False]["rows"] == best[True]["rows"]
        row = {"file": p.name, "before": best[False], "after": best[True], "rows_identical": same}
        out.append(row)
        b, a = best[False], best[True]
        print(f"{p.name}: строк {b['rows']}, входов {b['entries']}, результаты равны: {same}")
        for k in ("depth_calls", "sum_ms", "p50", "p95", "p99", "max"):
            print(f"  {k:<11} до {b[k]:>10}   после {a[k]:>10}")
    stamp = time.strftime("%Y%m%d")
    Path(__file__).resolve().parent.parent.joinpath("logs", f"depth_memo_bench_{stamp}.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    sys.exit(0 if all(r["rows_identical"] for r in out) else 1)


if __name__ == "__main__":
    main()
