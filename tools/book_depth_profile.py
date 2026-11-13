"""Профиль глубокого стакана (P0-S2, решение клиента 12.11 вечер / 13.11, п. 6) — исследование, ядро не меняется.

1. По журналу PAPER (paperrec_*, события PROC/LOOP_LAG/SIM_ENTRY/SIM_EXIT): время стадий кадра p50/p95/p99/max,
   очередь кадров, задержки цикла с местом (стек StallSampler), число открытых позиций CAP/MAIN в каждом кадре.
2. Микробенчмарк core.book.depth_usdt на стаканах 20/100/500/1000 уровней: стоимость одного вызова; сколько вызовов
   на кадр даёт check_exit (2 диагностических d07/d15 + 1 при depth_x на каждую открытую позицию).
3. Варианты ускорения на тех же данных — только как предложение: (a) остановка обхода за границей диапазона
   (уровни отсортированы), (b) один расчёт на кадр для всех позиций (общий стакан и bestBid). Проверяется побитовое
   равенство сумм с текущей функцией на случайных стаканах — без равенства вариант не предлагается.
  python3 tools/book_depth_profile.py [--journal logs/paperrec_BTCUSDT_*.jsonl.gz]
"""

import argparse
import gzip
import json
import random
import sys
import timeit
from itertools import takewhile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from core.book import depth_usdt  # noqa: E402


def depth_break(levels, lo: float, hi: float) -> float:
    """Вариант (a): bids по убыванию — после p < lo дальше только меньше. Те же слагаемые в том же порядке и тот же
    sum() (в Python 3.12 sum по float — компенсированное суммирование; свой цикл дал бы другие биты)."""
    return sum(p * q for p, q in takewhile(lambda x: x[0] >= lo, levels) if p <= hi)


def q(xs: list, p: float):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None


def journal_profile(path: Path) -> None:
    proc, lags, open_pos, frames_pos = [], [], 0, []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                e = json.loads(line)
            except ValueError:
                continue
            ch = e.get("ch")
            if ch == "SIM_ENTRY":
                open_pos += 1
            elif ch == "SIM_EXIT":
                open_pos -= 1
            elif ch == "PROC":
                proc.append(e["data"])
                frames_pos.append(open_pos)
            elif ch == "LOOP_LAG":
                lags.append(e["data"])
    print(f"журнал {path.name}: кадров {len(proc)}")
    print(f"  {'стадия':<7} {'p50':>7} {'p95':>7} {'p99':>7} {'max':>7} {'сумма':>8}  мс")
    for k in ("ms", "parse", "book", "eval", "base", "exp", "track", "jw"):
        xs = [d[k] for d in proc if isinstance(d.get(k), (int, float))]
        if xs:
            print(f"  {k:<7} {q(xs, .5):7.3f} {q(xs, .95):7.3f} {q(xs, .99):7.3f} {max(xs):7.3f} {sum(xs):8.1f}")
    qs = [d.get("q", 0) for d in proc]
    print(f"  очередь кадров: p50 {q(qs, .5)}, p95 {q(qs, .95)}, p99 {q(qs, .99)}, max {max(qs)}")
    if frames_pos:
        print(f"  открытых позиций в кадре: p50 {q(frames_pos, .5)}, max {max(frames_pos)}")
        heavy = sorted(zip((d.get("exp", 0) for d in proc), frames_pos), reverse=True)[:5]
        print("  самые тяжёлые кадры (эксп. ветки мс, открытых позиций): " + ", ".join(f"{a:.1f}/{b}" for a, b in heavy))
    ll = sorted(d["lag_ms"] for d in lags)
    print(f"  задержки цикла > 20 мс: {len(ll)}" + (f", p50 {q(ll, .5)}, p95 {q(ll, .95)}, max {ll[-1]}" if ll else ""))
    where = {}
    for d in lags:
        top = (d.get("where") or "?").split(" <- ")
        site = next((s for s in top if "cap_breakout" in s or "paper.py" in s), top[0])
        where.setdefault(site, []).append(d["lag_ms"])
    for site, xs in sorted(where.items(), key=lambda kv: -max(kv[1]))[:5]:
        print(f"    {site}: {len(xs)} раз, max {max(xs)} мс")


def bench() -> None:
    print("\nмикробенчмарк depth_usdt (bids по убыванию, диапазон −0,7 % и −1,5 % от bestBid):")
    print(f"  {'стакан':<34} {'уровней':>7} {'полный, мкс':>12} {'с остановкой, мкс':>18} {'равенство':>10}")
    rnd = random.Random(7)
    cases = [("плотный (BTC: шаг 0,01 при 100 000)", n, 0.01) for n in (20, 100, 500, 1000)] +             [("редкий (новый листинг: шаг ~0,5 %)", n, 500.0) for n in (20, 100, 500, 1000)]
    for kind, n, step in cases:
        bb = 100_000.0
        bids = sorted(((bb - i * step * rnd.uniform(0.5, 1.5), rnd.uniform(0.0001, 3.0)) for i in range(n)), reverse=True)
        bb = bids[0][0]
        args = [(bids, bb * 0.993, bb), (bids, bb * 0.985, bb)]
        full = min(timeit.repeat(lambda: [depth_usdt(*a) for a in args], number=200, repeat=5)) / 400 * 1e6
        brk = min(timeit.repeat(lambda: [depth_break(*a) for a in args], number=200, repeat=5)) / 400 * 1e6
        same = all(depth_usdt(*a) == depth_break(*a) for a in args)
        print(f"  {kind:<34} {n:>7} {full:12.1f} {brk:18.1f} {str(same):>10}")
    eq = 0
    for _ in range(2000):  # побитовое равенство на случайных стаканах и границах
        n = rnd.choice((5, 50, 1000))
        bids = sorted(((rnd.uniform(0.5, 2.0), rnd.uniform(0, 1e4)) for _ in range(n)), reverse=True)
        bb = bids[0][0]
        for coef in (0.007, 0.015, 0.05, rnd.random()):
            eq += depth_usdt(bids, bb * (1 - coef), bb) == depth_break(bids, bb * (1 - coef), bb)
    print(f"  побитовое равенство вариантов на 8000 случайных расчётах: {eq} из 8000")
    print("  на кадр при K открытых позициях check_exit делает 2–3·K вызовов на полном стакане; вариант (b) — 2–3 на кадр")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--journal", nargs="*", default=[])
    a = p.parse_args()
    for j in a.journal:
        journal_profile(Path(j))
    bench()


if __name__ == "__main__":
    main()
