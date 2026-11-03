"""Сводка PAPER по всем листингам: раздельно BASELINE, MAIN_EARLY, CAP BREAKOUT FAST / CONFIRMED / EARLY.

Читает SUMMARY из logs/paper_*.jsonl (их пишет tools/paper.py); листинг без SUMMARY (PAPER ещё идёт или прерван) —
пропускается. Задержка ордера и план — как в PAPER (50 мс, 10 USDT; у EARLY — все планы).
  python3 tools/paper_report.py [--days 14]
"""

import argparse
import json
import time
from collections import Counter

from core.cap_breakout import LOGS, no_entry_reason

GROUPS = [
    ("BASELINE (ТЗ v2.1)", lambda r: r["mode"] == "MAIN" and r["variant"] == "base" and r["profile"] == "MAIN_V21"),
    ("MAIN_EARLY", lambda r: r["mode"] == "MAIN_EARLY"),
    ("CAP BREAKOUT FAST", lambda r: r["mode"] == "FAST" and r["profile"] == "CAP_DYNAMIC"),
    ("CAP BREAKOUT CONFIRMED", lambda r: r["mode"] == "CONFIRMED" and r["profile"] == "CAP_DYNAMIC"),
    ("CAP BREAKOUT EARLY (время 10 с)", lambda r: r["mode"] == "EARLY" and r["variant"] == "base"
     and r["profile"] == "EARLY_TIME_10S"),
]


def summaries(days: float) -> list[dict]:
    out, since = [], time.time() - days * 86_400
    for p in sorted(LOGS.glob("paper_*.jsonl")):
        if p.stat().st_mtime < since:
            continue
        last = None
        with p.open(encoding="utf-8") as f:
            for line in f:
                if '"event": "SUMMARY"' in line:
                    last = json.loads(line)
        if last and last.get("open_ms"):
            last["symbol"] = p.name.split("_")[1]
            out.append(last)
    return out


def engines(summary: dict) -> dict:
    """Строки по движкам; у логов прежнего paper_cap.py строки — один список CAP BREAKOUT."""
    rows = summary.get("rows") or {}
    return {"cap": rows} if isinstance(rows, list) else rows


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--days", type=float, default=14, help="логи PAPER за N дней (14)")
    a = p.parse_args()
    items = summaries(a.days)
    print(f"PAPER за {a.days:g} дн.: листингов {len(items)} (с потолком {sum(1 for s in items if s.get('cap'))})\n")
    for title, pick in GROUPS:
        rows = [r for s in items for eng in engines(s).values() for r in eng if pick(r)]
        got = [r for r in rows if "pnl_usdt" in r]
        total = sum(r["pnl_usdt"] for r in got)
        wins = sum(r["pnl_usdt"] > 0 for r in got)
        line = f"{title}: веток {len(rows)}, входов {len(got)}"
        if got:
            line += (f", {wins}/{len(got) - wins}, итог {total:+.2f} USDT, худший {min(r['pnl_pct'] for r in got):+.1f} %, "
                     f"куплено {sum(r['buy_usdt'] for r in got):.1f}, остаток {sum(r['left_usdt'] for r in got):.2f}")
        print(line)
        skips = Counter(no_entry_reason(r) for r in rows if "entry_ms" not in r)
        if skips:
            print("  нет входа: " + ", ".join(f"{k} {v}" for k, v in skips.most_common(6)))
        for r in got[:30]:
            print(f"  {r['symbol'][:-4]:<9} {r['variant']:<16} {r['profile']:<18} {r['usdt']:>4g}  {r['pnl_pct']:+6.2f} % "
                  f"({r['reason']}, {r['hold_s']:.0f} с)")


if __name__ == "__main__":
    main()
