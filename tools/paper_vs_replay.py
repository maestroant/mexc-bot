"""P0: совпадают ли решения PAPER и REPLAY на одном и том же входном потоке.

Кадры записи listing_recorder прогоняются двумя путями ядра:
  PAPER  — как в tools/paper.py: MainEngine без заданного T0 (T0 — из exchangeInfo по ходу потока), кадры в порядке
           получения, exchangeInfo обновляет параметры инструмента до старта веток;
  REPLAY — как в tools/main_replay.py: T0 = firstOpenTime, события окна открытия, отсортированные по времени.
Конфигурация — одна (по умолчанию набор PAPER MAIN_EARLY_PAPER и BASELINE, задержка 50 мс). Строки сравниваются по
ключу ветки: время решения, вход, причина выхода, PnL. Любое расхождение печатается.
  python3 tools/paper_vs_replay.py logs/rec_VLXUSDT_*.jsonl.gz
  python3 tools/paper_vs_replay.py --all-usdt
"""

import argparse
import json
import sys
from argparse import Namespace
from pathlib import Path

from core.cap_breakout import MAIN_EARLY_PAPER, ORDER_USDT, MainEngine, Opts
from core.feed import events_of, frame_meta, iter_lines
from main_replay import load_open, usdt_records

KEYS = ("decision_ms", "entry_ms", "entry_px", "reason", "pnl_usdt", "exit_ms")


def paper_path(path: Path, cfg, t0: int, after_ms: int = 120_000) -> list[dict]:
    """Путь PAPER: строки записи в порядке получения, без поправок времени, T0 — из exchangeInfo в потоке."""
    eng = MainEngine(cfg, Opts(), None, {})
    for line in iter_lines(path):
        e = json.loads(line)
        data = e.get("data")
        if e.get("ch") == "exchangeInfo" and isinstance(data, dict):
            info = (data.get("symbols") or [{}])[0]
            if info and eng.release is None:
                eng.info = info
            continue
        for t, kind, ev in events_of(e):
            eng.on_event(t, kind, ev, *frame_meta(e))
        if e["t"] > t0 + after_ms:
            break
    eng.finish()
    return eng.rows(path.name.split("_")[1])


def replay_path(path: Path, cfg) -> tuple[int | None, list[dict]]:
    t0, events, info, _ = load_open(path)
    if t0 is None:
        return None, []
    eng = MainEngine(cfg, Opts(), t0, info)
    for t, kind, data, recv, exch in events:
        eng.on_event(t, kind, data, recv, exch)
    eng.finish(events[-1][0])
    return t0, eng.rows(path.name.split("_")[1])


def key(r: dict) -> tuple:
    return r["mode"], r["variant"], r["profile"], r["usdt"], r["latency"]


def compare(path: Path, cfg) -> tuple[int, int]:
    t0, rep = replay_path(path, cfg)
    if t0 is None:
        print(f"{path.name}: открытия нет")
        return 0, 0
    pap = {key(r): r for r in paper_path(path, cfg, t0)}
    diff = 0
    for r in rep:
        p = pap.get(key(r))
        a = tuple(r.get(k) for k in KEYS)
        b = tuple(p.get(k) for k in KEYS) if p else None
        if a != b:
            diff += 1
            if diff <= 5:
                rel = lambda v: v - t0 if isinstance(v, int) and v > 1e12 else v  # noqa: E731
                print(f"  {key(r)}\n    REPLAY {[rel(x) for x in a]}\n    PAPER  {[rel(x) for x in b] if b else 'нет ветки'}")
    print(f"{path.name}: веток {len(rep)}, расхождений {diff}")
    return len(rep), diff


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="*")
    p.add_argument("--all-usdt", action="store_true")
    a = p.parse_args()
    cfg = Namespace(latency=[50], usdt=[ORDER_USDT], sweep=False, main_early=MAIN_EARLY_PAPER)
    files = [Path(f) for f in a.files] + (usdt_records() if a.all_usdt else [])
    total = bad = 0
    for f in files:
        n, d = compare(f, cfg)
        total, bad = total + n, bad + d
    print(f"\nИтого веток {total}, расхождений {bad}", file=sys.stderr)


if __name__ == "__main__":
    main()
