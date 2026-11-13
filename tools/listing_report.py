"""Отчёт по записанным листингам: как вёл себя рынок в первые секунды и минуты и когда открылся API.

Читает logs/rec_<SYMBOL>_*.jsonl.gz (запись tools/listing_recorder.py) и, если есть,
logs/probe_<SYMBOL>_*.jsonl (проба API tools/probe_listing.py) и logs/watcher_state.json (зоны).
T0 — момент открытия, который определил рекордер (OPEN_DETECTED: первая сделка или двусторонний стакан).

По каждой паре:
  первая сделка (через сколько после T0, по какой цене);
  спред и глубина ±2 % от лучшей цены в USDT на T+1/5/30/60 с (стакан 20 уровней из WS);
  когда спред впервые стал ≤ --max-spread % и какую долю первой минуты он таким был;
  изменение цены от первой сделки через 1/5/15/60 мин, максимум и минимум за час;
  объём в USDT за 1/5/60 мин;
  API: первый принятый пробный ордер (ACCEPTED или 30004 — проверку пары прошёл) и сколько было 10007.

Запуск на сервере (записи лежат там):  python3 tools/listing_report.py [SYMBOL ...] [--json logs/report.json]
Без аргументов — все записи, кроме пар, которые так и не открылись.
"""

import argparse
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import core.data_marks as DM  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"

BOOK_AT_S = (1, 5, 30, 60)
PRICE_AT_MIN = (1, 5, 15, 60)
VOLUME_AT_MIN = (1, 5, 60)
# строки без этих подстрок не разбираем: diff-стакан и REST-стаканы на 1000 уровней — основная масса файла
WANTED = ('"OPEN_DETECTED"', "aggre.deals", "aggre.bookTicker", "limit.depth")


def fnum(x) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def spread_pct(bid: float, ask: float) -> float | None:
    if bid <= 0 or ask <= 0:
        return None
    return (ask - bid) / ((ask + bid) / 2) * 100


def depth_usdt(levels: list, best: float, pct: float, side: str) -> float:
    total = 0.0
    for lv in levels:
        p, q = fnum(lv.get("price")), fnum(lv.get("quantity"))
        if side == "bid" and p >= best * (1 - pct / 100) or side == "ask" and p <= best * (1 + pct / 100):
            total += p * q
    return total


def iter_lines(f):
    # запись, которая ещё идёт (или была прервана), обрывается без конца gzip-потока — читаем, что есть
    try:
        for line in f:
            if line.endswith("\n"):
                yield line
    except EOFError:
        return


def analyze(path: Path, max_spread: float) -> dict | None:
    open_ms = None
    deals: list[tuple[int, float, float]] = []  # (время биржи ms, цена, количество)
    book: list[tuple[int, float, float]] = []  # (t, bid, ask) из bookTicker
    snaps: dict[int, dict] = {}  # секунда → первый снимок стакана 20 уровней не раньше T0+секунда
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in iter_lines(f):
            if not any(w in line for w in WANTED):
                continue
            e = json.loads(line)
            ch, data = e.get("ch", ""), e.get("data") or {}
            if e.get("src") == "event" and ch == "OPEN_DETECTED":
                open_ms = data.get("open_ms") or e["t"]
            elif "aggre.deals" in ch:
                for d in data.get("publicAggreDeals", {}).get("deals", []):
                    deals.append((int(d.get("time") or e["t"]), fnum(d.get("price")), fnum(d.get("quantity"))))
            elif "aggre.bookTicker" in ch:
                bt = data.get("publicAggreBookTicker", {})
                # protobuf не пишет пустые поля: нет bidPrice — значит, покупателей в стакане нет
                book.append((e["t"], fnum(bt.get("bidPrice")), fnum(bt.get("askPrice"))))
            elif "limit.depth" in ch and open_ms is not None:
                for s in BOOK_AT_S:
                    if s not in snaps and e["t"] >= open_ms + s * 1000:
                        snaps[s] = data.get("publicLimitDepths", {})
    if open_ms is None:
        return None
    sym = path.name.split("_")[1]
    res: dict = {"symbol": sym, "file": path.name, "open_ms": open_ms}

    deals.sort()
    after = [d for d in deals if d[0] >= open_ms - 2000]
    if after:
        t1, p1, _ = after[0]
        res["first_deal_s"] = round((t1 - open_ms) / 1000, 2)
        res["first_price"] = p1

        def last_price(until_ms: int) -> float | None:
            ps = [p for t, p, _ in after if t <= until_ms]
            return ps[-1] if ps else None

        for m in PRICE_AT_MIN:
            p = last_price(open_ms + m * 60_000)
            res[f"chg_{m}m_pct"] = round((p / p1 - 1) * 100, 1) if p else None
        hour = [(t, p) for t, p, _ in after if t <= open_ms + 3_600_000]
        tmax, pmax = max(hour, key=lambda x: x[1])
        tmin, pmin = min(hour, key=lambda x: x[1])
        res["max_pct"] = round((pmax / p1 - 1) * 100, 1)
        res["max_at_s"] = round((tmax - open_ms) / 1000)
        res["min_pct"] = round((pmin / p1 - 1) * 100, 1)
        for m in VOLUME_AT_MIN:
            res[f"vol_{m}m_usdt"] = round(sum(p * q for t, p, q in after if t <= open_ms + m * 60_000))
        res["deals_1m"] = sum(1 for t, _, _ in after if t <= open_ms + 60_000)

    # спред по bookTicker: снимок на моменты T+s и доля первой минуты со спредом ≤ max_spread
    book.sort()
    for s in BOOK_AT_S:
        prev = [b for b in book if b[0] <= open_ms + s * 1000]
        if prev:
            sp = spread_pct(prev[-1][1], prev[-1][2])
            res[f"spread_{s}s_pct"] = round(sp, 2) if sp is not None else None
    good_ms, first_good = 0, None
    window = [b for b in book if open_ms <= b[0] <= open_ms + 60_000]
    for (t, bid, ask), nxt in zip(window, window[1:] + [(open_ms + 60_000, 0, 0)]):
        sp = spread_pct(bid, ask)
        if sp is not None and sp <= max_spread:
            good_ms += nxt[0] - t
            if first_good is None:
                first_good = t
    res["spread_ok_first_s"] = round((first_good - open_ms) / 1000, 2) if first_good else None
    res["spread_ok_share_1m"] = round(good_ms / 60_000 * 100)

    for s, d in snaps.items():
        bids, asks = d.get("bids", []), d.get("asks", [])
        bb = max((fnum(x.get("price")) for x in bids), default=0)
        ba = min((fnum(x.get("price")) for x in asks), default=0)
        res[f"depth2_{s}s_usdt"] = (round(depth_usdt(bids, bb, 2, "bid")) if bb else 0,
                                    round(depth_usdt(asks, ba, 2, "ask")) if ba else 0)
    return res


def api_stats(sym: str, rec_name: str) -> dict:
    # проба и запись стартуют одновременно, у них одинаковая метка времени в имени файла
    stamp = rec_name.split("_", 2)[2].replace(".jsonl.gz", "")
    path = LOGS / f"probe_{sym}_{stamp}.jsonl"
    if not path.exists():
        cands = sorted(LOGS.glob(f"probe_{sym}_*.jsonl"))
        if not cands:
            return {}
        path = cands[-1]
    first_pass, n10007, n_orders = None, 0, 0
    with path.open(encoding="utf-8") as f:
        for line in f:
            if '"real_order"' not in line:
                continue
            r = json.loads(line)
            if r.get("check") != "real_order" or "skipped" in str(r.get("state")):
                continue
            n_orders += 1
            state = str(r.get("state"))
            if "10007" in state:
                n10007 += 1
            elif (r.get("positive") or "30004" in state) and first_pass is None:
                first_pass = r.get("rel_ms")
    return {"api_first_ok_s": round(first_pass / 1000, 1) if first_pass is not None else None,
            "api_10007": n10007, "api_orders": n_orders}


def fmt(v, suffix="") -> str:
    if v is None:
        return "—"
    if isinstance(v, tuple):
        return "/".join(f"{x:,}".replace(",", " ") for x in v)
    if isinstance(v, float) and suffix == "%":
        return f"{v:+.1f}%"
    return f"{v}{suffix}"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("symbols", nargs="*", help="какие пары (по умолчанию все записи)")
    p.add_argument("--max-spread", type=float, default=1.2, help="порог спреда для входа, %% (1.2, как maxSpreadPct в ТЗ)")
    p.add_argument("--json", help="сохранить результаты в JSON")
    args = p.parse_args()
    try:
        zones = {k: v.get("zones") for k, v in json.loads((LOGS / "watcher_state.json").read_text())["handled"].items()}
    except (OSError, ValueError, KeyError):
        zones = {}
    files = sorted(LOGS.glob("rec_*.jsonl.gz"))
    if args.symbols:
        files = [f for f in files if f.name.split("_")[1] in args.symbols]
    rows = []
    for f in files:
        r = analyze(f, args.max_spread)
        if r is None:
            print(f"{f.name}: пара не открылась — пропускаю", flush=True)
            continue
        r.update(api_stats(r["symbol"], f.name))
        r["zones"] = zones.get(r["symbol"])
        rows.append(r)
        print(f"{f.name}: готово", flush=True)
    if args.json:
        Path(args.json).write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")

    print("\n| Пара | Зоны | API OK, с | 10007 | 1-я сделка, с | Спред 1/5/30/60 с, % | Спред ≤ порога: впервые, с / доля 1-й мин |"
          " Глубина ±2 % bid/ask на 5 с, USDT | Цена 1/5/15/60 мин | Макс (на с) / мин за час | Объём 1/5/60 мин, USDT |")
    print("|" + "---|" * 11)
    for r in rows:
        spreads = "/".join(fmt(r.get(f"spread_{s}s_pct")) for s in BOOK_AT_S)
        prices = " / ".join(fmt(r.get(f"chg_{m}m_pct"), "%") for m in PRICE_AT_MIN)
        vols = " / ".join(f"{r.get(f'vol_{m}m_usdt', 0):,}".replace(",", " ") for m in VOLUME_AT_MIN)
        mark = f" ({DM.MARKS[r['symbol']]['mark']})" if r["symbol"] in DM.MARKS else ""
        print(f"| {r['symbol']}{mark} | {', '.join(r.get('zones') or [])} | {fmt(r.get('api_first_ok_s'))} | {r.get('api_10007', '—')} "
              f"| {fmt(r.get('first_deal_s'))} | {spreads} | {fmt(r.get('spread_ok_first_s'))} / {r.get('spread_ok_share_1m')}% "
              f"| {fmt(r.get('depth2_5s_usdt'))} | {prices} | {fmt(r.get('max_pct'), '%')} ({r.get('max_at_s')}) / {fmt(r.get('min_pct'), '%')} "
              f"| {vols} |")


if __name__ == "__main__":
    main()
