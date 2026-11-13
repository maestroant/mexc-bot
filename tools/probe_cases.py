"""Разбор исторических MARKET-проб (решение клиента 11.11, п. 3.5): для каждого отменённого без исполнения MARKET
BUY / SELL и отказа MARKET SELL 30083 — время от открытия, цена ордера биржи против bid/ask на момент подачи, полоса
цены пары (PERCENT_PRICE_BY_SIDE), объём asks/bids до цены ордера по ближайшему REST-снимку записи, была ли сделка
к моменту запроса. Реальных ордеров не ставит — только логи пробы и записи.
  python3 tools/probe_cases.py"""

import glob
import json
from pathlib import Path

from core.feed import iter_lines, line_t

LOGS = Path(__file__).resolve().parent.parent / "logs"


def load_probe(path: str) -> list[dict]:
    out = []
    for ln in open(path, encoding="utf-8"):
        try:
            out.append(json.loads(ln))
        except ValueError:
            pass
    return out


def book_at(rec: Path, ts: int) -> tuple[dict | None, int | None, int | None, float | None]:
    """Ближайший REST-снимок стакана записи не позже ts, время первой сделки записи, цена последней сделки до ts."""
    snap, first_deal, last_px = None, None, None
    for line in iter_lines(rec):
        t = line_t(line)
        if '"publicAggreDeals"' in line:
            first_deal = first_deal or t
            if t <= ts:
                deals = json.loads(line)["data"]["publicAggreDeals"].get("deals") or []
                last_px = float(deals[-1]["price"]) if deals else last_px
        if '"src":"rest","ch":"depth"' in line and t <= ts:
            snap = (t, json.loads(line)["data"])
        if t > ts + 2000:
            break
    return (snap[1] if snap else None), (snap[0] if snap else None), first_deal, last_px


def vol_to(levels: list, price: float, side: str) -> float:
    """USDT объём уровней, доступных ордеру с предельной ценой price (asks ≤ price для BUY, bids ≥ price для SELL)."""
    ok = (lambda p: p <= price) if side == "BUY" else (lambda p: p >= price)
    return sum(float(p) * float(q) for p, q in levels if ok(float(p)))


def main() -> None:
    print("пара | проба | от открытия, с | статус / код | цена ордера биржи | bid / ask при подаче | цена / ask (BUY) "
          "или цена / bid (SELL) | цена / последняя сделка | полоса bidUp / askDown | объём до цены ордера, USDT | "
          "сделка до запроса")
    for f in sorted(glob.glob(str(LOGS / "probe_*_2*.jsonl"))):
        sym = Path(f).name.split("_")[1]
        rows = load_probe(f)
        info = next((r.get("symbol_info") for r in reversed(rows) if r.get("symbol_info")), {}) or {}
        band = next((x for x in info.get("filters") or [] if x.get("filterType") == "PERCENT_PRICE_BY_SIDE"), {})
        open_ms = next((r.get("open_ms") for r in rows if r.get("check") == "open_detected"), None)
        placed = {}
        for r in rows:
            c = r.get("check")
            if c in ("market_buy", "market_sell") and r.get("state") == "ACCEPTED":
                placed[c] = r
            case = None
            if c in ("market_buy_fill", "market_sell_fill") and "status=CANCELED" in r.get("state", "") \
                    and "qty=0 " in r.get("state", ""):
                side = "BUY" if c.startswith("market_buy") else "SELL"
                src = placed.get(c.replace("_fill", ""), {})
                case = (side, "CANCELED, executedQty = 0", (r.get("body") or {}).get("price"), src)
            elif c == "market_sell" and "30083" in r.get("state", ""):
                case = ("SELL", "30083 last price is null", None, r)
            if not case:
                continue
            side, what, px, src = case
            ts = src.get("ts") or r.get("ts")
            bid, ask = src.get("bid"), src.get("ask")
            recs = sorted(LOGS.glob(f"rec_{sym}_2*.jsonl.gz"))
            snap, snap_t, first_deal, last_px = book_at(recs[-1], ts) if recs else (None, None, None, None)
            ratio = vol = None
            if px:
                ref = float(ask) if side == "BUY" and ask else float(bid) if bid else None
                ratio = f"{float(px) / ref:.3f}" if ref else None
                if snap:
                    vol = round(vol_to(snap["asks"] if side == "BUY" else snap["bids"], float(px), side), 2)
            deal_before = (first_deal is not None and first_deal <= ts) if recs else None
            rel = (ts - open_ms) / 1000 if open_ms else None
            last = f"{float(px) / last_px:.3f}" if px and last_px else None
            print(f"{sym} | {side} | {rel:+.1f} | {what} | {px} | {bid} / {ask} | {ratio} | {last} ({last_px}) | "
                  f"{band.get('bidMultiplierUp')} / {band.get('askMultiplierDown')} | {vol} | "
                  f"{deal_before} (первая сделка {((first_deal - open_ms) / 1000) if first_deal and open_ms else None} с)")


if __name__ == "__main__":
    main()
