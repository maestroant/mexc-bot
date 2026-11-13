"""Исторический прогон лимита CAP-проб 3/6 (решение клиента 13.11, раздел 4) — только чтение логов, без ордеров.

Каждое правило применяется к фактической последовательности реальных LIMIT_MAKER-подач (cap_maker) после открытия:
правило только ВЫБРАСЫВАЕТ подачи (в те же моменты, по тем же ценам), поэтому ответы оставшихся подач — фактические,
моделирования ответа биржи нет.
  OLD        как было в логе (подача раз в 30 с до T+700 с);
  CUR        текущий код без 3/6: технические отказы 10007/30004/30005 — не больше 3 по пробе; при 30004 баланс
             котируемой валюты недостаточен — подач нет вовсе (real_buy_gate проверяет баланс до первой подачи);
  R36        CUR + стоп на первой 30010 + не больше 3 принятых + не больше 6 подач (правило клиента 13.11);
  R36+G      R36 + read-only условия, которые только убирают подачи (предложение, пределы не меняются):
             G1 — не подавать, пока пара ни разу не появилась в selfSymbols (API для пары не открыт; 10007 — без подачи);
             G2 — не подавать, если цена пробы не выше максимальной уже принятой цены пробы × (1 + G2_STEP):
                  принятая LIMIT_MAKER по цене P доказывает только «потолок выше P» — более низкая проба ничего нового
                  не скажет (в истории значение потолка после первой 30010 ни разу не менялось).
Дополнительно — read-only признак «цена прижата к максимуму» (ticker/24hr: price == highPrice N проверок подряд):
когда сработал бы на листингах с потолком и сколько раз ложно — на листингах без него.
  python tools/cap_probe_limit_study.py [--logs logs] [--out logs/cap_probe_limit_study_<date>.json]
"""

import argparse
import glob
import json
import re
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

MAX_ACCEPTED = 3
MAX_SUBMITS = 6
MAX_TECH_REJECTS = 3
G2_STEP = Decimal("0.10")
PIN_N = 3          # проверок ticker подряд (раз в 10 с)
CAP_WINDOW_MS = 700_000
KNOWN_CAPS = {"CHOMPUSDT": 5.0, "MTOUSDT": 0.2}  # = core.cap_breakout.KNOWN_CAPS (заморозка без 30010 в логе)


def cap_of(e: dict) -> str | None:
    b = e.get("body")
    if isinstance(b, dict) and b.get("code") == 30010:
        return str((b.get("_extend") or {}).get("price", "?"))
    return None


def outcome(e: dict) -> str:
    if cap_of(e) is not None:
        return "30010"
    st = str(e.get("state", ""))
    if st == "ACCEPTED":
        return "ACCEPTED"
    if st.split(" ")[0] in ("skipped:", "stop:", "suspended:", "waiting:", "blocked:"):
        return "NOT_SENT"
    m = re.match(r"ERR (-?\d+)", st)
    return m.group(1) if m else "OTHER:" + st[:30]


def load(path: str) -> dict:
    subs, other, self_in, prices = [], [], None, []
    for line in open(path, encoding="utf-8"):
        try:
            e = json.loads(line)
        except ValueError:
            continue
        ch, rel = e.get("check"), e.get("rel_ms")
        if ch == "cap_maker":
            oc = outcome(e)
            if oc != "NOT_SENT":
                subs.append({"rel_ms": rel, "outcome": oc, "cap": cap_of(e),
                             "price": Decimal(str((e.get("params") or {}).get("price") or 0))})
        elif ch in ("market_buy", "preopen_cap", "real_order") and cap_of(e) is not None:
            other.append({"check": ch, "rel_ms": rel, "cap": cap_of(e)})
        elif ch == "self_syms" and self_in is None and rel is not None and str(e.get("state")) == "in_list=True":
            self_in = rel
        elif ch == "public_prices" and rel is not None and 0 <= rel <= CAP_WINDOW_MS:
            t = (e.get("bodies") or {}).get("ticker24h") or {}
            pr = (e.get("bodies") or {}).get("price") or {}
            try:
                prices.append((rel, Decimal(str(pr.get("price") or 0)), Decimal(str(t.get("highPrice") or 0))))
            except Exception:  # noqa: BLE001
                pass
    return {"subs": subs, "other": other, "self_in": self_in, "prices": prices}


def status_of(kept: list[dict]) -> str:
    if any(s["outcome"] == "30010" for s in kept):
        return "CAP_FOUND"
    return "NO_CAP_OBSERVED" if any(s["outcome"] == "ACCEPTED" for s in kept) else "CAP_UNKNOWN"


def apply(subs: list[dict], self_in: int | None, rule: str) -> list[dict]:
    kept, tech, acc, max_acc = [], 0, 0, None
    if rule != "OLD" and subs and subs[0]["outcome"] == "30004":
        return []  # баланса котируемой валюты нет — real_buy_gate не даёт подавать
    for s in subs:
        if rule == "OLD":
            kept.append(s)
            continue
        if tech >= MAX_TECH_REJECTS:
            break
        if rule.startswith("R36"):
            if len(kept) >= MAX_SUBMITS or acc >= MAX_ACCEPTED or any(k["outcome"] == "30010" for k in kept):
                break
        if rule == "R36+G":
            if self_in is None or s["rel_ms"] is None or s["rel_ms"] < self_in:
                continue  # G1
            if max_acc is not None and s["price"] < max_acc * (1 + G2_STEP):
                continue  # G2
        kept.append(s)
        if s["outcome"] in ("10007", "30004", "30005"):
            tech += 1
        if s["outcome"] == "ACCEPTED":
            acc += 1
            max_acc = s["price"] if max_acc is None else max(max_acc, s["price"])
    return kept


def pinned(prices: list[tuple[int, Decimal, Decimal]]) -> int | None:
    """Первый момент, когда цена N проверок подряд равна дневному максимуму и максимум не менялся."""
    run, last_high = 0, None
    for rel, p, h in prices:
        if h > 0 and p == h and h == last_high:
            run += 1
            if run >= PIN_N - 1:
                return rel
        else:
            run = 0
        last_high = h
    return None


def study_file(path: str) -> dict | None:
    d = load(path)
    subs, other = d["subs"], d["other"]
    sym = Path(path).name.split("_")[1]
    if not subs and not other and sym not in KNOWN_CAPS:
        return None
    row = {"file": Path(path).name, "symbol": sym, "submits_old": len(subs), "self_symbols_first_rel_ms": d["self_in"],
           "outcomes_old": {k: sum(1 for s in subs if s["outcome"] == k) for k in sorted({s["outcome"] for s in subs})},
           "other_real_30010": other[:2], "other_real_30010_n": len(other),
           "known_cap_without_30010": KNOWN_CAPS.get(sym), "pinned_rel_ms": pinned(d["prices"])}
    for rule in ("OLD", "CUR", "R36", "R36+G"):
        kept = apply(subs, d["self_in"], rule)
        first = next((s for s in kept if s["outcome"] == "30010"), None)
        row[rule] = {"status": status_of(kept) if subs else "NO_CAP_PROBE", "submits": len(kept),
                     "accepted": sum(s["outcome"] == "ACCEPTED" for s in kept),
                     "rejects": sum(s["outcome"] not in ("ACCEPTED", "30010") for s in kept),
                     "first_30010_rel_ms": first["rel_ms"] if first else None,
                     "cap": first["cap"] if first else None}
    caps = [s for s in subs if s["outcome"] == "30010"]
    row["cap_values_old"] = sorted({s["cap"] for s in caps})
    row["last_30010_rel_ms_old"] = caps[-1]["rel_ms"] if caps else None
    return row


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--logs", default="logs")
    p.add_argument("--out", default=None)
    a = p.parse_args()
    rows = [r for f in sorted(glob.glob(str(Path(a.logs) / "probe_*.jsonl"))) if (r := study_file(f))]
    probed = [r for r in rows if r["submits_old"]]
    summary = {"rules": {"R36": f"стоп на первой 30010; ≤{MAX_ACCEPTED} принятых; ≤{MAX_SUBMITS} подач",
                         "R36+G": f"R36 + G1 (selfSymbols) + G2 (цена пробы ≥ макс. принятой × {1 + G2_STEP})"}}
    for rule in ("OLD", "CUR", "R36", "R36+G"):
        summary[rule] = {"cap_found": sorted(r["symbol"] for r in probed if r[rule]["status"] == "CAP_FOUND"),
                         "real_submits": sum(r[rule]["submits"] for r in probed),
                         "accepted_orders": sum(r[rule]["accepted"] for r in probed)}
    summary["lost_vs_OLD"] = {rule: sorted(set(summary["OLD"]["cap_found"]) - set(summary[rule]["cap_found"]))
                              for rule in ("CUR", "R36", "R36+G")}
    summary["detection_delay_ms_vs_OLD"] = {
        rule: {r["symbol"]: r[rule]["first_30010_rel_ms"] - r["OLD"]["first_30010_rel_ms"]
               for r in probed if r[rule]["first_30010_rel_ms"] and r["OLD"]["first_30010_rel_ms"]
               and r[rule]["first_30010_rel_ms"] != r["OLD"]["first_30010_rel_ms"]} for rule in ("CUR", "R36", "R36+G")}
    summary["cap_value_changed_after_first"] = [r["symbol"] for r in probed if len(r["cap_values_old"]) > 1]
    summary["frozen_without_cap_probe"] = [
        {"symbol": r["symbol"], "how_known": "30010 на " + ",".join(sorted({o["check"] for o in r["other_real_30010"]}))
         if r["other_real_30010_n"] else f"KNOWN_CAPS {r['known_cap_without_30010']}"}
        for r in rows if not r["submits_old"]]
    cap_syms = {r["symbol"] for r in rows if r["cap_values_old"] or r["other_real_30010_n"] or r["known_cap_without_30010"]}
    summary["pinned_trigger"] = {
        "cap_listings_fired": sorted(f"{r['symbol']}@{r['pinned_rel_ms']}" for r in rows
                                     if r["symbol"] in cap_syms and r["pinned_rel_ms"] is not None),
        "cap_listings_not_fired": sorted({r["symbol"] for r in rows if r["symbol"] in cap_syms
                                          and r["pinned_rel_ms"] is None}),
        "no_cap_listings_fired_false": sorted({r["symbol"] for r in rows if r["symbol"] not in cap_syms
                                               and r["pinned_rel_ms"] is not None}),
        "no_cap_listings_total": len({r["symbol"] for r in rows if r["symbol"] not in cap_syms}),
    }
    out = Path(a.out or Path(a.logs) / f"cap_probe_limit_study_{time.strftime('%Y%m%d')}.json")
    out.write_text(json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=1, default=str),
                   encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=1, default=str))
    print(f"\n{'пара':14} {'self':>7} | {'OLD':>22} | {'CUR':>22} | {'R36':>22} | {'R36+G':>22} | потолок")
    for r in rows:
        cells = []
        for rule in ("OLD", "CUR", "R36", "R36+G"):
            x = r[rule]
            cells.append(f"{x['status'][:9]:>9} {x['submits']:>2}п {x['accepted']:>2}пр "
                         f"{(str(x['first_30010_rel_ms'] // 1000) + 'с') if x['first_30010_rel_ms'] else '':>5}")
        extra = ",".join(r["cap_values_old"]) or (f"иной BUY 30010×{r['other_real_30010_n']}" if r["other_real_30010_n"]
                                                   else (f"KNOWN {r['known_cap_without_30010']}"
                                                         if r["known_cap_without_30010"] else ""))
        selfin = f"{r['self_symbols_first_rel_ms'] // 1000}с" if r["self_symbols_first_rel_ms"] is not None else "нет"
        print(f"{r['symbol'][:14]:14} {selfin:>7} | " + " | ".join(cells) + f" | {extra}"
              f"{'  pin@' + str(r['pinned_rel_ms'] // 1000) + 'с' if r['pinned_rel_ms'] is not None else ''}")
    print(f"\nотчёт: {out}")


if __name__ == "__main__":
    main()
