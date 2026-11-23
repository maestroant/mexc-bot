"""EvaluationEligibilityManifest (решения клиента 16.11 п. A и 19.11 v2 п. 2.4) — единый версионированный контракт допуска
листингов к оценочным выборкам. Строится ДО подсчёта PnL, только из метаданных записей, логов проб и меток качества;
результаты стратегий в него не входят.

Независимые измерения листинга (не один ярлык — каждое отдельно, причины не вытесняют друг друга):
  listing_status      LISTED / DELISTED / CANCELED / SYNTHETIC_FIXTURE
  data_quality        флаги: OK, DATA_AVAILABILITY_UNCONFIRMED (core/data_marks.py), INVALID_FOR_LISTING_REPLAY,
                      NO_OPEN_IN_RECORD, RECONSTRUCTED_FRAMES (кадры восстановлены после остановки записи)
  knowledge_provenance  T0 = firstOpenTime; когда впервые известен (known_at), сколько версий (переносы)
  api_access_as_of    по логу пробы в окне T0 … T0 + 30 с: API_UNAVAILABLE_AT_ENTRY (10007), API_OPEN_AT_ENTRY
                      (реальная проба принята), API_UNKNOWN (проб не было)
  cap_evidence        срез на конец окна входа (T0 + 30 с) и итоговый, у каждого — known_at и источник:
                      CAP_FOUND (реальный 30010: цена, момент, вид пробы), KNOWN_CAPS_HISTORICAL (потолок только из
                      исторической заморозки — не PIT), CAP_UNCERTAIN (подавленные G2 пробы выше принятой цены),
                      NO_CAP_OBSERVED (30010 не было при перечисленных принятых ценах), CAP_UNKNOWN (проб не было)
  book_quality_as_of  первый действительный двусторонний стакан от T0, недействительных интервалов в первые 60 с
  analysis_cohort     OPERATIONAL (все зарегистрированные листинги — intention-to-observe) и/или VALIDATION
  eligibility         по алгоритму (BASELINE, MAIN_EARLY, CAP_BREAKOUT): validation да/нет + ВСЕ причины исключения

Правила допуска к VALIDATION (заранее, без взгляда на PnL):
  все алгоритмы — LISTED, данные без DATA_AVAILABILITY_UNCONFIRMED / INVALID / NO_OPEN, API не закрыт на входе;
  MAIN_EARLY — дополнительно итоговое cap_evidence = NO_CAP_OBSERVED (CAP_UNCERTAIN, CAP_UNKNOWN, CAP_FOUND и
    KNOWN_CAPS_HISTORICAL — нет: «без потолка» не доказано или потолок есть);
  CAP_BREAKOUT — CAP_FOUND (реальный 30010); KNOWN_CAPS_HISTORICAL — только условный тест модели, не VALIDATION.
RECONSTRUCTED_FRAMES — флаг, не исключение листинга (строки отмечаются в отчётах отдельно, как раньше).

Файл — logs/eligibility_manifest.json; версия контракта MANIFEST_VERSION, отпечаток содержимого manifest_sha256,
SHA-256 каждой записи. Потребители: load() — нет файла, другая версия, запись не в манифесте или её SHA не совпадает →
ManifestError (FAIL, не «включить всё»).
  python3 tools/eligibility.py build            # построить (logs/)
  python3 tools/eligibility.py show             # таблица измерений и допуска
"""

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from core.book import Book  # noqa: E402
from core.cap_breakout import KNOWN_CAPS  # noqa: E402
from core.data_marks import DATA_AVAILABILITY_UNCONFIRMED, MARKS, SYNTHETIC_FIXTURE  # noqa: E402
from main_replay import SKIP_SYMBOLS, ListingContractError, load_open  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"
MANIFEST = "eligibility_manifest.json"
MANIFEST_VERSION = "EEM-1"
ENTRY_WINDOW_MS = 30_000  # окно входа для среза «на момент входа» (как api_closed: первые 30 с)
ALGOS = ("BASELINE", "MAIN_EARLY", "CAP_BREAKOUT")
STATUS_OF_SKIP = {"PFPUSDT": "DELISTED", "PALUSDT": "CANCELED"}
REAL_ORDER_CHECKS = ("cap_maker", "preopen_cap", "preopen_low", "real_order")


class ManifestError(SystemExit):
    pass


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def records(logs: Path) -> list[Path]:
    latest = {}
    for p in sorted(logs.glob("rec_*_2*.jsonl.gz")):
        latest[p.name.split("_")[1]] = p
    return sorted(latest.values(), key=lambda p: p.name.split("_", 2)[2])


def probe_events(logs: Path, sym: str) -> list[dict]:
    """События логов пробы, важные для допуска: доступность API и потолок (с абсолютным временем ts)."""
    out = []
    for p in sorted(logs.glob(f"probe_{sym}_2*.jsonl")):
        with p.open(encoding="utf-8") as f:
            for line in f:
                if not any(f'"check": "{c}"' in line for c in REAL_ORDER_CHECKS + ("cap_suppressed",)):
                    continue
                e = json.loads(line)
                if e.get("ts") is None:
                    continue
                st = str(e.get("state", ""))
                ev = {"ts": e["ts"], "check": e["check"], "state": st[:80]}
                if '"code": 30010' in line:
                    body = e.get("body") or {}
                    ext = (body.get("_extend") or {}) if isinstance(body, dict) else {}
                    ev.update(kind="30010", cap=str(ext.get("price", "")).replace("USDT", "") or None,
                              price=(e.get("params") or {}).get("price"))
                elif "10007" in st:
                    ev["kind"] = "10007"
                elif e["check"] == "cap_suppressed":
                    ev.update(kind="suppressed", gate=e.get("gate"), price=e.get("probe_price"),
                              missed=bool(e.get("cap_may_have_been_missed")))
                elif e["check"] == "cap_maker" and st == "ACCEPTED":
                    ev.update(kind="cap_accepted", price=(e.get("params") or {}).get("price"))
                elif e["check"] == "real_order" and e.get("positive"):
                    ev["kind"] = "api_accepted"
                else:
                    continue
                out.append(ev)
    return sorted(out, key=lambda e: e["ts"])


def cap_evidence(evs: list[dict], sym: str, as_of: int | None) -> dict:
    """Состояние потолка по событиям пробы с ts ≤ as_of (None — все): статус, known_at, источник, цены."""
    xs = [e for e in evs if as_of is None or e["ts"] <= as_of]
    found = next((e for e in xs if e.get("kind") == "30010"), None)
    acc = [(e["ts"], e.get("price")) for e in xs if e.get("kind") == "cap_accepted"]
    sup = [e for e in xs if e.get("kind") == "suppressed"]
    missed = [e for e in sup if e.get("missed")]
    if found:
        return dict(status="CAP_FOUND", known_at=found["ts"], cap=found.get("cap"), source=found["check"],
                    sent_price=found.get("price"))
    if sym in KNOWN_CAPS:
        return dict(status="KNOWN_CAPS_HISTORICAL", known_at=None, cap=str(KNOWN_CAPS[sym]),
                    source="KNOWN_CAPS (историческая заморозка, не PIT)", accepted=len(acc))
    if missed:
        return dict(status="CAP_UNCERTAIN", known_at=missed[0]["ts"], may_have_been_missed=len(missed),
                    suppressed=len(sup), accepted_prices=[p for _, p in acc][:10],
                    missed_prices=sorted({e.get("price") for e in missed if e.get("price")}))
    if acc:
        return dict(status="NO_CAP_OBSERVED", known_at=acc[0][0], accepted_prices=[p for _, p in acc][:10],
                    suppressed=len(sup))
    return dict(status="CAP_UNKNOWN", known_at=None, suppressed=len(sup))


def book_quality(events: list, t0: int) -> dict:
    b, first, invalid, was = Book(), None, 0, False
    for t, kind, data, *_ in events:
        if t > t0 + 60_000:
            break
        if kind == "snap":
            b.snapshot(data)
        elif kind == "diff":
            b.diff(data)
        else:
            continue
        if t >= t0 - 2000 and first is None and b.valid:
            bb, ba = b.best()
            if ba > bb > 0:
                first = t - t0
        if t >= t0 and was and not b.valid:
            invalid += 1
        was = b.valid
    return dict(first_valid_two_sided_rel_ms=first, invalid_intervals_60s=invalid)


def listing(path: Path, logs: Path) -> dict:
    sym = path.name.split("_")[1]
    out = dict(symbol=sym, record=path.name, record_sha256=sha256(path), data_quality=[], marks=None)
    mark = MARKS.get(sym)
    out["listing_status"] = (STATUS_OF_SKIP.get(sym) or ("SYNTHETIC_FIXTURE" if mark and mark["mark"] == SYNTHETIC_FIXTURE
                                                         else "LISTED"))
    if mark:
        out["marks"] = mark["mark"]
        if mark["mark"] == DATA_AVAILABILITY_UNCONFIRMED:
            out["data_quality"].append(DATA_AVAILABILITY_UNCONFIRMED)
    try:
        t0, events, _, times = load_open(path)
    except ListingContractError as e:
        out["data_quality"].append("INVALID_FOR_LISTING_REPLAY")
        out["contract"] = str(e)
        t0, events, times = None, [], {}
    if t0 is None:
        if "INVALID_FOR_LISTING_REPLAY" not in out["data_quality"]:
            out["data_quality"].append("NO_OPEN_IN_RECORD")
    else:
        infos = times.get("infos") or []
        fos = sorted({int(i["firstOpenTime"]) for _, i in infos if i.get("firstOpenTime")})
        known = next((t for t, i in infos if i.get("firstOpenTime")), None)
        out["knowledge_provenance"] = dict(t0=t0, first_open_known_at=known,
                                           known_before_t0_ms=None if known is None else t0 - known,
                                           first_open_versions=len(fos), record_start=times.get("record_start_ms"))
        if times.get("stalled_frames"):
            out["data_quality"].append("RECONSTRUCTED_FRAMES")
        out["book_quality_as_of"] = book_quality(events, t0)
    if not out["data_quality"]:
        out["data_quality"] = ["OK"]
    evs = probe_events(logs, sym)
    if t0 is not None:
        win = [e for e in evs if t0 <= e["ts"] <= t0 + ENTRY_WINDOW_MS]
        api = ("API_UNAVAILABLE_AT_ENTRY" if any(e.get("kind") == "10007" for e in win) else
               "API_OPEN_AT_ENTRY" if any(e.get("kind") in ("api_accepted", "cap_accepted") for e in win) else
               "API_UNKNOWN")
        out["api_access_as_of"] = dict(status=api, window_ms=ENTRY_WINDOW_MS,
                                       first_10007=next((e["ts"] for e in win if e.get("kind") == "10007"), None))
        out["cap_evidence"] = dict(at_entry=cap_evidence(evs, sym, t0 + ENTRY_WINDOW_MS), final=cap_evidence(evs, sym, None))
    out["eligibility"] = decide(out)
    out["analysis_cohort"] = (["OPERATIONAL"] if out["listing_status"] == "LISTED" else []) + (
        ["VALIDATION"] if any(v["validation"] for v in out["eligibility"].values()) else [])
    return out


def decide(e: dict) -> dict:
    """Допуск к VALIDATION по алгоритмам — все причины исключения списком."""
    common = []
    if e["listing_status"] != "LISTED":
        common.append(e["listing_status"])
    common += [q for q in e["data_quality"] if q in (DATA_AVAILABILITY_UNCONFIRMED, "INVALID_FOR_LISTING_REPLAY",
                                                      "NO_OPEN_IN_RECORD")]
    if (e.get("api_access_as_of") or {}).get("status") == "API_UNAVAILABLE_AT_ENTRY":
        common.append("API_UNAVAILABLE_AT_ENTRY")
    cap = ((e.get("cap_evidence") or {}).get("final") or {}).get("status", "CAP_UNKNOWN")
    out = {}
    for algo in ALGOS:
        why = list(common)
        if algo == "MAIN_EARLY" and cap != "NO_CAP_OBSERVED":
            why.append(cap)
        if algo == "CAP_BREAKOUT" and cap != "CAP_FOUND":
            why.append(cap if cap != "KNOWN_CAPS_HISTORICAL" else "KNOWN_CAPS_HISTORICAL (условный тест модели)")
        out[algo] = dict(validation=not why, reasons=why)
    return out


def build(logs: Path = LOGS, out: Path | None = None) -> dict:
    rows = {}
    for p in records(logs):
        r = listing(p, logs)
        rows[r["symbol"]] = r
        el = " ".join(f"{a}:{'V' if v['validation'] else '-'}" for a, v in r["eligibility"].items())
        print(f"{r['symbol']:<14} {r['listing_status']:<9} {','.join(r['data_quality']):<34} "
              f"{(r.get('api_access_as_of') or {}).get('status', '—'):<25} "
              f"{((r.get('cap_evidence') or {}).get('final') or {}).get('status', '—'):<22} {el}", flush=True)
    body = dict(version=MANIFEST_VERSION, built_at=time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
                rules="tools/eligibility.py (docstring): допуск до PnL, независимые измерения", listings=rows)
    body["manifest_sha256"] = hashlib.sha256(json.dumps(rows, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    dst = out or logs / MANIFEST
    dst.write_text(json.dumps(body, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nманифест {dst}: листингов {len(rows)}, SHA {body['manifest_sha256'][:16]}")
    return body


def load(logs: Path = LOGS, check: list[Path] | None = None) -> dict:
    """Манифест для потребителя. Нет файла, другая версия контракта, запись не в манифесте или изменилась → FAIL."""
    p = logs / MANIFEST
    if not p.is_file():
        raise ManifestError(f"ОШИБКА: нет манифеста допуска {p} — построить: python3 tools/eligibility.py build "
                            "(без манифеста отчёт не строится — решение клиента 19.11)")
    m = json.loads(p.read_text(encoding="utf-8"))
    if m.get("version") != MANIFEST_VERSION:
        raise ManifestError(f"ОШИБКА: версия манифеста {m.get('version')} ≠ {MANIFEST_VERSION}")
    for rec in check or []:
        e = m["listings"].get(rec.name.split("_")[1])
        if e is None or e["record"] != rec.name:
            raise ManifestError(f"ОШИБКА: запись {rec.name} не в манифесте — перестроить манифест")
    return m


def eligible(m: dict, symbol: str, algo: str) -> tuple[bool, list[str]]:
    e = m["listings"].get(symbol)
    if e is None:
        raise ManifestError(f"ОШИБКА: {symbol} нет в манифесте допуска")
    v = e["eligibility"][algo]
    return v["validation"], v["reasons"]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", choices=("build", "show"))
    p.add_argument("--logs", default=str(LOGS))
    p.add_argument("--out", help="build: куда записать (по умолчанию logs/eligibility_manifest.json)")
    a = p.parse_args()
    if a.action == "build":
        build(Path(a.logs), Path(a.out) if a.out else None)
        return
    m = load(Path(a.logs))
    print(f"манифест {m['version']} от {m['built_at']}, SHA {m['manifest_sha256'][:16]}")
    for algo in ALGOS:
        ok = [s for s in m["listings"] if m["listings"][s]["eligibility"][algo]["validation"]]
        print(f"{algo}: VALIDATION {len(ok)} — {', '.join(ok)}")


if __name__ == "__main__":
    main()
