"""P0-REPLAY-SYNTHETIC-MISMATCH (решение клиента 16.11, п. 6.3): синтетический журнал BTCUSDT — путь PAPER 122 входа,
путь REPLAY 0. Инструмент расследования и защиты от повторения.

  fixture FILE            SHA-256, начало записи, firstOpenTime, OPEN_DETECTED и вердикт контракта листинга
                          (main_replay.listing_contract): OK / INVALID_FOR_LISTING_REPLAY (HISTORICAL_FIRST_OPEN, LATE_START).
  diverge FILE            первое расхождение СОСТОЯНИЯ движков двух путей (не PnL): события сравниваются по порядку —
                          сначала вход (время, вид события), затем состояние после него (T0 движка, старт веток,
                          действительность и версия стакана, состояния всех веток), плюс что каждый путь знал из
                          exchangeInfo. Контракт здесь отключён — только для диагностики.
  knowledge [FILES|--all-usdt]
                          время знания exchangeInfo на реальных записях: T0 и параметры инструмента (firstOpenTime,
                          точности, фильтры, комиссии), которые REPLAY передаёт движку заранее, против тех, что путь
                          PAPER фактически имел на момент старта веток; был ли firstOpenTime известен PAPER до первого
                          рыночного события. Любое отличие — вопрос заглядывания REPLAY (переоткрыть P0 №7).
  make-valid SRC OUT      валидная синтетика из журнала уже торгуемой пары: T0 := OPEN_DETECTED (момент запуска),
                          firstOpenTime всех exchangeInfo := T0, копия первого exchangeInfo — перед первым рыночным
                          событием (контракт листинга: параметры известны до открытия), первая строка — SYNTHETIC_FIXTURE
                          с источником и его SHA-256. Пара помечена core/data_marks.py SYNTHETIC_FIXTURE — в выборки не входит.
  compare FILE            пути REPLAY и PAPER на валидной фикстуре (paper_vs_replay.compare) + первое расхождение.

Критерий приёмки для compare (зафиксирован ДО прогона, 16.11): PASS — 0 веток с отличием по ключам
(decision_ms, entry_ms, entry_px, reason, pnl_usdt, exit_ms). Если отличия есть, каждое допустимо только когда первое
расхождение — во ВХОДЕ (иной порядок / время событий: REPLAY сортирует по времени с поправкой на остановки записи,
PAPER — в порядке получения); первое расхождение СОСТОЯНИЯ при одинаковом входе — FAIL без исключений.
"""

import argparse
import gzip
import hashlib
import itertools
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import p0_stress as ps  # noqa: E402
import paper_vs_replay as pvr  # noqa: E402
from core.cap_breakout import MainEngine, Opts  # noqa: E402
from core.feed import events_of, frame_meta, iter_lines, line_t  # noqa: E402
from main_replay import ListingContractError, listing_contract, load_open, usdt_records  # noqa: E402

INFO_KEYS = ("firstOpenTime",) + ps.INFO_KEYS
MARKET = ('"publicAggreDeals"', '"publicAggreDepths"', '"src":"rest","ch":"depth"')


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def utc(ms: int | None) -> str:
    return "—" if not ms else time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ms / 1000)) + f".{ms % 1000:03d} UTC"


def fixture(path: Path) -> dict:
    start = first_mkt = detected = None
    first_open = info_at_first_mkt = None
    for line in iter_lines(path):
        t = line_t(line)
        start = t if start is None else start
        if '"ch":"exchangeInfo"' in line:
            s = ((json.loads(line).get("data") or {}).get("symbols") or [{}])[0] or {}
            if s.get("firstOpenTime"):
                first_open = int(s["firstOpenTime"])
        elif detected is None and '"OPEN_DETECTED"' in line:
            detected = (json.loads(line).get("data") or {}).get("open_ms") or t
        elif first_mkt is None and any(m in line for m in MARKET):
            first_mkt, info_at_first_mkt = t, first_open
        if first_mkt and detected and first_open:
            break
    broken = listing_contract(start, first_open)
    return dict(file=path.name, sha256=sha256(path), record_start=start, first_market_event=first_mkt,
                open_detected=detected, first_open=first_open, first_open_known_before_market=info_at_first_mkt is not None,
                contract="OK" if not broken else f"INVALID_FOR_LISTING_REPLAY ({broken[0]}): {broken[1]}")


def cmd_fixture(a) -> None:
    for f in a.files:
        r = fixture(Path(f))
        print(f"{r['file']}\n  SHA-256 {r['sha256']}\n  начало записи {utc(r['record_start'])}; первое рыночное событие "
              f"{utc(r['first_market_event'])}; OPEN_DETECTED {utc(r['open_detected'])}; firstOpenTime {utc(r['first_open'])}"
              f"\n  firstOpenTime известен до первого рыночного события: {r['first_open_known_before_market']}"
              f"\n  контракт листинга: {r['contract']}")


# --- первое расхождение состояния ---

def state(eng: MainEngine) -> dict:
    b = eng.book
    return {"open_ms": eng.open_ms, "release": eng.release, "book_valid": b.valid, "book_version": b.version,
            "sims": tuple(ps.sim_state(s) for s in eng.sims)}


def replay_steps(path: Path):
    t0, events, info, _ = load_open(path, contract=False)
    eng = MainEngine(ps.CFG, Opts(), t0, info)
    know = (info or {}).get("firstOpenTime")
    for t, kind, data, recv, exch in events:
        eng.on_event(t, kind, data, recv, exch)
        yield (t, kind), state(eng), know


def paper_steps(path: Path):
    rec = ps.Rec(path, contract=False)
    eng = MainEngine(ps.CFG, Opts(), None, {})
    for e in rec.entries:
        data = e.get("data")
        if e.get("ch") == "exchangeInfo":
            syms = data.get("symbols") if isinstance(data, dict) else None
            if syms and syms[0] and eng.release is None:
                eng.info = syms[0]
            continue
        for t, kind, ev in events_of(e):
            eng.on_event(t, kind, ev, *frame_meta(e))
            yield (t, kind), state(eng), (eng.info or {}).get("firstOpenTime")


def first_divergence(path: Path) -> dict:
    rp, pp = replay_steps(path), paper_steps(path)
    i = 0
    sentinel = object()
    for r, p in itertools.zip_longest(rp, pp, fillvalue=sentinel):
        if r is sentinel or p is sentinel:  # один путь получил больше событий — расхождение входа, не «совпали»
            return dict(index=i, where="ВХОД", note=f"у {'REPLAY' if r is sentinel else 'PAPER'} события кончились "
                                                    f"на {i}; у другого есть ещё")
        (ri, rs, rk), (pi, pst, pk) = r, p
        if ri != pi:
            return dict(index=i, where="ВХОД", replay_event=ri, paper_event=pi,
                        note="порядок/время событий различаются (сортировка REPLAY против порядка получения PAPER)")
        if rs != pst:
            fields = [k for k in rs if rs[k] != pst[k]]
            return dict(index=i, where="СОСТОЯНИЕ", event=ri, fields=fields,
                        replay={k: rs[k] for k in fields if k != "sims"}, paper={k: pst[k] for k in fields if k != "sims"},
                        replay_knows_first_open=rk, paper_knows_first_open=pk)
        i += 1
    return dict(index=i, where=None, note=f"все {i} событий обоих путей (поровну): вход и состояние совпадают")


def cmd_diverge(a) -> None:
    for f in a.files:
        d = first_divergence(Path(f))
        print(f"{Path(f).name}: {json.dumps(d, ensure_ascii=False, default=str)}")


# --- время знания exchangeInfo на реальных записях ---

def knowledge(path: Path) -> dict:
    from main_replay import SKIP_SYMBOLS
    if (why := SKIP_SYMBOLS.get(path.name.split("_")[1])):
        return dict(symbol=path.name.split("_")[1], status=f"пропуск: {why}")
    t0, _, info_rep, _ = load_open(path)
    if t0 is None:
        return dict(symbol=path.name.split("_")[1], status="открытия нет")
    rec = ps.Rec(path)
    run = ps.Run(rec, rec.entries)
    eng = run.eng  # eng.info обновляется только до старта веток — это знание PAPER на момент старта
    first_mkt = next((e["t"] for e in rec.entries if e.get("ch") != "exchangeInfo"), None)
    known = next((t for t, s in rec.infos if s.get("firstOpenTime")), None)
    diff = {k: (info_rep.get(k), (eng.info or {}).get(k)) for k in INFO_KEYS
            if str(info_rep.get(k)) != str((eng.info or {}).get(k))}
    return dict(symbol=rec.sym, t0_replay=t0, t0_paper=eng.open_ms, release_paper=eng.release, info_diff=diff,
                first_open_known_before_market=known is not None and first_mkt is not None and known <= first_mkt,
                infos_seen=len(rec.infos),
                first_open_values=sorted({int(s["firstOpenTime"]) for _, s in rec.infos if s.get("firstOpenTime")}),
                ok=t0 == eng.open_ms and not diff)


def cmd_knowledge(a) -> None:
    files = [Path(f) for f in a.files] + (usdt_records() if a.all_usdt else [])
    bad = 0
    out = []
    for f in files:
        try:
            r = knowledge(f)
        except ListingContractError as e:
            r = dict(symbol=f.name.split("_")[1], status=str(e), ok=False)
        out.append(r)
        if "status" in r:
            print(f"{r['symbol']}: {r['status']}")
            continue
        bad += not r["ok"]
        fo = r["first_open_values"]
        print(f"{r['symbol']}: T0 REPLAY {r['t0_replay']} / PAPER {r['t0_paper']}; параметры отличаются: "
              f"{r['info_diff'] or 'нет'}; firstOpenTime известен PAPER до первого рыночного события: "
              f"{r['first_open_known_before_market']}; значений firstOpenTime в записи {len(fo)}"
              + (f" ({', '.join(utc(x) for x in fo)})" if len(fo) > 1 else "") + ("" if r["ok"] else "  <-- РАСХОЖДЕНИЕ"))
    print(f"\nзаписей {len(files)}, с расхождением T0 или параметров {bad}")
    if a.json:
        Path(a.json).write_text(json.dumps(out, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    sys.exit(1 if bad else 0)


# --- генератор валидной синтетики ---

def make_valid(src: Path, out: Path) -> dict:
    lines = list(iter_lines(src))
    detected = next(((json.loads(x).get("data") or {}).get("open_ms") for x in lines if '"OPEN_DETECTED"' in x), None)
    first_mkt = next((i for i, x in enumerate(lines) if any(m in x for m in MARKET)), None)
    info_i = next((i for i, x in enumerate(lines) if '"ch":"exchangeInfo"' in x), None)
    if first_mkt is None or info_i is None:
        raise SystemExit("в источнике нет рыночных событий или exchangeInfo")
    t0 = int(detected or line_t(lines[first_mkt]))

    def retime(x: str) -> str:
        e = json.loads(x)
        for s in (e.get("data") or {}).get("symbols") or []:
            if s:
                s["firstOpenTime"] = t0
        return json.dumps(e, ensure_ascii=False, separators=(",", ":"))

    body = [retime(x) if '"ch":"exchangeInfo"' in x else x for x in lines]
    first_t = line_t(lines[0])
    pre_info = json.loads(retime(lines[info_i]))
    pre_info["t"] = min(first_t, t0) - 1
    head = {"t": min(first_t, t0) - 2, "src": "event", "ch": "SYNTHETIC_FIXTURE",
            "data": {"source": src.name, "source_sha256": sha256(src), "t0": t0,
                     "rule": "T0 := OPEN_DETECTED источника; firstOpenTime всех exchangeInfo := T0; копия первого "
                             "exchangeInfo перед первым рыночным событием", "made": time.strftime("%Y-%m-%d %H:%M:%S UTC",
                                                                                                 time.gmtime()),
                     "mark": "SYNTHETIC_FIXTURE — не листинг, в выборки не входит"}}
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8") as f:
        for x in [json.dumps(head, ensure_ascii=False, separators=(",", ":")),
                  json.dumps(pre_info, ensure_ascii=False, separators=(",", ":"))] + body:
            f.write(x.rstrip("\n") + "\n")
    return dict(out=str(out), sha256=sha256(out), source_sha256=head["data"]["source_sha256"], t0=t0, lines=len(body) + 2)


def cmd_make_valid(a) -> None:
    r = make_valid(Path(a.src), Path(a.out))
    print(json.dumps(r, ensure_ascii=False, indent=1))
    print(f"контракт: {fixture(Path(a.out))['contract']}")


def cmd_compare(a) -> None:
    for f in a.files:
        p = Path(f)
        print(f"контракт: {fixture(p)['contract']}")
        n, d = pvr.compare(p, ps.CFG)
        rec = ps.Rec(p)
        run = ps.Run(rec, rec.entries)
        _, rep = pvr.replay_path(p, ps.CFG)
        print(f"входов: PAPER {run.entries()}, REPLAY {sum(1 for r in rep if r.get('entry_ms'))}")
        fd = first_divergence(p)
        print(f"первое расхождение: {json.dumps(fd, ensure_ascii=False, default=str)}")
        verdict = "PASS" if d == 0 else ("FAIL" if fd.get("where") == "СОСТОЯНИЕ" else "ОТЛИЧИЯ ВО ВХОДЕ — разобрать")
        print(f"вердикт по заранее заданному критерию: {verdict}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("fixture", "diverge", "compare"):
        s = sub.add_parser(name)
        s.add_argument("files", nargs="+")
    s = sub.add_parser("knowledge")
    s.add_argument("files", nargs="*")
    s.add_argument("--all-usdt", action="store_true")
    s.add_argument("--json")
    s = sub.add_parser("make-valid")
    s.add_argument("src")
    s.add_argument("out")
    a = p.parse_args()
    {"fixture": cmd_fixture, "diverge": cmd_diverge, "knowledge": cmd_knowledge, "make-valid": cmd_make_valid,
     "compare": cmd_compare}[a.cmd](a)


if __name__ == "__main__":
    main()
