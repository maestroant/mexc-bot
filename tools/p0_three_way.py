"""P0: один листинг по трём независимым соединениям — запись (listing_recorder), PAPER A (стратегия), PAPER B (теневое).

Источники: запись logs/rec_<SYM>_*.jsonl.gz, журнал кадров PAPER logs/paperrec_<SYM>_*.jsonl.gz (A — полные кадры,
B — метаданные: sendTime, версии diff-ов, сделки), лог PAPER logs/paper_<SYM>_*.jsonl (решения, SUMMARY).
Что считается:
  1. шлюзы и переподключения каждого соединения; поправка часов процессов (запись и PAPER — разные процессы на
     одном сервере: время кадра в шкале биржи = локальное + поправка; кто получил раньше — по локальным часам);
  2. доступность одних и тех же событий: diff-ы стакана — по toVersion, сделки — по (время, цена, количество);
     окно — по sendTime биржи от T0 − 5 с до T0 + 60 с; чего нет на каком соединении;
  3. доставка: получение − sendTime по каждому соединению; разница получения одних и тех же событий A − запись,
     B − запись, A − B; отдельно первые 2 с после T0; первые diff-ы после T0 по всем трём;
  4. PAPER A локально: обработка кадра (PROC), медленные кадры, задержка цикла событий, стакан действителен/нет;
  5. разложение до первого решения: T0 → первый кадр после T0 (доставка) → действительный двусторонний стакан
     (ожидание стакана) → первое решение; по PAPER A и по REPLAY записи;
  6. REPLAY на журнале PAPER A против фактических решений PAPER A (SUMMARY), путь PAPER на том же журнале, и REPLAY
     записи против PAPER A — масштаб расхождения между соединениями.
  python3 tools/p0_three_way.py VLXUSDT
  python3 tools/p0_three_way.py --rec logs/rec_X.jsonl.gz --journal logs/paperrec_X.jsonl.gz --paper logs/paper_X.jsonl
"""

import argparse
import json
from argparse import Namespace
from pathlib import Path

import paper_vs_replay as pvr
from core.cap_breakout import MAIN_EARLY_PAPER, ORDER_USDT
from core.feed import iter_lines
from main_replay import LOGS, load_open

WIN_FROM_MS, WIN_TO_MS, FIRST_MS = -5_000, 60_000, 2_000
FIRST_VERSIONS = 15


def pct(xs: list, q: float):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def stats(xs: list) -> str:
    if not xs:
        return "нет данных"
    return f"n={len(xs)}, p1 {pct(xs, 0.01):+.0f}, p50 {pct(xs, 0.5):+.0f}, p99 {pct(xs, 0.99):+.0f}, " \
           f"min {min(xs):+.0f}, max {max(xs):+.0f} мс"


class Conn:
    """События одного соединения: depth[toVersion] = (получено, sendTime), deals[(time, price, qty)] = получено."""

    def __init__(self, name: str, offset: int):
        self.name, self.offset = name, offset
        self.depth: dict[int, tuple[int, int]] = {}
        self.deals: dict[tuple, tuple[int, int]] = {}
        self.delays: list[tuple[int, int]] = []  # (sendTime, получено − sendTime)
        self.first_after_t0: tuple[int, int] | None = None
        self.events: list[tuple[int, str, dict]] = []

    def frame(self, t: int, send: int | None, depth: tuple | None, deals: list, t0: int) -> None:
        if not depth and not deals:  # у записи есть и другие каналы; сравниваются только общие (сделки, diff-ы)
            return
        if send:
            self.delays.append((send, t - send))
        if t >= t0 and self.first_after_t0 is None:
            self.first_after_t0 = (t, send)
        if depth:
            self.depth.setdefault(int(depth[1]), (t, send))
        for d in deals:
            self.deals.setdefault((str(d[0]), str(d[1]), str(d[2])), (t, send))

    def raw(self, t: int) -> int:
        """Локальное время сервера (без поправки часов процесса) — для сравнения процессов между собой."""
        return t - self.offset


def find(pattern: str) -> Path | None:
    files = sorted(LOGS.glob(pattern))
    return files[-1] if files else None


def ws_parts(data: dict):
    dep, deals = data.get("publicAggreDepths"), data.get("publicAggreDeals")
    depth = (dep.get("fromVersion"), dep.get("toVersion")) if dep else None
    rows = [[d.get("time"), d.get("price"), d.get("quantity")] for d in (deals or {}).get("deals", [])]
    return depth, rows


def load_paper_log(path: Path) -> dict:
    out = dict(offset=None, clock=[], decisions=[], summary=None, start=None)
    with path.open(encoding="utf-8") as f:
        for line in f:
            e = json.loads(line)
            ev = e.get("event")
            if ev == "START_PAPER":
                out["offset"] = e.get("clock_offset_ms")
            elif ev == "CLOCK":
                out["clock"].append((e["t"], e.get("offset_ms")))
            elif ev == "DECISION":
                out["decisions"].append(e)
            elif ev == "SUMMARY":
                out["summary"] = e
            elif ev == "START" and e.get("engine") == "main":
                out["start"] = e
    return out


def paper_offset(log: dict, t0: int) -> int:
    """Поправка часов PAPER на открытии: последняя CLOCK до T0 (за 5 мин до открытия она заморожена)."""
    before = [o for t, o in log["clock"] if t <= t0 and o is not None]
    return before[-1] if before else (log["offset"] or 0)


def load_rec(path: Path, t0_hint: int | None):
    """Запись: T0 (firstOpenTime), поправка часов, события подключения, кадры окна."""
    offset, first_open, events, frames = 0, None, [], []
    for line in iter_lines(path):
        e = json.loads(line)
        src, ch, data = e.get("src"), e.get("ch"), e.get("data")
        if src == "event":
            if ch == "START":
                offset = (data or {}).get("clock_offset_ms") or 0
            if ch in ("WS_CONNECTED", "WS_ERROR", "WS_CLOSED", "START", "OPEN_DETECTED"):
                events.append((e["t"], ch, data))
            continue
        if ch == "exchangeInfo" and isinstance(data, dict):
            syms = data.get("symbols") or []
            if syms and syms[0].get("firstOpenTime"):
                first_open = int(syms[0]["firstOpenTime"])
            continue
        t0 = t0_hint or first_open
        if t0 and e["t"] > t0 + WIN_TO_MS + 30_000:
            break
        if t0 and e["t"] >= t0 + WIN_FROM_MS - 30_000:
            frames.append(e)
    return offset, first_open, events, frames


def load_journal(path: Path):
    a, b, ev = [], [], []
    open_ms = None
    for line in iter_lines(path):
        e = json.loads(line)
        src = e.get("src")
        if src == "event":
            ev.append((e["t"], e["ch"], e.get("data") or {}))
            if e["ch"] == "OPEN_DETECTED":
                open_ms = (e.get("data") or {}).get("open_ms")
        elif src == "ws_shadow":
            b.append(e)
        else:
            a.append(e)
    return open_ms, a, b, ev


def compare_rows(name: str, rows_a: list[dict], rows_b: list[dict], t0: int, show: int = 4) -> list[str]:
    ka = {pvr.key(r): r for r in rows_a}
    kb = {pvr.key(r): r for r in rows_b}
    diff, out = 0, []
    rel = lambda v: v - t0 if isinstance(v, int) and v > 1e12 else v  # noqa: E731
    for k in sorted(set(ka) | set(kb), key=str):
        a = tuple(ka.get(k, {}).get(x) for x in pvr.KEYS) if k in ka else None
        b = tuple(kb.get(k, {}).get(x) for x in pvr.KEYS) if k in kb else None
        if a != b:
            diff += 1
            if diff <= show:
                out.append(f"    {k}\n      {[rel(x) for x in a] if a else 'нет ветки'}\n"
                           f"      {[rel(x) for x in b] if b else 'нет ветки'}")
    return [f"  {name}: веток {len(set(ka) | set(kb))}, расхождений {diff} ({', '.join(pvr.KEYS)})"] + out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("symbol", nargs="?")
    p.add_argument("--rec")
    p.add_argument("--journal")
    p.add_argument("--paper")
    a = p.parse_args()
    sym = (a.symbol or "").upper()
    rec_path = Path(a.rec) if a.rec else find(f"rec_{sym}_2*.jsonl.gz")
    jr_path = Path(a.journal) if a.journal else find(f"paperrec_{sym}_2*.jsonl.gz")
    pl_path = Path(a.paper) if a.paper else find(f"paper_{sym}_2*.jsonl")
    print(f"запись {rec_path}\nжурнал PAPER {jr_path}\nлог PAPER {pl_path}")
    if not (rec_path and jr_path and pl_path):
        raise SystemExit("нужны все три файла")

    log = load_paper_log(pl_path)
    open_ms, a_frames, b_frames, jev = load_journal(jr_path)
    rec_offset, first_open, rec_ev, rec_frames = load_rec(rec_path, open_ms)
    t0 = first_open or open_ms
    p_off = paper_offset(log, t0)
    conns = {"запись": Conn("запись", rec_offset), "A": Conn("A", p_off), "B": Conn("B", p_off)}
    for e in rec_frames:
        if e.get("src") == "ws_pub" and isinstance(e.get("data"), dict):
            depth, deals = ws_parts(e["data"])
            conns["запись"].frame(e["t"], int(e["data"].get("sendTime") or 0) or None, depth, deals, t0)
    for e in a_frames:
        if e.get("src") == "ws_pub" and isinstance(e.get("data"), dict):
            depth, deals = ws_parts(e["data"])
            conns["A"].frame(e["t"], int(e["data"].get("sendTime") or 0) or None, depth, deals, t0)
    for e in b_frames:
        d = e.get("data") or {}
        depth = (d.get("fromVersion"), d.get("toVersion")) if d.get("toVersion") else None
        deals = d.get("deals") if isinstance(d.get("deals"), list) else []
        conns["B"].frame(e["t"], int(d.get("sendTime") or 0) or None, depth, deals, t0)

    out = [f"\n# {rec_path.name.split('_')[1]}: T0 {t0} (firstOpenTime {first_open}, OPEN_DETECTED PAPER {open_ms})"]
    out.append(f"поправка часов: запись {rec_offset} мс, PAPER {p_off} мс (разница {p_off - rec_offset:+d} мс); "
               f"время кадра каждого процесса — в шкале биржи; «раньше/позже» — по локальным часам сервера")

    st = log["start"] or {}
    out.append(f"версии: код PAPER {st.get('code')}, предрегистрация {st.get('prereg')}, модель исполнения "
               f"{st.get('exec_model')}, комиссии {st.get('fees')}, веток {st.get('sims')}; конфигурация — START_PAPER")
    out.append("\n## 1. Соединения")
    for t, ch, data in rec_ev:
        if ch.startswith("WS"):
            out.append(f"  запись {ch} T0{t - t0:+d} мс {data}")
    for t, ch, data in jev:
        if ch.startswith("WS") or ch == "CLOCK":
            out.append(f"  PAPER {ch} T0{t - t0:+d} мс {data}")

    lo, hi = t0 + WIN_FROM_MS, t0 + WIN_TO_MS
    missing: dict[str, dict] = {}
    out.append(f"\n## 2. Доступность событий (sendTime от T0{WIN_FROM_MS:+d} до T0+{WIN_TO_MS} мс)")
    for what in ("depth", "deals"):
        sets = {n: {k for k, (t, s) in getattr(c, what).items() if s and lo <= s <= hi} for n, c in conns.items()}
        union = set().union(*sets.values())
        miss = {n: len(union - s) for n, s in sets.items()}
        missing[what] = miss
        label = "diff-ы стакана (toVersion)" if what == "depth" else "сделки (время, цена, кол-во)"
        out.append(f"  {label}: всего {len(union)}; нет на соединении: "
                   + ", ".join(f"{n} {m}" for n, m in miss.items()))
        if what == "depth" and union:
            vs = sorted(union)
            holes = [v for v in range(vs[0], vs[-1] + 1) if v not in union]
            out.append(f"    версии {vs[0]}…{vs[-1]}, пропусков в общей последовательности {len(holes)} "
                       f"(aggre-поток склеивает версии; пропуск номера сам по себе не потеря)")

    out.append("\n## 3. Доставка")
    for n, c in conns.items():
        all_d = [d for s, d in c.delays if lo <= s <= hi]
        first = [d for s, d in c.delays if t0 <= s <= t0 + FIRST_MS]
        out.append(f"  {n}: получение − sendTime: окно {stats(all_d)}; первые {FIRST_MS} мс {stats(first)}")
    for x, y in (("A", "запись"), ("B", "запись"), ("A", "B")):
        cx, cy = conns[x], conns[y]
        for what in ("depth", "deals"):
            common = [k for k in getattr(cx, what) if k in getattr(cy, what)]
            dd = [cx.raw(getattr(cx, what)[k][0]) - cy.raw(getattr(cy, what)[k][0]) for k in common
                  if getattr(cx, what)[k][1] and lo <= getattr(cx, what)[k][1] <= hi]
            first = [cx.raw(getattr(cx, what)[k][0]) - cy.raw(getattr(cy, what)[k][0]) for k in common
                     if getattr(cx, what)[k][1] and t0 <= getattr(cx, what)[k][1] <= t0 + FIRST_MS]
            out.append(f"  {x} − {y}, {'diff-ы' if what == 'depth' else 'сделки'}: окно {stats(dd)}; "
                       f"первые {FIRST_MS} мс {stats(first)}")
    out.append("  первый WS-кадр после T0 (получен; sendTime): " + "; ".join(
        f"{n} T0{c.first_after_t0[0] - t0:+d} мс; T0{(c.first_after_t0[1] or t0) - t0:+d} мс"
        for n, c in conns.items() if c.first_after_t0))
    after = sorted({v for c in conns.values() for v, (t, s) in c.depth.items() if s and s >= t0})[:FIRST_VERSIONS]
    out.append("  первые diff-ы после T0: toVersion, sendTime − T0, получение − T0 (запись / A / B), мс")
    for v in after:
        send = next((c.depth[v][1] for c in conns.values() if v in c.depth), None)
        cells = [f"{c.depth[v][0] - t0:+d}" if v in c.depth else "нет" for c in conns.values()]
        out.append(f"    {v}: {send - t0:+d}; {' / '.join(cells)}")

    out.append("\n## 4. PAPER A локально (окно журнала)")
    proc = [d["ms"] for t, ch, d in jev if ch == "PROC"]
    slow = [d for t, ch, d in jev if ch == "SLOW_FRAME"]
    lag = [d["lag_ms"] for t, ch, d in jev if ch == "LOOP_LAG"]
    out.append(f"  обработка кадра: n={len(proc)}, p50 {pct(proc, 0.5)}, p99 {pct(proc, 0.99)}, max "
               f"{max(proc) if proc else None} мс; медленных кадров (> 20 мс) {len(slow)}; задержек цикла событий "
               f"(> 20 мс) {len(lag)}, max {max(lag) if lag else 0} мс")
    for t, ch, d in jev:
        if ch in ("BOOK_VALID", "BOOK_INVALID") and t >= lo - 60_000:
            out.append(f"  {ch} T0{t - t0:+d} мс {d}")
        elif ch in ("LOOP_LAG", "SLOW_FRAME") and t0 - FIRST_MS <= t <= t0 + FIRST_MS:
            out.append(f"  {ch} T0{t - t0:+d} мс {d}")
    first_proc = [(t - t0, d["ms"]) for t, ch, d in jev if ch == "PROC" and t0 <= t <= t0 + FIRST_MS]
    out.append(f"  обработка кадров в первые {FIRST_MS} мс: {len(first_proc)} кадров, сумма "
               f"{sum(m for _, m in first_proc):.1f} мс, дольше 5 мс: "
               + (", ".join(f"T0{t:+d} — {m:.1f}" for t, m in first_proc if m > 5) or "нет"))
    summ = log["summary"] or {}
    lagd = summ.get("lag") or {}
    out.append(f"  SUMMARY: {json.dumps(lagd, ensure_ascii=False)}")

    out.append("\n## 5. Разложение до первого решения")
    cfg = Namespace(latency=[50], usdt=[ORDER_USDT], sweep=False, main_early=MAIN_EARLY_PAPER)
    times = summ.get("times") or {}
    decs = sorted((e for e in log["decisions"] if e.get("engine") == "main"), key=lambda e: e["decision_ms"])
    first_a = conns["A"].first_after_t0
    first_rel = f"{first_a[0] - t0:+d} мс" if first_a else "нет"
    out.append(f"  PAPER A: первый кадр после T0 {first_rel}; двусторонний стакан "
               f"{times.get('first_book_rel_ms')} мс; первая сделка {times.get('first_trade_rel_ms')} мс; первое "
               f"решение " + (f"{decs[0]['decision_ms'] - t0:+d} мс (кадр: sendTime T0"
                              f"{(decs[0].get('exch_ms') or t0) - t0:+d}, получен T0{decs[0]['recv_ms'] - t0:+d}, "
                              f"версия {decs[0]['version']})" if decs else "нет"))
    rt0, rep_rec = pvr.replay_path(rec_path, cfg)
    if rt0:
        first_rec = min((r["decision_ms"] for r in rep_rec if r.get("decision_ms")), default=None)
        out.append("  REPLAY записи: первое решение " + (f"{first_rec - t0:+d} мс" if first_rec else "нет"))

    out.append("\n## 6. Решения")
    actual = (summ.get("rows") or {}).get("main", [])
    jt0, rep_j = pvr.replay_path(jr_path, cfg)
    out += compare_rows("REPLAY журнала A против фактического PAPER A", rep_j, actual, t0)
    if jt0:
        out += compare_rows("путь PAPER на журнале A против фактического PAPER A", pvr.paper_path(jr_path, cfg, jt0),
                            actual, t0)
    if rt0:
        out += compare_rows("REPLAY записи против фактического PAPER A (разные соединения)", rep_rec, actual, t0)
    out.append(f"\n## 7. Состояния данных (sendTime T0{WIN_FROM_MS:+d} … T0+{WIN_TO_MS} мс, PAPER A)")
    lost = missing.get("depth", {}).get("A", 0) + missing.get("deals", {}).get("A", 0)
    gaps = [t for t, ch, d in jev if ch == "BOOK_INVALID" and lo <= t <= hi]
    out.append(f"  DATA_CORRUPTED (потерянные события, разрывы версий стакана): событий нет на A {lost}, "
               f"разрывов {len(gaps)}" + (f" ({', '.join(f'T0{t - t0:+d}' for t in gaps)})" if gaps else ""))
    a_lags = [d for s_, d in conns["A"].delays if lo <= s_ <= hi]
    base = min(a_lags) if a_lags else 0
    over = {ms: sum(d - base > ms for d in a_lags) for ms in (300, 500, 800)}
    out.append(f"  DELIVERY_DELAYED (получение − sendTime сверх минимума {base} мс): "
               + ", ".join(f"> {ms} мс — {n}" for ms, n in over.items()) + f" из {len(a_lags)} кадров")
    skips: dict[str, int] = {}
    for r in actual:
        for k, v in (r.get("skips") or {}).items():
            skips[k] = skips.get(k, 0) + v
    out.append(f"  DATA_STALE (отказы веток по актуальности стакана): stale_book {skips.get('stale_book', 0)}, "
               f"book_invalid {skips.get('book_invalid', 0)}, momentum_not_ready {skips.get('momentum_not_ready', 0)}")
    out.append(f"  MODEL_UNVERIFIED (время кадров восстановлено после остановки процесса): журнал A "
               f"{(load_open(jr_path)[3] or {}).get('stalled_frames', 0)} кадров; запись "
               f"{(load_open(rec_path)[3] or {}).get('stalled_frames', 0)} кадров")
    print("\n".join(out))


if __name__ == "__main__":
    main()
