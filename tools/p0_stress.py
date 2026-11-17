"""P0: контролируемые тесты потока данных на существующих записях (без ожидания нового листинга).

Кадры записи listing_recorder прогоняются путём PAPER (MainEngine без заданного T0, кадры в порядке получения,
T0 — из exchangeInfo по ходу потока) без изменений (базовый прогон) и с искажениями потока. Конфигурация одна —
набор PAPER (MAIN_EARLY_PAPER и BASELINE, задержка ордера 50 мс). Для каждого теста — ожидаемый результат и
проверка фактического:
  delay    — все WS-кадры приходят позже на 0 / 200 / 500 / 800 мс (REST не задерживается); «пачка» как у VLX:
             кадры первых 800 мс после T0 приходят разом на T0 + 800 мс;
  snapshot — поток начат за 3 с до T0 (стакан ещё не собран): REST-снимки позже на 1 / 3 с, первый снимок потерян;
  gap      — потерян diff перед первым возможным BUY (разрыв версий стакана);
  late     — тот же diff приходит после восстановления стакана; повтор уже применённого diff-а без разрыва;
  order    — REST-снимки раньше / позже WS-кадров на 200 / 500 мс;
  future   — нет будущих данных: прогон, обрезанный в момент C, в момент C в точности равен полному прогону;
             плюс аудит подготовки данных REPLAY (exchangeInfo после T0, перенос firstOpenTime, восстановленные кадры);
  same     — PAPER = REPLAY на одинаковых кадрах и конфигурации (tools/paper_vs_replay.py).
Общие проверки каждого прогона: ни одного решения на недействительном стакане (до первого снимка и внутри разрыва
версий); стакан на одинаковых версиях совпадает с базовым (первые FP_LEVELS уровней обеих сторон).
  python3 tools/p0_stress.py --all-usdt --json logs/p0_stress.json
  python3 tools/p0_stress.py --dir <папка с записями> --all-usdt --tests delay,gap
"""

import argparse
import bisect
import json
import sys
import time
from argparse import Namespace
from pathlib import Path
from statistics import median

import paper_vs_replay
from core.cap_breakout import MAIN_EARLY_PAPER, ORDER_USDT, MainEngine, Opts
from core.feed import events_of, frame_meta, iter_lines, line_t
from main_replay import LOGS, load_open

CFG = Namespace(latency=[50], usdt=[ORDER_USDT], sweep=False, main_early=MAIN_EARLY_PAPER)
BEFORE_MS, AFTER_MS = 60_000, 120_000  # окно потока вокруг T0
FP_FROM_MS, FP_TO_MS, FP_LEVELS = -5_000, 60_000, 20  # сравнение стакана по версиям
LATE_JOIN_MS = 3_000
DELAYS_MS = (0, 200, 500, 800)
BURST_MS = 800
SNAP_DELAYS_MS = (1_000, 3_000)
ORDER_SHIFTS_MS = (-500, -200, 200, 500)
DUP_AFTER_MS = 300
FUTURE_CUTS = 8
DECISION_KEYS = ("decision_ms", "entry_ms", "entry_px")
EXIT_KEYS = ("reason", "exit_ms", "pnl_usdt")
INFO_KEYS = ("quotePrecision", "baseAssetPrecision", "filters", "takerCommission", "makerCommission")
TESTS = ("delay", "snapshot", "gap", "late", "order", "future", "same")


class Rec:
    """Окно записи: T0, строки (exchangeInfo, REST-снимки, WS-кадры) в порядке получения, сведения REPLAY."""

    def __init__(self, path: Path, contract: bool = True):
        self.path, self.sym = path, path.name.split("_")[1]
        self.t0, _, self.info_replay, self.times = load_open(path, contract=contract)
        self.entries, self.infos = [], []
        if self.t0 is None:
            return
        lo, hi = self.t0 - BEFORE_MS, self.t0 + AFTER_MS
        info_line = None
        for line in iter_lines(path):
            t = line_t(line)
            if t > hi:
                break
            is_info = '"ch":"exchangeInfo"' in line
            if is_info:
                syms = (json.loads(line).get("data") or {}).get("symbols") or []
                if syms and syms[0]:
                    self.infos.append((t, syms[0]))
            if t < lo:
                info_line = line if is_info else info_line
                continue
            if is_info or '"src":"rest","ch":"depth"' in line or '"publicAggreDepths"' in line \
                    or '"publicAggreDeals"' in line:
                self.entries.append(json.loads(line))
        if info_line:  # параметры инструмента известны с начала окна, как у PAPER, запущенного заранее
            self.entries.insert(0, dict(json.loads(info_line), t=lo))


def key(r: dict) -> tuple:
    return r["mode"], r["variant"], r["profile"], r["usdt"], r["latency"]


def sim_state(s) -> tuple:
    return (s.state, s.result.get("decision_ms"), s.entry_ms, round(s.entry_px, 12), s.exit_ms, s.exit_reason,
            round(s.Q, 12), round(s.R, 12), s.tp1_done, s.guard_armed, s.buy_limit)


def eng_state(eng: MainEngine) -> tuple:
    b = eng.book
    return eng.release, eng.open_ms, b.version, b.valid, tuple(sim_state(s) for s in eng.sims)


def book_fp(b) -> tuple:
    return tuple(b.levels("bids")[:FP_LEVELS]), tuple(b.levels("asks")[:FP_LEVELS])


class Run:
    """Один прогон путём PAPER. fp — стакан по версиям, tl — (t, bestBid, bestAsk), at_cut — состояние в моменты cuts,
    first_valid / invalid — когда стакан стал действительным и интервалы [a, b) недействительного после этого."""

    def __init__(self, rec: Rec, entries: list[dict], until: int | None = None, cuts=(), timeline: bool = False):
        eng = MainEngine(CFG, Opts(), None, {})
        self.fp, self.tl, self.at_cut = {}, [], {}
        self.first_valid, self.invalid = None, []
        self.snap_jumps = []  # REST-снимок новее стакана из diff-ов (WS отстаёт от REST): (t от T0, версий вперёд)
        cuts, ci, valid = sorted(cuts), 0, False
        lo, hi = rec.t0 + FP_FROM_MS, rec.t0 + FP_TO_MS
        for e in entries:
            t = e["t"]
            if until is not None and t > until:
                break
            while ci < len(cuts) and t > cuts[ci]:
                self.at_cut[cuts[ci]] = eng_state(eng)
                ci += 1
            data = e.get("data")
            if e.get("ch") == "exchangeInfo":
                syms = data.get("symbols") if isinstance(data, dict) else None
                if syms and syms[0] and eng.release is None:
                    eng.info = syms[0]
                continue
            b = eng.book
            was = b.valid, b.version
            for et, kind, ev in events_of(e):
                eng.on_event(et, kind, ev, *frame_meta(e))
            if is_snap(e) and was[0] and b.version != was[1] and lo <= t <= hi:
                self.snap_jumps.append((t - rec.t0, b.version - was[1]))
            if b.valid != valid:
                if not b.valid:
                    self.invalid.append([t, None])
                elif self.first_valid is None:
                    self.first_valid = t
                elif self.invalid and self.invalid[-1][1] is None:
                    self.invalid[-1][1] = t
                valid = b.valid
            if b.valid and lo <= t <= hi:
                if b.version not in self.fp:
                    self.fp[b.version] = book_fp(b)
                if timeline:
                    self.tl.append((t, *b.best()))
        for c in cuts[ci:]:
            self.at_cut[c] = eng_state(eng)
        self.end_state = eng_state(eng)
        eng.finish()
        self.eng = eng
        self.rows = {key(r): r for r in eng.rows(rec.sym)} if eng.release is not None else {}

    def violations(self) -> int:
        """Решения на недействительном стакане: до первого действительного или внутри разрыва версий."""
        bad = 0
        for s in self.eng.sims:
            d = s.result.get("decision_ms")
            if d is None:
                continue
            if self.first_valid is None or d < self.first_valid or any(
                    a < d < (b if b is not None else float("inf")) for a, b in self.invalid):
                bad += 1
        return bad

    def entries(self) -> int:
        return sum(1 for r in self.rows.values() if r.get("entry_ms"))

    def pnl(self) -> float:
        return sum(r.get("pnl_usdt") or 0.0 for r in self.rows.values())

    def first_book_rel(self) -> int | None:
        return self.eng.times().get("first_book_rel_ms")


def diff_rows(a: Run, b: Run) -> dict:
    """Ветки с другим решением или входом, с тем же входом, но другим выходом; потерянные и новые входы."""
    dec = ex = ex_pnl = lost = new = 0
    ex_dt = []
    for k in set(a.rows) | set(b.rows):
        ra, rb = a.rows.get(k, {}), b.rows.get(k, {})
        if tuple(ra.get(x) for x in DECISION_KEYS) != tuple(rb.get(x) for x in DECISION_KEYS):
            dec += 1
        elif tuple(ra.get(x) for x in EXIT_KEYS) != tuple(rb.get(x) for x in EXIT_KEYS):
            ex += 1
            ex_pnl += abs((ra.get("pnl_usdt") or 0) - (rb.get("pnl_usdt") or 0)) > 1e-9
            if ra.get("exit_ms") and rb.get("exit_ms"):
                ex_dt.append(abs(ra["exit_ms"] - rb["exit_ms"]))
        lost += bool(ra.get("entry_ms")) and not rb.get("entry_ms")
        new += bool(rb.get("entry_ms")) and not ra.get("entry_ms")
    return dict(branches=len(set(a.rows) | set(b.rows)), decision_diff=dec, exit_diff=ex, exit_pnl_diff=ex_pnl,
                exit_dt_max_ms=max(ex_dt, default=0), lost_entries=lost, new_entries=new)


def fp_check(a: Run, b: Run) -> dict:
    common = [v for v in a.fp if v in b.fp]
    return dict(fp_versions=len(common), fp_bad=sum(a.fp[v] != b.fp[v] for v in common))


def staleness(base: Run, run: Run) -> dict:
    """Цена решения против фактического рынка в тот же момент: bestAsk базового (незадержанного) потока на время
    решения (после всех кадров этой миллисекунды — отсюда шум и при задержке 0). unfillable — лимит BUY ниже
    фактического ask: на приходе ордер не исполнится и будет ждать (модель исполнения этого не видит — она исполняет
    по тому же задержанному стакану)."""
    ts = [x[0] for x in base.tl]
    gaps, unfill = [], 0
    for s in run.eng.sims:
        d = s.result.get("decision_ms")
        if d is None:
            continue
        i = bisect.bisect_right(ts, d) - 1
        if i < 0 or base.tl[i][2] <= 0:
            continue
        true_ask = base.tl[i][2]
        gaps.append(100 * (s.decision_ask / true_ask - 1))
        unfill += s.buy_limit < true_ask * (1 - 1e-9)
    return dict(decisions=len(gaps), ask_gap_med_pct=median(gaps) if gaps else None,
                ask_gap_min_pct=min(gaps) if gaps else None, unfillable=unfill,
                behind_05=sum(g < -0.5 for g in gaps), behind_1=sum(g < -1 for g in gaps))


def summary(base: Run, run: Run) -> dict:
    return dict(**diff_rows(base, run), **fp_check(base, run), violations=run.violations(),
                entries_base=base.entries(), entries=run.entries(), pnl_base=base.pnl(), pnl=run.pnl(),
                first_book_base=base.first_book_rel(), first_book=run.first_book_rel(),
                first_valid_rel=None if run.first_valid is None else run.first_valid - base.eng.open_ms if
                base.eng.open_ms else None)


# --- искажения потока ---
def is_ws(e: dict) -> bool:
    return e.get("src") == "ws_pub"


def is_snap(e: dict) -> bool:
    return e.get("src") == "rest" and e.get("ch") == "depth"


def is_diff(e: dict) -> bool:
    return is_ws(e) and isinstance(e.get("data"), dict) and "publicAggreDepths" in e["data"]


def retime(entries: list[dict], fn) -> list[dict]:
    """Новое время получения fn(e) (None — без изменений); порядок — по новому времени, при равенстве прежний."""
    out = []
    for e in entries:
        t = fn(e)
        out.append(e if t is None else dict(e, t=t))
    return sorted(out, key=lambda e: e["t"])


def late_join(rec: Rec) -> list[dict]:
    start = rec.t0 - LATE_JOIN_MS
    return [e for e in rec.entries if e.get("ch") == "exchangeInfo" or e["t"] >= start]


# --- тесты ---
def t_delay(rec, base, out):
    for ms in DELAYS_MS:
        run = Run(rec, retime(rec.entries, lambda e: e["t"] + ms if is_ws(e) else None))
        out[f"delay_{ms}"] = dict(summary(base, run), **staleness(base, run), shift=ms)
    t0 = rec.t0
    run = Run(rec, retime(rec.entries, lambda e: t0 + BURST_MS if is_ws(e) and t0 <= e["t"] < t0 + BURST_MS else None))
    out[f"burst_{BURST_MS}"] = dict(summary(base, run), **staleness(base, run))


def t_snapshot(rec, base, out):
    joined = late_join(rec)
    out["join_3s"] = summary(base, Run(rec, joined))
    for ms in SNAP_DELAYS_MS:
        out[f"snap_delay_{ms}"] = summary(base, Run(rec, retime(joined, lambda e: e["t"] + ms if is_snap(e) else None)))
    first = next((i for i, e in enumerate(joined) if is_snap(e)), None)
    if first is not None:
        run = Run(rec, joined[:first] + joined[first + 1:])
        out["snap_lost"] = dict(summary(base, run), lost_snap_rel=joined[first]["t"] - rec.t0)


def first_decision(base: Run) -> int | None:
    ds = [s.result["decision_ms"] for s in base.eng.sims if s.result.get("decision_ms")]
    return min(ds) if ds else None


def gap_case(rec, base):
    """Потерять последний diff не позже первого решения базового прогона: (индекс, прогон) или None."""
    d = first_decision(base)
    if d is None:
        return None
    idx = max((i for i, e in enumerate(rec.entries) if is_diff(e) and e["t"] <= d), default=None)
    if idx is None:
        return None
    return idx, Run(rec, rec.entries[:idx] + rec.entries[idx + 1:])


def t_gap(rec, base, out, cache):
    case = gap_case(rec, base)
    cache["gap"] = case
    if case is None:
        return
    idx, run = case
    lost_t = rec.entries[idx]["t"]
    gaps = [[a, b] for a, b in run.invalid if a >= lost_t]
    wait = gaps[0][1] - gaps[0][0] if gaps and gaps[0][1] is not None else None
    out["gap"] = dict(summary(base, run), lost_rel=lost_t - rec.t0, gap_detected=bool(gaps), recovery_ms=wait,
                      first_decision_rel=first_decision(base) - rec.t0)


def t_late(rec, base, out, cache):
    case = cache.get("gap") or gap_case(rec, base)
    if case is None:
        return
    idx, gap_run = case
    lost = rec.entries[idx]
    gaps = [[a, b] for a, b in gap_run.invalid if a >= lost["t"]]
    if gaps and gaps[0][1] is not None:  # потерянный diff приходит после восстановления стакана
        entries = rec.entries[:idx] + rec.entries[idx + 1:]
        run = Run(rec, retime(entries + [dict(lost, t=gaps[0][1] + 1)], lambda e: None))
        out["late_after_recovery"] = dict(**diff_rows(gap_run, run), **fp_check(gap_run, run),
                                          violations=run.violations(), late_rel=gaps[0][1] + 1 - rec.t0)
    run = Run(rec, retime(rec.entries + [dict(lost, t=lost["t"] + DUP_AFTER_MS)], lambda e: None))
    out["duplicate"] = summary(base, run)


def t_order(rec, base, out):
    for ms in ORDER_SHIFTS_MS:
        out[f"order_snap_{ms:+d}"] = summary(base, Run(rec, retime(rec.entries, lambda e: e["t"] + ms if is_snap(e)
                                                                    else None)))


def pick(xs: list[int], n: int) -> list[int]:
    xs = sorted(set(xs))
    if len(xs) <= n:
        return xs
    return [xs[round(i * (len(xs) - 1) / (n - 1))] for i in range(n)]


def t_future(rec, base, out):
    moments = [s.result["decision_ms"] for s in base.eng.sims if s.result.get("decision_ms")]
    moments += [s.exit_ms for s in base.eng.sims if s.exit_ms]
    moments += [rec.t0 + 1_000, rec.t0 + 5_000]
    cuts = pick(moments, FUTURE_CUTS)
    full = Run(rec, rec.entries, cuts=cuts)
    bad = []
    for c in cuts:
        if Run(rec, rec.entries, until=c).end_state != full.at_cut[c]:
            bad.append(c - rec.t0)
    info_t0 = next((i for t, i in reversed(rec.infos) if t <= rec.t0), {})
    info_diff = [k for k in INFO_KEYS if info_t0.get(k) != rec.info_replay.get(k)]
    opens = sorted({int(i["firstOpenTime"]) for _, i in rec.infos if i.get("firstOpenTime")})
    out["future"] = dict(cuts=len(cuts), mismatches=len(bad), bad_cuts_rel=bad,
                         replay_info_after_t0_diff=info_diff, first_open_values=len(opens),
                         stalled_frames=(rec.times or {}).get("stalled_frames", 0))


def t_same(rec, base, out):
    n, d = paper_vs_replay.compare(rec.path, CFG)
    out["same"] = dict(branches=n, diff=d, stalled_frames=(rec.times or {}).get("stalled_frames", 0))


# --- ожидания и проверка ---
def ok_common(r: dict) -> bool:
    return r.get("violations", 0) == 0 and r.get("fp_bad", 0) == 0


CHECKS = {
    "delay_0": ("совпадает с базовым прогоном полностью",
                lambda r: r["decision_diff"] == r["exit_diff"] == 0 and ok_common(r)),
    "delay": ("решений на недействительном стакане 0; стакан по версиям совпадает; первая отметка стакана позже "
              "на величину задержки; часть входов меняется; цена решения отстаёт от фактической", ok_common),
    "burst": ("как у задержки; после пачки стакан тот же по версиям", ok_common),
    "join_3s": ("до первого снимка решений нет; стакан по версиям совпадает; решения после T0 — как в базовом",
                lambda r: ok_common(r) and r["decision_diff"] == 0),
    "snap_delay": ("до снимка стакан недействителен, решений нет; после снимка стакан совпадает по версиям; "
                   "решения меняются, только если снимок пришёл после T0", ok_common),
    "snap_lost": ("стакан действителен со следующего снимка (~1 с), решений до него нет; стакан совпадает", ok_common),
    "gap": ("разрыв обнаружен на следующем diff; решений внутри разрыва нет; стакан действителен со следующего "
            "снимка и совпадает по версиям", lambda r: r["gap_detected"] and ok_common(r)),
    "late_after_recovery": ("запоздалый diff отброшен (версия ниже стакана): решения и стакан — как без него",
                            lambda r: r["decision_diff"] == 0 and ok_common(r)),
    "duplicate": ("повтор отброшен: стакан и решения как в базовом; выходы по времени могут сработать на лишнем кадре",
                  lambda r: r["decision_diff"] == 0 and ok_common(r)),
    "order_snap": ("стакан по версиям совпадает; решений на недействительном стакане нет", ok_common),
    "future": ("состояние всех веток в момент C совпадает с полным прогоном", lambda r: r["mismatches"] == 0),
    "same": ("PAPER = REPLAY; расхождения допустимы только на записях с восстановленными кадрами",
             lambda r: r["diff"] == 0 or r["stalled_frames"] > 0),
}


def check_for(name: str):
    for k in (name, name.rsplit("_", 1)[0], name.split("_")[0]):
        if k in CHECKS:
            return CHECKS[k]
    raise KeyError(name)


def run_record(path: Path, tests: set[str]) -> dict:
    rec = Rec(path)
    if rec.t0 is None or not rec.entries:
        return {}
    base = Run(rec, rec.entries, timeline=True)
    out, cache = {"_base": dict(entries=base.entries(), branches=len(base.rows), first_book=base.first_book_rel(),
                                first_decision_rel=(first_decision(base) - rec.t0) if first_decision(base) else None,
                                snap_jumps=base.snap_jumps)}, {}
    if "delay" in tests:
        t_delay(rec, base, out)
    if "snapshot" in tests:
        t_snapshot(rec, base, out)
    if "gap" in tests:
        t_gap(rec, base, out, cache)
    if "late" in tests:
        t_late(rec, base, out, cache)
    if "order" in tests:
        t_order(rec, base, out)
    if "future" in tests:
        t_future(rec, base, out)
    if "same" in tests:
        t_same(rec, base, out)
    for name, r in out.items():
        if not name.startswith("_"):
            r["pass"] = bool(check_for(name)[1](r))
    return out


def fmt(v, nd=0):
    if v is None:
        return "—"
    return f"{v:.{nd}f}" if isinstance(v, float) else str(v)


def report(results: dict) -> str:
    names = []
    for res in results.values():
        names += [n for n in res if not n.startswith("_") and n not in names]
    jumps = {s: res["_base"]["snap_jumps"] for s, res in results.items() if res["_base"].get("snap_jumps")}
    lines = ["## базовый прогон: REST-снимок новее стакана из WS diff-ов (T0 − 5 с … T0 + 60 с)",
             "  " + ("; ".join(f"{s}: {len(j)} раз, первый на T0{j[0][0]:+d} мс (+{j[0][1]} версий), "
                               f"больше всего +{max(v for _, v in j)} версий" for s, j in jumps.items()) or "ни разу")]
    for name in names:
        rs = {s: res[name] for s, res in results.items() if name in res}
        exp = check_for(name)[0]
        fails = [s for s, r in rs.items() if not r["pass"]]
        tot = lambda k: sum(r.get(k) or 0 for r in rs.values())  # noqa: E731
        lines.append(f"\n## {name}: записей {len(rs)}, прошли {len(rs) - len(fails)}"
                     + (f", не прошли: {', '.join(fails)}" if fails else ""))
        lines.append(f"  ожидание: {exp}")
        if "branches" in next(iter(rs.values())) and name != "same":
            lines.append(f"  веток {tot('branches')}: другое решение/вход {tot('decision_diff')}, тот же вход и другой "
                         f"выход {tot('exit_diff')} (из них с другим PnL {tot('exit_pnl_diff')}, сдвиг выхода до "
                         f"{max(r.get('exit_dt_max_ms') or 0 for r in rs.values())} мс), входов потеряно "
                         f"{tot('lost_entries')}, новых {tot('new_entries')}")
        if "fp_versions" in next(iter(rs.values())):
            lines.append(f"  стакан: версий сравнено {tot('fp_versions')}, расхождений {tot('fp_bad')}; решений на "
                         f"недействительном стакане {tot('violations')}")
        if "entries" in next(iter(rs.values())):
            lines.append(f"  входов {tot('entries_base')} → {tot('entries')}; PnL {tot('pnl_base'):+.2f} → "
                         f"{tot('pnl'):+.2f} USDT (модель исполнения по тому же, задержанному стакану)")
        if "ask_gap_med_pct" in next(iter(rs.values())):
            g = [r["ask_gap_med_pct"] for r in rs.values() if r["ask_gap_med_pct"] is not None]
            lines.append(f"  цена решения против фактического ask в тот же момент: медиана по листингам "
                         f"{fmt(median(g), 2) if g else '—'} %, худший листинг {fmt(min(g), 2) if g else '—'} %; "
                         f"лимит BUY ниже фактического ask (ордер не исполнится сразу): {tot('unfillable')} из "
                         f"{tot('decisions')} решений; цена решения ниже фактической более чем на 0,5 % — "
                         f"{tot('behind_05')}, на 1 % — {tot('behind_1')}")
        if "shift" in next(iter(rs.values())) or name.startswith("burst"):
            sh = [r["first_book"] - r["first_book_base"] for r in rs.values()
                  if r["first_book"] is not None and r["first_book_base"] is not None]
            lines.append(f"  сдвиг первого действительного двустороннего стакана: "
                         f"{', '.join(str(x) for x in sorted(set(sh)))} мс")
        if name.startswith(("join", "snap")):
            fv = [r["first_valid_rel"] for r in rs.values() if r["first_valid_rel"] is not None]
            lines.append(f"  стакан действителен с T0 {fmt(min(fv))}…{fmt(max(fv))} мс" if fv else "")
        if name == "gap":
            w = [r["recovery_ms"] for r in rs.values() if r["recovery_ms"] is not None]
            lines.append(f"  разрыв обнаружен {sum(r['gap_detected'] for r in rs.values())} из {len(rs)}; ожидание "
                         f"снимка: медиана {fmt(median(w)) if w else '—'} мс, max {fmt(max(w)) if w else '—'} мс; "
                         f"не восстановлен в окне: "
                         f"{sum(r['recovery_ms'] is None and r['gap_detected'] for r in rs.values())}")
        if name == "future":
            info = ", ".join(s + ":" + "/".join(r["replay_info_after_t0_diff"]) for s, r in rs.items()
                             if r["replay_info_after_t0_diff"]) or "нигде"
            moved = ", ".join(s for s, r in rs.items() if r["first_open_values"] > 1) or "нет"
            stalled = ", ".join(s for s, r in rs.items() if r["stalled_frames"]) or "нет"
            lines.append(f"  точек обрезки {tot('cuts')}, несовпадений {tot('mismatches')}; exchangeInfo REPLAY "
                         f"(до T0+10 с) отличается от известного к T0: {info}; перенос firstOpenTime: {moved}; "
                         f"записи с восстановленными кадрами: {stalled}")
        if name == "same":
            where = ", ".join(f"{s} {r['diff']}" for s, r in rs.items() if r["diff"]) or "нет"
            lines.append(f"  веток {tot('branches')}, расхождений {tot('diff')}: {where}")
    return "\n".join(x for x in lines if x)


def records(folder: Path) -> list[Path]:
    latest = {}
    for p in sorted(folder.glob("rec_*USDT_2*.jsonl.gz")):
        latest[p.name.split("_")[1]] = p
    skip = {"PFPUSDT", "PALUSDT"}
    return sorted((p for s, p in latest.items() if s not in skip), key=lambda p: p.name.split("_", 2)[2])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="*")
    p.add_argument("--all-usdt", action="store_true")
    p.add_argument("--dir", default=str(LOGS), help="папка с записями rec_*.jsonl.gz")
    p.add_argument("--tests", default=",".join(TESTS))
    p.add_argument("--json", help="результаты по записям в JSON")
    a = p.parse_args()
    tests = set(a.tests.split(","))
    files = [Path(f) for f in a.files] + (records(Path(a.dir)) if a.all_usdt else [])
    results = {}
    for f in files:
        started = time.time()
        res = run_record(f, tests)
        if not res:
            print(f"{f.name}: открытия нет", file=sys.stderr)
            continue
        sym = f.name.split("_")[1]
        results[sym] = res
        bad = [n for n, r in res.items() if not n.startswith("_") and not r["pass"]]
        print(f"{sym}: {time.time() - started:.0f} с, тестов {len(res) - 1}, не прошли: {', '.join(bad) or 'нет'}",
              file=sys.stderr, flush=True)
        if a.json:
            Path(a.json).write_text(json.dumps(results, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    print(report(results))


if __name__ == "__main__":
    main()
