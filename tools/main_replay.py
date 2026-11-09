"""REPLAY основной стратегии: контрольный BASELINE (ТЗ v2.1) и экспериментальный MAIN_EARLY — по записям
listing_recorder на общем ядре (core.cap_breakout.MainEngine), тем же исполнением, что CAP BREAKOUT.

BASELINE (ТЗ v2.1, doc/logika_torgovli_listingov_mexc_v2_1_revised.docx + doc/resheniya_po_voprosam.md):
  старт        T0 — первая сделка (OPEN_DETECTED записи); прогрев 500 мс; окно входа 8 с; варианты «окно 30 с»,
               «конкуренция 1 с»;
  фильтры      спред ≤ 1,2 %, depthBid0.7/1.5 ≥ 15× / 30× от bestBid, depthAsk0.7 ≥ 8× от bestAsk, exitVWAP и его
               проскальзывание ≤ 1,5 %, updatesPerSec ≥ 6 и bestUpdatesPerSec ≥ 3, стакан не старше 500 мс, tickPct < 0,3;
  BUY          LIMIT по bestAsk +1 % (не выше полосы биржи и потолка замороженного листинга); исполняется по asks при
               приходе (задержка 50 мс); ничего — ждёт до 1200 мс; частичное — остаток отменяется;
  позиция      MAIN_V21: TP +2 % (50 %) и +4 %, HARD_STOP −8 %, FAST_WINDOW_DUMP −6 % exitVWAP за 1 с, FAST_DROP −2,5 %
               в первые 3 с, BE +1,5 → +0,15 %, TIME_STOP 3 с / −0,3 %, NO_EDGE 5 с / 0,3 % + спред, NO_MOMENTUM 8 с,
               удержание 15 с; MAIN_NO_TIME_STOPS — для сравнения;
  выход        LIMIT по bestBid −0,5 %, 3 перестановки через 400 мс, затем продажа по bids до −20 %.
MAIN_EARLY (клиент 03.11, только REPLAY, только листинги без потолка) — сетка MAIN_EARLY_GRID ядра: окно 1/2/3/5 с,
  рост от первой сделки ≤ 20/40/60 %, импульс за 0/100/250/500 мс, глубина bids 2/5/15 × план, план 2/5/10 USDT,
  выход MAIN_V21 или MAIN_TP_TRAIL (TP1 +2 % на половину, остаток — trailing 5 % от пика exitVWAP, до 30 с).
  Решение — только по данным до его момента. Комбинации сортируются по итогу без лучшего листинга.
Комиссии — taker/maker инструмента из exchangeInfo записи (решения и итог по одной ставке). Пропуск: API пары закрыт
в первые 30 с (10007 в логе пробы) — отдельная группа. Реализованный PnL, непроданный остаток — отдельно.

Запуск на сервере (записи там):
  python3 tools/main_replay.py --all-usdt
  python3 tools/main_replay.py --all-usdt --no-early --sweep          # только BASELINE и смягчение по одному
  python3 tools/main_replay.py logs/rec_GANGUSDT_*.jsonl.gz --latency 20,50,100
"""

import argparse
import json
import re
import sys
import time
from argparse import Namespace
from collections import Counter, defaultdict
from pathlib import Path

from core.cap_breakout import (HOLD_MARKS_S, KNOWN_CAPS, LOGS, MAIN_EARLY_GRID, ORDER_USDT, MainEngine, Opts,
                               find_cap, no_entry_reason)
from core.feed import events_of, frame_meta, frame_time, iter_lines, line_t, ws_base_lag

PROFILE_LABEL = {"MAIN_V21": "стандартный", "MAIN_NO_TIME_STOPS": "без врем. стопов", "MAIN_TP_TRAIL": "TP1+trailing",
                 "MAIN_TP_TRAIL_NT": "TP1+trail, без вр.ст.", "MAIN_TP_FULL": "полный TP +2 %",
                 "MAIN_TRAIL_ALL": "trailing всей поз."}
RECON = "reconstructed"  # качество: интервал восстановлен по sendTime после остановки записи — не для выводов
SKIP_SYMBOLS = {"PFPUSDT": "делистинг, не листинг", "PALUSDT": "листинг отменён"}
GROUPS = {"no_cap": "Без потолка", "cap": "С потолком (замороженные)", "api": "API закрыт в начальном окне"}


def api_closed(symbol: str, within_ms: int = 30_000) -> bool:
    """Ордера пробы получали 10007 в первые within_ms после открытия — API пары закрыт на старте."""
    for p in sorted(LOGS.glob(f"probe_{symbol}_2*.jsonl")):
        with p.open(encoding="utf-8") as f:
            for line in f:
                if "ERR 10007" in line:
                    m = re.search(r'"rel_ms": (-?[0-9.]+)', line)
                    if m and 0 <= float(m.group(1)) <= within_ms:
                        return True
    return False


def load_open(path: Path, before_ms: int = 60_000, after_ms: int = 120_000):
    """События вокруг открытия. T0 — официальное время открытия (firstOpenTime в exchangeInfo записи), нет его —
    OPEN_DETECTED записи. Окно — от T0 − before_ms до T0 + after_ms; exchangeInfo — последнее до T0 + 10 с.
    Время WS-кадров — с поправкой на остановки процесса записи (core.feed.frame_time).
    Возвращает T0, события, exchangeInfo, сведения о времени."""
    detected, first_open = None, None
    for line in iter_lines(path):
        if '"ch":"exchangeInfo"' in line and '"firstOpenTime"' in line:
            syms = (json.loads(line)["data"] or {}).get("symbols") or []
            if syms and syms[0].get("firstOpenTime"):
                first_open = int(syms[0]["firstOpenTime"])
        elif '"OPEN_DETECTED"' in line:
            e = json.loads(line)
            detected = e["data"].get("open_ms") or e["t"]
            break
    if detected is None:
        return None, [], {}, {}
    t0 = first_open or detected
    lo, hi = min(t0, detected) - before_ms, max(t0, detected) + after_ms
    entries, info = [], {}
    for line in iter_lines(path):
        t = line_t(line)
        if t > hi + 10_000:  # кадры, задержанные остановкой записи, получены позже своего времени
            break
        if '"ch":"exchangeInfo"' in line:
            syms = (json.loads(line)["data"] or {}).get("symbols") or []
            if syms and t <= t0 + 10_000:
                info = syms[0]
        elif t < lo:
            continue
        elif ('"src":"rest","ch":"depth"' in line or '"publicAggreDepths"' in line
              or '"publicAggreDeals"' in line):
            entries.append(json.loads(line))
    if first_open and not info.get("firstOpenTime"):
        info = dict(info, firstOpenTime=first_open)
    base = ws_base_lag(entries)
    stalled = [e for e in entries if frame_time(e, base) != e["t"]]
    events = sorted((ev + frame_meta(e) for e in entries for ev in events_of(e, base) if ev[0] <= hi),
                    key=lambda ev: ev[0])
    spans = []  # интервалы восстановленных кадров (время модели); разрыв > 2 с — новый интервал
    for ft in sorted(frame_time(e, base) for e in stalled):
        if spans and ft - spans[-1][1] <= 2000:
            spans[-1][1] = ft
        else:
            spans.append([ft, ft])
    times = dict(exchange_open_ms=first_open, detected_rel_ms=detected - t0, stalled_frames=len(stalled),
                 stall_max_ms=max((e["t"] - frame_time(e, base) for e in stalled), default=0), recon_spans=spans)
    return t0, events, info, times


def trajectory(events: list, open_ms: int, ref: float | None = None) -> dict:
    """Ход цены сделок за первые 60 с от ref (первый действительный ask; нет — первая сделка): максимум (и когда),
    минимум, на 60-й секунде. Первая сделка бывает случайным принтом (EMYA: 0,03 при рынке 0,055)."""
    deals = [(t, d[0]) for t, kind, d, *_ in events if kind == "deal" and open_ms <= t <= open_ms + 60_000]
    if not deals:
        return {}
    first = ref or deals[0][1]
    t_max, p_max = max(deals, key=lambda x: x[1])
    at = lambda ms: next((100 * (p / first - 1) for t, p in reversed(deals) if t <= open_ms + ms), None)  # noqa: E731
    return dict(first=first, max_rise_pct=100 * (p_max / first - 1), max_at_s=(t_max - open_ms) / 1000,
                at1_pct=at(1000), at5_pct=at(5000), at60_pct=100 * (deals[-1][1] / first - 1),
                min_pct=100 * (min(p for _, p in deals) / first - 1))


def run_listing(path: Path, args) -> tuple[list[dict], dict]:
    symbol = path.name.split("_")[1]
    meta = dict(symbol=symbol, rec=path.name, status="", group=None, cap=None)
    if symbol in SKIP_SYMBOLS:
        meta["status"] = SKIP_SYMBOLS[symbol]
        return [], meta
    open_ms, events, info, times = load_open(path)
    meta["times"] = times
    if open_ms is None or not events:
        meta["status"] = "открытия в записи нет"
        return [], meta
    cap = find_cap(symbol) or ((KNOWN_CAPS[symbol], "заморозка") if symbol in KNOWN_CAPS else None)
    meta.update(traj=trajectory(events, open_ms), zones=", ".join(info.get("conceptPlates") or []),
                cap=cap[0] if cap else None, taker=info.get("takerCommission"), maker=info.get("makerCommission"))
    if api_closed(symbol):
        meta.update(status="SKIP_API_CLOSED", group="api")
        return [], meta
    meta.update(status="ok", group="cap" if cap else "no_cap")
    opts = Opts(fee=(args.fee or 0.08) / 100, fee_from_info=args.fee is None)
    engine = MainEngine(args, opts, open_ms, info, cap[0] if cap else None)
    for t, kind, data, recv, exch in events:
        engine.on_event(t, kind, data, recv, exch)
    engine.finish(events[-1][0])
    meta["traj"] = trajectory(events, open_ms, engine.book.first_ask)
    meta["times"].update(engine.times())
    spans = unreliable_spans(meta["times"], engine.book_gaps, open_ms)
    rows = engine.rows(symbol)
    hit = lambda lo, hi: any(a <= hi and b >= lo for a, b in spans)  # noqa: E731
    for r in rows:
        r["open_ms"] = open_ms
        # качество — по интервалу входа (T0 − 2 с … конец окна), одинаковому для всех выходов: исключение строки
        # не должно зависеть от того, сколько держали позицию; восстановленные кадры в удержании — отдельная пометка
        r["data_quality"] = RECON if hit(open_ms - MainEngine.PRE_MS, window_end(r, open_ms)) else "ok"
        if r.get("exit_ms") and r["data_quality"] == "ok" and hit(r["entry_ms"], r["exit_ms"]):
            r["exit_quality"] = RECON
    meta["data_quality"] = RECON if any(r["data_quality"] == RECON for r in rows) else "ok"
    return rows, meta


def unreliable_spans(times: dict, book_gaps: list, open_ms: int) -> list[list]:
    """Интервалы (время модели), где кадры восстановлены по sendTime после остановки записи; если за остановкой
    стакан стал недействителен (потеряны diff-ы, KAMIRAI) — до нового снимка. Выводы по ним не делаются."""
    spans = [list(s) for s in times.get("recon_spans") or []]
    for span in spans:
        for a, b in book_gaps:
            if span[0] - 500 <= a <= span[1] + 500 and b is not None:
                span[1] = max(span[1], b)
    times["unreliable_rel_ms"] = [[a - open_ms, b - open_ms] for a, b in spans]
    return spans


def window_end(r: dict, open_ms: int) -> int:
    """Конец окна входа ветки от T0 (MAIN_EARLY — W из метки, BASELINE — 8 с или 30 с)."""
    m = re.match(r"W(\d+) ", r.get("variant") or "")
    if m:
        return open_ms + int(m.group(1)) * 1000
    return open_ms + (30_000 if "30" in (r.get("variant") or "") else 8_000)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else 0.0


def short(sym: str) -> str:
    return sym.removesuffix("USDT")


def traj_text(meta: dict) -> str:
    tr = meta.get("traj") or {}
    return (f"сделки за 60 с от первого ask: макс {tr['max_rise_pct']:+.0f} % (на {tr['max_at_s']:.0f} с), "
            f"на 60-й с {tr['at60_pct']:+.0f} %"
            if tr else "")


def entry_text(r: dict) -> str:
    """Вход: время от открытия, рост до входа, спред и глубина в момент решения, итог."""
    rise, rise_ask = r.get("rise_from_first_pct"), r.get("rise_from_first_ask_pct")
    return (f"вход +{(r['entry_ms'] - r['open_ms']) / 1000:.1f} с"
            + (f", рост до входа {rise:+.0f} % от первой сделки" if rise is not None else "")
            + (f" / {rise_ask:+.0f} % от первого ask" if rise_ask is not None else "")
            + f", спред {r['decision_spread_pct']:.2f} %, bid0.7 {r['decision_bid07_usdt']:.0f} USDT, "
            f"куплено {r['buy_usdt']:.2f} → {r['pnl_pct']:+.2f} % ({r['reason']}, {r['hold_s']:.0f} с), "
            f"MFE {r['mfe']:+.1f} / MAE {r['mae']:+.1f}")


def print_baseline(rows: list[dict], metas: list[dict], latency: int, usdt: float) -> None:
    print(f"BASELINE (ТЗ v2.1, стандартный выход), {usdt:g} USDT, задержка {latency} мс; % — реализованный PnL позиции")
    for group, title in GROUPS.items():
        ms = [m for m in metas if m["group"] == group]
        print(f"\n{title}: {len(ms)}")
        for m in ms:
            if m["status"] != "ok":
                print(f"  {short(m['symbol']):<9} {m['status']}; {traj_text(m)}")
                continue
            r = next((r for r in rows if r["symbol"] == m["symbol"] and r["mode"] == "MAIN" and r["variant"] == "base"
                      and r["profile"] == "MAIN_V21" and r["latency"] == latency and r["usdt"] == usdt), None)
            cap = f" [потолок {m['cap']:g}]" if m["cap"] else ""
            if r is None or "entry_ms" not in r:
                sk = (r or {}).get("skips") or {}
                why = ", ".join(f"{k} {v}" for k, v in sorted(sk.items(), key=lambda x: -x[1])[:3]) or (r or {}).get("reason")
                print(f"  {short(m['symbol']):<9} нет входа ({why}){cap}; {traj_text(m)}")
            else:
                print(f"  {short(m['symbol']):<9} {entry_text(r)}{cap}; {traj_text(m)}")


def print_moments(metas: list[dict]) -> None:
    """Три момента листинга (клиент 04.11): стакан, первая сделка, первый исполнимый BUY — от T0 (официальное время)."""
    print("\nТри момента, мс от T0 (официальное время открытия); рост — к первому действительному ask:")
    for group in ("no_cap", "cap"):
        print(f"  {GROUPS[group]}:")
        for m in metas:
            tm = m.get("times") or {}
            if m["group"] != group or "first_book_rel_ms" not in tm:
                continue
            ask = tm.get("first_ask")
            rise = lambda px: f"{100 * (px / ask - 1):+.0f} %" if px and ask else "—"  # noqa: E731
            ex = tm.get("first_exec") or {}
            exec_txt = ", ".join(f"{u} USDT +{v['rel_ms']} ({rise(v['ask'])})" for u, v in ex.items()) or "не было"
            tr = m.get("traj") or {}
            path_txt = (f"; цена сделок к первому ask: 1 с {tr['at1_pct']:+.0f} %, 5 с {tr['at5_pct']:+.0f} %"
                        if tr.get("at1_pct") is not None and tr.get("at5_pct") is not None else "")
            un = tm.get("unreliable_rel_ms")
            stall = (f"; запись стояла до {tm['stall_max_ms']} мс — кадры восстановлены по sendTime, интервал "
                     + ", ".join(f"{a:+}…{b:+}" for a, b in un) + f" мс помечен {RECON}" if un else "")
            print(f"    {short(m['symbol']):<9} стакан +{tm['first_book_rel_ms']} (ask {ask:.6g}), первая сделка "
                  f"+{tm.get('first_trade_rel_ms')} ({rise(tm.get('first_trade'))}), BUY исполним целиком: "
                  f"{exec_txt}{path_txt}{stall}")


def evaluate(rs: list[dict]) -> dict:
    """Оценка набора сделок не только по итогу (клиент 05.11): худшая сделка, итог без лучшего листинга,
    исполненный объём."""
    by_sym = defaultdict(float)
    for r in rs:
        by_sym[r["symbol"]] += r["pnl_usdt"]
    total = sum(by_sym.values())
    best_sym = max(by_sym, key=by_sym.get) if by_sym else None
    return dict(n=len(rs), total=total, wins=sum(r["pnl_usdt"] > 0 for r in rs),
                worst_pct=min((r["pnl_pct"] for r in rs), default=0.0),
                worst_usdt=min((r["pnl_usdt"] for r in rs), default=0.0),
                best_sym=best_sym, wo_best=total - max(by_sym[best_sym], 0.0) if best_sym else 0.0,
                volume=sum(r["buy_usdt"] for r in rs), left=sum(r["left_usdt"] for r in rs))


def eval_text(rs_all: list[dict]) -> str:
    ok = [r for r in rs_all if "pnl_usdt" in r and r.get("data_quality") != RECON]
    rec = [r for r in rs_all if "pnl_usdt" in r and r.get("data_quality") == RECON]
    e = evaluate(ok)
    txt = (f"входов {e['n']}, {e['wins']}/{e['n'] - e['wins']}, итог {e['total']:+.2f} USDT, без лучшего листинга"
           f"{' (' + short(e['best_sym']) + ')' if e['best_sym'] else ''} {e['wo_best']:+.2f}, худшая "
           f"{e['worst_usdt']:+.2f} USDT ({e['worst_pct']:+.1f} %), куплено {e['volume']:.1f} USDT, "
           f"остаток {e['left']:.2f}")
    ex = sum(1 for r in ok if r.get("exit_quality") == RECON)
    if ex:
        txt += f"; в удержании были восстановленные кадры у {ex}"
    if rec:
        er = evaluate(rec)
        txt += (f"; отдельно, восстановленные данные: входов {er['n']}, итог {er['total']:+.2f} "
                f"({', '.join(sorted({short(r['symbol']) for r in rec}))})")
    return txt


def stat_line(rs_all: list[dict]) -> str:
    rs = [r for r in rs_all if "pnl_usdt" in r]
    if not rs:
        return f"входов 0 из {len(rs_all)}"
    total = sum(r["pnl_usdt"] for r in rs)
    wins = sum(r["pnl_usdt"] > 0 for r in rs)
    return (f"входов {len(rs)} из {len(rs_all)}, {wins}/{len(rs) - wins}, итог {total:+.2f} USDT, худший "
            f"{min(r['pnl_pct'] for r in rs):+.1f} %, MFE/MAE {mean([r['mfe'] for r in rs]):+.1f}/"
            f"{mean([r['mae'] for r in rs]):+.1f} %, куплено {sum(r['buy_usdt'] for r in rs):.1f}, "
            f"остаток {sum(r['left_usdt'] for r in rs):.2f}; вход ср. +{mean([(r['entry_ms'] - r['open_ms']) / 1000 for r in rs]):.1f} с, "
            f"рост до входа ср. {mean([r.get('rise_from_first_pct') for r in rs]):+.0f} %; если держать 10 с "
            f"{mean([r['hold_marks'].get(10) for r in rs]):+.1f} %"
            + (f"; из них на восстановленных данных: {', '.join(rec)}" if (rec := sorted(
                {short(r['symbol']) for r in rs if r.get('data_quality') == RECON})) else ""))


def print_baseline_summary(rows: list[dict], metas: list[dict], latency: int, usdt: float) -> None:
    print("\nИтог BASELINE по группам (вход по ТЗ v2.1):")
    variants = sorted({(r["variant"], r["profile"]) for r in rows if r["mode"] == "MAIN"},
                      key=lambda k: (k[0] != "base", k[0], k[1] != "MAIN_V21"))
    for group in ("no_cap", "cap"):
        syms = {m["symbol"] for m in metas if m["group"] == group}
        print(f"  {GROUPS[group]} ({len(syms)}):")
        for variant, prof in variants:
            rs = [r for r in rows if r["symbol"] in syms and r["mode"] == "MAIN" and r["variant"] == variant
                  and r["profile"] == prof and r["latency"] == latency and r["usdt"] == usdt]
            if rs:
                print(f"    {variant:<34} {PROFILE_LABEL.get(prof, prof):<16} {stat_line(rs)}")
        skips = Counter(no_entry_reason(r) for r in rows if r["symbol"] in syms and r["mode"] == "MAIN"
                        and r["variant"] == "base" and r["profile"] == "MAIN_V21" and "entry_ms" not in r
                        and r["latency"] == latency and r["usdt"] == usdt)
        if skips:
            print("    причины отказа: " + ", ".join(f"{k} {v}" for k, v in skips.most_common()))


def print_early(rows: list[dict], metas: list[dict], latency: int, top: int) -> None:
    syms = sorted({m["symbol"] for m in metas if m["group"] == "no_cap"})
    early = [r for r in rows if r["mode"] == "MAIN_EARLY" and r["latency"] == latency]
    if not early:
        return
    rec = [r for r in early if r.get("data_quality") == RECON]
    if rec:
        print(f"\nMAIN_EARLY: строки с восстановленными данными ({RECON}: "
              f"{', '.join(sorted({short(r['symbol']) for r in rec}))}) в рейтинг и выводы не входят; отдельно — "
              f"{len(rec)} строк, входов {sum('pnl_usdt' in r for r in rec)}, итог "
              f"{sum(r.get('pnl_usdt', 0.0) for r in rec):+.2f} USDT")
    early = [r for r in early if r.get("data_quality") != RECON]
    combos = defaultdict(list)
    for r in early:
        combos[(r["variant"], r["profile"], r["usdt"])].append(r)
    stats = []
    for key, rs_all in combos.items():
        rs = [r for r in rs_all if "pnl_usdt" in r]
        total = sum(r["pnl_usdt"] for r in rs)
        best = max((r["pnl_usdt"] for r in rs), default=0.0)
        stats.append((total - max(best, 0.0), total, key, rs, rs_all))
    stats.sort(key=lambda x: (-x[0], -x[1]))
    positive = sum(1 for s in stats if s[1] > 0)
    robust = sum(1 for s in stats if s[0] > 0)
    few = sum(1 for s in stats if len(s[3]) < 3)
    print(f"\nMAIN_EARLY — {len(stats)} комбинаций на {len(syms)} листингах без потолка: итог > 0 у {positive}, "
          f"итог без лучшего листинга > 0 у {robust}; меньше 3 входов у {few} (в рейтинг не идут). "
          f"Лучшие {top} по итогу без лучшего листинга:")
    stats = [s for s in stats if len(s[3]) >= 3]
    print(f"  {'комбинация':<22} {'выход':<21} {'план':>4}  входов  +/−   итог  без лучш.  остаток  худший %  "
          f"MFE / MAE %  вход с  рост до входа %  спред %  bid0.7 USDT  держать 10 с %")
    for robust_total, total, (variant, prof, usdt), rs, rs_all in stats[:top]:
        wins = sum(r["pnl_usdt"] > 0 for r in rs)
        print(f"  {variant:<22} {PROFILE_LABEL[prof]:<21} {usdt:>4g}  {len(rs):>2} из {len(rs_all):<2} {wins}/{len(rs) - wins}"
              f"  {total:>+5.2f}  {robust_total:>+8.2f}  {sum(r['left_usdt'] for r in rs):>7.2f}"
              f"  {min((r['pnl_pct'] for r in rs), default=0):>+8.1f}"
              f"  {mean([r['mfe'] for r in rs]):>+5.1f} / {mean([r['mae'] for r in rs]):>+5.1f}"
              f"  {mean([(r['entry_ms'] - r['open_ms']) / 1000 for r in rs]):>6.1f}"
              f"  {mean([r.get('rise_from_first_ask_pct') for r in rs]):>+15.0f}"
              f"  {mean([r['decision_spread_pct'] for r in rs]):>7.2f}"
              f"  {mean([r['decision_bid07_usdt'] for r in rs]):>11.0f}  {mean([r['hold_marks'].get(10) for r in rs]):>+14.1f}")
    print(f"\nВыходы на одних и тех же входах (все комбинации, по планам; без восстановленных данных):")
    for usdt in sorted({r["usdt"] for r in early}):
        for prof in PROFILE_LABEL:
            rs = [r for r in early if r["profile"] == prof and r["usdt"] == usdt]
            if rs:
                single = sum(1 for r in rs if r.get("single_tp"))
                print(f"  {usdt:>4g} USDT  {PROFILE_LABEL[prof]:<22} {eval_text(rs)}"
                      + (f"; единый TP у {single}" if single else ""))
    for robust_total, total, (variant, prof, usdt), rs, rs_all in stats[:1]:
        print(f"\nЛучшая комбинация {variant}, выход {PROFILE_LABEL[prof]}, {usdt:g} USDT — по листингам:")
        for r in sorted(rs_all, key=lambda r: r["symbol"]):
            if "entry_ms" in r:
                print(f"  {short(r['symbol']):<9} {entry_text(r)}")
            else:
                sk = r.get("skips") or {}
                why = ", ".join(f"{k} {v}" for k, v in sorted(sk.items(), key=lambda x: -x[1])[:3]) or r.get("reason")
                print(f"  {short(r['symbol']):<9} нет входа ({why})")
    reasons = Counter(no_entry_reason(r) for r in early if "entry_ms" not in r)
    print("\nMAIN_EARLY, причины отказа по всем комбинациям (первый отказавший фильтр): "
          + ", ".join(f"{k} {v}" for k, v in reasons.most_common()))


def usdt_records() -> list[Path]:
    """Последняя запись каждой пары к USDT, по дате записи."""
    latest = {}
    for p in sorted(LOGS.glob("rec_*USDT_2*.jsonl.gz")):
        latest[p.name.split("_")[1]] = p
    return sorted(latest.values(), key=lambda p: p.name.split("_", 2)[2])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="*", help="записи logs/rec_<SYMBOL>_*.jsonl.gz")
    p.add_argument("--all-usdt", action="store_true", help="все пары к USDT из logs/ (последняя запись пары)")
    p.add_argument("--latency", default="50", help="задержка ордера в мс, через запятую (50)")
    p.add_argument("--usdt", default=f"{ORDER_USDT:g}", help=f"сумма входа BASELINE, через запятую ({ORDER_USDT:g})")
    p.add_argument("--fee", type=float, help="своя комиссия taker, %% (по умолчанию — из exchangeInfo инструмента)")
    p.add_argument("--no-early", action="store_true", help="без сетки MAIN_EARLY")
    p.add_argument("--sweep", action="store_true", help="ещё MAIN_SWEEP: смягчение фильтров BASELINE по одному")
    p.add_argument("--top", type=int, default=15, help="сколько лучших комбинаций MAIN_EARLY показать (15)")
    p.add_argument("--json", help="сохранить строки результатов и сведения о листингах в JSON")
    a = p.parse_args()
    args = Namespace(latency=[int(x) for x in a.latency.split(",")], usdt=[float(x) for x in a.usdt.split(",")],
                     fee=a.fee, sweep=a.sweep, main_early=None if a.no_early else MAIN_EARLY_GRID)
    files = [Path(f) for f in a.files] + (usdt_records() if a.all_usdt else [])
    if not files:
        p.error("нужны записи или --all-usdt")
    started = time.monotonic()
    rows, metas = [], []
    for f in files:
        listing_rows, meta = run_listing(f, args)
        rows += listing_rows
        metas.append(meta)
    skipped = [m for m in metas if m["group"] is None]
    if skipped:
        print("Не листинги или без открытия: " + ", ".join(f"{short(m['symbol'])} ({m['status']})" for m in skipped) + "\n")
    print_baseline(rows, metas, args.latency[0], args.usdt[0])
    print_baseline_summary(rows, metas, args.latency[0], args.usdt[0])
    print_moments(metas)
    print_early(rows, metas, args.latency[0], a.top)
    print(f"\n({len(files)} записей, {time.monotonic() - started:.0f} с)", file=sys.stderr)
    if a.json:
        Path(a.json).write_text(json.dumps({"rows": rows, "metas": metas}, ensure_ascii=False, default=str),
                                encoding="utf-8")


if __name__ == "__main__":
    main()
