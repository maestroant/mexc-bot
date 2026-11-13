"""REPLAY стратегии CAP BREAKOUT по записям listing_recorder (ТЗ v2 + решения клиента, doc/cap_breakout_resheniya.md).

Для каждой записи замороженного листинга:
  потолок      из ответа 30010 в логе пробы (logs/probe_<SYMBOL>_*.jsonl) или --cap SYMBOL=цена;
  снятие       первая сделка выше потолка, затем bid локального стакана выше потолка;
  вход         FAST (окно 10 с, без стабилизации) и CONFIRMED (окно 30 с, фильтры держатся ≥200 мс и ≥3 версии стакана);
               фильтры §4.3: спред, depthBid0.7/1.5, depthAsk0.7, проскальзывание выхода, tickPct, entryStressLoss
               (по buyVWAP с резервом 0,5 % или — для сравнения — по bestAsk +1 %); по желанию — фильтр падения
               bid-VWAP перед покупкой; LIMIT BUY по bestAsk +1 % исполняется по asks в момент прихода ордера;
               EARLY — эксперимент клиента 02.11 (только REPLAY): окно FAST с ослабленными фильтрами EARLY_FILTERS,
               LIMIT BUY до bestAsk +5 %, позиция — то, что исполнилось (частично тоже), при неисполнении — повтор;
               цена BUY любого режима — не выше последней сделки × (1 + bidMultiplierUp) (PERCENT_PRICE_BY_SIDE);
               EARLY идёт с планом 2 / 5 / 10 USDT и с конкуренцией за asks: base — наш BUY получает их первым,
               «конкуренция 200 мс / 1 с» — объём, ушедший с уровня за это время после прихода BUY, достался другим,
               «доля 50 %» — нам половина каждого уровня; позиция — только исполненное;
               --early-sweep — тот же вход с изменением одного фактора (спред, глубина, проскальзывание, размер, задержка);
  позиция      профили CAP_DYNAMIC (свой для FAST и CONFIRMED, EARLY — как FAST), CAP_NO_TIME_STOPS (тот же без
               TIME_STOP_*) и LEGACY_STOPS (старые стопы, TP +2/+4 %, удержание 15 с); TP — LIMIT в очереди своего уровня;
               у EARLY ещё EARLY_EXITS (клиент 02.11): окно 5 / 10 с без ценовых стопов (ликвидность — только
               диагностика), затем A — продать всё, или B — trailing по пику exitVWAP (MFE +10 %, откат 15 / 20 %,
               удержание до 30 с) вместе с аварийными стопами и HARD_STOP профиля FAST;
  выход        отмена TP, затем LIMIT SELL по bestBid −0,5 %, до 3 перестановок через 400 мс, затем продажа по bids
               до −20 % (предел PERCENT_PRICE_BY_SIDE; веб-fallback не моделируется), непроданное — остаток.
Локальный стакан: REST-снимок (lastUpdateId) + поток aggre.depth с проверкой fromVersion/toVersion;
при разрыве версий стакан недействителен до следующего снимка (запись делает их раз в секунду).

Исполнение: количество округляется вниз до шага пары, ордер меньше 1 USDT не ставится (остаток — пыль);
TP исполняется сделкой выше его цены целиком, сделкой по его цене — после очереди, стоявшей на уровне при постановке;
комиссии — taker и maker инструмента из exchangeInfo записи (takerCommission/makerCommission; на листинговых парах
0,08 % и 0, списываются в USDT); --fee — своя ставка taker, --fee-in-coin — прежняя модель с BUY-комиссией в монете. Наш ордер не меняет стакан.

Запуск на сервере (записи лежат там):
  python3 tools/cap_replay.py logs/rec_LALAUSDT_*.jsonl.gz --latency 20,50,100,200
  python3 tools/cap_replay.py logs/rec_CHOMPUSDT_*.jsonl.gz --depth-persist-ms 2000 --summary-only
  python3 tools/cap_replay.py --all-frozen --modes EARLY --early-sweep --summary-only
  python3 tools/cap_replay.py --all-frozen --report --send     # сводка в Telegram (наблюдатель — раз в две недели)
"""

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

from core.cap_breakout import (EARLY_COMPETE, EARLY_EXITS, EARLY_FILTERS, EARLY_USDT, ENTRY, HOLD_MARKS_S,
                               KNOWN_CAPS, LOGS, ORDER_USDT, TAKER_FEE, CapEngine, Opts, find_cap, market_params,
                               no_entry_reason)
from core.feed import events_of, iter_lines, line_t

ROOT = Path(__file__).resolve().parent.parent

# --- данные ---------------------------------------------------------------------

def frozen_records() -> list[Path]:
    """Последняя запись каждого замороженного листинга (30010 в логе пробы или KNOWN_CAPS), по дате записи."""
    latest = {}
    for p in sorted(LOGS.glob("rec_*_2*.jsonl.gz")):
        latest[p.name.split("_")[1]] = p
    found = [p for sym, p in latest.items() if sym in KNOWN_CAPS or find_cap(sym)]
    return sorted(found, key=lambda p: p.name.split("_", 2)[2])


CAP_AFTER_MS = 170_000  # ветки ведутся до снятия + 170 с — как PAPER --after-release
RELEASE_WAIT_MS = 30 * 60_000  # снятия ждём до T0 + 30 мин — как PAPER --release-wait


def open_time(path: Path) -> int | None:
    """T0 по единой модели ядра (как MainEngine в PAPER): firstOpenTime из exchangeInfo, полученного до открытия,
    иначе OPEN_DETECTED записи."""
    first_open = None
    for line in iter_lines(path):
        if '"ch":"exchangeInfo"' in line and '"firstOpenTime"' in line:
            syms = (json.loads(line)["data"] or {}).get("symbols") or []
            if syms and syms[0].get("firstOpenTime"):
                first_open = int(syms[0]["firstOpenTime"])
        elif '"OPEN_DETECTED"' in line:
            e = json.loads(line)
            return first_open or e["data"].get("open_ms") or e["t"]
    return first_open


def stream(path: Path, open_ms: int):
    """Поток записи (или журнала PAPER A) в порядке получения, как его видел бы PAPER: ("info", t, exchangeInfo)
    и события движка (t, kind, data) начиная с T0. Без сортировки и без знания будущего."""
    for line in iter_lines(path):
        if '"ch":"exchangeInfo"' in line:
            syms = (json.loads(line)["data"] or {}).get("symbols") or []
            if syms:
                yield "info", line_t(line), syms[0]
            continue
        if line_t(line) < open_ms:
            continue
        if ('"src":"rest","ch":"depth"' in line or '"publicAggreDepths"' in line
                or '"publicAggreDeals"' in line):
            yield from events_of(json.loads(line))


def run_causal(path: Path, cfg, opts: Opts, cap: float, cut_ms: int | None = None, on_event=None):
    """CAP BREAKOUT REPLAY без заглядывания в будущее (решение клиента 11.11): движок с T0, первая сделка выше
    потолка и снятие находятся по потоку в порядке получения — как в PAPER; exchangeInfo применяется, пока потолок
    не снят; ветки — до снятия + CAP_AFTER_MS, без снятия — до T0 + RELEASE_WAIT_MS. cut_ms — оборвать поток на
    этом времени без завершения веток (проверка независимости от будущего); on_event(i, t, kind, engine) — после
    каждого события. Возвращает (движок, T0)."""
    open_ms = open_time(path)
    if open_ms is None:
        return None, None
    eng = CapEngine(cfg, opts, cap, open_ms, {})
    last, i = None, 0
    for item in stream(path, open_ms):
        if item[0] == "info":
            if eng.release is None:
                eng.info = item[2]
            continue
        t, kind, data = item
        if cut_ms is not None and t > cut_ms:
            return eng, open_ms
        if eng.release is not None and t > eng.release + CAP_AFTER_MS:
            break
        if eng.release is None and t > open_ms + RELEASE_WAIT_MS:
            break
        eng.on_event(t, kind, data)
        if on_event:
            on_event(i, t, kind, eng)
        last, i = t, i + 1
    if eng.release is not None and cut_ms is None:
        eng.finish(min(eng.release + CAP_AFTER_MS, last))
    return eng, open_ms


# --- прогон -------------------------------------------------------------------

def run_listing(path: Path, args, caps: dict) -> tuple[list[dict], dict]:
    """Прогон одной записи: строки результатов по веткам и сведения о листинге (потолок, снятие)."""
    symbol = path.name.split("_")[1]
    meta = dict(symbol=symbol, status="no_cap", zones="", cap=None, src=None, release_ms=None, open_ms=None,
                rec=path.name)
    cap_src = caps.get(symbol) or find_cap(symbol) or (
        (KNOWN_CAPS[symbol], "заморозка в записи") if symbol in KNOWN_CAPS else None)
    if not cap_src:
        if not args.summary_only:
            print(f"{symbol}: нет потолка (ни 30010 в логе пробы, ни --cap) — пропуск")
        return [], meta
    cap, src = cap_src
    meta.update(status="not_released", cap=cap, src=src)
    opts = Opts(args.stops_basis == "stressed", args.depth_band / 100, args.depth_persist_ms, args.entry_stress,
                args.entry_basis, args.pre_buy_drop, (args.fee or 100 * TAKER_FEE) / 100, args.fee_in_coin,
                fee_from_info=args.fee is None)
    engine, open_ms = run_causal(path, args, opts, cap)
    if engine is None or engine.cand_ms is None:
        if not args.summary_only:
            print(f"{symbol}: потолок {cap}, но сделок выше него в записи не было — пропуск")
        return [], meta
    cand_ms = engine.cand_ms
    step, qty_step, _, zones = market_params(engine.info)
    release, book, sims = engine.release, engine.book, engine.sims
    rows = engine.rows(symbol)
    meta.update(zones=zones, open_ms=open_ms, release_ms=release, status="released" if release else "not_released")
    if not args.summary_only:
        head = (f"\n=== {symbol} [{zones}] потолок {cap} ({src}), первая сделка выше потолка "
                f"T+{(cand_ms - open_ms) / 1000:.3f} с, CAP_RELEASED T+{(release - open_ms) / 1000:.3f} с"
                if release else f"\n=== {symbol}: bid так и не поднялся выше потолка")
        print(head + f"; шаг цены {step:g}, шаг кол-ва {qty_step:g}, разрывов версий стакана: {book.gaps}")
    for s, r in zip(sims, rows):
        if args.summary_only:
            continue
        tag = f"  {s.mode:<9} {s.variant:<19} {s.profile.name:<17} {s.latency:>3} мс {s.usdt:>3g} USDT:"
        if "entry_ms" not in r:
            top = sorted(r.get("skips", {}).items(), key=lambda x: -x[1])[:3]
            misses = f", ордер не исполнился {r['buy_misses']} раз" if r.get("buy_misses") else ""
            print(f"{tag} нет входа ({r.get('reason')}; отказы: {', '.join(f'{k} {v}' for k, v in top)}{misses})")
            continue
        drop = r.get("drop_1s_at_decision")
        gap = r.get("exit_gap_pct")
        fill = f", исполнено {r['fill_pct']:.0f} % плана" if r["fill_pct"] < 99.9 else ""
        print(f"{tag} вход +{(r['entry_ms'] - release) / 1000:.2f} с по {r['entry_px']:.6g} "
              f"({r['entry_px'] / cap:.2f}× потолок, к ask {r['slippage_pct']:+.2f} %{fill}"
              + (f", bid-VWAP за 1 с до решения {drop:+.1f} %" if drop is not None else "")
              + f") → {r['reason']} через {r['hold_s']:.1f} с: {r['pnl_pct']:+.2f} % ({r['pnl_usdt']:+.3f} USDT), "
              f"MFE {r['mfe']:+.1f} / MAE {r['mae']:+.1f} %, TP-исполнений {r['tp_fills']}, "
              f"выход: попыток {r['exit_rounds']}{', до −20 %' if r['critical'] else ''}"
              + (f", к exitVWAP {gap:+.2f} %" if gap is not None else "")
              + f", остаток {r['left_usdt']:.3f} USDT, комиссии {r['fees_usdt']:.4f}; "
              f"глубина min ×{r['min_depth07_x']:.0f} (0,7 %) / ×{r['min_depth15_x']:.0f} (1,5 %); если держать: "
              + " ".join(f"+{sec} с {v:+.1f} %" if v is not None else f"+{sec} с —"
                         for sec, v in r["hold_marks"].items()) + f"  [{r['detail']}]")
    return rows, meta


PROFILE_LABEL = {"CAP_DYNAMIC": "CAP_DYNAMIC", "CAP_NO_TIME_STOPS": "без врем. стопов", "LEGACY_STOPS": "LEGACY",
                 **{p.name: (f"время {p.grace_ms // 1000} с" if p.time_exit else
                             f"окно {p.grace_ms // 1000} с + trailing {p.peak_pp} %") for p in EARLY_EXITS}}
EARLY_REF = "EARLY_TIME_10S"  # профиль EARLY для строк по листингам и по размеру


def base_rows(rows: list[dict], latency: int, usdt: float, variant: str = "base") -> list[dict]:
    """Ветки FAST/CONFIRMED и EARLY варианта variant с заданными задержкой и суммой входа."""
    return [r for r in rows if r["variant"] in ("", variant) and r["latency"] == latency and r["usdt"] == usdt]


def mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def hold_mean(rs: list[dict], sec: int) -> float:
    """Средний netPnL % «если держать sec секунд» — по входам, где стакан тогда выдерживал продажу."""
    return mean([v for r in rs if (v := r["hold_marks"].get(sec)) is not None])


def hold_usdt(rs: list[dict], sec: int) -> float:
    """Условный PnL удержания в USDT: вся купленная позиция продана по bids через sec секунд после входа."""
    return sum(r["buy_usdt"] * v / 100 for r in rs if (v := r["hold_marks"].get(sec)) is not None)


def prof_line(rs: list[dict], label: str) -> str:
    """Итог профиля: реализованный PnL отдельно от непроданного остатка."""
    total = sum(r["pnl_usdt"] for r in rs)
    wins = sum(r["pnl_usdt"] > 0 for r in rs)
    return (f"  {label}: {total:+.2f} USDT ({wins}/{len(rs) - wins}), худший {min(r['pnl_pct'] for r in rs):+.1f} %, "
            f"остаток {sum(r['left_usdt'] for r in rs):.2f}; MFE/MAE {mean([r['mfe'] for r in rs]):+.1f}/"
            f"{mean([r['mae'] for r in rs]):+.1f} %, удержание {mean([r['hold_s'] for r in rs]):.0f} с")


def bought_line(main: list[dict]) -> str:
    """Купленный объём и причины, по которым входа или исполнения не было (по ветке CAP_DYNAMIC каждого листинга)."""
    entered = [r for r in main if "entry_ms" in r]
    misses = sum(r.get("buy_misses") or 0 for r in main)
    skips = Counter(no_entry_reason(r) for r in main if "entry_ms" not in r)
    line = f"входов {len(entered)} из {len(main)}"
    if entered:
        line += (f", куплено {sum(r['buy_usdt'] for r in entered):.1f} USDT "
                 f"(ср. {mean([r['fill_pct'] for r in entered]):.0f} % плана)")
    if misses:
        line += f"; BUY без исполнения {misses} раз"
    if skips:
        line += "; нет входа: " + ", ".join(f"{k} {v}" for k, v in skips.most_common())
    return line


def early_compact(rs: list[dict]) -> str:
    """Итоги профилей EARLY_EXITS одной строкой: реализованный PnL, USDT."""
    parts = []
    for p in EARLY_EXITS:
        prs = [r for r in rs if r["profile"] == p.name and "pnl_usdt" in r]
        parts.append(f"{PROFILE_LABEL[p.name]} {sum(r['pnl_usdt'] for r in prs):+.2f}")
    return "; ".join(parts)


def brief(meta: dict, rows: list[dict], latency: int, usdt: float) -> str:
    """Короткий итог одного листинга для Telegram (наблюдатель шлёт его после записи замороженного листинга)."""
    head = f"📊 REPLAY CAP BREAKOUT — {meta['symbol']}" + (f" [{meta['zones']}]" if meta["zones"] else "")
    if meta["status"] == "no_cap":
        return f"{head}\nПотолка нет (30010 не было) — листинг не замороженный"
    if meta["status"] == "not_released":
        return (f"{head}\nПотолок {meta['cap']} ({meta['src']}), подтверждённого снятия нет: сделок выше потолка "
                "в записи не было — торгового входа нет")
    lines = [head, f"Потолок {meta['cap']} ({meta['src']}), снят на T+{(meta['release_ms'] - meta['open_ms']) / 1000:.0f} с"]
    base = base_rows(rows, latency, usdt)
    for mode in ENTRY:
        rs = [r for r in base if r["mode"] == mode]
        if not rs:
            continue
        r0 = rs[0]
        label = mode + (f" (эксперимент, план {usdt:g} USDT, наш BUY первым)" if mode == "EARLY" else "")
        if "entry_ms" not in r0:
            misses = f", BUY без исполнения {r0['buy_misses']} раз" if r0.get("buy_misses") else ""
            lines.append(f"{label}: нет входа ({no_entry_reason(r0)}{misses})")
        else:
            lines.append(f"{label}: вход через {(r0['entry_ms'] - meta['release_ms']) / 1000:.1f} с "
                         f"по {r0['entry_px'] / meta['cap']:.2f}× потолка, куплено {r0['buy_usdt']:.2f} USDT")
            for r in rs:
                if r["profile"] in PROFILE_LABEL and (mode != "EARLY" or r["profile"] == "CAP_DYNAMIC"):
                    left = f", остаток {r['left_usdt']:.2f} USDT" if r["left_usdt"] >= 0.01 else ""
                    lines.append(f"  {PROFILE_LABEL[r['profile']]}: {r['pnl_pct']:+.2f} % "
                                 f"({r['reason']}, {r['hold_s']:.0f} с{left})")
            if mode == "EARLY":
                lines.append("  выходы EARLY, USDT: " + early_compact(rs))
            lines.append("  если держать: " + ", ".join(f"{sec} с {v:+.1f} %" for sec, v in r0["hold_marks"].items()
                                                        if v is not None))
        if mode == "EARLY":
            for variant in EARLY_COMPETE:
                cr = [r for r in base_rows(rows, latency, usdt, variant) if r["mode"] == mode
                      and r["profile"] == EARLY_REF]
                if cr:
                    c0 = cr[0]
                    got = (f"куплено {c0['buy_usdt']:.2f} USDT, время 10 с {c0['pnl_usdt']:+.2f} USDT"
                           if "entry_ms" in c0 else f"нет входа ({no_entry_reason(c0)})")
                    lines.append(f"  {variant}: {got}")
    lines.append(f"{usdt:g} USDT на вход, задержка {latency} мс; модель упрощённая — см. doc/cap_breakout_resheniya.md")
    return "\n".join(lines)


def report(rows: list[dict], metas: list[dict], latency: int, usdt: float, fee: float, days: int) -> list[str]:
    """Сводка REPLAY по всем замороженным листингам — три сообщения Telegram: FAST и CONFIRMED, EARLY, по листингам."""
    short = lambda s: s.removesuffix("USDT")  # noqa: E731
    since = datetime.now().timestamp() - days * 86_400
    new = [m["symbol"] for m in metas
           if datetime.strptime("_".join(m["rec"].split("_")[2:4])[:15], "%Y%m%d_%H%M%S").timestamp() >= since]
    released = [m for m in metas if m["status"] == "released"]
    unreleased = [short(m["symbol"]) for m in metas if m["status"] != "released"]
    head = [f"📊 CAP BREAKOUT — сводный REPLAY на {datetime.now():%d.%m.%Y}",
            f"Замороженных листингов: {len(metas)} — со снятием и пробоем потолка {len(released)}"
            + (f", без подтверждённого снятия и без входа {len(unreleased)} ({', '.join(unreleased)})"
               if unreleased else "") + f"; новые за {days} дн.: {', '.join(map(short, new)) or 'нет'}",
            f"{usdt:g} USDT на вход, задержка {latency} мс, taker {100 * fee:g} % в USDT. Итог — реализованный PnL; "
            "остаток — непроданное по bid, в итог не входит; «если держать» — условный PnL удержания."]
    base = base_rows(rows, latency, usdt)
    for mode in ("FAST", "CONFIRMED"):
        rs_mode = [r for r in base if r["mode"] == mode]
        if not rs_mode:
            continue
        main = [r for r in rs_mode if r["profile"] == "CAP_DYNAMIC"]
        entered = [r for r in main if "entry_ms" in r]
        head.append(f"\n{mode}: {bought_line(main)}")
        for prof, label in PROFILE_LABEL.items():
            rs = [r for r in rs_mode if r["profile"] == prof and "pnl_usdt" in r]
            if rs:
                head.append(prof_line(rs, label))
        if entered:
            gaps = [r["exit_gap_pct"] for r in rs_mode if r.get("exit_gap_pct") is not None]
            head.append(f"  проскальзывание: вход {mean([r['slippage_pct'] for r in entered]):+.2f} % к ask решения"
                        + (f", выход {mean(gaps):+.2f} % к exitVWAP сигнала" if gaps else ""))
            head.append("  если держать всю позицию: " + ", ".join(
                f"{sec} с {hold_usdt(entered, sec):+.2f} USDT ({hold_mean(entered, sec):+.1f} %)" for sec in (10, 60)))
    early = [f"EARLY — эксперимент, только REPLAY; план {usdt:g} USDT, задержка {latency} мс"]
    rs_mode = [r for r in base if r["mode"] == "EARLY"]
    if rs_mode:
        main = [r for r in rs_mode if r["profile"] == "CAP_DYNAMIC"]
        entered = [r for r in main if "entry_ms" in r]
        early.append(f"Наш BUY получает asks первым (верхняя оценка): {bought_line(main)}")
        for prof, label in PROFILE_LABEL.items():
            rs = [r for r in rs_mode if r["profile"] == prof and "pnl_usdt" in r]
            if rs:
                early.append(prof_line(rs, label))
        if entered:
            early.append("  если держать всю позицию: " + ", ".join(
                f"{sec} с {hold_usdt(entered, sec):+.2f} USDT" for sec in (10, 60)))
        for variant in EARLY_COMPETE:
            vr = [r for r in base_rows(rows, latency, usdt, variant) if r["mode"] == "EARLY"]
            vmain = [r for r in vr if r["profile"] == "CAP_DYNAMIC"]
            if vmain:
                early.append(f"\n{variant}: {bought_line(vmain)}")
                early.append("  выходы, USDT: " + early_compact(vr))
        sizes = sorted({r["usdt"] for r in rows if r["mode"] == "EARLY" and r["variant"] == "base"})
        parts = []
        for size in sizes:
            sr = [r for r in base_rows(rows, latency, size) if r["mode"] == "EARLY" and r["profile"] == EARLY_REF]
            got = [r for r in sr if "entry_ms" in r]
            parts.append(f"{size:g} USDT — входов {len(got)}, куплено {sum(r['buy_usdt'] for r in got):.1f}, "
                         f"итог {sum(r['pnl_usdt'] for r in got):+.2f}")
        early.append(f"\nПо размеру плана ({PROFILE_LABEL[EARLY_REF]}, наш BUY первым): " + "; ".join(parts))
    per = [f"По листингам — % позиции: FAST и CONFIRMED с CAP_DYNAMIC, EARLY с выходом «{PROFILE_LABEL[EARLY_REF]}» "
           "(наш BUY первым); — нет входа:"]
    for m in metas[-20:]:
        if m["status"] != "released":
            per.append(f"  {short(m['symbol'])}: без подтверждённого снятия потолка — торгового входа нет")
            continue
        parts = []
        for mode in ENTRY:
            prof = EARLY_REF if mode == "EARLY" else "CAP_DYNAMIC"
            r = next((r for r in base if r["symbol"] == m["symbol"] and r["mode"] == mode and r["profile"] == prof),
                     None)
            if r is not None:
                parts.append(f"{mode} {r['pnl_pct']:+.1f}" if "pnl_pct" in r else f"{mode} — ({no_entry_reason(r)})")
        per.append(f"  {short(m['symbol'])}: " + ", ".join(parts))
    return ["\n".join(head), "\n".join(early), "\n".join(per)]


def summary(rows: list[dict]) -> None:
    print("\nИтого (листинги без входа в сумму не входят; остаток — непроданное по bid, в сумму не входит):")
    print(f"  {'режим':<9} {'вариант':<19} {'профиль':<19} {'мс':>3} {'USDT':>4}  входов  +/−   сумма USDT  средний %"
          f"  худший %  MFE ср. %  MAE ср. %  удерж. с  к ask %  куплено USDT  исполн. %  остаток USDT  "
          + "  ".join(f"держать {sec} с" for sec in HOLD_MARKS_S))
    order = {m: i for i, m in enumerate(ENTRY)}
    keys = sorted({(r["mode"], r["variant"], r["profile"], r["latency"], r["usdt"]) for r in rows},
                  key=lambda k: (order[k[0]], k[1] != "base", k[1], k[2], k[3], k[4]))
    for k in keys:
        tag = f"  {k[0]:<9} {k[1]:<19} {k[2]:<19} {k[3]:>3} {k[4]:>4g}"
        rs = [r for r in rows if (r["mode"], r["variant"], r["profile"], r["latency"], r["usdt"]) == k
              and "pnl_usdt" in r]
        if not rs:
            print(f"{tag}  {0:>6}")
            continue
        total = sum(r["pnl_usdt"] for r in rs)
        wins, losses = sum(r["pnl_usdt"] > 0 for r in rs), sum(r["pnl_usdt"] <= 0 for r in rs)
        bought = sum(r["buy_usdt"] for r in rs)
        print(f"{tag}  {len(rs):>6}  {wins}/{losses}  {total:>+10.3f}  {100 * total / bought:>+9.2f}"
              f"  {min(r['pnl_pct'] for r in rs):>+8.2f}  {mean([r['mfe'] for r in rs]):>+9.2f}"
              f"  {mean([r['mae'] for r in rs]):>+9.2f}  {mean([r['hold_s'] for r in rs]):>8.1f}"
              f"  {mean([r['slippage_pct'] for r in rs]):>+7.2f}  {bought:>12.2f}  {mean([r['fill_pct'] for r in rs]):>9.0f}"
              f"  {sum(r['left_usdt'] for r in rs):>12.3f}  "
              + "  ".join(f"{hold_mean(rs, sec):>+{10 + len(str(sec))}.2f}" for sec in HOLD_MARKS_S))


def parse_early(text: str) -> dict:
    """--early max_spread=20,min_fill=0,ask07=off → изменения EARLY_FILTERS (off — фильтр выключен)."""
    out = {}
    for item in filter(None, text.split(",")):
        key, val = item.split("=")
        if key not in EARLY_FILTERS:
            raise SystemExit(f"--early: нет фильтра {key}; есть {', '.join(EARLY_FILTERS)}")
        out[key] = None if val == "off" else float(val)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="*", help="записи logs/rec_<SYMBOL>_*.jsonl.gz")
    p.add_argument("--all-frozen", action="store_true", help="все замороженные листинги из logs/ (последняя запись пары)")
    p.add_argument("--cap", action="append", default=[], help="потолок без 30010 в логе: SYMBOL=цена (можно несколько)")
    p.add_argument("--latency", default="50", help="задержка ордера в мс, через запятую (50)")
    p.add_argument("--usdt", default=f"{ORDER_USDT:g}", help=f"сумма входа в USDT, через запятую ({ORDER_USDT:g})")
    p.add_argument("--early-usdt", default=",".join(f"{x:g}" for x in EARLY_USDT),
                   help=f"плановый вход EARLY в USDT, через запятую ({','.join(f'{x:g}' for x in EARLY_USDT)})")
    p.add_argument("--modes", default="FAST,CONFIRMED,EARLY")
    p.add_argument("--early", type=parse_early, default={},
                   help="изменить фильтры EARLY: max_spread=20,min_fill=0,... (off — выключить фильтр)")
    p.add_argument("--early-sweep", action="store_true",
                   help="EARLY с изменением одного фактора за раз (EARLY_SWEEP), только профиль CAP_DYNAMIC")
    p.add_argument("--fee", type=float, help="комиссия taker, %% (по умолчанию — takerCommission/makerCommission "
                                                 "инструмента из exchangeInfo записи)")
    p.add_argument("--fee-in-coin", action="store_true", help="BUY-комиссия в купленной монете (прежняя модель)")
    p.add_argument("--stops-basis", choices=("plain", "stressed"), default="plain",
                   help="стопы CAP_DYNAMIC по netPnL без резерва проскальзывания (plain) или с ним (stressed)")
    p.add_argument("--depth-band", type=float, default=1.5, help="DEPTH_DISAPPEARED: глубина bids в пределах N %% (1.5)")
    p.add_argument("--depth-persist-ms", type=int, default=1000,
                   help="DEPTH_DISAPPEARED: глубина ниже порога непрерывно N мс (1000)")
    p.add_argument("--entry-stress", type=float, default=0.5, help="резерв в фильтре entryStressLoss, %% (0.5)")
    p.add_argument("--entry-basis", choices=("vwap", "max"), default="vwap",
                   help="entryStressLoss от ожидаемой цены покупки (vwap) или от максимальной bestAsk +1 %% (max)")
    p.add_argument("--pre-buy-drop", action="store_true",
                   help="не покупать, если bid-VWAP за 1 с упал на порог «падения bid-VWAP» профиля (FAST −4, CONFIRMED −3)")
    p.add_argument("--summary-only", action="store_true", help="печатать только итог")
    p.add_argument("--brief", action="store_true", help="только короткий итог по каждому листингу (для Telegram)")
    p.add_argument("--report", action="store_true", help="сводка по всем листингам (для Telegram)")
    p.add_argument("--report-days", type=int, default=14, help="«новые» листинги в сводке — за N дней (14)")
    p.add_argument("--send", action="store_true", help="отправить --brief / --report в Telegram (TG_CHAT_ID)")
    p.add_argument("--json", help="сохранить строки результатов в JSON")
    args = p.parse_args()
    args.latency = [int(x) for x in args.latency.split(",")]
    args.usdt = [float(x) for x in args.usdt.split(",")]
    args.early_usdt = [float(x) for x in args.early_usdt.split(",")]
    args.modes = args.modes.split(",")
    caps = {}
    for c in args.cap:
        sym, val = c.split("=")
        caps[sym.upper()] = (float(val), "--cap")
    files = [Path(f) for f in args.files] + (frozen_records() if args.all_frozen else [])
    if not files:
        p.error("нужны записи или --all-frozen")
    args.summary_only |= args.brief or args.report
    started = time.monotonic()
    rows, metas, texts = [], [], []
    for f in files:
        listing_rows, meta = run_listing(f, args, caps)
        rows += listing_rows
        metas.append(meta)
        if args.brief:
            texts.append(brief(meta, listing_rows, args.latency[0], args.usdt[0]))
    if args.report:
        texts += report(rows, metas, args.latency[0], args.usdt[0], (args.fee or 100 * TAKER_FEE) / 100,
                        args.report_days)
    if texts:
        print("\n\n".join(texts))
    else:
        from core.data_marks import excluded, mark_line
        for sym in sorted({r["symbol"] for r in rows if excluded(r["symbol"])}):
            print(mark_line(sym))  # строки остаются в --json и в регрессии; в итог не входят
        summary([r for r in rows if not excluded(r["symbol"])])
    print(f"\n({len(files)} записей, {time.monotonic() - started:.0f} с)", file=sys.stderr)
    if args.json:
        Path(args.json).write_text(json.dumps(rows, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    if args.send and texts:
        from notify import notify
        from probe_listing import load_dotenv
        load_dotenv(ROOT / ".env")
        for text in texts:
            notify(text)


if __name__ == "__main__":
    main()
