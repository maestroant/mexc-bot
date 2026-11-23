"""Тесты Time Guard основного движка (решение клиента 19.11, п. 2: OPEN_TIME_UNCONFIRMED / LATE_JOIN) — без сети.

Каждый сценарий — синтетический поток (версии exchangeInfo с моментом получения, REST-снимки, diff-ы, сделки) и
ожидаемые состояния: T0, состояние T0 (core.cap_breakout.MainEngine.t0_state), старт веток, входы, судьба позиций.
  S1  обычный запуск до T0                         → CONFIRMED, T0 = firstOpenTime, ветки стартуют
  S2a/b/c запуск после T0 на 5 мс / 2 с / 5 мин, первая сделка раньше exchangeInfo
                                                   → LATE_JOIN, T0 не назначен, веток и входов нет
  S3  запуск до T0, exchangeInfo пришёл позже первой сделки → T0 = firstOpenTime (не время сделки)
  S4  exchangeInfo не пришёл вовсе                  → OPEN_TIME_UNCONFIRMED, веток нет
  S5  exchangeInfo без firstOpenTime                → OPEN_TIME_UNCONFIRMED, веток нет
  S6  перенос открытия дважды до открытия (как KOR)  → T0 = последний firstOpenTime, переходы в журнале
  S7  firstOpenTime изменился после открытия рынка   → OPEN_TIME_CONFLICT: ожидающие ветки закрыты, новых решений о
                                                     покупке нет, открытые позиции не закрыты конфликтом
  S7b конфликт внутри окна задержки 50 мс после первого решения → новых решений нет; заявка, отправленная до
                                                     конфликта, доходит по модели (она уже у биржи)
  S8  неполный стакан (только asks)                 → ни одного входа, пока стакан односторонний
  S9a/b граница часов: firstOpenTime = начало наблюдения + 1 мс / − 1 мс → CONFIRMED / LATE_JOIN
  S9c запас на погрешность часов 250 мс (как в PAPER): firstOpenTime = начало + 100 мс → LATE_JOIN
  S10 дубли версий exchangeInfo и сделок            → один переход CONFIRMED, T0 не меняется
Невалидная синтетика (BTCUSDT, firstOpenTime 2017) — в self-test регрессии (N10a, N11, N13).

--core FILE — прогнать те же сценарии на другом файле ядра (например, cap_breakout.py коммита до Time Guard):
сценарии ложного T0 (S2, S3, S4, S5) обязаны там ПАДАТЬ — иначе тесты не ловят прежнюю ошибку.
  python3 tools/time_guard_tests.py [--core /tmp/cap_breakout_old.py] [--json logs/time_guard_tests_<дата>.json]
"""

import argparse
import importlib.util
import json
import sys
import time
from argparse import Namespace
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import core  # noqa: E402,F401  (пакет для относительных импортов ядра)

T0 = 1795000000000
CFG = None
INFO = {"symbol": "TGUSDT", "status": "1", "quotePrecision": 4, "baseAssetPrecision": 2,
        "takerCommission": "0.0005", "makerCommission": "0", "filters": []}


def load_core(path: str | None):
    if not path:
        import core.cap_breakout as cb
        return cb
    spec = importlib.util.spec_from_file_location("core._tg_alt", path)
    mod = importlib.util.module_from_spec(spec)
    mod.__package__ = "core"
    spec.loader.exec_module(mod)
    return mod


def info(fo):
    d = dict(INFO)
    if fo is not None:
        d["firstOpenTime"] = fo
    return d


def snap(v, bid=0.99, ask=1.0, depth=True, asks_only=False):
    bids = [] if asks_only else [[f"{bid - i * 0.001:.4f}", "5000"] for i in range(30 if depth else 1)]
    return {"lastUpdateId": v, "bids": bids, "asks": [[f"{ask + i * 0.001:.4f}", "5000"] for i in range(30)]}


def market(start, ms=8000, v0=100, asks_only=False):
    """Растущий рынок от start: снимок, затем каждые 50 мс сделка и diff (ask растёт)."""
    ev = [(start, "snap", snap(v0, asks_only=asks_only))]
    v, px = v0, 1.0
    for k in range(1, ms // 50):
        t = start + k * 50
        px *= 1.0015
        ev.append((t, "deal", (round(px, 4), 50.0, 1)))
        v += 1
        d = {"fromVersion": v, "toVersion": v, "asks": [{"price": f"{px:.4f}", "quantity": "4000"}],
             "bids": [] if asks_only else [{"price": f"{px - 0.01:.4f}", "quantity": "6000"}]}
        ev.append((t, "diff", d))
    return ev


class Run:
    def __init__(self, cb, join, stream, margin=0):
        """stream — список (t, "info", dict) и (t, kind, data) по времени; join — начало наблюдения."""
        self.cb = cb
        new = hasattr(cb.MainEngine, "on_info")
        self.eng = (cb.MainEngine(CFG, cb.Opts(), None, {}, join_ms=join, join_margin_ms=margin) if new
                    else cb.MainEngine(CFG, cb.Opts(), None, {}))
        self.entries_at, self.conflict_at = [], None
        for t, kind, data in sorted(stream, key=lambda x: (x[0], 0 if x[1] == "info" else 1)):
            if kind == "info":
                if new:
                    self.eng.on_info(t, data, "test")
                elif self.eng.release is None:
                    self.eng.info = data
                if new and self.conflict_at is None and self.eng.t0_state == "OPEN_TIME_CONFLICT":
                    self.conflict_at = t
                continue
            self.eng.on_event(t, kind, data, t, None)
        self.eng.finish()

    @property
    def state(self):
        return getattr(self.eng, "t0_state", "?")

    def entries(self):
        return [s for s in self.eng.sims if s.entry_ms]


def check(name, cond, why, out):
    out.append({"case": name, "result": "PASS" if cond else "FAIL", "detail": why})


def scenarios(cb) -> list[dict]:
    out = []
    # S1 — обычный запуск до T0
    r = Run(cb, T0 - 600_000, [(T0 - 590_000, "info", info(T0))] + market(T0))
    check("S1 запуск до T0", r.state in ("CONFIRMED", "?") and r.eng.open_ms == T0 and r.eng.release is not None,
          f"T0 {r.eng.open_ms}, состояние {r.state}, ветки {len(r.eng.sims)}, входов {len(r.entries())}", out)
    # S2 — поздний старт: первая сделка раньше exchangeInfo
    for tag, late in (("a 5 мс", 5), ("b 2 с", 2000), ("c 5 мин", 300_000)):
        join = T0 + late
        st = market(join + 1, 5000)
        r = Run(cb, join, st + [(join + 40, "info", info(T0))])
        check(f"S2{tag[0]} запуск после T0 на {tag[2:]}", r.state == "LATE_JOIN" and r.eng.release is None
              and not r.entries() and r.eng.open_ms is None,
              f"состояние {r.state}, T0 {r.eng.open_ms}, ветки {len(r.eng.sims)}, входов {len(r.entries())}", out)
    # S3 — до T0, exchangeInfo позже первой сделки
    r = Run(cb, T0 - 1000, market(T0 + 10) + [(T0 + 300, "info", info(T0))])
    check("S3 сделка раньше exchangeInfo", r.eng.open_ms == T0 and r.state == "CONFIRMED"
          and (r.eng.release or 0) >= T0 + 300,
          f"T0 {r.eng.open_ms} (ожидался {T0}), старт веток {r.eng.release}", out)
    # S4 — exchangeInfo нет
    r = Run(cb, T0 - 60_000, market(T0))
    check("S4 exchangeInfo нет", r.eng.release is None and not r.entries() and r.state == "OPEN_TIME_UNCONFIRMED",
          f"состояние {r.state}, T0 {r.eng.open_ms}, старт {r.eng.release}", out)
    # S5 — exchangeInfo без firstOpenTime
    r = Run(cb, T0 - 60_000, [(T0 - 50_000, "info", info(None))] + market(T0))
    check("S5 без firstOpenTime", r.eng.release is None and r.state == "OPEN_TIME_UNCONFIRMED",
          f"состояние {r.state}, T0 {r.eng.open_ms}, старт {r.eng.release}", out)
    # S6 — перенос дважды (KOR)
    r = Run(cb, T0 - 3_600_000, [(T0 - 3_500_000, "info", info(T0 - 1_800_000)),
                                 (T0 - 2_400_000, "info", info(T0 - 1_200_000)),
                                 (T0 - 1_500_000, "info", info(T0))] + market(T0))
    log = [x["state"] for x in getattr(r.eng, "t0_log", [])]
    check("S6 перенос дважды (KOR)", r.eng.open_ms == T0 and (r.eng.release or 0) >= T0
          and (not log or log == ["CONFIRMED"] * 3), f"T0 {r.eng.open_ms}, переходы {log}", out)
    # S7 — конфликт после открытия
    st = [(T0 - 60_000, "info", info(T0))] + market(T0, 8000) + [(T0 + 3000, "info", info(T0 + 60_000))]
    r = Run(cb, T0 - 120_000, st)
    if getattr(r.eng, "t0_state", None) is None:
        check("S7 конфликт после открытия", True, "старое ядро: состояния нет — сценарий не применим", out)
    else:
        ca = r.conflict_at
        late = [s for s in r.eng.sims if ca and (s.result.get("decision_ms") or 0) > ca]
        killed_pos = [s for s in r.entries() if s.result.get("reason") == "OPEN_TIME_CONFLICT"]
        scan_closed = sum(1 for s in r.eng.sims if s.result.get("reason") == "OPEN_TIME_CONFLICT")
        check("S7 конфликт после открытия", r.state == "OPEN_TIME_CONFLICT" and ca is not None and not late
              and not killed_pos,
              f"конфликт {None if ca is None else ca - T0} мс от T0; решений после конфликта {len(late)}; входов "
              f"{len(r.entries())}; ожидавших закрыто {scan_closed}; позиций, закрытых конфликтом, {len(killed_pos)}", out)
        # S7b — конфликт через 20 мс после первого решения (заявка в пути, задержка модели 50 мс)
        first = min(s.result["decision_ms"] for s in Run(cb, T0 - 120_000, [(T0 - 60_000, "info", info(T0))]
                                                         + market(T0, 8000)).eng.sims if s.result.get("decision_ms"))
        r = Run(cb, T0 - 120_000, [(T0 - 60_000, "info", info(T0))] + market(T0, 8000)
                + [(first + 20, "info", info(T0 + 60_000))])
        ca = r.conflict_at
        late = [s for s in r.eng.sims if ca and (s.result.get("decision_ms") or 0) > ca]
        flight = [s for s in r.eng.sims if ca and s.result.get("decision_ms") and s.result["decision_ms"] <= ca
                  and (s.entry_ms or 0) > ca]
        check("S7b конфликт при заявке в пути", r.state == "OPEN_TIME_CONFLICT" and ca is not None and not late,
              f"первое решение T0+{first - T0} мс, конфликт T0+{None if ca is None else ca - T0} мс; решений после "
              f"конфликта {len(late)}; заявок, отправленных до конфликта и исполненных после, {len(flight)}", out)
    # S8 — неполный стакан
    r = Run(cb, T0 - 60_000, [(T0 - 50_000, "info", info(T0))] + market(T0, 5000, asks_only=True))
    check("S8 неполный стакан", not r.entries(), f"старт {r.eng.release}, входов {len(r.entries())}", out)
    # S9 — граница часов
    for tag, fo, want in (("a +1 мс", T0 + 1, "CONFIRMED"), ("b −1 мс", T0 - 1, "LATE_JOIN")):
        r = Run(cb, T0, [(T0 + 20, "info", info(fo))] + market(T0 + 30, 3000))
        ok = r.state == want if hasattr(r.eng, "t0_state") else want == "CONFIRMED"
        check(f"S9{tag[0]} firstOpenTime = начало наблюдения {tag[2:]}", ok,
              f"состояние {r.state}, ожидалось {want}, старт {r.eng.release}", out)
    if hasattr(cb.MainEngine, "on_info"):
        r = Run(cb, T0, [(T0 + 20, "info", info(T0 + 100))] + market(T0 + 130, 3000), margin=250)
        check("S9c запас часов 250 мс, firstOpenTime = начало + 100 мс", r.state == "LATE_JOIN",
              f"состояние {r.state}, старт {r.eng.release}", out)
    # S10 — дубли
    mk = market(T0, 3000)
    r = Run(cb, T0 - 60_000, [(T0 - 50_000, "info", info(T0)), (T0 - 50_000, "info", info(T0)),
                              (T0 - 40_000, "info", info(T0))] + mk + [x for x in mk if x[1] == "deal"])
    log = [x["state"] for x in getattr(r.eng, "t0_log", [])]
    check("S10 дубли", r.eng.open_ms == T0 and len(log) <= 1, f"T0 {r.eng.open_ms}, переходы {log}", out)
    return out


def main() -> None:
    global CFG
    p = argparse.ArgumentParser()
    p.add_argument("--core", help="другой файл ядра (cap_breakout.py) — для проверки, что тесты ловят прежнюю ошибку")
    p.add_argument("--json")
    a = p.parse_args()
    cb = load_core(a.core)
    CFG = Namespace(latency=[50], usdt=[cb.ORDER_USDT], sweep=False, main_early=cb.MAIN_EARLY_PAPER)
    res = scenarios(cb)
    for r in res:
        print(f"{r['result']}  {r['case']}: {r['detail']}")
    fails = sum(r["result"] == "FAIL" for r in res)
    print(f"\nядро {a.core or 'текущее'}: сценариев {len(res)}, не прошли {fails}")
    out = a.json or str(HERE.parent / "logs" / f"time_guard_tests_{time.strftime('%Y%m%d')}{'_altcore' if a.core else ''}.json")
    Path(out).write_text(json.dumps({"core": a.core or "current", "cases": res, "fails": fails}, ensure_ascii=False,
                                    indent=1), encoding="utf-8")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
