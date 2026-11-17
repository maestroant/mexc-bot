"""Отрицательные тесты стакана для варианта (b) depth memo (решение клиента 16.11, п. 6.2.1–6.2.2) — без сети, без биржи.

Что проверяется: core.cap_breakout.depth_bids с DEPTH_MEMO=1 после КАЖДОЙ операции со стаканом возвращает побитово то же,
что (1) depth_usdt по тем же уровням без memo и (2) depth_usdt по независимо отсортированной копии словаря стакана
(оракул, не зависящий от кеша Book.levels). Запросы — несколько диапазонов lo/hi от текущего bestBid, как в check_exit
(0,7 %, 1,5 %, depth_band), плюс произвольные.

Сценарии клиента: пустой стакан; односторонний (только asks / только bids / bids исчезли diff-ами); дубль diff-а;
out-of-order diff (разрыв версий → недействителен → pending); разрыв и поздний снимок (старее стакана и старее pending);
ресинхронизация (снимок новее стакана); смена диапазона и bestBid; смена источника — два стакана (как main и cap в
PAPER) с ОДИНАКОВЫМ счётчиком updates и разным содержимым, попеременно; некорректные уровни (q = 0, q < 0, nan, inf,
мусор — ValueError у обоих путей одинаково); быстрые переходы — фазз 20 000 случайных операций на двух стаканах.

Ключ кеша (п. 6.2.2): каждый список уровней, попавший в memo, запоминается копией; после каждой операции проверяется, что
ни один такой список не изменён на месте (неизменяемость содержимого); memo держит сильную ссылку на свой список, поэтому
его id не может быть переиспользован другим объектом, пока ключ жив.

Обратный контроль (тест должен ловить ошибку): два заведомо неверных варианта memo — ключ только (lo, hi) без объекта
списка и ключ по Book.updates (общий для разных стаканов) — обязаны дать FAIL. Если не дают — тест бесполезен, выход 1.
  python3 tools/depth_memo_negative.py [--json logs/depth_memo_negative_<дата>.json]
"""

import argparse
import json
import math
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import core.cap_breakout as cb  # noqa: E402
from core.book import Book, depth_usdt  # noqa: E402

BANDS = (0.007, 0.015, 0.02, 0.05, 1.0)


def same(a: float, b: float) -> bool:
    return (math.isnan(a) and math.isnan(b)) or a == b


class Checker:
    def __init__(self, memo_impl=None):
        self.memo_impl = memo_impl or cb.depth_bids
        self.checks = self.fails = 0
        self.first_fail: str | None = None
        self.held: list[tuple[list, list]] = []  # (список из memo, его копия) — контроль мутаций на месте

    def query(self, book: Book, label: str, extra: tuple = ()) -> None:
        bids = book.levels("bids")
        oracle_levels = sorted(book.bids.items(), reverse=True)
        bb = bids[0][0] if bids else 0.0
        ranges = [(bb * (1 - c), bb) for c in BANDS] + list(extra)
        for lo, hi in ranges:
            got = self.memo_impl(bids, lo, hi)
            direct = depth_usdt(bids, lo, hi)
            indep = depth_usdt(oracle_levels, lo, hi) if not any(math.isnan(p) for p, _ in oracle_levels) else direct
            self.checks += 1
            if not (same(got, direct) and same(got, indep)):
                self.fails += 1
                if self.first_fail is None:
                    self.first_fail = f"{label}: lo={lo!r} hi={hi!r} memo={got!r} без memo={direct!r} оракул={indep!r}"
        if not any(lv is bids for lv, _ in self.held[-4:]):
            self.held.append((bids, list(bids)))
        for lv, copy in self.held[-64:]:  # недавние списки (memo держит последний); все — O(n²) на фаззе
            if lv != copy:
                self.fails += 1
                self.first_fail = self.first_fail or f"{label}: список уровней изменён на месте"


def snap(v: int, bids=(), asks=()) -> dict:
    return {"lastUpdateId": v, "bids": [[str(p), str(q)] for p, q in bids], "asks": [[str(p), str(q)] for p, q in asks]}


def diff(fr: int, to: int, bids=(), asks=()) -> dict:
    return {"fromVersion": fr, "toVersion": to, "bids": [{"price": str(p), "quantity": str(q)} for p, q in bids],
            "asks": [{"price": str(p), "quantity": str(q)} for p, q in asks]}


def scenarios(c: Checker) -> list[dict]:
    out = []

    def run(name: str, steps) -> None:
        c0, f0 = c.checks, c.fails
        err = None
        try:
            steps()
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {e}"
        out.append({"scenario": name, "checks": c.checks - c0, "fails": c.fails - f0, "error": err})

    def empty():
        b = Book()
        c.query(b, "пустой, без снимка")
        b.snapshot(snap(10))
        c.query(b, "пустой снимок")
        b.diff(diff(11, 11))
        c.query(b, "пустой diff")

    def one_sided():
        b = Book()
        b.snapshot(snap(1, asks=[(101, 5), (102, 7)]))
        c.query(b, "только asks", extra=((0.0, 1e9),))
        b.snapshot(snap(2, bids=[(99, 3), (98, 4)]))
        c.query(b, "только bids")
        b.diff(diff(3, 3, bids=[(99, 0)]))
        c.query(b, "bids: один удалён")
        b.diff(diff(4, 4, bids=[(98, 0)]))
        c.query(b, "bids исчезли diff-ами")

    def dup_and_order():
        b = Book()
        b.snapshot(snap(100, bids=[(99, 1), (98, 2)], asks=[(101, 1)]))
        d1 = diff(101, 101, bids=[(99, 5)])
        b.diff(d1)
        c.query(b, "diff 101")
        b.diff(d1)
        c.query(b, "дубль diff 101")
        b.diff(diff(103, 103, bids=[(97, 9)]))  # 102 пропущен — разрыв, недействителен
        c.query(b, "out-of-order 103 (разрыв)")
        b.diff(diff(102, 102, bids=[(96, 9)]))  # запоздавший — в pending
        c.query(b, "запоздавший 102 при недействительном")

    def late_snapshot_resync():
        b = Book()
        b.snapshot(snap(200, bids=[(10, 1), (9, 1)], asks=[(11, 1)]))
        b.diff(diff(201, 201, bids=[(10, 2)]))
        b.snapshot(snap(150, bids=[(5, 5)]))  # старее стакана — игнорируется
        c.query(b, "поздний снимок старее стакана")
        b.diff(diff(205, 205, bids=[(8, 1)]))  # разрыв
        b.snapshot(snap(190, bids=[(7, 7)]))  # старее pending
        c.query(b, "снимок старее pending после разрыва")
        b.snapshot(snap(210, bids=[(12, 3), (11.5, 1)], asks=[(13, 1)]))  # ресинхронизация
        c.query(b, "ресинхронизация")
        b.snapshot(snap(400, bids=[(20, 1)], asks=[(21, 1)]))  # снимок новее стакана при действительном
        c.query(b, "снимок новее (скачок версии)")

    def range_change():
        b = Book()
        b.snapshot(snap(1, bids=[(100 - i * 0.1, 1 + i) for i in range(300)], asks=[(100.1, 1)]))
        for k in range(50):
            lo = 100 * (1 - 0.001 * k)
            c.query(b, f"диапазон {k}", extra=((lo, 100.0), (lo, 99.95), (lo * 0.5, lo)))
        b.diff(diff(2, 2, bids=[(100, 0)]))  # bestBid сменился — диапазоны от нового bb
        c.query(b, "bestBid сменился")

    def two_sources():
        a, z = Book(), Book()
        a.snapshot(snap(1, bids=[(10, 1), (9, 2)], asks=[(11, 1)]))
        z.snapshot(snap(1, bids=[(10, 5), (9, 6)], asks=[(11, 1)]))  # тот же updates, другое содержимое
        assert a.updates == z.updates
        for i in range(20):
            c.query(a if i % 2 == 0 else z, f"источник {'A' if i % 2 == 0 else 'B'} #{i}")
        a.diff(diff(2, 2, bids=[(9, 0)]))
        z.diff(diff(2, 2, bids=[(10, 0)]))
        for i in range(6):
            c.query(a if i % 2 == 0 else z, f"источники после diff #{i}")

    def bad_levels():
        b = Book()
        b.snapshot(snap(1, bids=[(10, 0), (9, -2), (8, 3)], asks=[(11, 1)]))  # q = 0 и q < 0 в снимке
        c.query(b, "q=0 и q<0 в снимке")
        b.diff(diff(2, 2, bids=[(7, -1)]))  # q < 0 в diff — удаление
        c.query(b, "q<0 в diff")
        b.snapshot(snap(5, bids=[(float("inf"), 1), (9, 1)], asks=[(11, 1)]))
        c.query(b, "inf цена")
        b.snapshot(snap(6, bids=[(float("nan"), 1), (9, 1)], asks=[(11, 1)]))
        c.query(b, "nan цена")
        for bad in ({"lastUpdateId": 7, "bids": [["abc", "1"]]}, {"lastUpdateId": 8, "bids": [["1"]]}):
            errs = []
            for memo in (False, True):
                cb.DEPTH_MEMO = memo
                bb = Book()
                try:
                    bb.snapshot(bad)
                    errs.append(None)
                except (ValueError, IndexError) as e:
                    errs.append(type(e).__name__)
            cb.DEPTH_MEMO = True
            c.checks += 1
            if errs[0] != errs[1] or errs[0] is None:
                c.fails += 1
                c.first_fail = c.first_fail or f"мусорный уровень {bad}: без memo {errs[0]}, с memo {errs[1]}"

    def fuzz():
        rnd = random.Random(20261116)
        books = [Book(), Book()]
        vers = [0, 0]
        for i in range(20_000):
            k = rnd.randrange(2)
            b = books[k]
            op = rnd.random()
            if op < 0.05 or not b.valid and op < 0.3:
                vers[k] += rnd.choice((0, 1, 5, -3))
                lv = [(round(100 - rnd.random() * 5, 2), round(rnd.random() * 10, 3)) for _ in range(rnd.randrange(0, 40))]
                b.snapshot(snap(max(1, vers[k]), bids=lv, asks=[(100.5, 1)]))
                vers[k] = max(vers[k], b.version)
            else:
                jump = rnd.choice((1, 1, 1, 1, 0, 2, -1))  # дубль, разрыв, запоздавший
                fr = b.version + jump
                lv = [(round(100 - rnd.random() * 5, 2), rnd.choice((0, round(rnd.random() * 10, 3))))
                      for _ in range(rnd.randrange(1, 6))]
                b.diff(diff(fr, fr, bids=lv))
            if rnd.random() < 0.5:
                c.query(b, f"фазз шаг {i}", extra=((rnd.uniform(90, 100), rnd.uniform(95, 101)),))

    cb.DEPTH_MEMO = True
    cb._MEMO.update(lv=None, vals={})
    for name, fn in (("пустой стакан", empty), ("односторонний", one_sided), ("дубль и out-of-order", dup_and_order),
                     ("поздний снимок и ресинхронизация", late_snapshot_resync), ("смена диапазона/bestBid", range_change),
                     ("два источника с равным updates", two_sources), ("некорректные уровни", bad_levels),
                     ("быстрые переходы (фазз 20 000)", fuzz)):
        run(name, fn)
    cb.DEPTH_MEMO = False
    return out


def broken_by_range():
    memo: dict = {}

    def f(levels, lo, hi):
        return memo.setdefault((lo, hi), depth_usdt(levels, lo, hi))
    return f


def broken_by_updates():
    """Ключ по длине и первому уровню списка — имитация ключа по счётчику/версии без объекта."""
    memo: dict = {}

    def f(levels, lo, hi):
        key = (len(levels), levels[0] if levels else None, lo, hi)
        return memo.setdefault(key, depth_usdt(levels, lo, hi))
    return f


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--json")
    a = p.parse_args()
    c = Checker()
    res = scenarios(c)
    ok = c.fails == 0 and not any(r["error"] for r in res)
    for r in res:
        print(f"{'PASS' if not r['fails'] and not r['error'] else 'FAIL'}  {r['scenario']}: проверок {r['checks']}, "
              f"ошибок {r['fails']}" + (f", исключение {r['error']}" if r["error"] else ""))
    if c.first_fail:
        print(f"  первая ошибка: {c.first_fail}")
    print(f"списков уровней под контролем мутаций: {len(c.held)}")
    controls = []
    for name, impl in (("ключ только (lo, hi)", broken_by_range()), ("ключ без объекта списка", broken_by_updates())):
        cc = Checker(impl)
        scenarios(cc)
        caught = cc.fails > 0
        controls.append({"control": name, "fails": cc.fails, "caught": caught, "first": cc.first_fail})
        print(f"{'PASS' if caught else 'FAIL'}  обратный контроль «{name}»: тест поймал ошибок {cc.fails}"
              + (f" (первая: {cc.first_fail[:120]})" if cc.first_fail else ""))
    ok = ok and all(x["caught"] for x in controls)
    print(f"\nитог: {'PASS' if ok else 'FAIL'}; проверок {c.checks}")
    out = a.json or str(Path(__file__).resolve().parent.parent / "logs" / f"depth_memo_negative_{time.strftime('%Y%m%d')}.json")
    Path(out).write_text(json.dumps({"scenarios": res, "checks": c.checks, "fails": c.fails, "first_fail": c.first_fail,
                                     "controls": controls, "result": "PASS" if ok else "FAIL"}, ensure_ascii=False,
                                    indent=1), encoding="utf-8")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
