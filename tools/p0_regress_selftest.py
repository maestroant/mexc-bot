"""Отрицательные тесты самой регрессии (решение клиента 13.11, A–E, п. 6): p0_regress check обязан давать FAIL
(ненулевой код выхода) и названную причину, а не PASS или тихий пропуск, когда проверять нечего или входы испорчены.

  N1 пустые папки окон и CAP            → FAIL, «0 сравнений» / «нет в данных»
  N2 папки окон нет                     → FAIL, «не существует»
  N3 нет --cap-dir                      → FAIL
  N4 частичный набор (3 окна)           → FAIL, «есть в эталоне, нет в данных»
  N5 повреждённая запись (обрезанный gz) → FAIL, имя файла и «не прочитана» (с тестами p0_stress и без)
  N6 эталон недоступен                  → FAIL
  N7 эталон повреждён (не JSON)         → FAIL
  N8 отпечаток листинга в эталоне изменён → FAIL, «строки изменились» по этому листингу
  N9 SHA-256 входа в манифесте не совпадает → FAIL, «входные данные изменились»
  N10a запись не листинга (рукописная: сделка → exchangeInfo с firstOpenTime двое суток назад → снимок)
                                        → FAIL, «INVALID_FOR_LISTING_REPLAY (HISTORICAL_FIRST_OPEN)», не «0 входов»
  N10b запись начата через 5 с после firstOpenTime → FAIL, «INVALID_FOR_LISTING_REPLAY (LATE_START)»
  N11 отрицательный оракул P0-REPLAY-SYNTHETIC-MISMATCH: на фикстуре N10a с отключённым контрактом
      p0_synthetic_mismatch diverge обязан найти первое расхождение СОСТОЯНИЯ (open_ms) на событии 0
  N12 замороженный листинг без снятия (KNTQ), не объявленный в эталоне ожидаемым без снятия
                                        → FAIL, «не объявлен ожидаемым без снятия» (решение 16.11, п. 7)
Положительный контроль — обычный полный check (отдельно, ~8 мин): должен дать код 0.
  python3 tools/p0_regress_selftest.py --win /tmp/p0win      # протокол logs/p0_regress_selftest_<дата>.json
"""

import argparse
import gzip
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
GOLDEN = HERE / "regress_golden.json"


def run(args: list[str]) -> tuple[int, str]:
    r = subprocess.run([sys.executable, str(HERE / "p0_regress.py"), "check", *args], capture_output=True, text=True,
                       cwd=HERE.parent, timeout=1800)
    return r.returncode, r.stdout + r.stderr


def fixture(path: Path, first_open_ago_ms: int, start_after_open: bool = False) -> None:
    """Минимальная рукописная запись: сделка, OPEN_DETECTED, exchangeInfo, REST-снимок (решение клиента 16.11, п. 6.3)."""
    t = 1794481710000
    fo = t - first_open_ago_ms
    sym = path.name.split("_")[1]
    deal = {"t": t, "src": "ws_pub", "ch": f"spot@public.aggre.deals.v3.api.pb@10ms@{sym}", "conn": "A",
            "data": {"symbol": sym, "sendTime": str(t - 5), "publicAggreDeals": {"deals": [
                {"price": "100", "quantity": "1", "tradeType": 1, "time": str(t - 6)}]}}}
    det = {"t": t, "src": "event", "ch": "OPEN_DETECTED", "data": {"open_ms": t}}
    info = {"t": t + 30, "src": "rest", "ch": "exchangeInfo", "data": {"symbols": [
        {"symbol": sym, "status": "1", "firstOpenTime": fo, "quotePrecision": 2, "baseAssetPrecision": 4,
         "takerCommission": "0.0005", "makerCommission": "0"}]}}
    snap = {"t": t + 60, "src": "rest", "ch": "depth", "data": {"lastUpdateId": 1, "bids": [["99", "50"]],
                                                                "asks": [["101", "50"]]}}
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for e in (deal, det, info, snap):
            f.write(json.dumps(e, separators=(",", ":")) + "\n")  # core.feed.line_t: {"t":<13 цифр> без пробела


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--win", default="/tmp/p0win", help="папка с полным набором окон (источник для копий)")
    p.add_argument("--cap-src", default=str(HERE.parent / "logs"), help="полные записи и логи проб (для N12)")
    a = p.parse_args()
    src = sorted(Path(a.win).glob("rec_*USDT_2*.jsonl.gz"))
    if len(src) < 3:
        sys.exit(f"в {a.win} меньше 3 окон — самотест невозможен")
    gold = json.loads(GOLDEN.read_text(encoding="utf-8"))
    report, fails = [], 0
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        empty, cap_empty, part, corrupt = d / "empty", d / "cap_empty", d / "part", d / "corrupt"
        for x in (empty, cap_empty, part, corrupt):
            x.mkdir()
        three = [s for s in src if s.name.split("_")[1] in gold["main"]][:3]
        for s in three:
            shutil.copy(s, part / s.name)
            shutil.copy(s, corrupt / s.name)
        bad = corrupt / three[0].name
        raw = bad.read_bytes()
        bad.write_bytes(raw[: len(raw) // 3])
        sym0 = three[0].name.split("_")[1]
        g8 = json.loads(json.dumps(gold))
        g8["main"][sym0]["replay"] = "0" * 16
        (d / "g8.json").write_text(json.dumps(g8), encoding="utf-8")
        g9 = json.loads(json.dumps(gold))
        g9.setdefault("inputs", {}).setdefault("main", {})[three[0].name] = "f" * 16
        (d / "g9.json").write_text(json.dumps(g9), encoding="utf-8")
        (d / "g7.json").write_text("{не json", encoding="utf-8")
        g12 = json.loads(json.dumps(gold))
        g12.pop("cap_expected_no_release", None)
        (d / "g12.json").write_text(json.dumps(g12), encoding="utf-8")
        kntq = d / "cap_kntq"
        kntq.mkdir()
        for f in list(Path(a.cap_src).glob("rec_KNTQUSDT_2*.jsonl.gz"))[-1:] + list(Path(a.cap_src).glob("probe_KNTQUSDT_2*.jsonl")):
            (kntq / f.name).symlink_to(f.resolve())
        hist, late = d / "hist", d / "late"
        for x, ago in ((hist, 2 * 86_400_000), (late, 5_000)):
            x.mkdir()
            for s in three:
                shutil.copy(s, x / s.name)
            fixture(x / "rec_SYNTHTESTUSDT_20261116_000000.jsonl.gz", ago)
        cases = [
            ("N1 пустые папки", ["--win", str(empty), "--cap-dir", str(cap_empty)], ["0 сравнений", "нет в данных"]),
            ("N2 папки окон нет", ["--win", str(d / "nope"), "--cap-dir", str(cap_empty)], ["не существует"]),
            ("N3 нет --cap-dir", ["--win", str(part)], ["--cap-dir"]),
            ("N4 частичный набор", ["--win", str(part), "--cap-dir", str(cap_empty), "--skip-stress"],
             ["есть в эталоне, нет в данных"]),
            ("N5a повреждённая запись", ["--win", str(corrupt), "--cap-dir", str(cap_empty), "--skip-stress"],
             [bad.name, "не прочитана"]),
            ("N5b повреждённая запись + p0_stress", ["--win", str(corrupt), "--cap-dir", str(cap_empty)],
             ["запись не прочитана", "тест не прошёл"]),
            ("N6 эталон недоступен", ["--win", str(part), "--cap-dir", str(cap_empty), "--golden", str(d / "no.json")],
             ["недоступен"]),
            ("N7 эталон повреждён", ["--win", str(part), "--cap-dir", str(cap_empty), "--golden", str(d / "g7.json")],
             ["повреждён"]),
            ("N8 отпечаток изменён", ["--win", str(part), "--cap-dir", str(cap_empty), "--skip-stress",
                                      "--golden", str(d / "g8.json")], [f"{sym0}: строки изменились"]),
            ("N9 SHA-256 входа", ["--win", str(part), "--cap-dir", str(cap_empty), "--skip-stress",
                                  "--golden", str(d / "g9.json")], ["входные данные изменились"]),
            ("N10a запись не листинга", ["--win", str(hist), "--cap-dir", str(cap_empty), "--skip-stress"],
             ["INVALID_FOR_LISTING_REPLAY (HISTORICAL_FIRST_OPEN)", "SYNTHTESTUSDT"]),
            ("N10b поздний старт записи", ["--win", str(late), "--cap-dir", str(cap_empty), "--skip-stress"],
             ["INVALID_FOR_LISTING_REPLAY (LATE_START)", "SYNTHTESTUSDT"]),
            ("N12 без снятия, не объявлен", ["--win", str(part), "--cap-dir", str(kntq), "--skip-stress",
                                             "--golden", str(d / "g12.json")],
             ["KNTQUSDT: строк CAP нет, а в эталоне листинг не объявлен ожидаемым без снятия"]),
        ]
        for name, args, must in cases:
            code, out = run(args)
            ok = code != 0 and all(m in out for m in must) and "пройдена (" not in out.replace("НЕ ПРОЙДЕНА", "")
            fails += not ok
            last = [x for x in out.strip().splitlines() if x.strip()][-1:] or [""]
            report.append({"case": name, "exit_code": code, "expected": must, "result": "PASS" if ok else "FAIL",
                           "last_line": last[0][:300]})
            print(f"{'PASS' if ok else 'FAIL'}  {name}: код {code}; {last[0][:160]}", flush=True)
        r = subprocess.run([sys.executable, str(HERE / "p0_synthetic_mismatch.py"), "diverge",
                            str(hist / "rec_SYNTHTESTUSDT_20261116_000000.jsonl.gz")], capture_output=True, text=True,
                           cwd=HERE.parent, timeout=600)
        out11 = r.stdout + r.stderr
        must = ['"index": 0', '"where": "СОСТОЯНИЕ"', '"fields": ["open_ms"]']
        ok = r.returncode == 0 and all(m in out11 for m in must)
        fails += not ok
        report.append({"case": "N11 оракул первого расхождения", "exit_code": r.returncode, "expected": must,
                       "result": "PASS" if ok else "FAIL", "last_line": out11.strip()[-300:]})
        print(f"{'PASS' if ok else 'FAIL'}  N11 оракул первого расхождения: {out11.strip()[-200:]}", flush=True)
    out = HERE.parent / "logs" / f"p0_regress_selftest_{time.strftime('%Y%m%d')}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nотрицательных случаев {len(report)}, не прошли {fails}; протокол {out}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
