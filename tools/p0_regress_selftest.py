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
Положительный контроль — обычный полный check (отдельно, ~8 мин): должен дать код 0.
  python3 tools/p0_regress_selftest.py --win /tmp/p0win      # протокол logs/p0_regress_selftest_<дата>.json
"""

import argparse
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


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--win", default="/tmp/p0win", help="папка с полным набором окон (источник для копий)")
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
        ]
        for name, args, must in cases:
            code, out = run(args)
            ok = code != 0 and all(m in out for m in must) and "пройдена (" not in out.replace("НЕ ПРОЙДЕНА", "")
            fails += not ok
            last = [x for x in out.strip().splitlines() if x.strip()][-1:] or [""]
            report.append({"case": name, "exit_code": code, "expected": must, "result": "PASS" if ok else "FAIL",
                           "last_line": last[0][:300]})
            print(f"{'PASS' if ok else 'FAIL'}  {name}: код {code}; {last[0][:160]}", flush=True)
    out = HERE.parent / "logs" / f"p0_regress_selftest_{time.strftime('%Y%m%d')}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nотрицательных случаев {len(report)}, не прошли {fails}; протокол {out}")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
