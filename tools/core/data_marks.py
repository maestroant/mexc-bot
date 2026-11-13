"""Постоянные метки качества данных листингов — одно место для всех отчётов (решение клиента 13.11, п. 2.1).

Листинг с меткой DATA_AVAILABILITY_UNCONFIRMED считается событием с неподтверждённой доступностью данных во времени:
REPLAY по его записи не знает, когда те же кадры были доступны живому процессу. Поэтому:
  * результаты REPLAY и PAPER по нему ИСКЛЮЧАЮТСЯ из основной оценочной выборки (итоги BASELINE / MAIN_EARLY /
    CAP BREAKOUT, предрегистрация) и показываются отдельной группой;
  * сами записи, лог фактического PAPER и журнал диагностики НЕ удаляются и НЕ переписываются;
  * отсутствие повторения расхождения на других листингах не доказывает исторической причины (статус задачи P0 №1 —
    HISTORICAL_EVIDENCE_UNRECOVERABLE, задача №8 — открыта).
Читают: main_replay, paper_report, paper_vs_replay, p0_three_way, listing_report, cap_replay.
Снять метку можно только решением клиента — с записью в doc/resheniya_po_voprosam.md.
"""

DATA_AVAILABILITY_UNCONFIRMED = "DATA_AVAILABILITY_UNCONFIRMED"

MARKS: dict[str, dict] = {
    "VLXUSDT": {
        "mark": DATA_AVAILABILITY_UNCONFIRMED,
        "task": "P0 №1 HISTORICAL_EVIDENCE_UNRECOVERABLE (решение клиента 13.11); P0 №8 OPEN",
        "since": "2026-11-13",
        "reason": "стакан у записи с T0+93 мс, у фактического PAPER — с T0+893 мс; покадрового журнала PAPER на VLX "
                  "не было (журнал кадров появился 09.11), второго соединения PAPER B не было — какие кадры и когда "
                  "получил PAPER, восстановить нельзя",
        "kept": ["rec_VLXUSDT_20261106_050543.jsonl.gz (полная запись)",
                 "paper_VLXUSDT_20261106_050543.jsonl (START_PAPER, OPEN, START, 24 NO_ENTRY, SUMMARY)",
                 "paper_VLXUSDT_console.txt", "probe_VLXUSDT_20261106_050543.jsonl"],
    },
}


def mark_of(symbol: str) -> dict | None:
    return MARKS.get(symbol)


def excluded(symbol: str) -> bool:
    """Исключать ли листинг из основной оценочной выборки."""
    m = MARKS.get(symbol)
    return bool(m and m["mark"] == DATA_AVAILABILITY_UNCONFIRMED)


def mark_line(symbol: str) -> str | None:
    m = MARKS.get(symbol)
    if not m:
        return None
    return (f"!!! {symbol}: {m['mark']} — {m['task']}. Исключён из основной оценочной выборки; данные сохранены. "
            f"Причина: {m['reason']}")
