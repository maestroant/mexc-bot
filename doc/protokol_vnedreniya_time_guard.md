# Протокол внедрения Time Guard (LATE_JOIN / OPEN_TIME_UNCONFIRMED) в рабочий PAPER

Решение клиента 19.11.2026: исправление разрешено; в рабочий PAPER — только по отдельному протоколу внедрения с откатом.
Статус на момент составления: **TESTED_OFFLINE** (копия `~/durtest`), в бою не развёрнуто.

## 1. Что меняется

| Файл | Изменение |
|---|---|
| `tools/core/cap_breakout.py` | `MainEngine`: состояние T0 (`OPEN_TIME_UNCONFIRMED` → `CONFIRMED` / `LATE_JOIN` / `OPEN_TIME_CONFLICT`), `on_info(known_at, info)`, `join_ms` и `join_margin_ms`; T0 только по `firstOpenTime`, без запасного варианта «первая сделка / первый стакан»; журнал переходов `t0_log` |
| `tools/paper.py` | наблюдение с момента запуска (`join_ms`), запас часов `JOIN_MARGIN_MS = 250`, `exchangeInfo` — в момент ответа REST, событие `T0_STATE` в лог и журнал кадров (known_at, firstOpenTime, join_ms, поправка часов), `LATE_JOIN` — завершение без веток |
| `tools/main_replay.py`, `tools/paper_vs_replay.py`, `tools/p0_stress.py`, `tools/p0_synthetic_mismatch.py` | REPLAY и путь PAPER на записях получают версии `exchangeInfo` по моменту получения (`feed_causal`), без предзагрузки; тест `meta` (неизменность префикса по метаданным) в обязательной регрессии |
| `tools/time_guard_tests.py` (новый), `tools/p0_regress_selftest.py` | 15 сценариев Time Guard; self-test N11 (прежний REPLAY — оракул), N13 (причинный REPLAY = PAPER на не листинге) |

Код depth memo в релиз **не входит**: релизная сборка ядра — боевой файл плюс только правки Time Guard (дифф проверен построчно).

| Сборка | SHA-256 (сырые байты) |
|---|---|
| `cap_breakout.py` в бою сейчас | `301c1e11159d4c95be66a7725ae589992c07512871bebacf139405b96ec506fe` |
| `cap_breakout.py` релиз Time Guard | `3e03c3ab066bb369a34e206a37340dd1fe0c0ff79ca8bb23a73095ce00bcd2a8` |

## 2. Условия перед внедрением

1. Согласие клиента на этот протокол.
2. Ни одного листинга в ближайшие 2 часа (`logs/watcher_state.json`, анонсы) и ни одного живого процесса пары (`pgrep -fa "paper.py|probe_listing|listing_recorder"` — пусто).
3. На копии `~/durtest` с релизной сборкой пройдены: `time_guard_tests.py` 15/15; то же на ядре 1bb2381 — сценарии ложного T0 падают; `p0_regress.py check --win /tmp/p0win --cap-dir logs` без изменения отпечатков (32 / 8 / 2) и с тестом `meta`; `p0_regress_selftest.py` — все отрицательные случаи; `p0_three_way.py` WASTED и SANC — 186/0; синтетика BTC — PASS; дымовой PAPER BTCUSDT — `LATE_JOIN` и чистое завершение.

## 3. Шаги

1. Резервная копия: `mkdir -p ~/mexc-bot/backup_<дата>_timeguard/core` и копии всех файлов из таблицы раздела 1 и `VERSION`.
2. Копирование релизных файлов (`scp`), `VERSION` := `<commit>+P0-<дата>-timeguard`.
3. Проверка отпечатков: SHA-256 `tools/core/cap_breakout.py` равен релизному; `paper.code_version()` записан в журнал внедрения.
4. Службу наблюдателя **не перезапускать**: PAPER, пробу и запись наблюдатель запускает новыми процессами на каждый листинг — новый код подхватит следующий листинг. Пробы и супервизор Time Guard не затрагивает.
5. На боевом каталоге: `time_guard_tests.py`, `p0_regress.py check --win /tmp/p0win --cap-dir logs`, `p0_regress_selftest.py`.
6. Дымовой PAPER на BTCUSDT из боевого каталога — только без Telegram (`env -u TG_BOT_TOKEN -u TG_CHAT_ID -u TG_LISTING_CHAT_ID`, `.env` боевого каталога содержит получателей клиента — проверить, что процесс их не читает, или запускать из копии), затем файлы `paper_BTCUSDT_*` / `paperrec_BTCUSDT_*` убрать из `logs/` в `~/p0test/logs/`, чтобы их не взяли отчёты.

## 4. Критерии успеха после внедрения

- На ближайшем листинге в логе PAPER: `T0_STATE CONFIRMED` с `known_at` раньше T0, `first_open` = `firstOpenTime`, затем `OPEN` с тем же T0; трёхстороннее сравнение 186/0.
- Ни одного `LATE_JOIN` у PAPER, запущенного наблюдателем заранее.

## 5. Откат

Условия: любая ошибка шагов 3–6, `T0_STATE` отличный от `CONFIRMED` у PAPER, запущенного до открытия, исключение в PAPER, расхождение трёхстороннего сравнения.

1. Скопировать файлы из `backup_<дата>_timeguard/` обратно, вернуть `VERSION`.
2. Проверить SHA-256 `cap_breakout.py` = `301c1e11…506fe`, регрессия на боевом каталоге.
3. Идущий PAPER не трогать (у него код в памяти); следующий листинг пойдёт на прежнем коде.

## 6. Остаточный риск

- Запас часов 250 мс: PAPER, запущенный менее чем за 250 мс до открытия, уйдёт в `LATE_JOIN` (fail-closed). Наблюдатель запускает PAPER за 10–12 мин.
- `OPEN_TIME_CONFLICT` после старта веток: новых решений о покупке нет, отправленная до конфликта заявка доходит по модели (она уже у биржи), позиции и выходы не трогаются.
- Метаданные CAP (`KNOWN_CAPS`, потолок) тестом `meta` не покрыты — пункты 2–3 блока I.
