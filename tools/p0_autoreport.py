"""P0: трёхстороннее сравнение после каждого листинга без участия человека (запуск из cron раз в 5 минут).

Для каждого журнала PAPER logs/paperrec_<SYM>_<stamp>.jsonl.gz, чей лог logs/paper_<SYM>_<stamp>.jsonl уже содержит
SUMMARY, а отчёта logs/p0_three_way_<SYM>_<stamp>.txt ещё нет: запускает tools/p0_three_way.py с этой записью,
журналом и логом, сохраняет отчёт и шлёт короткую сводку в Telegram (TG_CHAT_ID).
Не запускается, если по watcher.log старт листинга ожидается в ближайшие 15 минут или был менее 5 минут назад:
разбор занимает процессор, а в это время идут измерения задержек на открытии.
  */5 * * * * cd ~/mexc-bot && nice -n 15 .venv/bin/python tools/p0_autoreport.py >> logs/p0_autoreport.log 2>&1
"""

import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from notify import notify
from probe_listing import ROOT, load_dotenv

LOGS = ROOT / "logs"
QUIET_BEFORE, QUIET_AFTER = timedelta(minutes=15), timedelta(minutes=5)
SUMMARY_LINES = ("нет на соединении", "первый WS-кадр", "A − запись, diff-ы", "A − B, diff-ы", "LOOP_LAG T0",
                 "SLOW_FRAME T0", "против фактического PAPER A", "DATA_CORRUPTED", "DELIVERY_DELAYED", "DATA_STALE",
                 "MODEL_UNVERIFIED", "версии:")


def listing_soon(now: datetime) -> str | None:
    """Анонс старта из watcher.log, попадающий в «тихое» окно вокруг now (UTC)."""
    path = LOGS / "watcher.log"
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines()[-300:]:
        m = re.search(r"старт (\d\d)\.(\d\d) (\d\d):(\d\d) UTC", line)
        if not m:
            continue
        day, month, hour, minute = map(int, m.groups())
        start = datetime(now.year, month, day, hour, minute, tzinfo=timezone.utc)
        if start - QUIET_BEFORE <= now <= start + QUIET_AFTER:
            return line.strip()
    return None


def record_for(sym: str, stamp: str) -> Path | None:
    """Запись того же запуска наблюдателя: метка совпадает или отличается на секунды; иначе — последняя."""
    recs = sorted(LOGS.glob(f"rec_{sym}_2*.jsonl.gz"))
    if not recs:
        return None
    t = datetime.strptime(stamp, "%Y%m%d_%H%M%S")
    near = [p for p in recs if abs((datetime.strptime(p.name[len(f"rec_{sym}_"):-9], "%Y%m%d_%H%M%S") - t)
                                   .total_seconds()) <= 300]
    return near[-1] if near else recs[-1]


def main() -> None:
    load_dotenv(ROOT / ".env")
    try:  # контроль работоспособности супервизора восстановления проб (решение клиента 12.11 вечер, C-04)
        import probe_risk
        stale = probe_risk.supervisor_stale()
        if stale:
            print(stale, flush=True)
    except Exception as e:  # noqa: BLE001 — разбор листингов не должен падать из-за проверки
        print(f"проверка супервизора: {type(e).__name__}: {e}", flush=True)
    now = datetime.now(timezone.utc)
    soon = listing_soon(now)
    for journal in sorted(LOGS.glob("paperrec_*_2*.jsonl.gz")):
        m = re.match(r"paperrec_(.+)_(\d{8}_\d{6})\.jsonl\.gz$", journal.name)
        if not m:
            continue
        sym, stamp = m.groups()
        out = LOGS / f"p0_three_way_{sym}_{stamp}.txt"
        paper_log = LOGS / f"paper_{sym}_{stamp}.jsonl"
        if out.exists() or not paper_log.exists() or '"event": "SUMMARY"' not in paper_log.read_text(encoding="utf-8"):
            continue
        if soon:
            print(f"{now:%Y-%m-%d %H:%M} {sym}: отложено — {soon}", flush=True)
            return
        rec = record_for(sym, stamp)
        if rec is None:
            print(f"{now:%Y-%m-%d %H:%M} {sym}: записи нет", flush=True)
            continue
        r = subprocess.run([sys.executable, str(ROOT / "tools" / "p0_three_way.py"), "--rec", str(rec),
                            "--journal", str(journal), "--paper", str(paper_log)],
                           capture_output=True, text=True, timeout=900, cwd=ROOT / "tools")
        out.write_text(r.stdout + (f"\n--- stderr ---\n{r.stderr}" if r.returncode else ""), encoding="utf-8")
        lines = [ln.strip() for ln in r.stdout.splitlines() if any(k in ln for k in SUMMARY_LINES)]
        status = "ошибка разбора" if r.returncode else "готово"
        notify(f"P0: три соединения — {sym} ({status})\n" + "\n".join(lines)[:3500] + f"\nотчёт: logs/{out.name}")
        print(f"{now:%Y-%m-%d %H:%M} {sym}: {status}, {out.name}", flush=True)


if __name__ == "__main__":
    main()
