"""Уведомления в Telegram. Без TG_BOT_TOKEN / TG_CHAT_ID в окружении ничего не делает.
TG_CHAT_ID — один id или несколько через запятую (каждому получателю нужно сначала нажать /start у бота).
TG_LISTING_CHAT_ID — получатели анонсов листингов (notify(..., to="TG_LISTING_CHAT_ID")); не задан — TG_CHAT_ID.

notify() не ждёт Telegram: сообщение уходит в очередь, его отправляет фоновый поток. С сервера вызов идёт
0.7–1 с на получателя, а IPv6-маршрут иногда висит до таймаута 10 с — синхронная отправка в момент
открытия листинга останавливала пробу на 11 с. Порядок сообщений сохраняется; при выходе процесс
дожидается отправки всего, что в очереди."""

import atexit
import json
import os
import queue
import threading
import urllib.request

_queue: queue.Queue = queue.Queue()
_worker: threading.Thread | None = None
_lock = threading.Lock()


def recipients(var: str) -> list[str]:
    return [c.strip() for c in os.environ.get(var, "").split(",") if c.strip()]


def notify(text: str, to: str = "TG_CHAT_ID") -> None:
    token = os.environ.get("TG_BOT_TOKEN")
    chat_ids = recipients(to) or recipients("TG_CHAT_ID")
    if not token or not chat_ids:
        return
    _start_worker()
    _queue.put((token, chat_ids, text))


def _start_worker() -> None:
    global _worker
    with _lock:
        if _worker is None:
            _worker = threading.Thread(target=_run, name="telegram", daemon=True)
            _worker.start()
            atexit.register(_queue.join)  # демон-поток жив во время atexit: дождаться отправки очереди


def _run() -> None:
    while True:
        token, chat_ids, text = _queue.get()
        try:
            for chat_id in chat_ids:
                _send(token, chat_id, text)
        finally:
            _queue.task_done()


def _send(token: str, chat_id: str, text: str) -> None:
    body = json.dumps({"chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": True}).encode()
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:  # уведомление не должно ронять пробу
        print(f"telegram {chat_id}: {type(e).__name__}: {e}", flush=True)
