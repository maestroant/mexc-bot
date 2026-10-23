"""Уведомления в Telegram. Без TG_BOT_TOKEN / TG_CHAT_ID в окружении ничего не делает.
TG_CHAT_ID — один id или несколько через запятую (каждому получателю нужно сначала нажать /start у бота).
TG_LISTING_CHAT_ID — получатели анонсов листингов (notify(..., to="TG_LISTING_CHAT_ID")); не задан — TG_CHAT_ID."""

import json
import os
import urllib.request


def recipients(var: str) -> list[str]:
    return [c.strip() for c in os.environ.get(var, "").split(",") if c.strip()]


def notify(text: str, to: str = "TG_CHAT_ID") -> None:
    token = os.environ.get("TG_BOT_TOKEN")
    chat_ids = recipients(to) or recipients("TG_CHAT_ID")
    if not token or not chat_ids:
        return
    for chat_id in chat_ids:
        body = json.dumps({"chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=body,
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=10).read()
        except Exception as e:  # уведомление не должно ронять пробу
            print(f"telegram {chat_id}: {type(e).__name__}: {e}", flush=True)
