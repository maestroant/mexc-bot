"""Уведомления в Telegram. Без TG_BOT_TOKEN / TG_CHAT_ID в окружении ничего не делает."""

import json
import os
import urllib.request


def notify(text: str) -> None:
    token = os.environ.get("TG_BOT_TOKEN")
    chat_id = os.environ.get("TG_CHAT_ID")
    if not token or not chat_id:
        return
    body = json.dumps({"chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": True}).encode()
    req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage", data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:  # уведомление не должно ронять пробу
        print(f"telegram: {type(e).__name__}: {e}", flush=True)
