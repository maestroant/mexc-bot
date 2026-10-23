# _trash — отказались, не используется

С 23.10.2026 проект работает только через официальный API MEXC (Spot API v3: REST `api.mexc.com`, WebSocket `wbs-api.mexc.com`). Сюда перенесено всё, что опиралось на неофициальный API сайта и автоматизацию браузера:

- `tools/web_channel.py` — веб-канал (вариант B): ордера через внутреннюю функцию `request` сайта в залогиненном Playwright.
- `doc/veb_kanal.md` — решение о веб-канале, разведка веб-API и замеры (вынесено из `doc/zhurnal_rabot.md` и `doc/resheniya_po_voprosam.md`).
- `.browser/` — профиль Chromium с сессией MEXC и `mexc.com.cookies.json` — экспорт cookies. **Оба равны паролю от аккаунта**, в git не попадают (`.gitignore`). Если больше не нужны — удалить и завершить сессии в настройках безопасности MEXC.
