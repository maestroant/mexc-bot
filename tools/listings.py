"""Листинги MEXC по официальному API: будущие пары и время старта из GET /api/v3/exchangeInfo.

Как листинг выглядит в exchangeInfo (13 листингов 06–22.10.2026):
  * крупные листинги появляются за сутки и больше со status=2, firstOpenTime пустой;
  * за 5–85 мин до старта status становится 1 и появляется firstOpenTime — время открытия торгов (мс, UTC);
    на всех 13 листингах оно совпало с фактическим открытием до секунды.
    «Быстрые» листинги сразу появляются со status=1 и firstOpenTime за 5–10 мин до старта;
  * status «мигает» (1 → 2 → 1), и вместе с ним пропадает firstOpenTime — пустое значение не считается изменением;
  * перенесённый листинг пропадает из exchangeInfo (MTO 13.10) и потом появляется снова со status=2;
  * status=2 у пары с историей сделок — приостановка или делистинг, а не листинг.

ListingTracker сравнивает очередной ответ exchangeInfo с прошлым и возвращает события:
  upcoming     новая пара со status≠1, время старта ещё не объявлено
  scheduled    стало известно время старта
  rescheduled  время старта изменилось
  reminder     до старта осталось remind_ms (если о времени объявили больше чем за 15 мин)
  removed      пара пропала из exchangeInfo до старта (перенос или отмена)
format_event() — текст уведомления в Telegram.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

LISTED = "1"
REMOVED_AFTER_MS = 5 * 60_000  # пропуск пары в одном ответе бывает — снятой считаем после 5 мин отсутствия
JUST_OPENED_MS = 10 * 60_000  # новая пара со status=1, открывшаяся не раньше 10 мин назад, — ещё листинг
FORGET_AFTER_MS = 60 * 60_000  # через час после старта пара больше не отслеживается
REMIND_MIN_NOTICE_MS = 15 * 60_000  # «быстрым» листингам (время объявлено за 5–10 мин) напоминание не нужно
LOCAL_TZ = "Europe/Kyiv"


@dataclass
class Event:
    kind: str  # upcoming | scheduled | rescheduled | reminder | removed
    symbol: str
    base: str
    quote: str
    name: str | None
    zones: list[str] = field(default_factory=list)
    open_ms: int | None = None
    prev_open_ms: int | None = None
    api_allowed: bool | None = None  # isSpotTradingAllowed на момент события


class ListingTracker:
    """tracked и ignored — словари из файла состояния; трекер меняет их на месте.
    tracked: SYMBOL → {base, quote, name, zones, open_ms, first_seen, missing_since, announced, reminded};
    ignored: SYMBOL → {ts, status} — пары со status≠1 и историей сделок (не листинги).
    remind_ms — за сколько мс до старта напомнить (0 — без напоминаний)."""

    def __init__(self, tracked: dict, ignored: dict, remind_ms: int = 5 * 60_000):
        self.tracked = tracked
        self.ignored = ignored
        self.remind_ms = remind_ms

    def update(self, symbols: dict[str, dict], now: int, has_trades: Callable[[str], bool]) -> list[Event]:
        """symbols — пары из exchangeInfo по имени, now — время в мс. has_trades вызывается
        только для новых пар со status≠1, чтобы отличить листинг от приостановки."""
        events = []
        for sym in [s for s in self.ignored if s not in symbols]:
            del self.ignored[sym]
        for sym, rec in list(self.tracked.items()):
            opened = rec["open_ms"] is not None and rec["open_ms"] <= now
            if sym in symbols:
                rec["missing_since"] = None
            elif rec["missing_since"] is None:
                rec["missing_since"] = now
            elif now - rec["missing_since"] >= REMOVED_AFTER_MS:
                del self.tracked[sym]
                if not opened:
                    events.append(self._event("removed", sym, rec, open_ms=rec["open_ms"]))
                continue
            if opened and now - rec["open_ms"] > FORGET_AFTER_MS:
                del self.tracked[sym]

        for sym, info in symbols.items():
            if sym in self.ignored:
                continue
            open_ms = info.get("firstOpenTime") or None
            rec = self.tracked.get(sym)
            if rec is None:
                if info.get("status") == LISTED:
                    if not open_ms or open_ms < now - JUST_OPENED_MS:
                        continue  # давно торгуется
                elif has_trades(sym):
                    self.ignored[sym] = {"ts": now, "status": info.get("status")}
                    continue
                rec = self.tracked[sym] = {"base": info.get("baseAsset"), "quote": info.get("quoteAsset"),
                                           "open_ms": None, "first_seen": now, "missing_since": None}
                rec.update(name=info.get("fullName"), zones=info.get("conceptPlates") or [])
                if not open_ms:
                    events.append(self._event("upcoming", sym, rec, info))
            rec.update(name=info.get("fullName") or rec["name"], zones=info.get("conceptPlates") or rec["zones"])
            if open_ms and open_ms != rec["open_ms"]:
                kind = "scheduled" if rec["open_ms"] is None else "rescheduled"
                events.append(self._event(kind, sym, rec, info, open_ms=open_ms, prev_open_ms=rec["open_ms"]))
                rec.update(open_ms=open_ms, announced=now, reminded=False)
            start = rec["open_ms"]
            if self.remind_ms and start and not rec.get("reminded") and start - self.remind_ms <= now < start:
                rec["reminded"] = True
                if start - rec.get("announced", now) >= REMIND_MIN_NOTICE_MS:
                    events.append(self._event("reminder", sym, rec, info, open_ms=start))
        return events

    @staticmethod
    def _event(kind: str, sym: str, rec: dict, info: dict | None = None, **times) -> Event:
        return Event(kind, sym, rec["base"], rec["quote"], rec["name"], list(rec["zones"]),
                     api_allowed=info.get("isSpotTradingAllowed") if info else None, **times)


def fmt_time(ms: int) -> str:
    """'22.10 12:00 UTC (15:00 Киев)'; без базы часовых поясов (Windows без tzdata) — только UTC."""
    utc = datetime.fromtimestamp(ms / 1000, timezone.utc)
    text = f"{utc:%d.%m %H:%M} UTC"
    try:
        text += f" ({utc.astimezone(ZoneInfo(LOCAL_TZ)):%H:%M} Киев)"
    except ZoneInfoNotFoundError:
        pass
    return text


def fmt_left(delta_ms: int) -> str:
    if delta_ms <= 0:
        return "торги уже открыты"
    minutes = -(-delta_ms // 60_000)  # вверх до минуты
    if minutes < 60:
        return f"через {minutes} мин"
    return f"через {minutes // 60} ч {minutes % 60:02d} мин"


def format_event(ev: Event, now: int) -> str:
    pair = f"{ev.base}/{ev.quote}"
    if ev.kind == "upcoming":
        head = f"🆕 MEXC: будущий листинг {pair}. Время старта пока не объявлено"
    elif ev.kind == "removed":
        was = f", старт был назначен на {fmt_time(ev.open_ms)}" if ev.open_ms else ""
        head = f"❌ MEXC: {pair} убрана из списка пар до старта{was}. Листинг перенесён или отменён"
    elif ev.kind == "rescheduled":
        head = (f"🔁 MEXC: {pair} — старт перенесён с {fmt_time(ev.prev_open_ms)} "
                f"на {fmt_time(ev.open_ms)}, {fmt_left(ev.open_ms - now)}")
    elif ev.kind == "reminder":
        head = f"🔔 MEXC: {pair} — старт {fmt_left(ev.open_ms - now)}, в {fmt_time(ev.open_ms)}"
    else:
        head = f"⏰ MEXC: {pair} — старт {fmt_time(ev.open_ms)}, {fmt_left(ev.open_ms - now)}"
    lines = [head, " · ".join(x for x in (ev.name, ", ".join(ev.zones)) if x)]
    if ev.kind in ("scheduled", "rescheduled", "reminder") and ev.api_allowed is not None:
        lines.append(f"Торговля через API сейчас {'открыта' if ev.api_allowed else 'закрыта'}")
    lines.append(f"https://www.mexc.com/exchange/{ev.base}_{ev.quote}")
    return "\n".join(line for line in lines if line)
