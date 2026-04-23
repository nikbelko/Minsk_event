"""
Общие константы и вспомогательные SQL-функции проекта MinskDvizh.
Используются в api.py и bot_enhanced.py.
"""

import os
from datetime import timezone, timedelta, date as _date

# ── Timezone ─────────────────────────────────────────────────────────────────

MINSK_TZ = timezone(timedelta(hours=3))  # UTC+3

# ── Инфраструктура ────────────────────────────────────────────────────────────

DB_PATH  = os.getenv("DB_PATH", "/data/events_final.db")
ADMIN_ID = int(os.getenv("ADMIN_ID", "502917728"))

# ── Часы работы заведений без явного времени (выставки, музеи и т.п.) ────────
# Вне этого окна события без show_time скрываются (считаются закрытыми)

VENUE_OPEN_TIME  = "09:00"
VENUE_CLOSE_TIME = "21:00"

# ── Batch-загрузка событий ────────────────────────────────────────────────────

BATCH_TEMPLATE_HEADERS = [
    "title", "details", "category", "event_date", "show_time",
    "place", "address", "price", "description", "source_url", "is_promo"
]

BATCH_TEMPLATE_EXAMPLE = [
    "Концерт джаза", "Вечер живой музыки для всех", "concert",
    "15.05.2026", "19:00", "Джаз-клуб Blue Note", "ул. Немига, 3",
    "от 20 руб", "Программа из классики и современного джаза", "https://example.com", "0"
]

# ── Общие SQL-утилиты (используются и в api.py, и в bot_enhanced.py) ─────────


def _build_time_filter(date_filter: str, today: str, now_time: str) -> tuple[str, list]:
    """SQL-условие БЕЗ 'AND' для фильтрации прошедших событий на сегодня.
    Возвращает ("", []) для любой даты кроме today.

    Логика:
      нет show_time → показываем до VENUE_CLOSE_TIME (21:00).
      есть end_time → фильтруем по end_time > now (учитывает переход через полночь).
      нет end_time  → фильтруем по show_time > now.
    """
    if date_filter != today:
        return "", []

    venue_open = now_time < VENUE_CLOSE_TIME

    if venue_open:
        return (
            "(show_time = '' OR show_time IS NULL "
            "OR ((end_time != '' AND end_time IS NOT NULL AND (end_time > ? OR end_time < show_time)) "
            "OR ((end_time = '' OR end_time IS NULL) AND show_time > ?)))"
        ), [now_time, now_time]
    else:
        # Вне рабочих часов — только события с явным временем, которые ещё не закончились
        return (
            "((end_time != '' AND end_time IS NOT NULL AND (end_time > ? OR end_time < show_time)) "
            "OR ((end_time = '' OR end_time IS NULL) AND show_time != '' AND show_time IS NOT NULL AND show_time > ?))"
        ), [now_time, now_time]


def _build_overnight_union(
    target_date_str: str,
    now_t: str | None,
    category: str | None,
) -> tuple[str, list] | None:
    """SQL-фрагмент для событий, переходящих через полночь (хранятся в D-1, видны в D).

    Midnight-crossing: show_time >= '20:00', end_time <= '08:00', end_time < show_time.
    now_t — текущее HH:MM (для сегодня, фильтрует уже закончившиеся); None для будущих дат.
    Возвращает None, если overnight-события гарантированно отсутствуют (now_t >= '08:00').

    Первый параметр в SQL — prev_date (включён в возвращаемый список params).
    """
    if now_t is not None and now_t >= "08:00":
        return None

    prev_date = (_date.fromisoformat(target_date_str) - timedelta(days=1)).isoformat()

    sql = """
        SELECT id, title, details, description, event_date, show_time, end_time,
               place, location, price, category, source_url, source_name, is_kids
        FROM events
        WHERE event_date = ?
          AND show_time >= '20:00'
          AND end_time IS NOT NULL AND end_time != ''
          AND end_time <= '08:00'
          AND end_time < show_time
    """
    params: list = [prev_date]

    if category == "free":
        sql += " AND price = 'Бесплатно'"
    elif category == "kids":
        sql += " AND is_kids = 1"
    elif category and category != "all":
        sql += " AND category = ?"
        params.append(category)

    if now_t is not None:
        sql += " AND end_time > ?"
        params.append(now_t)

    return sql, params


BATCH_CATEGORY_MAP = {
    "кино": "cinema", "cinema": "cinema",
    "концерт": "concert", "концерты": "concert", "concert": "concert",
    "театр": "theater", "theater": "theater",
    "выставка": "exhibition", "выставки": "exhibition", "exhibition": "exhibition",
    "детям": "kids", "дети": "kids", "kids": "kids",
    "спорт": "sport", "sport": "sport",
    "движ": "party", "вечеринка": "party", "party": "party",
    "бесплатно": "free", "free": "free",
    "экскурсия": "excursion", "excursion": "excursion",
    "маркет": "market", "market": "market",
    "мастер-класс": "masterclass", "masterclass": "masterclass",
    "настолки": "boardgames", "boardgames": "boardgames",
    "трансляция": "broadcast", "broadcast": "broadcast",
    "обучение": "education", "education": "education",
    "квиз": "quiz", "quiz": "quiz",
    "фест": "fest", "фесты": "fest", "fest": "fest",
}
