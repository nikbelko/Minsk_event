"""
Общие константы проекта MinskDvizh.
Используются в api.py и bot_enhanced.py.
"""

import os
from datetime import timezone, timedelta

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
}
