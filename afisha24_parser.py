#!/usr/bin/env python3
# afisha24_parser.py
# Парсер для 24afisha.by — театры и спорт в Минске

import os
import re
import sqlite3
import logging
import time
from datetime import date, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup
from normalizer import (
    normalize_place, normalize_title, normalize_price,
    parse_text_date, is_future_date, titles_are_similar
)

# ── БД ───────────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "/data/events_final.db")
if not os.path.exists("/data"):
    DB_PATH = "events_final.db"

# ── Логирование ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("afisha24_parser.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Константы ─────────────────────────────────────────────────────────────────
BASE_URL    = "https://24afisha.by"
SOURCE_NAME = "24afisha.by"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
}

# ── Конфигурация страниц ──────────────────────────────────────────────────────
# path         — URL-путь
# category     — категория в БД
# label        — для логов
# venue        — если страница конкретного места: фиксированное название place
#                если None — place берётся из h2 перед каждым ul (общая страница)
PAGES = [
    {
        "path":     "/ru/minsk/events/gorkogo",
        "category": "theater",
        "label":    "Театр Горького",
        "venue":    "Театр им. Горького",  # fixed_venue для venue-страниц
    },
    {
        "path":     "/ru/minsk/events/bolshoj",
        "category": "theater",
        "label":    "Большой театр",
        "venue":    "Большой театр",
    },
]

# DATED_PAGES — страницы с датой в URL: /path/DDMMYYYY
# venue=None    → h2 перед ul задаёт место (venue-страницы типа Горького/Большого)
# venue_fallback → если h2 нет (dated-страницы /theatre/DDMMYYYY) — используем это имя
DATED_PAGES = [
    {
        "path":           "/ru/minsk/events/theatre",
        "category":       "theater",
        "label":          "Театры",
        "venue":          None,   # place из h2 если есть
        "venue_fallback": "",     # "" = загружать место со страницы события
        "fetch_places":   True,   # запрашивать detail-страницу для получения места
    },
    # Спорт /events/sport/DDMMYYYY — рендерится клиентским JS, ul всегда пустой.
    # Отключён до нахождения рабочего API.
    # {
    #     "path":           "/ru/minsk/events/sport",
    #     "category":       "sport",
    #     "label":          "Спорт",
    #     "venue":          None,
    #     "venue_fallback": "",
    #     "fetch_places":   True,
    # },
]

# Сколько дней вперёд перебирать для DATED_PAGES
DATED_DAYS_AHEAD = 30


# ── Утилиты ───────────────────────────────────────────────────────────────────

def fetch_page(url: str, retries: int = 3) -> Optional[str]:
    for attempt in range(retries):
        try:
            logger.info(f"Попытка {attempt + 1}/{retries}: {url}")
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            r.encoding = "utf-8"
            logger.info(f"Загружено ({len(r.text)} симв.)")
            return r.text
        except Exception as e:
            logger.warning(f"Ошибка: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None


def parse_date_field(raw: str) -> str:
    """
    Парсит текст даты из карточки:
      "28 марта"    → "2026-03-28"
      "с 28 марта"  → "2026-03-28"  (начало периода — берём как event_date)
      "с 1 января"  → следующий год если уже прошло
    Возвращает "" если не распознано.
    """
    if not raw:
        return ""
    # Убираем префикс "с " / "по "
    clean = re.sub(r"^(с|по)\s+", "", raw.strip(), flags=re.IGNORECASE)
    return parse_text_date(clean)


def parse_price(raw: str) -> str:
    """
    "от  20.00 BYN" / "до  38.00 BYN" → normalize_price
    """
    if not raw:
        return ""
    # "до X BYN" — тоже цена, нормализуем как обычно
    clean = re.sub(r"^до\s+", "", raw.strip(), flags=re.IGNORECASE)
    return normalize_price(clean)


# ── Кеш: место по event_id ────────────────────────────────────────────────────
# Заполняется при парсинге dated-страниц; один запрос на уникальный event_id.
_place_cache: dict[str, str] = {}


def fetch_event_place(event_id: str) -> str:
    """
    Загружает страницу события и извлекает место из JSON-LD.
    Кеширует результат: повторный вызов с тем же ID не делает HTTP-запрос.
    Возвращает "" при ошибке или если место не найдено.
    """
    if event_id in _place_cache:
        return _place_cache[event_id]

    url = f"{BASE_URL}/ru/minsk/event/{event_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        place = ""
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                import json as _json
                d = _json.loads(s.string or "")
                if d.get("@type") == "Event":
                    loc = d.get("location", {})
                    place = normalize_place(loc.get("name", "")) or loc.get("name", "")
                    if not place:
                        # Fallback: адрес
                        addr = loc.get("address", {}).get("streetAddress", "")
                        place = addr
                    break
            except Exception:
                continue

        _place_cache[event_id] = place
        if place:
            logger.debug(f"  Место для {event_id}: {place}")
        return place

    except Exception as e:
        logger.debug(f"  fetch_event_place({event_id}): {e}")
        _place_cache[event_id] = ""
        return ""


# ── Парсинг HTML ──────────────────────────────────────────────────────────────

def parse_html(html: str, fixed_venue: Optional[str], category: str,
               venue_fallback: str = "", fetch_places: bool = False) -> list[dict]:
    """
    Разбирает HTML страницы 24afisha.by.

    Три режима в зависимости от fixed_venue/venue_fallback:

    1. fixed_venue задан (venue-страница: Горького, Большой):
       Берём только ul у которых нет h2 или h2 совпадает с fixed_venue.
       Все события получают place = fixed_venue.

    2. fixed_venue=None, venue_fallback="" (dated-страница /theatre/DDMMYYYY):
       На этих страницах h2 отсутствует. Берём все ul.
       Если fetch_places=True — место загружается со страницы события (JSON-LD),
       с кешированием по event_id.

    3. fixed_venue=None, venue_fallback не задан (общая страница со смешанными h2):
       Берём ul только у которых h2 = конкретное место (не рубрика из SKIP_H2).
    """
    # Рубрики-заголовки которые НЕ являются названиями мест
    SKIP_H2 = {
        "рекомендуем", "кино", "театр", "квесты и квизы", "детям",
        "концерты", "спорт", "фестивали", "музеи и выставки",
        "обучение и курсы", "активный отдых", "катки", "вечеринки",
        "выставки", "цирк", "стендап", "юмор",
    }

    soup = BeautifulSoup(html, "lxml")
    events = []
    seen_keys = set()

    # Режим dated-страницы: venue=None явно передан через venue_fallback=""
    is_dated_mode = (fixed_venue is None and venue_fallback is not None)

    for ul in soup.find_all("ul", class_="events__list"):
        h2 = ul.find_previous_sibling("h2")
        h2_text = h2.get_text(strip=True) if h2 else ""

        if fixed_venue:
            # Режим 1: venue-страница — берём только совпадающие ul
            if h2_text and not titles_are_similar(h2_text, fixed_venue, threshold=0.5):
                continue
            venue = fixed_venue

        elif is_dated_mode:
            # Режим 2: dated-страница — h2 нет, venue будет определён ниже по event_id
            if h2_text and h2_text.lower() not in SKIP_H2:
                venue = normalize_place(h2_text) or h2_text
            else:
                venue = venue_fallback  # будет перезаписан при fetch_places

        else:
            # Режим 3: общая страница — venue только из h2, рубрики пропускаем
            if not h2_text or h2_text.lower() in SKIP_H2:
                continue
            venue = normalize_place(h2_text) or h2_text

        for li in ul.find_all("li", class_="gu24-event"):
            # Определяем место через detail-страницу если нужно
            card_venue = venue
            if fetch_places and is_dated_mode and not venue:
                a_tag = li.find("a")
                if a_tag:
                    href = a_tag.get("href", "")
                    m = re.search(r"/event/(\d+)", href)
                    if m:
                        card_venue = fetch_event_place(m.group(1))

            ev = parse_card(li, card_venue, category)
            if ev is None:
                continue

            key = (normalize_title(ev["title"]), ev["event_date"])
            if key in seen_keys:
                logger.debug(f"  Внутр. дубль: {ev['title']}")
                continue
            seen_keys.add(key)
            events.append(ev)

    logger.info(f"Распарсено событий: {len(events)}")
    return events


def parse_card(li: BeautifulSoup, venue: str, category: str) -> Optional[dict]:
    """Парсит одну карточку li.gu24-event."""
    try:
        a = li.find("a")
        if not a:
            return None
        href = a.get("href", "")
        source_url = BASE_URL + href if href.startswith("/") else href

        # Название
        name_el = li.find("p", class_="gu24-event__name")
        if not name_el:
            return None
        title = name_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        # Дата
        date_el = li.find("div", class_="gu24-event__price")
        if date_el:
            for svg in date_el.find_all("svg"):
                svg.decompose()
            date_raw = date_el.get_text(strip=True)
        else:
            date_raw = ""
        event_date = parse_date_field(date_raw)

        if not event_date:
            logger.debug(f"  Нет даты: {title} / {date_raw!r}")
            return None

        if not is_future_date(event_date, max_days=180):
            logger.debug(f"  Устарело: {title} / {event_date}")
            return None

        # Цена
        price_el = li.find("span", class_="prices-bottom")
        price = parse_price(price_el.get_text(strip=True)) if price_el else ""

        # Описание
        description = f"{_category_emoji(category)} {title}"
        if venue:
            description += f"\n🏢 {venue}"
        if price:
            description += f"\n💰 {price}"

        return {
            "title":       title,
            "details":     "",
            "description": description,
            "event_date":  event_date,
            "show_time":   "",          # 24afisha не даёт время сеанса на листинге
            "place":       venue,
            "location":    "Минск",
            "price":       price,
            "category":    category,
            "source_url":  source_url,
            "source_name": SOURCE_NAME,
        }

    except Exception as e:
        logger.error(f"Ошибка парсинга карточки: {e}")
        return None


def _category_emoji(category: str) -> str:
    return {
        "theater": "🎭",
        "sport":   "🏟️",
    }.get(category, "🎉")


# ── БД: индекс и дедупликация ─────────────────────────────────────────────────

def load_existing_index() -> dict:
    """Загружает события НЕ из 24afisha для проверки дублей."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT title, event_date, place
        FROM events
        WHERE source_name != ?
    """, (SOURCE_NAME,))
    rows = cursor.fetchall()
    conn.close()

    index = {}
    for title, ev_date, place in rows:
        norm = normalize_title(title)
        index.setdefault(ev_date, []).append((norm, place or ""))
    logger.info(f"Индекс: {sum(len(v) for v in index.values())} событий из других источников")
    return index


def is_duplicate(ev: dict, index: dict) -> bool:
    """Проверяет дубль с событиями из других источников."""
    candidates = index.get(ev["event_date"], [])
    if not candidates:
        return False
    norm = normalize_title(ev["title"])
    place = ev["place"] or ""
    for norm_ex, ex_place in candidates:
        # Точное совпадение нормализованного названия
        if norm and norm == norm_ex:
            return True
        # Мягкое совпадение + совпадение места
        if place and ex_place == place and titles_are_similar(ev["title"], norm_ex):
            return True
    return False


# ── БД: сохранение ────────────────────────────────────────────────────────────

def clean_old_events():
    """Удаляем устаревшие события 24afisha из БД."""
    today = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM events WHERE source_name = ? AND event_date < ?",
        (SOURCE_NAME, today)
    )
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted:
        logger.info(f"Удалено устаревших: {deleted}")


def save_events(events: list) -> int:
    """Сохраняет события через INSERT OR IGNORE (уникальность по title+date+place)."""
    if not events:
        return 0
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    saved = 0
    for ev in events:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO events
                  (title, details, description, event_date, show_time,
                   place, location, price, category, source_url, source_name)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ev["title"], ev["details"], ev["description"],
                ev["event_date"], ev["show_time"],
                ev["place"], ev["location"], ev["price"],
                ev["category"], ev["source_url"], ev["source_name"],
            ))
            if cursor.rowcount > 0:
                saved += 1
        except Exception as e:
            logger.error(f"Ошибка сохранения '{ev['title']}': {e}")
    conn.commit()
    conn.close()
    return saved


# ── Главный запуск ────────────────────────────────────────────────────────────

def _process_page(url: str, venue: Optional[str], category: str,
                  index: dict, venue_fallback: str = "",
                  fetch_places: bool = False) -> tuple[int, int, int]:
    """
    Загружает одну страницу, парсит, фильтрует дубли, сохраняет.
    Возвращает (found, saved, dup).
    """
    html = fetch_page(url)
    if not html:
        logger.error(f"Не удалось загрузить {url}")
        return 0, 0, 0

    events = parse_html(html, fixed_venue=venue, category=category,
                        venue_fallback=venue_fallback, fetch_places=fetch_places)
    if not events:
        return 0, 0, 0

    unique, dup = [], 0
    for ev in events:
        if is_duplicate(ev, index):
            logger.debug(f"  Дубль: {ev['title']}")
            dup += 1
        else:
            unique.append(ev)
            norm = normalize_title(ev["title"])
            index.setdefault(ev["event_date"], []).append((norm, ev["place"] or ""))

    saved = save_events(unique)
    return len(events), saved, dup


def run():
    logger.info("=" * 60)
    logger.info("🎭 24AFISHA парсер запущен")
    logger.info("=" * 60)

    clean_old_events()
    index = load_existing_index()

    total_found = total_saved = total_dup = 0

    # ── Venue-страницы (SSR, без даты в URL) ──────────────────────────────────
    for page in PAGES:
        url = BASE_URL + page["path"]
        label, category, venue = page["label"], page["category"], page["venue"]
        logger.info(f"\n📥 {label}: {url}")

        found, saved, dup = _process_page(url, venue, category, index)
        total_found += found; total_saved += saved; total_dup += dup

        logger.info(f"  Найдено: {found}, уникальных: {found-dup}, сохранено: {saved}")
        print(f"RESULT:{label}:{found}:{saved}")
        time.sleep(1)

    # ── Dated-страницы: перебор дней /path/DDMMYYYY ───────────────────────────
    for page in DATED_PAGES:
        label    = page["label"]
        category = page["category"]
        venue    = page["venue"]
        fallback = page.get("venue_fallback", "")
        do_fetch = page.get("fetch_places", False)
        label_found = label_saved = label_dup = 0

        logger.info(f"\n📅 {label} (dated, {DATED_DAYS_AHEAD} дней)")
        today = date.today()

        for delta in range(DATED_DAYS_AHEAD):
            day = today + timedelta(days=delta)
            date_str = day.strftime("%d%m%Y")
            url = f"{BASE_URL}{page['path']}/{date_str}"

            found, saved, dup = _process_page(url, venue, category, index,
                                              venue_fallback=fallback,
                                              fetch_places=do_fetch)
            label_found += found; label_saved += saved; label_dup += dup
            total_found += found; total_saved += saved; total_dup += dup

            if found:
                logger.info(f"  {day.isoformat()}: найдено {found}, сохранено {saved}")
            time.sleep(0.5)

        print(f"RESULT:{label}:{label_found}:{label_saved}")

    logger.info(f"\n📊 Итог: найдено {total_found}, дублей {total_dup}, сохранено {total_saved}")
    logger.info("=" * 60)
    return total_saved


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "dump":
        # Сохраняет реальный HTML dated-страниц для отладки структуры
        today = date.today()
        date_str = today.strftime("%d%m%Y")
        for section, fname in [
            (f"/ru/minsk/events/theatre/{date_str}", "dump_theatre.html"),
            (f"/ru/minsk/events/sport/{date_str}",   "dump_sport.html"),
        ]:
            url = BASE_URL + section
            print(f"Загружаю {url}...")
            html = fetch_page(url)
            if html:
                with open(fname, "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"  Сохранено в {fname} ({len(html)} симв.)")
                soup2 = BeautifulSoup(html, "lxml")
                print(f"  ul.events__list: {len(soup2.find_all('ul', class_='events__list'))}")
                print(f"  li.gu24-event:   {len(soup2.find_all('li', class_='gu24-event'))}")
                print(f"  h2: {[h.get_text(strip=True) for h in soup2.find_all('h2')]}")
            else:
                print(f"  Ошибка загрузки")
    else:
        run()
