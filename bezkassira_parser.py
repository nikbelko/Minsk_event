#!/usr/bin/env python3
# bezkassira_parser.py
# Парсер для bezkassira.by — концерты и вечеринки в Минске

import os
import json
import re
import sqlite3
import logging
import time
from datetime import datetime, date
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
from normalizer import normalize_place, normalize_title, parse_iso_datetime, parse_text_date, format_price_from_offers, is_future_date

# ── БД ──────────────────────────────────────────────────────────────────────
if os.path.exists('/data'):
    DB_PATH = '/data/events_final.db'
else:
    DB_PATH = 'events_final.db'

# ── Логирование ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bezkassira_parser.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Константы ────────────────────────────────────────────────────────────────
BASE_URL     = "https://bezkassira.by"
MINSK_CITY_ID = "24811"
SOURCE_NAME  = "bezkassira.by"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}

CATEGORIES = [
    {"url": f"{BASE_URL}/events/concert/", "category": "concert",  "label": "концертов"},
    {"url": f"{BASE_URL}/events/party/",   "category": "party",    "label": "вечеринок"},
    {"url": f"{BASE_URL}/events/humor-minsk/",  "category": "concert",  "label": "юмора"},
    {"url": f"{BASE_URL}/events/biznes/",  "category": "education",  "label": "обучений"},
    {"url": f"{BASE_URL}/events/it_i_internet/",  "category": "education",  "label": "обучений"},
]






# ── Вспомогательные функции ──────────────────────────────────────────────────

def fetch_page(url: str, retries: int = 3) -> Optional[str]:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.warning(f"Попытка {attempt+1}/{retries} не удалась: {url} — {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None










# ── Основной класс ──────────────────────────────────────────────────────────

class BezkassiraParser:

    def __init__(self):
        self.stats = {
            "found": 0,
            "saved": 0,  # накапливается
            "duplicates": 0,
            "non_minsk": 0,
            "errors": 0,
            "by_category": {},   # label → {found, saved}
        }

    # ── Загрузка индекса существующих событий ──

    def load_existing_index(self) -> dict:
        """Загружает все не-bezkassira события в память для проверки дублей."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT title, event_date, place, show_time
            FROM events
            WHERE source_name != ?
        """, (SOURCE_NAME,))
        rows = cursor.fetchall()
        conn.close()
        index = {}
        for title, ev_date, place, show_time in rows:
            norm = normalize_title(title)
            index.setdefault(ev_date, []).append((norm, place or "", show_time or ""))
        logger.info(f"📋 Индекс: {sum(len(v) for v in index.values())} событий для проверки дублей")
        return index

    def is_duplicate(self, title: str, event_date: str, place: str,
                     show_time: str, index: dict) -> bool:
        if not title or not event_date:
            return False
        candidates = index.get(event_date, [])
        if not candidates:
            return False
        norm = normalize_title(title)
        place = place or ""
        show_time = show_time or ""
        for norm_ex, ex_place, ex_time in candidates:
            # Совпадение по месту+времени — точный дубль
            if place and show_time and ex_place == place and ex_time == show_time:
                return True
            # Совпадение по нормализованному названию
            if norm and norm == norm_ex:
                return True
        return False

    # ── Парсинг одной карточки ──

    def parse_card(self, thumb: BeautifulSoup, category: str,
                   index: dict) -> Optional[Dict]:
        try:
            # 1. Только Минск — проверяем data-city_id И текст города из hint
            city_id = thumb.get("data-city_id", "")
            hint_tag = thumb.find("small", class_="hint")
            hint_city = ""
            if hint_tag:
                lines = [l.strip() for l in hint_tag.get_text("\n").split("\n") if l.strip()]
                hint_city = lines[-1] if lines else ""

            # Главный критерий — текст города из hint ("Минск")
            # city_id используем только как запасной вариант если hint пустой
            is_minsk = "Минск" in hint_city if hint_city else (city_id == MINSK_CITY_ID)
            if not is_minsk:
                self.stats["non_minsk"] += 1
                return None

            caption = thumb.find("div", class_="caption")
            if not caption:
                return None

            # 2. Название
            h5 = caption.find("h5")
            if not h5:
                return None
            title = h5.get_text(strip=True)
            if not title:
                return None

            # 3. Дата и время из JSON-LD (приоритет) или текстового div.date
            event_date = show_time = ""
            script = thumb.find("script", type="application/ld+json")
            ld = {}
            if script and script.string:
                try:
                    raw = re.sub(r'[\x00-\x1f\x7f]', ' ', script.string)
                    ld = json.loads(raw)
                except Exception:
                    pass

            if ld.get("startDate"):
                event_date, show_time = parse_iso_datetime(ld["startDate"])
            else:
                date_div = caption.find("div", class_="date")
                if date_div:
                    event_date = parse_text_date(date_div.get_text(strip=True))

            if not event_date:
                return None

            # 4. Фильтр: только будущие события (не дальше 6 месяцев)
            if not is_future_date(event_date, max_days=180):
                return None

            # 5. Место (hint уже найден выше)
            hint = hint_tag
            place = ""
            if hint:
                lines = [l.strip() for l in hint.get_text('\n').split('\n') if l.strip()]
                place = normalize_place(lines[0]) if lines else ""

            # 6. URL
            a = caption.find("a", href=True)
            url = a["href"] if a else ""
            if url and not url.startswith('http'):
                url = BASE_URL + url

            # 7. Цена
            price = ""
            offers = ld.get("offers", {})
            if offers:
                price = format_price_from_offers(offers)

            # 8. Описание
            description = ld.get("description", "")
            if description and len(description) > 300:
                description = description[:297] + "..."

            # 9. Проверка дублей
            if self.is_duplicate(title, event_date, place, show_time, index):
                self.stats["duplicates"] += 1
                logger.debug(f"  ↩ дубль: {title} / {event_date}")
                return None

            return {
                "title":       title,
                "details":     "",
                "description": description,
                "event_date":  event_date,
                "show_time":   show_time,
                "place":       place,
                "location":    "Минск",
                "price":       price,
                "category":    category,
                "source_name": SOURCE_NAME,
                "source_url":  url,
            }

        except Exception as e:
            logger.error(f"Ошибка парсинга карточки: {e}")
            self.stats["errors"] += 1
            return None

    # ── Парсинг одной страницы категории ──

    def parse_category(self, url: str, category: str, label: str,
                       index: dict) -> List[Dict]:
        logger.info(f"📥 Загружаю {label}: {url}")
        html = fetch_page(url)
        if not html:
            logger.error(f"Не удалось загрузить {url}")
            return []

        soup = BeautifulSoup(html, "lxml")
        thumbnails = soup.find_all("div", class_="thumbnail")
        logger.info(f"  Карточек на странице: {len(thumbnails)}")

        events = []
        for thumb in thumbnails:
            ev = self.parse_card(thumb, category, index)
            if ev:
                events.append(ev)

        self.stats["found"] += len(thumbnails)
        if label not in self.stats["by_category"]:
            self.stats["by_category"][label] = {"found": 0, "saved": 0}
        self.stats["by_category"][label]["found"] += len(thumbnails)

        logger.info(f"  → Минск / не дубль: {len(events)}")
        return events

    # ── Сохранение в БД ──
    def save_events(self, events: List[Dict]) -> int:
        if not events:
            return 0

        # Дедупликация внутри текущего запуска
        seen = set()
        unique_events = []
        for ev in events:
            key = (ev['title'], ev['event_date'], ev.get('show_time', ''), ev.get('place', ''))
            if key in seen:
                continue
            seen.add(key)
            unique_events.append(ev)

        if len(unique_events) != len(events):
            logger.info(f"🔂 Убрано дублей внутри запуска: {len(events) - len(unique_events)}")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # удаляем все свои старые записи
        cursor.execute("DELETE FROM events WHERE source_name = ?", (SOURCE_NAME,))
        deleted = cursor.rowcount
        if deleted:
            logger.info(f"🗑️ Удалено {deleted} старых записей {SOURCE_NAME}")

        saved = 0
        for ev in unique_events:
            try:
                cursor.execute("""
                    INSERT INTO events
                      (title, details, description, event_date, show_time,
                       place, location, price, category, source_name, source_url)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    ev["title"], ev["details"], ev["description"],
                    ev["event_date"], ev["show_time"],
                    ev["place"], ev["location"], ev["price"],
                    ev["category"], ev["source_name"], ev["source_url"],
                ))
                saved += 1
            except Exception as e:
                logger.error(f"Ошибка сохранения: {ev.get('title')} — {e}")
        conn.commit()
        conn.close()
        return saved

    # ── Очистка устаревших событий ──

    def clean_old_events(self):
        today = date.today().isoformat()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Удаляем устаревшие по дате
        cursor.execute(
            "DELETE FROM events WHERE source_name = ? AND event_date < ?",
            (SOURCE_NAME, today)
        )
        deleted_old = cursor.rowcount

        # Удаляем не-минские записи (могли попасть до улучшения фильтра)
        non_minsk_keywords = [
            'гродн', 'гомель', 'витебск', 'могилев', 'брест', 'бобруйск',
            'солигорск', 'борисов', 'орша', 'пинск', 'слуцк', 'полоцк',
            'областная филармония', 'областной дворец',
        ]
        deleted_nonminsk = 0
        cursor.execute(
            "SELECT id, place FROM events WHERE source_name = ?", (SOURCE_NAME,)
        )
        rows = cursor.fetchall()
        bad_ids = []
        for row_id, place in rows:
            if place and any(kw in place.lower() for kw in non_minsk_keywords):
                bad_ids.append(row_id)
        if bad_ids:
            cursor.executemany("DELETE FROM events WHERE id = ?", [(i,) for i in bad_ids])
            deleted_nonminsk = len(bad_ids)
            logger.info(f"🗑 Удалено не-минских: {deleted_nonminsk} ({bad_ids})")

        conn.commit()
        conn.close()
        if deleted_old:
            logger.info(f"🗑 Удалено устаревших: {deleted_old}")

    # ── Главный запуск ──

    def run(self):
        logger.info("=" * 50)
        logger.info("🚀 BezKassira парсер запущен")
        logger.info("=" * 50)

        # 1. Очищаем старые записи ОДИН РАЗ
        self.clean_old_events()
    
        # 2. Удаляем ВСЕ старые bezkassira записи ДО парсинга
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM events WHERE source_name = ?", (SOURCE_NAME,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        if deleted:
            logger.info(f"🗑️ Удалено {deleted} старых записей {SOURCE_NAME}")
    
        # 3. Загружаем индекс для проверки дублей с другими источниками
        index = self.load_existing_index()

        for cat in CATEGORIES:
            events = self.parse_category(
                cat["url"], cat["category"], cat["label"], index
            )
        
            # Сохраняем события ЭТОЙ категории (не удаляя другие)
            cat_saved = self._save_category_events(events)

            self.stats["by_category"][cat["label"]]["saved"] = cat_saved
            self.stats["saved"] += cat_saved
        
            # Обновляем индекс для дедупликации между категориями
            for ev in events:
                norm = normalize_title(ev['title'])
                index.setdefault(ev['event_date'], []).append(
                    (norm, ev.get('place', ''), ev.get('show_time', ''))
                )

            time.sleep(1)


        saved = self.stats["saved"]

        # ── Отчёт ──
        total_found = sum(v["found"] for v in self.stats["by_category"].values())
        logger.info(f"\n📊 Итог:")
        logger.info(f"  Карточек всего: {total_found}")
        logger.info(f"  Не Минск: {self.stats['non_minsk']}")
        logger.info(f"  Дубликаты: {self.stats['duplicates']}")
        logger.info(f"  Сохранено: {saved}")

        # RESULT строки для run_all_parsers.py
        for label, s in self.stats["by_category"].items():
            print(f"RESULT:{label}:{s['found']}:{s.get('saved', 0)}")

        return saved

    def _save_category_events(self, events: List[Dict]) -> int:
        """Сохраняет события категории без удаления других записей."""
        if not events:
            return 0

        # Дедупликация внутри текущего запуска
        seen = set()
        unique_events = []
        for ev in events:
            key = (ev['title'], ev['event_date'], ev.get('show_time', ''), ev.get('place', ''))
            if key in seen:
                continue
            seen.add(key)
            unique_events.append(ev)

        if len(unique_events) != len(events):
            logger.info(f"🔂 Убрано дублей внутри запуска: {len(events) - len(unique_events)}")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        saved = 0
        for ev in unique_events:
            try:
                cursor.execute("""
                    INSERT INTO events
                      (title, details, description, event_date, show_time,
                       place, location, price, category, source_name, source_url)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    ev["title"], ev["details"], ev["description"],
                    ev["event_date"], ev["show_time"],
                    ev["place"], ev["location"], ev["price"],
                    ev["category"], ev["source_name"], ev["source_url"],
                ))
                saved += 1
            except Exception as e:
                logger.error(f"Ошибка сохранения: {ev.get('title')} — {e}")
    
        conn.commit()
        conn.close()
        return saved




if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Тестовый режим — парсим без сохранения
        parser = BezkassiraParser()
        index = parser.load_existing_index()
        for cat in CATEGORIES:
            events = parser.parse_category(cat["url"], cat["category"], cat["label"], index)
            print(f"\n=== {cat['label'].upper()} — {len(events)} событий ===")
            for ev in events[:5]:
                print(f"  {ev['event_date']} {ev['show_time']:5s}  {ev['title'][:40]:40s}  {ev['place'][:30]}  {ev['price']}")
    else:
        BezkassiraParser().run()
