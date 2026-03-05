#!/usr/bin/env python3
# relax_parser.py
# Единый модуль для всех Relax-парсеров (theatre, concert, exhibition, kids)

import os
import re
import sqlite3
import logging
import time
from datetime import datetime
from collections import defaultdict

import requests
from bs4 import BeautifulSoup

# ---------------------- Путь к БД ----------------------

DB_PATH = os.getenv("DB_PATH", "/data/events_final.db")

# ---------------------- Логирование ----------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("relax_parser.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------------- Базовый парсер ----------------------

class RelaxBaseParser:
    """
    Базовый класс для парсеров afisha.relax.by.
    Наследники задают только конфиг — url, category, known_venues и т.д.
    """

    # --- Переопределяется в наследнике ---
    path = ""               # /theatre/minsk/
    category = ""           # theater / concert / exhibition / kids
    source_name = ""        # relax.by/theatre
    emoji = "🎉"
    clear_label = "событий"
    known_venues: list = []

    def __init__(self):
        self.base_url = "https://afisha.relax.by"
        self.section_url = self.base_url + self.path

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        })

        self.months = {
            "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
            "мая": 5, "июня": 6, "июля": 7, "августа": 8,
            "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
        }
        self.current_year = datetime.now().year

    # ---------------------- Утилиты ----------------------

    def fetch_page(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries):
            try:
                logger.info(f"Попытка {attempt + 1}/{retries}: {url}")
                resp = self.session.get(url, timeout=30)
                resp.encoding = "utf-8"
                if resp.status_code == 200:
                    logger.info(f"Загружено ({len(resp.text)} символов)")
                    return resp.text
                logger.warning(f"HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"Ошибка: {e}")
            if attempt < retries - 1:
                time.sleep(5)
        return None

    def parse_date_from_header(self, header_text: str) -> str | None:
        if not header_text:
            return None
        text = header_text.strip().lower()
        m = re.search(r"(\d{1,2})\s+([а-я]+)", text)
        if not m:
            return None
        day = int(m.group(1))
        month = self.months.get(m.group(2))
        if not month:
            return None
        today = datetime.now()
        year = self.current_year
        if month < today.month or (month == today.month and day < today.day):
            year += 1
        return f"{year}-{month:02d}-{day:02d}"

    def normalize_place(self, place: str) -> str | None:
        if not place or len(place) < 3:
            return None
        place_lower = place.lower()
        for venue in self.known_venues:
            if venue.lower() in place_lower:
                return venue
        # Убираем адресные части
        place = re.sub(r"ул\.?\s*\w+", "", place)
        place = re.sub(r"пр-?т\.?\s*\w+", "", place)
        place = re.sub(r"пл\.?\s*\w+", "", place)
        place = re.sub(r"пер\.?\s*\w+", "", place)
        place = re.sub(r"\s+", " ", place).strip()
        return place if len(place) > 3 else None

    def extract_time(self, text: str) -> str:
        for pattern in [
            r"начало\s*в\s*(\d{1,2}[:\.]\d{2})",
            r"в\s*(\d{1,2}[:\.]\d{2})",
            r"(\d{2}[:\.]\d{2})",
            r"(\d{1,2}[:\.]\d{2})\s*ч",
        ]:
            m = re.search(pattern, text)
            if m:
                t = m.group(1).replace(".", ":")
                if len(t) == 4:
                    t = t[:2] + ":" + t[2:]
                return t
        return ""

    def extract_price(self, text: str) -> str:
        for pattern in [
            r"(от\s*\d+[\.,]?\d*\s*руб)",
            r"(\d+[\.,]?\d*\s*руб)",
            r"(\d+[\.,]?\d*\s*р\.)",
            r"(\d+[\.,]?\d*\s*₽)",
            r"(вход\s*свободный)",
            r"(бесплатно)",
        ]:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                return m.group(1)
        return ""

    def build_url(self, href: str) -> str:
        if not href:
            return ""
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return self.base_url + href
        return ""

    # ---------------------- Парсинг страницы ----------------------

    def parse_page(self, url: str) -> list:
        html = self.fetch_page(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        events = []
        skip_no_place = skip_no_title = skip_no_date = 0

        # Структура relax.by: schedule__list (день) > schedule__table--movie__item (место+событие)
        day_blocks = soup.find_all("div", class_="schedule__list")
        logger.info(f"Найдено дней: {len(day_blocks)}")

        for day_block in day_blocks:
            # Дата из h5
            h5 = day_block.find("h5")
            if not h5:
                skip_no_date += 1
                continue
            event_date = self.parse_date_from_header(h5.get_text())
            if not event_date:
                skip_no_date += 1
                continue

            last_place = None
            last_location = "Минск"

            # Каждый movie__item = одно место + одно событие
            for movie_item in day_block.find_all("div", class_="schedule__table--movie__item"):
                # Обновляем место только при FILL; EMPTY наследует last_place
                place_div = movie_item.find("div", class_="schedule__place--fill")
                if place_div:
                    place_a = place_div.find("a", class_="js-schedule__place-link")
                    if place_a:
                        last_place = place_a.get_text(strip=True)
                    addr_span = place_div.find("span", class_="schedule__place-link")
                    last_location = addr_span.get_text(strip=True) if addr_span else "Минск"

                if not last_place:
                    skip_no_place += 1
                    continue

                place = last_place
                location = last_location

                # Событие
                item = movie_item.find("div", class_="schedule__item")
                if not item:
                    skip_no_title += 1
                    continue
                title_a = item.find("a", class_="js-schedule__event-link")
                if not title_a:
                    skip_no_title += 1
                    continue
                title = title_a.get_text(strip=True)
                if not title or len(title) < 3:
                    skip_no_title += 1
                    continue

                href = title_a.get("href", "")
                source_url = self.build_url(href)

                details_a = item.find("a", class_="schedule__event-dscr")
                details = details_a.get_text(strip=True) if details_a else ""

                time_span = item.find("span", class_="schedule__seance-time")
                show_time = time_span.get_text(strip=True) if time_span else ""

                price_span = item.find("span", class_="seance-price")
                price = price_span.get_text(strip=True) if price_span else ""

                description = f"{self.emoji} {title}"
                if details:
                    description += f"\n📖 {details}"
                if location:
                    description += f"\n📍 {location}"
                if price:
                    description += f"\n💰 {price}"

                events.append({
                    "title": title,
                    "details": details,
                    "description": description,
                    "event_date": event_date,
                    "show_time": show_time,
                    "place": place,
                    "location": location,
                    "price": price,
                    "category": self.category,
                    "source_url": source_url,
                    "source_name": self.source_name,
                })

                t = show_time or "     "
                p = price or "без цены"
                logger.info(f"  ✅ {event_date} | {t:5} | {title[:25]:25} | {place[:20]:20} | {p}")

        logger.info(f"Всего найдено {self.clear_label}: {len(events)}")
        logger.info(f"Пропущено: нет даты={skip_no_date}, нет места={skip_no_place}, нет названия={skip_no_title}")
        return events


    # ---------------------- Сохранение ----------------------

    def save_events(self, events: list) -> int:
        if not events:
            logger.info("Нет событий для сохранения")
            return 0

        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            cursor.execute("DELETE FROM events WHERE category = ?", (self.category,))
            deleted = cursor.rowcount
            logger.info(f"Удалено старых записей: {deleted}")

            new_count = 0
            for event in events:
                try:
                    cursor.execute("""
                        INSERT INTO events (
                            title, details, description, event_date, show_time,
                            place, location, price, category, source_url, source_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event["title"], event["details"], event["description"],
                        event["event_date"], event["show_time"], event["place"],
                        event["location"], event["price"], event["category"],
                        event["source_url"], event["source_name"],
                    ))
                    new_count += 1
                except Exception as e:
                    logger.error(f"Ошибка сохранения '{event['title']}': {e}")

            conn.commit()
            conn.close()
            logger.info(f"Сохранено: {new_count}")
            return new_count

        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            return 0

    # ---------------------- Запуск ----------------------

    def run(self):
        logger.info("=" * 60)
        logger.info(f"{self.emoji} ПАРСЕР: {self.source_name.upper()}")
        logger.info("=" * 60)

        events = self.parse_page(self.section_url)

        if events:
            saved = self.save_events(events)
            logger.info(f"Итого: найдено {len(events)}, сохранено {saved}")
            print(f"   🧹 Очищены старые записи ({self.clear_label})")
            print(f"   📊 Результаты:")
            print(f"      ✅ Добавлено новых {self.clear_label}: {saved}")
        else:
            logger.warning(f"{self.clear_label.capitalize()} не найдены")
            print(f"   ⚠️ {self.clear_label.capitalize()} не найдено")

        logger.info("=" * 60)


# ============================================================
# Конкретные парсеры — только конфиг
# ============================================================

class RelaxTheatreParser(RelaxBaseParser):
    path = "/theatre/minsk/"
    category = "theater"
    source_name = "relax.by/theatre"
    emoji = "🎭"
    clear_label = "спектаклей"
    known_venues = [
        "Молодёжный театр", "Молодежный театр",
        "Молодёжный театр эстрады", "Молодежный театр эстрады",
        "Театр им. Горького", "Театр имени Горького",
        "Театр им. Янки Купалы", "Театр имени Янки Купалы", "Купаловский",
        "Театр оперы и балета", "Большой театр",
        "Театр сатиры и юмора", "Театр сатиры",
        "Театр-студия киноактера", "Театр киноактера",
        "Новый драматический театр", "Новый театр",
        "Театр юного зрителя", "ТЮЗ",
        "Дворец Республики",
        "Дворец Профсоюзов",
        "Белорусская государственная филармония", "Филармония",
        "Центральный Дом офицеров", "Дом офицеров",
        "Музыкальный театр",
        "Дом литератора",
        "SKYLINE Cinema",
        "mooon в ТРЦ Dana Mall",
        "Центральный",
    ]


class RelaxConcertParser(RelaxBaseParser):
    path = "/conserts/minsk/"
    category = "concert"
    source_name = "relax.by/concerts"
    emoji = "🎵"
    clear_label = "концертов"
    known_venues = [
        "Дворец Профсоюзов",
        "Дворец Республики",
        "Минск-Арена",
        "Чижовка-Арена",
        "Prime Hall",
        "Республиканский дворец культуры",
        "Дворец культуры МАЗ",
        "КЗ Минск",
        "ДК Железнодорожников",
        "Центральный Дом офицеров",
        "Белгосфилармония",
        "Верхний город",
        "Музыкальная гостиная",
        "Ресторан Граффити",
        "Liberty Club",
        "Club Re:Public",
        "DoZari Club",
        "Невидимый мир",
        "Районный центр культуры г. Дзержинск",
    ]


class RelaxExhibitionParser(RelaxBaseParser):
    path = "/expo/minsk/"
    category = "exhibition"
    source_name = "relax.by/expo"
    emoji = "🖼️"
    clear_label = "выставок"
    known_venues = [
        "Национальный художественный музей",
        "Художественный музей",
        "Музей шоколада",
        "Музей истории города Минска",
        "Музей Великой Отечественной войны",
        "Музей природы и экологии",
        "Арт-галерея Дома Москвы",
        "Галерея Мастацтва",
        "Галерея Ў",
        "АртХаос",
        "ТРЦ Dana Mall",
        "ТРЦ Galileo",
        "ТРЦ Arena City",
        "ТРЦ Palazzo",
        "Национальная библиотека",
        "Дворец искусств",
        "Республиканская художественная галерея",
        "Центр современных искусств",
        "Арт-пространство ТЦ Корона",
        "Выставочный зал на Октябрьской",
    ]


class RelaxKidsParser(RelaxBaseParser):
    path = "/kids/minsk/"
    category = "kids"
    source_name = "relax.by/kids"
    emoji = "🧸"
    clear_label = "детских событий"
    known_venues = [
        "Цирк",
        "Белгосцирк",
        "Театр кукол",
        "Театр юного зрителя", "ТЮЗ",
        "Детский театр",
        "Кукольный театр",
        "Детская железная дорога",
        "Зоопарк",
        "Дельфинарий",
        "Парк Горького",
        "Парк Челюскинцев",
        "Детский развлекательный центр",
        "Семейный парк",
        "Кидзания",
        "Детский клуб",
        "Ботанический сад",
    ]



class RelaxKinoParser(RelaxBaseParser):
    path = "/kino/minsk/"
    category = "cinema"
    source_name = "relax.by/kino"
    emoji = "🎬"
    clear_label = "сеансов"
    known_venues = []  # кинотеатры берём напрямую из HTML

    SKIP_TITLES = {
        "Вся афиша", "Кино", "Спектакли", "Квесты", "Концерты",
        "События", "Выставки", "Детская афиша", "Вечеринки", "Stand Up",
        "Популярное", "Сегодня", "Завтра", "Премьеры", "Кинотеатры",
        "Фильмы", "Афиша", "Экскурсии", "Обучение", "Спорт", "Хоккей",
        "Бесплатные мероприятия",
    }

    def _parse_date_attr(self, date_str: str) -> str | None:
        """MM/DD/YYYY → YYYY-MM-DD"""
        if not date_str:
            return None
        m = re.search(r"(\d{2})/(\d{2})/(\d{4})", date_str)
        if m:
            month, day, year = m.groups()
            return f"{year}-{month}-{day}"
        return None

    def parse_page(self, url: str) -> list:
        html = self.fetch_page(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        movies = []
        seen = set()  # дедупликация (title, date, time, place)

        # Структура: schedule__list (день) > schedule__table--movie >
        #   schedule__table--movie__item (FILL|EMPTY + schedule__item)
        # Один FILL задаёт кинотеатр, следующие EMPTY наследуют его — last_place в рамках таблицы
        for day_block in soup.find_all("div", class_="schedule__list"):
            h5 = day_block.find("h5")
            if not h5:
                continue
            event_date = self.parse_date_from_header(h5.get_text())
            if not event_date:
                continue

            for table in day_block.find_all("div", class_="schedule__table--movie"):
                last_place = None
                last_location = "Минск"

                for movie_item in table.find_all("div", class_="schedule__table--movie__item"):
                    # Обновляем кинотеатр если FILL
                    place_fill = movie_item.find("div", class_="schedule__place--fill")
                    if place_fill:
                        place_a = place_fill.find("a", class_="js-schedule__place-link")
                        if place_a:
                            last_place = place_a.get_text(strip=True)
                        addr = place_fill.find("span", class_="schedule__place-link")
                        last_location = addr.get_text(strip=True) if addr else "Минск"

                    if not last_place:
                        continue

                    item = movie_item.find("div", class_="schedule__item")
                    if not item:
                        continue

                    title_a = item.find("a", class_="js-schedule__event-link")
                    if not title_a:
                        continue
                    title = title_a.get_text(strip=True)
                    if not title or len(title) < 3 or title in self.SKIP_TITLES:
                        continue

                    source_url = self.build_url(title_a.get("href", ""))
                    details_a = item.find("a", class_="schedule__event-dscr")
                    details = details_a.get_text(strip=True) if details_a else ""

                    for seance in item.find_all("div", class_="schedule__seance"):
                        # время — <a> для активных сеансов, <span> для закрытых (buy-timeout)
                        time_elem = seance.find("a", class_="schedule__seance-time") or                                     seance.find("span", class_="schedule__seance-time")
                        show_time = time_elem.get_text(strip=True) if time_elem else ""
                        # цена — сначала в data-summ, иначе в span
                        price_span = seance.find("span", class_="seance-price")
                        if price_span:
                            price = price_span.get_text(strip=True)
                        else:
                            data_summ = seance.get("data-summ", "").strip()
                            price = data_summ if data_summ else ""

                        key = (title, event_date, show_time, last_place)
                        if key in seen:
                            continue
                        seen.add(key)

                        description = f"🎬 {title}"
                        if details:
                            description += f"\n🎭 {details}"
                        if last_location:
                            description += f"\n📍 {last_location}"
                        if price:
                            description += f"\n💰 {price}"

                        movies.append({
                            "title": title,
                            "details": details,
                            "description": description,
                            "event_date": event_date,
                            "show_time": show_time,
                            "place": last_place,
                            "location": last_location,
                            "price": price,
                            "category": self.category,
                            "source_url": source_url,
                            "source_name": self.source_name,
                        })

        logger.info(f"Всего найдено сеансов: {len(movies)}")
        return movies

    def save_events(self, events: list) -> int:
        """Для кино: сначала чистим, потом вставляем с дедупликацией по title+date+time+place."""
        if not events:
            return 0
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM events WHERE category = 'cinema'")
            logger.info(f"Удалено старых сеансов: {cursor.rowcount}")

            seen = set()
            new_count = 0
            for e in events:
                key = (e["title"], e["event_date"], e["show_time"], e["place"])
                if key in seen:
                    continue
                seen.add(key)
                try:
                    cursor.execute("""
                        INSERT INTO events (
                            title, details, description, event_date, show_time,
                            place, location, price, category, source_url, source_name
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        e["title"], e["details"], e["description"],
                        e["event_date"], e["show_time"], e["place"],
                        e["location"], e["price"], e["category"],
                        e["source_url"], e["source_name"],
                    ))
                    new_count += 1
                except Exception as ex:
                    logger.error(f"Ошибка сохранения '{e['title']}': {ex}")

            conn.commit()
            conn.close()
            logger.info(f"Сохранено сеансов: {new_count}")
            return new_count
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            return 0

# ---------------------- Запуск отдельных парсеров ----------------------

if __name__ == "__main__":
    import sys

    PARSERS = {
        "theatre":    RelaxTheatreParser,
        "concert":    RelaxConcertParser,
        "exhibition": RelaxExhibitionParser,
        "kids":       RelaxKidsParser,
        "kino":       RelaxKinoParser,
    }

    if len(sys.argv) > 1:
        name = sys.argv[1]
        if name in PARSERS:
            PARSERS[name]().run()
        else:
            print(f"Неизвестный парсер: {name}")
            print(f"Доступные: {', '.join(PARSERS)}")
            sys.exit(1)
    else:
        # Запуск всех
        for name, cls in PARSERS.items():
            cls().run()
