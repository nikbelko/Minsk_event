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
        events_raw = []  # все сеансы по порядку, без дедупликации

        html = self.fetch_page(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")

        # Ищем блоки событий
        event_blocks = soup.find_all("div", class_=re.compile(r"event|schema|item"))
        if not event_blocks:
            event_blocks = soup.find_all("a", href=re.compile(r"/event/|" + self.path.rstrip("/")))

        logger.info(f"Найдено блоков: {len(event_blocks)}")

        last_place = None
        last_location = "Минск"
        last_date = None

        # Стоп-слова для названий
        stop_titles = {"Купить билет", "Подробнее", "Афиша", "Выставки",
                       "Концерты", "Детям", "Театр", "Кино"}

        # seen — только для точных дублей (title+date+place+time)
        seen = set()

        for block in event_blocks:
            try:
                # Находим ссылку с названием
                if block.name == "a":
                    title_elem = block
                else:
                    title_elem = block.find("a", href=re.compile(r"/event/|" + self.path.rstrip("/")))
                if not title_elem:
                    continue

                title = title_elem.get_text(strip=True)
                if not title or len(title) < 3 or title in stop_titles:
                    continue

                # Ищем дату через h5 заголовок
                event_date = None
                parent = block.find_parent()
                for _ in range(5):
                    if not parent:
                        break
                    date_header = parent.find_previous("h5")
                    if date_header:
                        event_date = self.parse_date_from_header(date_header.get_text())
                        if event_date:
                            break
                    parent = parent.parent
                if not event_date:
                    continue

                # Сбрасываем last_place при смене даты
                if last_date != event_date:
                    last_place = None
                    last_location = "Минск"
                    last_date = event_date

                block_text = block.get_text()
                show_time = self.extract_time(block_text)
                price = self.extract_price(block_text)

                # Ищем место в текущем блоке
                place = None
                place_elem = block.find(
                    ["a", "span", "div"],
                    class_=re.compile(r"place|theatre|venue|location")
                )
                if place_elem:
                    place = self.normalize_place(place_elem.get_text(strip=True))

                if place:
                    last_place = place
                    loc_elem = block.find(
                        ["span", "div"],
                        class_=re.compile(r"address|street|metro")
                    )
                    if loc_elem:
                        last_location = loc_elem.get_text(strip=True)
                elif last_place:
                    place = last_place
                else:
                    continue

                # Жанр/описание
                details = ""
                details_elem = block.find(
                    ["div", "span"],
                    class_=re.compile(r"genre|dscr|desc|type")
                )
                if details_elem:
                    details = details_elem.get_text(strip=True)

                source_url = self.build_url(title_elem.get("href", ""))

                # Пропускаем только точные дубли (одинаковый сеанс)
                dedup_key = (title, event_date, place, show_time)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                events_raw.append({
                    "title": title,
                    "details": details,
                    "event_date": event_date,
                    "show_time": show_time,
                    "place": place,
                    "location": last_location,
                    "price": price,
                    "source_url": source_url,
                })

            except Exception as e:
                logger.error(f"Ошибка при обработке блока: {e}")
                continue

        # Собираем итоговый список
        events = []
        for data in events_raw:
            description = f"{self.emoji} {data['title']}"
            if data["details"]:
                description += f"\n📖 {data['details']}"
            if data["location"]:
                description += f"\n📍 {data['location']}"
            if data["price"]:
                description += f"\n💰 {data['price']}"

            events.append({
                "title": data["title"],
                "details": data["details"],
                "description": description,
                "event_date": data["event_date"],
                "show_time": data["show_time"],
                "place": data["place"],
                "location": data["location"],
                "price": data["price"],
                "category": self.category,
                "source_url": data["source_url"],
                "source_name": self.source_name,
            })

            t = data["show_time"] or "     "
            p = data["price"] or "без цены"
            logger.info(
                f"  ✅ {data['event_date']} | {t:5} | "
                f"{data['title'][:25]:25} | {data['place'][:20]:20} | {p}"
            )

        logger.info(f"Всего найдено {self.clear_label}: {len(events)}")
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
        current_date = datetime.now().strftime("%Y-%m-%d")
        movies = []

        cinema_blocks = soup.find_all("div", class_="schedule__place--fill")
        cinema_blocks += soup.find_all("div", class_="schedule__place--empty")
        logger.info(f"Найдено кинотеатров: {len(cinema_blocks)}")

        for cinema_block in cinema_blocks:
            # Название кинотеатра
            name_elem = cinema_block.find("a", class_="js-schedule__place-link")
            place = name_elem.get_text(strip=True) if name_elem else None
            if not place or place == "Кинотеатр":
                continue

            addr_elem = cinema_block.find("span", class_="schedule__place-link")
            location = addr_elem.get_text(strip=True) if addr_elem else ""

            # Идём по следующим элементам до следующего кинотеатра
            current = cinema_block.find_next()
            while current:
                classes = current.get("class", [])
                if current.name == "div" and (
                    "schedule__place--fill" in classes or
                    "schedule__place--empty" in classes
                ):
                    break  # следующий кинотеатр

                if current.name == "div" and "schedule__item" in classes and "table_by_place" in classes:
                    try:
                        title_elem = current.find("a", class_="js-schedule__event-link")
                        if not title_elem:
                            current = current.find_next()
                            continue

                        title = title_elem.get_text(strip=True)
                        if not title or len(title) < 3 or title in self.SKIP_TITLES:
                            current = current.find_next()
                            continue

                        source_url = self.build_url(title_elem.get("href", ""))

                        details_elem = current.find("a", class_="schedule__event-dscr")
                        details = details_elem.get_text(strip=True) if details_elem else ""

                        for seance in current.find_all("div", class_="schedule__seance"):
                            time_elem = seance.find("a", class_="schedule__seance-time")
                            if not time_elem:
                                continue
                            show_time = time_elem.get_text(strip=True)
                            if not show_time:
                                continue

                            price_elem = seance.find("span", class_="seance-price")
                            price = price_elem.get_text(strip=True) if price_elem else ""

                            event_date = (
                                self._parse_date_attr(time_elem.get("data-date-format", ""))
                                or current_date
                            )

                            description = f"🎬 {title}"
                            if details:
                                description += f"\n🎭 {details}"
                            if location:
                                description += f"\n📍 {location}"
                            if price:
                                description += f"\n💰 {price}"

                            movies.append({
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
                    except Exception as e:
                        logger.error(f"Ошибка при обработке сеанса: {e}")

                current = current.find_next()

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
