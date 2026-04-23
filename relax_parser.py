#!/usr/bin/env python3
# relax_parser.py
# Единый модуль для всех Relax-парсеров (theatre, concert, exhibition, kids, free)

import os
import re
import sqlite3
import logging
import time
import json
from datetime import datetime
from collections import defaultdict

import requests
from bs4 import BeautifulSoup
from normalizer import normalize_place, extract_time, parse_text_date, normalize_price

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
    # Флаг: если True, парсер не сохраняет в БД, а возвращает события через JSON
    return_events_json: bool = False
    # Флаг: если False, /kino/ ссылки на странице не пропускаются (для kids-pass)
    skip_kino_urls: bool = True

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
            event_date = parse_text_date(h5.get_text())
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
                        raw_place = place_a.get_text(strip=True)
                        last_place = normalize_place(raw_place, known_venues=self.known_venues) or raw_place
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

                # Фикс 3: пропускаем если URL содержит /kino/ а мы не кино-парсер
                # (страница kids намеренно включает фильмы — skip_kino_urls=False)
                if "/kino/" in href and self.category != "cinema" and self.skip_kino_urls:
                    skip_no_title += 1
                    continue

                source_url = self.build_url(href)

                details_a = item.find("a", class_="schedule__event-dscr")
                details = details_a.get_text(strip=True) if details_a else ""

                # Итерируемся по всем сеансам события (обычно 1, иногда 2+)
                # Каждый div.schedule__seance = отдельное время начала
                seances = item.find_all("div", class_="schedule__seance")
                if not seances:
                    # Нет сеансов — берём хотя бы время из любого элемента
                    seances = [item]  # fallback на весь item

                for seance_div in seances:
                    # Время начала — <a> для активных, <span> для закрытых
                    time_a    = seance_div.find("a",    class_="schedule__seance-time")
                    time_span = seance_div.find("span", class_="schedule__seance-time")
                    time_elem = time_a or time_span

                    if time_elem:
                        raw_time = time_elem.get_text(strip=True)
                        show_time = raw_time if re.match(r"^\d{1,2}:\d{2}$", raw_time) else ""
                    else:
                        show_time = ""

                    # Наличие билетов:
                    # schedule__seance--buy      → есть онлайн-покупка (активная кнопка)
                    # schedule__seance--buy-timeout → продажа закрыта
                    # schedule__seance--timeout  → нет билетов / продажа закончена
                    # span (не a)                → нет онлайн-продажи (касса театра)
                    tickets = ""
                    if time_a:
                        cls = time_a.get("class", [])
                        if "schedule__seance--buy" in cls and "schedule__seance--buy-timeout" not in cls:
                            tickets = "buy"      # есть онлайн-билеты
                        else:
                            tickets = "timeout"  # продажа закончена
                    elif time_span:
                        cls = time_span.get("class", [])
                        if "schedule__seance--timeout" in cls or "schedule__seance--buy-timeout" in cls:
                            tickets = "timeout"  # нет билетов
                        # иначе — span без timeout = просто нет онлайн-продажи (касса театра)

                    # Цена: span.seance-price или data-summ на seance_div
                    price_span = seance_div.find("span", class_="seance-price")
                    if price_span:
                        price = price_span.get_text(strip=True)
                    else:
                        price = seance_div.get("data-summ", "").strip() if seance_div.name != "div" or "schedule__seance" in seance_div.get("class", []) else ""
                        if not price:
                            # fallback: ищем в родительском item
                            seance_any = item.find("div", class_="schedule__seance")
                            price = seance_any.get("data-summ", "").strip() if seance_any else ""
                    price = normalize_price(price)

                    # Если билеты закончились — отмечаем в цене
                    if tickets == "timeout" and not price:
                        price = "Нет билетов"

                    description = f"{self.emoji} {title}"
                    if details:
                        description += f"\n📖 {details}"
                    if location:
                        description += f"\n📍 {location}"
                    if price:
                        description += f"\n💰 {price}"

                    events.append({
                        "title":       title,
                        "details":     details,
                        "description": description,
                        "event_date":  event_date,
                        "show_time":   show_time,
                        "place":       place,
                        "location":    location,
                        "price":       price,
                        "category":    self.category,
                        "source_url":  source_url,
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

            # Удаляем ТОЛЬКО свои старые записи этой категории
            cursor.execute("DELETE FROM events WHERE source_name = 'relax.by' AND category = ?", (self.category,))
            deleted = cursor.rowcount
            logger.info(f"Удалено старых записей relax: {deleted}")

            # Загружаем для проверки дубликатов:
            # 1. Другие категории relax (чтобы не дублировать внутри relax)
            # 2. Пользовательские события (чтобы не создавать дубли с пользователями)
            cursor.execute("""
                SELECT title, event_date, place FROM events
                WHERE (source_name = 'relax.by' AND category != ?)  -- другие категории relax
                   OR source_name = 'user_submitted'                 -- пользовательские события
            """, (self.category,))
            
            existing_other = set((r[0], r[1], r[2]) for r in cursor.fetchall())
            logger.info(f"Загружено для проверки дублей: {len(existing_other)} записей")

            new_count = skip_dup = 0
            for event in events:
                dup_key = (event["title"], event["event_date"], event["place"])
                if dup_key in existing_other:
                    skip_dup += 1
                    logger.debug(f"Дубликат с другой категорией relax или пользователем: {event['title']}")
                    continue
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
            logger.info(f"Сохранено: {new_count}, пропущено дублей: {skip_dup}")
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
            if self.return_events_json:
                # Возвращаем события через JSON для централизованной обработки
                logger.info(f"📦 Возвращаю {len(events)} событий через JSON")
                print(f"EVENTS_JSON:{json.dumps(events, ensure_ascii=False)}")
                # Для статистики всё равно выводим RESULT
                print(f"RESULT:{self.clear_label}:{len(events)}:0")
            else:
                # Стандартное сохранение в БД
                saved = self.save_events(events)
                logger.info(f"Итого: найдено {len(events)}, сохранено {saved}")
                print(f"   🧹 Очищены старые записи ({self.clear_label})")
                print(f"   📊 Результаты:")
                print(f"      ✅ Добавлено новых {self.clear_label}: {saved}")
                print(f"RESULT:{self.clear_label}:{len(events)}:{saved}")
        else:
            logger.warning(f"{self.clear_label.capitalize()} не найдены")
            print(f"   ⚠️ {self.clear_label.capitalize()} не найдено")
            print(f"RESULT:{self.clear_label}:0:0")

        logger.info("=" * 60)


# ============================================================
# Конкретные парсеры — только конфиг
# ============================================================

class RelaxTheatreParser(RelaxBaseParser):
    path = "/theatre/minsk/"
    category = "theater"
    source_name = "relax.by"
    emoji = "🎭"
    clear_label = "спектаклей"
    known_venues = [
        "Молодёжный театр", "Молодежный театр",
        "Молодёжный театр эстрады", "Молодежный театр эстрады",
        "Театр им. Горького", "Театр имени Горького",
        "Театр им. Янки Купалы", "Театр имени Янки Купалы", "Купаловский",
        "Театр оперы и балета", "Большой театр",
        "Большой театр Беларуси", "Государственный академический Большой театр",
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
    source_name = "relax.by"
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
    source_name = "relax.by"
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
    """
    Kids-pass: скачивает relax.by/kids/, возвращает события через JSON.
    Вызывающий код (run_all_parsers / daytime_update) передаёт их в
    normalizer.apply_kids_pass() — совпадения помечаются is_kids=1,
    уникальные события (цирк, зоопарк и т.п.) добавляются в БД с category='kids'.
    """
    path = "/kids/minsk/"
    category = "kids"
    source_name = "relax.by"
    emoji = "🧸"
    clear_label = "детских событий"
    return_events_json = True   # не сохраняем напрямую — обработка через apply_kids_pass
    skip_kino_urls = False      # фильмы на странице kids нужны для маркировки is_kids=1
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


class RelaxPartyParser(RelaxBaseParser):
    path = "/clubs/minsk/"
    category = "party"
    source_name = "relax.by"
    emoji = "🎉"
    clear_label = "вечеринок"
    known_venues = [
        "Prime Hall", "Re:Public", "Club Re:Public",
        "ZAVOD", "Dozari", "DoZari Club",
        "Falcon Club", "Zoccolo",
        "Liberty Club", "Арт-центр Корпус",
        "Граффити", "Ресторан Граффити",
        "Лофт", "Стадион Локомотив",
        "Harat's Pub", "Пианобар",
    ]


class RelaxFreeParser(RelaxBaseParser):
    """
    Парсер бесплатных событий.
    Возвращает события через JSON для обработки в run_all_parsers.py.
    """
    path = "/free/minsk/"
    category = "free"
    source_name = "relax.by"
    emoji = "🆓"
    clear_label = "бесплатных событий"
    known_venues = []   # принимаем все места — бесплатные мероприятия везде
    return_events_json = True  # включаем режим возврата JSON

    def parse_page(self, url: str) -> list:
        """Парсит бесплатные события и проставляет цену, если её нет."""
        events = super().parse_page(url)
        
        # Проставляем цену "Бесплатно" для всех событий из этой секции
        for event in events:
            if not event.get("price") or event.get("price") == "":
                event["price"] = "Бесплатно"
            # Добавляем флаг, что событие из free-секции
            event["_from_free_section"] = True
        
        logger.info(f"🆓 Бесплатных событий после обработки: {len(events)}")
        return events


class RelaxKinoParser(RelaxBaseParser):
    path = "/kino/minsk/"
    category = "cinema"
    source_name = "relax.by"
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
            event_date = parse_text_date(h5.get_text())
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
                            raw_place = place_a.get_text(strip=True)
                            last_place = normalize_place(raw_place, known_venues=self.known_venues) or raw_place
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

                    details_a = item.find("a", class_="schedule__event-dscr")
                    details = details_a.get_text(strip=True) if details_a else ""
                    film_href = (details_a.get("href", "") if details_a else "") or title_a.get("href", "")
                    source_url = self.build_url(film_href)

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
                        price = normalize_price(price)

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
            cursor.execute("DELETE FROM events WHERE source_name = 'relax.by' AND category = 'cinema'")
            logger.info(f"Удалено старых сеансов relax: {cursor.rowcount}")

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
        "party":      RelaxPartyParser,
        "free":       RelaxFreeParser,
    }

    # Парсеры, которые возвращают JSON вместо прямого сохранения в БД
    JSON_PARSERS = {"free", "kids"}

    if len(sys.argv) > 1:
        name = sys.argv[1]
        if name in PARSERS:
            parser_class = PARSERS[name]
            if name in JSON_PARSERS:
                parser = parser_class()
                parser.run()
            else:
                parser_class().run()
        else:
            print(f"Неизвестный парсер: {name}")
            print(f"Доступные: {', '.join(PARSERS)}")
            sys.exit(1)
    else:
        # Запуск всех
        for name, cls in PARSERS.items():
            if name in JSON_PARSERS:
                parser = cls()
                parser.run()
            else:
                cls().run()
