import os
#!/usr/bin/env python3
# ticketpro_parser.py
# Парсер для Ticketpro с нормализацией мест и улучшенной защитой от дубликатов

import json
import re
import sqlite3
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
# Определяем путь к БД (локально или на Railway)
if os.path.exists('/data'):
    DB_PATH = '/data/events_final.db'  # Railway volume
else:
    DB_PATH = 'events_final.db'        # локально


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ticketpro_parser.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Словарь для нормализации названий мест
PLACE_ALIASES = {
    # КЗ Минск
    'кз минск': 'КЗ Минск',
    'кз "минск"': 'КЗ Минск',
    'концертный зал минск': 'КЗ Минск',
    
    # Дворец спорта
    'дворец спорта': 'Дворец спорта',
    'дворец спорта, 4': 'Дворец спорта',
    'дворец спорта 4': 'Дворец спорта',
    
    # Белгосфилармония
    'белгосфилармония': 'Белорусская государственная филармония',
    'белорусская государственная филармония': 'Белорусская государственная филармония',
    
    # Молодёжный театр
    'молодёжный театр': 'Молодёжный театр',
    'молодежный театр': 'Молодёжный театр',
    'молодёжный театр эстрады': 'Молодёжный театр эстрады',
    'молодежный театр эстрады': 'Молодёжный театр эстрады',
    
    # Дворец Республики
    'дворец республики': 'Дворец Республики',
    'гу дворец республики': 'Дворец Республики',
    
    # Центральный дом офицеров
    'центральный дом офицеров': 'Центральный дом офицеров',
    'дом офицеров': 'Центральный дом офицеров',
    
    # Дом литератора
    'дом литератора': 'Дом литератора',
    
    # Музыкальный театр
    'музыкальный театр': 'Музыкальный театр',
    
    # Театр юного зрителя
    'театр юного зрителя': 'ТЮЗ',
    'тюз': 'ТЮЗ',
    
    # Falcon Club
    'falcon club': 'Falcon Club Arena',
    'falcon club arena': 'Falcon Club Arena',
    
    # Prime Hall
    'prime hall': 'Prime Hall',
    'прайм холл': 'Prime Hall',
    
    # ДК МАЗ
    'дк маз': 'ДК МАЗ',
    
    # Верхний город
    'верхний город': 'Концертный зал Верхний город',
    'концертный зал верхний город': 'Концертный зал Верхний город',
}

class TicketproParser:
    def __init__(self, DB_PATH=os.getenv("DB_PATH", "/data/events_final.db")):
        DB_PATH = DB_PATH
        self.base_url = 'https://www.ticketpro.by'
        
        self.categories = [
            ('/bilety-na-sportivnye-meropriyatiya/', 'sport', 'Спорт'),
            ('/bilety-na-koncert/', 'concert', 'Концерты'),
            ('/bilety-v-teatr/', 'theater', 'Театр'),
            ('/detskie-meropriyatiya/', 'kids', 'Детям'),
        ]
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })
        
        self.stats = {
            'total_pages': 0,
            'total_events_found': 0,
            'minsk_events': 0,
            'filtered_out': 0,
            'duplicates_within_run': 0,
            'duplicates_with_relax': 0,
            'by_category': {}
        }

    def fetch_page(self, url: str) -> Optional[str]:
        try:
            logger.info(f"Загрузка {url}")
            response = self.session.get(url, timeout=30)
            response.encoding = 'utf-8'
            if response.status_code == 200:
                return response.text
        except Exception as e:
            logger.error(f"Ошибка загрузки: {e}")
        return None

    def is_minsk_event(self, place_text: str) -> bool:
        """Проверяет, относится ли событие к Минску."""
        if not place_text or place_text == '':
            return True
        
        place_lower = place_text.lower()
        
        # Список городов для исключения (обновлённый)
        other_cities = [
            'гомель', 'gomel', 'витебск', 'vitebsk', 'могилев', 'mogilev',
            'гродно', 'grodno', 'брест', 'brest', 'бобруйск', 'bobruisk',
            'солигорск', 'soligorsk', 'орша', 'orsha', 'пинск', 'pinsk',
            'лида', 'lida', 'новополоцк', 'novopolotsk', 'молодечно', 'molodechno',
            'кобрин', 'kobrin', 'жодино', 'zhodino', 'речица', 'rechitsa',
            'берёза', 'bereza', 'мозырь', 'mozyr', 'борисов', 'borisov',
            'барановичи', 'baranovichi', 'несвиж', 'nesvizh', 'дзержинск', 'dzerzhinsk',
            'пружаны', 'pruzhany'
        ]
        
        for city in other_cities:
            if city in place_lower:
                return False
        
        return True

    def clean_place(self, place_text: str) -> str:
        """Очищает место от лишних слов и кавычек, приводит к единому виду."""
        if not place_text:
            return ""
        
        # Убираем "Минск," в начале
        cleaned = re.sub(r'^Минск,\s*', '', place_text)
        cleaned = re.sub(r'^г\.\s*Минск,\s*', '', cleaned)
        
        # Убираем кавычки всех видов
        cleaned = re.sub(r'[«»"]', '', cleaned)
        
        # Убираем лишние пробелы
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Приводим к нижнему регистру для поиска по словарю
        cleaned_lower = cleaned.lower()
        
        # Нормализуем по словарю
        for alias, normalized in PLACE_ALIASES.items():
            if alias in cleaned_lower:
                cleaned = normalized
                break
        
        return cleaned

    def normalize_title(self, title: str) -> str:
        """Нормализует название для сравнения."""
        if not title:
            return ""
        
        # Приводим к нижнему регистру
        norm = title.lower()
        
        # Убираем общие слова в начале и конце
        norm = re.sub(r'^(концерт|концертная\s+программа|спектакль|шоу|юбилейный\s+концерт|сольный\s+концерт|гала-концерт|праздничный\s+концерт|отчетный\s+концерт|эстрадный\s+караоке-спектакль)\s+', '', norm)
        norm = re.sub(r'\s+(концерт|спектакль|шоу|программа|фестиваль)$', '', norm)
        
        # Убираем кавычки всех видов
        norm = re.sub(r'[«»"\'`]', '', norm)
        
        # Убираем точки в конце
        norm = re.sub(r'\.+$', '', norm)
        
        # Убираем многоточия
        norm = re.sub(r'\.{2,}', '', norm)
        
        # Заменяем "и", "&" на общий разделитель
        norm = re.sub(r'\s+и\s+', ' & ', norm)
        norm = re.sub(r'&', ' & ', norm)
        
        # Убираем лишние пробелы
        norm = re.sub(r'\s+', ' ', norm).strip()
        
        # Унифицируем дефисы и тире
        norm = re.sub(r'[—–-]', '-', norm)
        
        # Убираем знаки препинания, оставляем буквы, цифры, пробелы, дефис, амперсанд
        norm = re.sub(r'[^\w\s\-&]', '', norm)
        
        return norm

    def load_relax_index(self) -> dict:
        """Загружает все non-Ticketpro события одним запросом в память."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT title, event_date, place, show_time
            FROM events WHERE source_name != 'ticketpro.by'
        """)
        rows = cursor.fetchall()
        conn.close()
        index = {}
        for title, date, place, show_time in rows:
            norm = self.normalize_title(title)
            index.setdefault(date, []).append((norm, place or "", show_time or ""))
        logger.info(f"📋 Загружено {sum(len(v) for v in index.values())} событий из БД для проверки дублей")
        return index

    def is_duplicate(self, title: str, event_date: str, place: str,
                     show_time: str, relax_index: dict) -> bool:
        """Проверяет дубликат по индексу в памяти — без запросов к БД."""
        if not title or not event_date:
            return False
        candidates = relax_index.get(event_date, [])
        if not candidates:
            return False
        norm_title = self.normalize_title(title)
        place = place or ""
        show_time = show_time or ""
        for norm_existing, ex_place, ex_time in candidates:
            if place and show_time and ex_place == place and ex_time == show_time:
                self.stats['duplicates_with_relax'] += 1
                return True
            if norm_title == norm_existing:
                self.stats['duplicates_with_relax'] += 1
                return True
        return False

    def parse_event_from_html(self, event_html, category: str, display_name: str, relax_index: dict = None) -> Optional[Dict]:
        try:
            title_tag = event_html.find('div', class_='event-box__title')
            if not title_tag:
                return None
            title = title_tag.get_text(strip=True)

            # === JSON-LD — основной источник данных ===
            ld_url = ld_desc = ld_place = ld_price = ""
            script = event_html.find('script', type='application/ld+json')
            if script and script.string:
                try:
                    raw = re.sub(r'[\x00-\x1f\x7f]', ' ', script.string)
                    ld = json.loads(raw)
                    ld_url   = ld.get('url', '')
                    ld_desc  = ld.get('description', '').strip()
                    if len(ld_desc) > 300:
                        ld_desc = ld_desc[:297] + '...'
                    loc = ld.get('location', {})
                    ld_place = loc.get('name', '')
                    offers = ld.get('offers', {})
                    if offers.get('price'):
                        ld_price = f"от {offers['price']} {offers.get('priceCurrency', 'BYN')}"
                except Exception:
                    pass

            # === Место: JSON-LD → HTML fallback ===
            place_raw = ld_place
            if not place_raw:
                place_tag = event_html.find('div', class_='event-box__place')
                place_raw = place_tag.get_text(strip=True) if place_tag else ''

            # Проверка на Минск
            if not self.is_minsk_event(place_raw):
                self.stats['filtered_out'] += 1
                return None

            place = self.clean_place(place_raw)

            # === Дата и время ===
            date_tag = event_html.find('div', class_='event-box__date')
            date_text = date_tag.get_text(strip=True) if date_tag else ''
            date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_text)
            event_date = show_time = ""
            if date_match:
                day, month, year = date_match.groups()
                event_date = f"{year}-{month}-{day}"
                time_match = re.search(r'(\d{2}:\d{2})', date_text)
                if time_match:
                    show_time = time_match.group(1)

            # Если даты или времени нет в HTML — берём из JSON-LD startDate
            if (not event_date or not show_time) and script and script.string:
                try:
                    raw = re.sub(r'[\x00-\x1f\x7f]', ' ', script.string)
                    ld = json.loads(raw)
                    sd = ld.get('startDate', '')  # "2026-03-05T19:00:00+0300"
                    if sd and 'T' in sd:
                        dt = datetime.fromisoformat(sd)
                        if not event_date:
                            event_date = dt.strftime('%Y-%m-%d')
                        if not show_time and dt.hour != 0:
                            show_time = dt.strftime('%H:%M')
                except Exception:
                    pass

            # Если времени всё ещё нет — пробуем страницу покупки /kupit-bilet/
            if event_date and not show_time:
                buy_link = event_html.find('a', class_='btn-pink', href=True)
                if buy_link:
                    buy_url = self.base_url + buy_link['href']
                    try:
                        buy_html = self.fetch_page(buy_url)
                        if buy_html:
                            buy_soup = BeautifulSoup(buy_html, 'lxml')
                            # Ищем время в странице покупки
                            for sel in ['div.event-time', 'span.time', '.schedule-time',
                                        'div.ticket-time', 'span.event-time']:
                                t = buy_soup.select_one(sel)
                                if t:
                                    m = re.search(r'\d{1,2}:\d{2}', t.get_text())
                                    if m:
                                        show_time = m.group(0)
                                        break
                            # Fallback: JSON-LD на странице покупки
                            if not show_time:
                                ld_script = buy_soup.find('script', type='application/ld+json')
                                if ld_script and ld_script.string:
                                    raw2 = re.sub(r'[\x00-\x1f\x7f]', ' ', ld_script.string)
                                    ld2 = json.loads(raw2)
                                    sd2 = ld2.get('startDate', '')
                                    if sd2 and 'T' in sd2:
                                        dt2 = datetime.fromisoformat(sd2)
                                        if dt2.hour != 0:
                                            show_time = dt2.strftime('%H:%M')
                    except Exception:
                        pass

            if not event_date:
                return None

            # === Цена: JSON-LD → HTML fallback ===
            price = ld_price
            if not price:
                price_tag = event_html.find('div', class_='event-box__price')
                price_text = price_tag.get_text(strip=True) if price_tag else ''
                m = re.search(r'(\d+[.,]\d*)\s*BYN', price_text)
                price = f"от {m.group(1)} BYN" if m else ""

            # === URL: JSON-LD → первый <a> fallback ===
            event_url = ld_url
            if not event_url:
                first_link = event_html.find('a', href=True)
                event_url = self.base_url + first_link['href'] if first_link else self.base_url

            # === Проверка дублей ===
            if relax_index is not None and self.is_duplicate(title, event_date, place, show_time, relax_index):
                return None

            description = f"🎫 {title}"
            if place:
                description += f"\n📍 {place}"
            if price:
                description += f"\n💰 {price}"

            self.stats['minsk_events'] += 1
            self.stats['by_category'][display_name] = self.stats['by_category'].get(display_name, 0) + 1
            logger.info(f"✅ {display_name}: {title[:40]} | {place[:25]} | {price}")

            return {
                'title': title,
                'details': '',        # описание не нужно (по запросу пользователя)
                'description': description,
                'event_date': event_date,
                'show_time': show_time,
                'place': place,
                'location': 'Минск',
                'price': price,
                'category': category,
                'source_url': event_url,
                'source_name': 'ticketpro.by'
            }

        except Exception as e:
            logger.error(f"Ошибка парсинга HTML: {e}")
            return None

    def parse_category_page(self, category_url: str, category: str, display_name: str) -> List[Dict]:
        """Парсит все страницы категории."""
        events = []
        page = 1
        base_url = self.base_url + category_url
        max_pages = 50
        
        relax_index = self.load_relax_index()
        while page <= max_pages:
            url = f"{base_url}?page={page}" if page > 1 else base_url
            
            logger.info(f"Загрузка страницы {page} для {display_name}")
            html = self.fetch_page(url)
            if not html:
                break
            
            soup = BeautifulSoup(html, 'lxml')
            
            if page == 1:
                title_info = soup.find('span', class_='title-info')
                if title_info:
                    logger.info(f"Всего событий: {title_info.get_text(strip=True)}")
            
            event_boxes = soup.find_all('div', class_='event-box')
            logger.info(f"Страница {page}: найдено {len(event_boxes)} блоков")
            
            if not event_boxes:
                logger.info(f"Нет событий на странице {page}, завершаем")
                break
            
            for event_box in event_boxes:
                self.stats['total_events_found'] += 1
                event = self.parse_event_from_html(event_box, category, display_name, relax_index)
                if event:
                    events.append(event)
            
            logger.info(f"Страница {page}: накоплено {len(events)} событий")
            self.stats['total_pages'] += 1
            
            pagination = soup.find('div', class_='pagination')
            if not pagination:
                logger.info("Пагинация не найдена, завершаем")
                break
            
            next_link = pagination.find('a', class_='page-next')
            if not next_link or 'disabled' in next_link.get('class', []):
                logger.info("Нет следующей страницы, завершаем")
                break
            
            page += 1
            time.sleep(1)
        
        return events

    def save_events(self, all_events: List[Dict]) -> int:
        """Сначала удаляет старые, потом сохраняет новые."""
        if not all_events:
            logger.info("Нет событий для сохранения")
            return 0
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 1. Удаляем все старые события Ticketpro
        cursor.execute("DELETE FROM events WHERE source_name='ticketpro.by'")
        deleted = cursor.rowcount
        logger.info(f"🗑️ Удалено старых записей: {deleted}")
        
        # 2. Убираем дубликаты ВНУТРИ ЭТОГО ЗАПУСКА
        seen = set()
        unique_events = []
        
        for event in all_events:
            key = (event['title'], event['event_date'], event.get('show_time', ''))
            if key in seen:
                self.stats['duplicates_within_run'] += 1
                logger.debug(f"🔄 Дубликат в этом запуске: {event['title'][:50]}...")
            else:
                seen.add(key)
                unique_events.append(event)
        
        # 3. Сохраняем уникальные события
        new_count = 0
        for event in unique_events:
            try:
                cursor.execute("""
                    INSERT INTO events (
                        title, details, description, event_date, show_time,
                        place, location, price, category, source_url, source_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event['title'], event['details'], event['description'],
                    event['event_date'], event['show_time'], event['place'],
                    event['location'], event['price'], event['category'],
                    event['source_url'], event['source_name']
                ))
                new_count += 1
            except Exception as e:
                logger.error(f"Ошибка сохранения: {e}")
        
        conn.commit()
        conn.close()
        
        return new_count

    def run(self):
        logger.info("="*60)
        logger.info("🎫 ПАРСЕР TICKETPRO (С НОРМАЛИЗАЦИЕЙ МЕСТ)")
        logger.info("="*60)
        
        all_events = []
        for cat_url, category, display_name in self.categories:
            logger.info(f"\n--- Парсинг категории: {display_name} ---")
            events = self.parse_category_page(cat_url, category, display_name)
            all_events.extend(events)
        
        if all_events:
            saved = self.save_events(all_events)
            logger.info("\n" + "="*60)
            logger.info("📊 СТАТИСТИКА ЗАПУСКА")
            logger.info(f"   📄 Всего страниц: {self.stats['total_pages']}")
            logger.info(f"   🔍 Найдено событий: {self.stats['total_events_found']}")
            logger.info(f"   ✅ Прошли фильтр Минска: {self.stats['minsk_events']}")
            logger.info(f"   ❌ Отфильтровано (не Минск): {self.stats['filtered_out']}")
            logger.info(f"   🔁 Дубликатов с Relax: {self.stats['duplicates_with_relax']}")
            logger.info(f"   🔂 Дубликатов внутри запуска: {self.stats['duplicates_within_run']}")
            logger.info("\n   📊 По категориям:")
            for cat, count in self.stats['by_category'].items():
                logger.info(f"     {cat}: {count}")
            logger.info(f"\n   💾 Сохранено в БД: {saved}")
            print(f"   📊 Результаты:")
            print(f"      ✅ Добавлено новых событий: {saved}")
            print(f"      🔁 Дубликатов с Relax: {self.stats['duplicates_with_relax']}")
            print(f"      ❌ Не Минск: {self.stats['filtered_out']}")
            # По категориям ticketpro
            for cat_name, cnt in self.stats['by_category'].items():
                print(f"RESULT:{cat_name}:{cnt}:{cnt}")
        else:
            logger.warning("❌ События не найдены")
            print("   ⚠️ Ticketpro: события не найдены")
        logger.info("="*60)

if __name__ == "__main__":
    parser = TicketproParser()
    parser.run()
