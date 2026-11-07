#!/usr/bin/env python3
# ticketpro_parser.py
# Парсер для Ticketpro с нормализацией мест и улучшенной защитой от дубликатов

import os
import json
import re
import sqlite3
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
from normalizer import normalize_place, normalize_title, is_minsk_event, format_price_from_offers, normalize_price
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


class TicketproParser:
    def __init__(self, DB_PATH=os.getenv("DB_PATH", "/data/events_final.db")):
        DB_PATH = DB_PATH
        self.base_url = 'https://www.ticketpro.by'
        
        self.categories = [
            ('/bilety-na-sportivnye-meropriyatiya/', 'sport', 'Спорт'),
            ('/bilety-na-koncert/', 'concert', 'Концерты'),
            ('/bilety-v-teatr/', 'theater', 'Театр'),
            ('/detskie-meropriyatiya/', 'kids', 'Детям'),
            ('/bilety-na-shou/yumor/', 'concert', 'Концерты'),
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
            norm = normalize_title(title)
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
        norm_title = normalize_title(title)
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

    @staticmethod
    def _month_name_to_number(month_name: str) -> str:
        mapping = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04', 'мая': '05', 'июня': '06',
            'июля': '07', 'августа': '08', 'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12',
            'yanvarya': '01', 'fevralya': '02', 'mart': '03', 'aprelya': '04', 'maya': '05', 'iyunya': '06',
            'iyulya': '07', 'avgusta': '08', 'sentyabrya': '09', 'oktyabrya': '10', 'noyabrya': '11', 'dekabrya': '12',
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05', 'jun': '06',
            'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12',
        }
        return mapping.get((month_name or '').lower(), '')

    def _parse_event_meta_from_url(self, url: str) -> tuple[str, str]:
        if not url:
            return '', ''
        slug = url.rstrip('/').split('/')[-1]
        m = re.search(r'(?P<day>\d{1,2})-(?P<month>[a-z]+)-(?P<year>\d{4})$', slug)
        if not m:
            return '', ''
        day = int(m.group('day'))
        month_num = self._month_name_to_number(m.group('month'))
        year = m.group('year')
        if not month_num:
            return '', ''
        event_date = f"{year}-{month_num}-{day:02d}"
        title_prefix = slug[:m.start()].replace('bilety-na-', '').replace('bilety-v-', '').replace('bilety-', '')
        title_prefix = title_prefix.replace('-', ' ').strip()
        title = title_prefix.title() if title_prefix else ''
        return title, event_date

    def _extract_place_from_html(self, soup: BeautifulSoup, fallback_text: str = '', title_hint: str = '', source_url: str = '') -> str:
        candidates = [title_hint]
        if source_url:
            parsed_title, _ = self._parse_event_meta_from_url(source_url)
            if parsed_title:
                candidates.append(parsed_title)
        for needle in candidates:
            if not needle:
                continue
            text = fallback_text or soup.get_text(' ', strip=True)
            idx = text.lower().find(needle.lower())
            if idx == -1:
                continue
            window = text[max(0, idx - 200): idx + 600]
            match = re.search(r'([A-Za-zА-Яа-яЁё0-9"«»()\-\s\./]{2,80}?)\s*,\s*Минск', window, re.I | re.U)
            if match:
                candidate = match.group(1).strip()
                if candidate and not re.search(r'©|ООО|свидетельство|лицензия|все права|государственной регистрации', candidate, re.I):
                    return normalize_place(candidate)
            match = re.search(r'([A-Za-zА-Яа-яЁё0-9"«»()\-\s\./]{2,80}?)\s*Минск', window, re.I | re.U)
            if match:
                candidate = match.group(1).strip()
                if candidate and not re.search(r'©|ООО|свидетельство|лицензия|все права|государственной регистрации', candidate, re.I):
                    return normalize_place(candidate)

        for candidate in [
            'meta[property="og:description"]',
            'meta[name="description"]',
            'div[class*="place"]',
            'div[class*="venue"]',
            'address',
            'p',
        ]:
            for tag in soup.select(candidate):
                text = tag.get_text(' ', strip=True)
                if not text:
                    continue
                if 'Минск' in text or 'минск' in text.lower():
                    if re.search(r'©|ООО|свидетельство|лицензия|все права|государственной регистрации', text, re.I):
                        continue
                    match = re.search(r'([A-Za-zА-Яа-яЁё0-9"«»()\-\s\./]{2,80}?),\s*Минск', text, re.I | re.U)
                    if match:
                        candidate = match.group(1).strip()
                        if candidate and len(candidate) < 80:
                            return normalize_place(candidate)
                    if len(text) < 80:
                        return normalize_place(text)

        if fallback_text:
            match = re.search(r'([A-Za-zА-Яа-яЁё0-9"«»()\-\s\./]{2,80}?),\s*Минск', fallback_text, re.I | re.U)
            if match:
                candidate = match.group(1).strip()
                if candidate and not re.search(r'©|ООО|свидетельство|лицензия|все права|государственной регистрации', candidate, re.I):
                    return normalize_place(candidate)
        return ''

    def _extract_price_from_html(self, soup: BeautifulSoup, fallback_text: str = '') -> str:
        candidates = []
        for tag in soup.select('div, span, p, a'):
            text = tag.get_text(' ', strip=True)
            if re.search(r'(от\s*\d|\d{2,3}[.,]?\d{0,2}\s*(?:BYN|р\.|руб|бел\.р\.|Br))', text, re.I):
                candidates.append(text)
        if candidates:
            for text in candidates[:30]:
                m = re.search(r'(?:от\s*)?(\d{1,3}(?:[.,]\d{1,2})?)\s*(?:BYN|р\.|руб|бел\.р\.|Br)', text, re.I)
                if m:
                    return normalize_price(m.group(0))
                m = re.search(r'(?:от\s*)?(\d{1,3}(?:[.,]\d{1,2})?)', text)
                if m and (text.lower().count('от') or 'бил' in text.lower() or 'цена' in text.lower() or 'ticket' in text.lower()):
                    return normalize_price(m.group(1))
        if fallback_text:
            m = re.search(r'(?:от\s*)?(\d{1,3}(?:[.,]\d{1,2})?)\s*(?:BYN|р\.|руб|бел\.р\.|Br)', fallback_text, re.I)
            if m:
                return normalize_price(m.group(0))
        return ''

    def _extract_title_from_text(self, text: str) -> str:
        if not text:
            return ''
        normalized = re.sub(r'\s+', ' ', text).strip()
        candidates = [
            r'([А-ЯЁA-Z][A-Za-zА-Яа-яЁё0-9\s\"\'\-/.:,()&]{2,80}?)\s+\d{1,2}\s+[а-я]+\s+\d{4}',
            r'([А-ЯЁA-Z][A-Za-zА-Яа-яЁё0-9\s\"\'\-/.:,()&]{2,80}?)\s+(?:сб|вс|пн|вт|ср|чт|пт)\s*,\s*\d{1,2}:\d{2}',
            r'([А-ЯЁA-Z][A-Za-zА-Яа-яЁё0-9\s\"\'\-/.:,()&]{2,80}?)\s+(?:с\s+\d{1,2}\s+по\s+\d{1,2}|\d{1,2}\s+[а-я]+)',
        ]
        for pattern in candidates:
            match = re.search(pattern, normalized, flags=re.I | re.U)
            if match:
                title = re.sub(r'\s+', ' ', match.group(1)).strip(' -—–:,.\"\'')
                if title and len(title) > 1:
                    return title
        return ''

    def parse_event_from_html(self, event_html, category: str, display_name: str, relax_index: dict = None, title_hint: str = '') -> Optional[Dict]:
        try:
            title = ''
            if title_hint:
                title = title_hint
                title = re.sub(r'\s+', ' ', title).strip()
                title = re.sub(r'\s+\d{1,2}\s+[а-я]+(?:\s+\d{4})?(?:\s+(?:сб|вс|пн|вт|ср|чт|пт))?(?:,\s*\d{1,2}:\d{2})?.*$', '', title, flags=re.I | re.U)
                title = title.strip(' -—–:,.\"\'')

            title_tag = event_html.find('div', class_='event-box__title')
            if not title and title_tag:
                title = title_tag.get_text(strip=True)
            elif not title and event_html.name == 'a' and event_html.get('href'):
                href = event_html.get('href')
                title, _ = self._parse_event_meta_from_url(href)
            if not title:
                img = event_html.find('img', alt=True)
                alt = img.get('alt', '').strip() if img else ''
                if alt and not alt.lower().startswith('banner') and not alt.lower().startswith('ticketpro logo'):
                    title = alt
            if not title:
                page_text = event_html.get_text(' ', strip=True)
                title = self._extract_title_from_text(page_text)
            if not title:
                return None

            source_url = ''
            if event_html.name == 'a' and event_html.get('href'):
                source_url = event_html['href']
                if not source_url.startswith('http'):
                    source_url = self.base_url + source_url
            else:
                source_url = self.base_url

            # === JSON-LD — основной источник данных ===
            ld_url = ld_desc = ld_place = ld_price = ld_address = ""
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
                    ld_address = loc.get('address', {})
                    if isinstance(ld_address, dict):
                        ld_address = ld_address.get('addressLocality', '') or ld_address.get('streetAddress', '')
                    offers = ld.get('offers', {})
                    if offers:
                        ld_price = format_price_from_offers(offers)
                except Exception:
                    pass

            place_raw = ld_place
            page_text = event_html.get_text(' ', strip=True)
            if not place_raw:
                place_tag = event_html.find('div', class_='event-box__place')
                if place_tag:
                    place_raw = place_tag.get_text(strip=True)
                else:
                    place_raw = self._extract_place_from_html(BeautifulSoup(str(event_html), 'lxml'), page_text, title_hint or title, source_url)

            # Проверка на Минск — в TicketPro место события обычно уже явно содержит "Минск" или
            # нормализованное название площадки. Для таких страниц не нужно сканировать весь page_text,
            # потому что рядом лежат другие события и их названия городов создают ложные срабатывания.
            check_str = ' '.join(filter(None, [place_raw, ld_address]))
            if not check_str:
                event_window = page_text
                needle_text = title_hint or title
                positions = []
                for token in [needle_text, place_raw, ld_address]:
                    token = (token or '').strip()
                    if not token:
                        continue
                    idx = page_text.lower().find(token.lower())
                    if idx != -1:
                        positions.append(idx)
                if positions:
                    start = max(0, min(positions) - 60)
                    end = min(len(page_text), max(positions) + 300)
                    event_window = page_text[start:end]
                check_str = event_window
            if not is_minsk_event(check_str):
                self.stats['filtered_out'] += 1
                return None

            place = normalize_place(place_raw) if place_raw else ''

            date_text = ''
            date_tag = event_html.find('div', class_='event-box__date')
            if date_tag:
                date_text = date_tag.get_text(strip=True)
            if not date_text and title_hint:
                m = re.search(r'(\d{1,2})\s+[а-я]+\s+(\d{4})', title_hint, flags=re.I | re.U)
                if m:
                    day, year = m.groups()
                    month_name = re.search(r'\d{1,2}\s+([а-я]+)\s+\d{4}', title_hint, flags=re.I | re.U)
                    if month_name:
                        month_num = self._month_name_to_number(month_name.group(1))
                        if month_num:
                            date_text = f"{day} {month_name.group(1)} {year}"
            if not date_text:
                text = page_text
                date_match = re.search(r'(\d{1,2})\s+(?:[а-я]+)\s+(\d{4})', text)
                if date_match:
                    day, year = date_match.groups()
                    month_match = re.search(r'(?:\d{1,2})\s+([а-я]+)\s+(\d{4})', text)
                    if month_match:
                        month_name = month_match.group(1)
                        month_num = self._month_name_to_number(month_name)
                        if month_num:
                            date_text = f"{day} {month_name} {year}"
            
            event_date = show_time = ""
            if date_text:
                date_match = re.search(r'(\d{1,2})\s+([а-я]+)\s+(\d{4})', date_text, flags=re.I)
                if date_match:
                    day, month_name, year = date_match.groups()
                    month_num = self._month_name_to_number(month_name)
                    if month_num:
                        event_date = f"{year}-{month_num}-{int(day):02d}"
                time_match = re.search(r'(\d{1,2}:\d{2})', date_text)
                if time_match:
                    show_time = time_match.group(1)

            if (not event_date or not show_time) and script and script.string:
                try:
                    raw = re.sub(r'[\x00-\x1f\x7f]', ' ', script.string)
                    ld = json.loads(raw)
                    sd = ld.get('startDate', '')
                    if sd and 'T' in sd:
                        dt = datetime.fromisoformat(sd)
                        if not event_date:
                            event_date = dt.strftime('%Y-%m-%d')
                        if not show_time and dt.hour != 0:
                            show_time = dt.strftime('%H:%M')
                except Exception:
                    pass

            if not event_date:
                title_from_url, event_date = self._parse_event_meta_from_url(source_url)
                if title_from_url and not title:
                    title = title_from_url

            if event_date and not show_time:
                m = re.search(r'(\d{1,2}:\d{2})', str(event_html), re.I)
                if m:
                    show_time = m.group(1)

            if not event_date:
                return None

            price = ld_price
            if not price:
                price_text = ''
                price_tag = event_html.find('div', class_='event-box__price')
                if price_tag:
                    price_text = price_tag.get_text(strip=True)
                else:
                    price_text = self._extract_price_from_html(BeautifulSoup(str(event_html), 'lxml'), str(event_html))
                price = normalize_price(price_text) if price_text else ""

            if not source_url or source_url == self.base_url:
                first_link = event_html.find('a', href=True)
                if first_link:
                    source_url = self.base_url + first_link['href'] if not first_link['href'].startswith('http') else first_link['href']

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
                'details': '',
                'description': description,
                'event_date': event_date,
                'show_time': show_time,
                'place': place,
                'location': 'Минск',
                'price': price,
                'category': category,
                'source_url': source_url,
                'source_name': 'ticketpro.by'
            }

        except Exception as e:
            logger.error(f"Ошибка парсинга HTML: {e}")
            return None

    def parse_category_page(self, category_url: str, category: str, display_name: str) -> List[Dict]:
        """Парсит все страницы категории.

        В новой версии TicketPro старые классы `.event-box` больше не используются.
        Поэтому сначала собираем ссылки `/ru/event/...` прямо из карточек страницы,
        а затем грузим каждое событие по отдельной странице и парсим уже детальную разметку.
        """
        events = []
        page = 1
        base_url = self.base_url + category_url
        max_pages = 6

        relax_index = self.load_relax_index()
        while page <= max_pages:
            url = f"{base_url}?page={page}" if page > 1 else base_url
            logger.info(f"Загрузка страницы {page} для {display_name}")
            html = self.fetch_page(url)
            if not html:
                break

            soup = BeautifulSoup(html, 'lxml')
            event_links = []
            seen_urls = set()
            for link in soup.select('a[href*="/ru/event/"]'):
                href = link.get('href', '')
                if not href or '/ru/event/' not in href:
                    continue
                full_url = href if href.startswith('http') else (self.base_url + href)
                if full_url not in seen_urls:
                    seen_urls.add(full_url)
                    event_links.append(full_url)

            old_event_boxes = soup.find_all('div', class_='event-box')
            if old_event_boxes:
                for event_box in old_event_boxes:
                    event = self.parse_event_from_html(event_box, category, display_name, relax_index)
                    if event:
                        events.append(event)

            logger.info(f"Страница {page}: обнаружено {len(event_links)} ссылок на события")
            if not event_links and not old_event_boxes:
                logger.info(f"Нет ссылок/блоков на странице {page}, завершаем")
                break

            for link in soup.select('a[href*="/ru/event/"]'):
                href = link.get('href', '')
                if not href or '/ru/event/' not in href:
                    continue
                full_url = href if href.startswith('http') else (self.base_url + href)
                if full_url not in seen_urls:
                    continue
                title_hint = re.sub(r'\s+', ' ', link.get_text(' ', strip=True))
                self.stats['total_events_found'] += 1
                detail_html = self.fetch_page(full_url)
                if not detail_html:
                    continue
                detail_soup = BeautifulSoup(detail_html, 'lxml')
                event = self.parse_event_from_html(detail_soup, category, display_name, relax_index, title_hint=title_hint)
                if event:
                    events.append(event)

            logger.info(f"Страница {page}: накоплено {len(events)} событий")
            self.stats['total_pages'] += 1

            # Пагинация: если у категории нет page-next, но есть другие ссылки, мы всё равно
            # завершаем — новый TicketPro рендерит список без старой пагинации.
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
