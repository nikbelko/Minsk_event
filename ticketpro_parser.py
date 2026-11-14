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
from normalizer import (
    normalize_place,
    normalize_title,
    is_minsk_event,
    format_price_from_offers,
    normalize_price,
    ensure_default_end_time,
)
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
    @staticmethod
    def _normalize_url(url: str) -> str:
        if not url:
            return url
        return url.replace('https://www.ticketpro.by', 'https://ticketpro.by').replace('http://www.ticketpro.by', 'http://ticketpro.by')

    def __init__(self, DB_PATH=os.getenv("DB_PATH", "/data/events_final.db")):
        DB_PATH = DB_PATH
        self.base_url = 'https://ticketpro.by'
        
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

    def _extract_event_ld(self, soup: BeautifulSoup) -> dict:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text(' ', strip=True)
            if not raw:
                continue
            try:
                data = json.loads(re.sub(r'[\x00-\x1f\x7f]', ' ', raw))
            except Exception:
                continue

            def walk(node):
                if isinstance(node, dict):
                    ctype = node.get('@type')
                    types = ctype if isinstance(ctype, list) else ([ctype] if ctype else [])
                    if any('Event' in str(t) for t in types):
                        return node
                    for value in node.values():
                        result = walk(value)
                        if result:
                            return result
                elif isinstance(node, list):
                    for item in node:
                        result = walk(item)
                        if result:
                            return result
                return {}

            result = walk(data)
            if result and (result.get('name') or result.get('startDate') or result.get('location')):
                return result

            if isinstance(data, dict):
                if data.get('@type') == 'Event' or (data.get('name') and data.get('startDate')):
                    return data
                if '@graph' in data:
                    graph = data.get('@graph')
                    if isinstance(graph, list):
                        for item in graph:
                            if isinstance(item, dict) and ('Event' in str(item.get('@type', '')) or item.get('name') and item.get('startDate')):
                                return item
        return {}

    @staticmethod
    def _parse_iso_date_from_text(value: str) -> Optional[str]:
        """Extract ISO date from TicketPro text like '13 февраля 2027' or '3 Декабря 2026'."""
        if not value:
            return None
        matches = []
        for m in re.finditer(r'(\d{1,2})\s+[A-Za-zА-Яа-яЁё]+\s+([A-Za-zА-Яа-яЁё]+)\s+(\d{4})', value, flags=re.I | re.U):
            day = int(m.group(1))
            month_name = m.group(2).strip()
            year = int(m.group(3))
            month_num = TicketproParser._month_name_to_number(month_name)
            if month_num:
                matches.append(f"{year}-{month_num}-{day:02d}")
        if matches:
            return max(matches)
        return None

    def _extract_event_urls_from_page(self, page_html: str) -> List[str]:
        """Return event URLs from TicketPro's embedded JS payload when present.

        TicketPro renders the category list through a Next.js payload and hides the
        full dataset behind the UI's 'show more' control. The visible DOM only exposes
        a partial list, so we prefer the embedded `eventsPage.items` payload and fall
        back to DOM links only when that payload is absent.
        """
        urls: set[str] = set()

        marker = '"eventsPage"'
        idx = page_html.find(marker)
        if idx != -1:
            start = page_html.find('{', idx + len(marker))
            if start != -1:
                in_string = False
                escape = False
                depth = 0
                end = None
                for i in range(start, len(page_html)):
                    ch = page_html[i]
                    if in_string:
                        if escape:
                            escape = False
                        elif ch == '\\':
                            escape = True
                        elif ch == '"':
                            in_string = False
                    else:
                        if ch == '"':
                            in_string = True
                        elif ch == '{':
                            depth += 1
                        elif ch == '}':
                            depth -= 1
                            if depth == 0:
                                end = i + 1
                                break

                if end is not None:
                    candidate = page_html[start:end]
                    candidate = candidate.replace('\\"', '"').replace('\\/', '/')
                    try:
                        data = json.loads(candidate)
                        items = data.get('items', []) if isinstance(data, dict) else []
                        for item in items:
                            slug = item.get('slug')
                            if slug:
                                url = slug if slug.startswith('http') else f"{self.base_url}/ru/event/{slug.lstrip('/')}"
                                urls.add(self._normalize_url(url))
                    except Exception:
                        pass

        if urls:
            return sorted(urls)

        soup = BeautifulSoup(page_html, 'lxml')
        for link in soup.select('a[href*="/ru/event/"]'):
            href = link.get('href', '')
            if not href:
                continue
            full_url = href if href.startswith('http') else (self.base_url + href)
            urls.add(self._normalize_url(full_url))
        return sorted(urls)

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

    @staticmethod
    def _clean_display_title(title: str) -> str:
        if not title:
            return ''
        clean = re.sub(r'\s+', ' ', title).strip()

        # TicketPro-specific garbage: repeated phrases like "ПЕРЕНОС ПЕРЕНОС".
        clean = re.sub(r'(?iu)\b([а-яёa-z]+)\s+\1\b', r'\1', clean)
        clean = re.sub(r'(?iu)\b(перенос)\s+\1\b', r'\1', clean)

        # Remove a trailing single-letter "С" only for obvious noisy fallback text,
        # not for valid names such as "Виктор С" or "Песня С".
        if re.search(r'(?iu)\b(?:перенос|переход|переносы|переносить)\s+[с]$', clean):
            clean = re.sub(r'(?iu)\s+[с]$', '', clean)

        clean = re.sub(r'\s+[|]\s*$', '', clean)
        clean = clean.strip(' -—–:,."\'')
        return clean

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
                title = re.sub(r'\s+', ' ', match.group(1)).strip(' -—–:,."\'')
                title = self._clean_display_title(title)
                if title and len(title) > 1:
                    return title
        return ''

    def parse_event_from_html(self, event_html, category: str, display_name: str, relax_index: dict = None, title_hint: str = '', source_url_hint: str = '') -> Optional[Dict]:
        try:
            source_url = source_url_hint.strip() if source_url_hint else ''
            if not source_url:
                canonical = event_html.select_one('link[rel="canonical"]')
                og_url = event_html.select_one('meta[property="og:url"]')
                if canonical and canonical.get('href'):
                    source_url = self._normalize_url(canonical['href'])
                elif og_url and og_url.get('content'):
                    source_url = self._normalize_url(og_url['content'])
                elif event_html.name == 'a' and event_html.get('href'):
                    source_url = event_html['href']
                    if not source_url.startswith('http'):
                        source_url = self.base_url + source_url
                    source_url = self._normalize_url(source_url)
                else:
                    source_url = self.base_url
            else:
                source_url = self._normalize_url(source_url)

            ld = self._extract_event_ld(event_html)
            ld_url = ld.get('url', '') or source_url
            ld_desc = (ld.get('description', '') or '').strip()
            if len(ld_desc) > 300:
                ld_desc = ld_desc[:297] + '...'

            location_node = ld.get('location', {})
            ld_place = ''
            ld_address = ''
            if isinstance(location_node, dict):
                ld_place = location_node.get('name', '')
                address_node = location_node.get('address', {})
                if isinstance(address_node, dict):
                    street = (address_node.get('streetAddress') or '').strip()
                    locality = (address_node.get('addressLocality') or '').strip()
                    if street and locality:
                        ld_address = f"{street}, {locality}"
                    else:
                        ld_address = street or locality
            elif isinstance(location_node, str):
                ld_place = location_node

            offers = ld.get('offers', {})
            if isinstance(offers, list):
                offers = offers[0] if offers else {}
            ld_price = format_price_from_offers(offers) if offers else ''

            title = ''
            if title_hint:
                title = title_hint
                title = re.sub(r'\s+', ' ', title).strip()
                title = re.sub(r'\s+\d{1,2}\s+[а-я]+(?:\s+\d{4})?(?:\s+(?:сб|вс|пн|вт|ср|чт|пт))?(?:,\s*\d{1,2}:\d{2})?.*$', '', title, flags=re.I | re.U)
                title = title.strip(' -—–:,.\"\'')
            elif ld.get('name'):
                ld_name = str(ld.get('name')).strip()
                if not re.search(r'офис\s+\d+|свидетельство\s+о\s+государственной\s+регистрации|юридического\s+лица|ticketpro|ООО', ld_name, re.I | re.U):
                    title = ld_name

            if not title:
                title_tag = event_html.find('div', class_='event-box__title')
                if title_tag:
                    title = title_tag.get_text(strip=True)
            if not title and event_html.name == 'a' and event_html.get('href'):
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
            if title:
                title = self._clean_display_title(title)
                title = re.sub(r'\s+', ' ', title).strip()
            if not title:
                return None

            page_text = event_html.get_text(' ', strip=True)

            place_raw = ld_place
            if not place_raw:
                place_tag = event_html.find('div', class_='event-box__place')
                if place_tag:
                    place_raw = place_tag.get_text(strip=True)
                else:
                    place_raw = self._extract_place_from_html(BeautifulSoup(str(event_html), 'lxml'), page_text, title_hint or title, source_url)

            event_date = show_time = ""
            start_date = ld.get('startDate', '')
            if start_date:
                try:
                    if 'T' in str(start_date):
                        dt = datetime.fromisoformat(str(start_date))
                        event_date = dt.strftime('%Y-%m-%d')
                        if dt.hour != 0:
                            show_time = dt.strftime('%H:%M')
                    else:
                        event_date = str(start_date)[:10]
                except Exception:
                    pass

            if not event_date:
                url_title, url_date = self._parse_event_meta_from_url(source_url)
                if url_date:
                    event_date = url_date
                    if url_title and not title:
                        title = url_title

            if not event_date or not show_time:
                for script in event_html.find_all('script'):
                    raw = script.string or script.get_text(' ', strip=True)
                    if not raw or 'startTime' not in raw:
                        continue
                    m = re.search(r'"startTime":"(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})', raw)
                    if m:
                        if not event_date:
                            event_date = m.group(1)
                        if not show_time:
                            show_time = m.group(2)
                        break

            if event_date and not show_time:
                m = re.search(r'"startTime":"\d{4}-\d{2}-\d{2}T(\d{2}:\d{2})', str(event_html), re.I)
                if m:
                    show_time = m.group(1)

            if not event_date:
                # Fallback: parse a valid date from the page metadata or visible text.
                event_date = self._parse_iso_date_from_text(page_text)

            if not event_date:
                event_date = self._parse_iso_date_from_text(page_text)

            # If TicketPro shows a transfer marker, it may expose both a stale old date and a fresh replacement date.
            # Prefer the newest valid date from the metadata, not the URL slug date.
            if 'перенос' in page_text.lower():
                page_valid_date = self._parse_iso_date_from_text(page_text)
                if page_valid_date and (not event_date or page_valid_date > event_date):
                    event_date = page_valid_date

            if not event_date:
                return None

            # Some TicketPro pages include unrelated announcement/news blocks with publishedAt dates.
            # We intentionally ignore broad page-text dates here, because they often belong to blog/news cards
            # rather than the actual event being parsed.

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

            # Проверка на Минск — event schema уже идентифицирует это событие, поэтому не делаем
            # широкие поиски по всему page_text, если есть точное место или адрес площадки.
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

            event_record = {
                'title': title,
                'details': '',
                'description': description,
                'event_date': event_date,
                'show_time': show_time,
                'place': place,
                'location': ld_address or 'Минск',
                'price': price,
                'category': category,
                'source_url': source_url,
                'source_name': 'ticketpro.by'
            }
            return ensure_default_end_time(event_record)

        except Exception as e:
            logger.error(f"Ошибка парсинга HTML: {e}")
            return None

    def parse_category_page(self, category_url: str, category: str, display_name: str) -> List[Dict]:
        """Парсит все страницы категории.

        В новой версии TicketPro старые классы `.event-box` больше не используются.
        Поэтому сначала собираем ссылки `/ru/event/...` из embedded payload, а затем
        добавляем видимые ссылки из DOM как fallback. Это позволяет сохранить полный
        список даже при отсутствии нормальной пагинации.
        """
        events = []
        page = 1
        base_url = self.base_url + category_url
        max_pages = 20
        seen_urls = set()

        relax_index = self.load_relax_index()
        while page <= max_pages:
            url = f"{base_url}?page={page}" if page > 1 else base_url
            logger.info(f"Загрузка страницы {page} для {display_name}")
            html = self.fetch_page(url)
            if not html:
                break

            soup = BeautifulSoup(html, 'lxml')
            event_links = set(self._extract_event_urls_from_page(html))
            visible_links = {
                self._normalize_url(
                    href if href.startswith('http') else (self.base_url + href)
                )
                for link in soup.select('a[href*="/ru/event/"]')
                for href in [link.get('href', '')]
                if href
            }
            candidate_urls = event_links | visible_links
            logger.info(f"Страница {page}: обнаружено {len(candidate_urls)} уникальных ссылок на события")

            old_event_boxes = soup.find_all('div', class_='event-box')
            if old_event_boxes:
                for event_box in old_event_boxes:
                    event = self.parse_event_from_html(event_box, category, display_name, relax_index)
                    if event:
                        events.append(event)

            if not candidate_urls and not old_event_boxes:
                logger.info(f"Нет ссылок/блоков на странице {page}, завершаем")
                break

            for full_url in sorted(candidate_urls):
                if not full_url or full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                # Prefer the visible link text as a title hint when it exists.
                title_hint = ''
                for link in soup.select('a[href*="/ru/event/"]'):
                    href = link.get('href', '')
                    if not href:
                        continue
                    candidate = href if href.startswith('http') else (self.base_url + href)
                    candidate = self._normalize_url(candidate)
                    if candidate == full_url:
                        title_hint = re.sub(r'\s+', ' ', link.get_text(' ', strip=True))
                        break

                self.stats['total_events_found'] += 1
                detail_html = self.fetch_page(full_url)
                if not detail_html:
                    continue
                detail_soup = BeautifulSoup(detail_html, 'lxml')
                event = self.parse_event_from_html(
                    detail_soup,
                    category,
                    display_name,
                    relax_index,
                    title_hint=title_hint,
                    source_url_hint=full_url,
                )
                if event:
                    events.append(event)

            logger.info(f"Страница {page}: накоплено {len(events)} событий")
            self.stats['total_pages'] += 1

            pagination = soup.find('div', class_='pagination')
            next_link = pagination.find('a', class_='page-next') if pagination else None
            if not next_link or 'disabled' in next_link.get('class', []):
                logger.info("Нет следующей страницы или пагинация не поддерживается — выходим после текущей страницы")
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
                        title, details, description, event_date, show_time, end_time,
                        place, location, price, category, source_url, source_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event['title'], event['details'], event['description'],
                    event['event_date'], event['show_time'], event.get('end_time') or '',
                    event['place'], event['location'], event['price'], event['category'],
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
