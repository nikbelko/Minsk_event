#!/usr/bin/env python3
# telegram_web.py
# Парсер для Telegram каналов с анонсами

import os
import re
import sqlite3
import logging
import time
from datetime import datetime, date, timedelta

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path

from normalizer import normalize_place, normalize_price, normalize_title

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ── Конфиг ───────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "/data/events_final.db")
SOURCE_NAME = "tg_partywall"  # по умолчанию
CHANNEL = "partywall_minsk"

# Можно передавать через переменную окружения или аргументы
# CHANNEL = os.getenv("TG_CHANNEL", "partywall_minsk")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("telegram_parser.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}

# ── Месяцы для парсинга дат ──────────────────────────────────────────────────
MONTHS = {
    "янв": 1, "фев": 2, "мар": 3, "апр": 4, "мая": 5, "май": 5,
    "июн": 6, "июл": 7, "авг": 8, "сен": 9, "окт": 10, "ноя": 11, "дек": 12,
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

# ── Маппинг хэштегов в формат события (для partywall) ────────────────────────
HASHTAG_TO_FORMAT = {
    'retro': 'ретро-вечеринка',
    'pop': 'поп-вечеринка',
    'rap': 'хип-хоп вечеринка',
    'hiphop': 'хип-хоп вечеринка',
    'hardtechno': 'хард-техно вечеринка',
    'techno': 'техно-вечеринка',
    'trance': 'транс-вечеринка',
    'electronic': 'электронная вечеринка',
    'allelectronic': 'электронная вечеринка',
    'hardtrance': 'хард-транс вечеринка',
    'disco': 'дискотека',
    'house': 'хаус-вечеринка',
    'drumandbass': 'драм-н-бейс',
    'dnb': 'драм-н-бейс',
    'rock': 'рок-концерт',
    'indie': 'инди-вечеринка',
}


def detect_channel_type(channel: str) -> str:
    """Определяет тип канала по имени"""
    if channel == "partywall_minsk":
        return "partywall"
    elif channel == "minsk_kuda":
        return "kuda"
    else:
        return "unknown"


# ==================== ОБЩИЕ ФУНКЦИИ ====================

def parse_url_from_text(text: str) -> str | None:
    """Извлекает ссылку из текста"""
    match = re.search(r'(https?://[^\s\n]+)', text)
    if match:
        return match.group(1)
    return None


def clean_address(address: str) -> str:
    """Очищает адрес от "Минск" и лишних пробелов"""
    if not address:
        return ""
    address = re.sub(r'^Минск,\s*', '', address)
    address = re.sub(r'^г\.\s*Минск,\s*', '', address)
    address = re.sub(r',\s*Минск$', '', address, flags=re.IGNORECASE)
    address = re.sub(r'\s*Минск$', '', address, flags=re.IGNORECASE)
    address = re.sub(r'\s+', ' ', address).strip()
    return address


def parse_date_from_text_kuda(text: str) -> tuple[str | None, str | None]:
    """Парсит дату из формата @minsk_kuda"""
    text = text.lower()
    
    # Формат: "28 марта (сб) в 18:00"
    match = re.search(r'(\d{1,2})\s+([а-я]+)\s*\([^)]+\)\s+в\s+(\d{1,2}:\d{2})', text, re.IGNORECASE)
    if match:
        day, month_name, time_str = match.groups()
        month = MONTHS.get(month_name)
        if month:
            year = datetime.now().year
            if month < datetime.now().month:
                year += 1
            try:
                ev_date = datetime(year, month, int(day))
                return ev_date.strftime("%Y-%m-%d"), time_str
            except ValueError:
                pass
    
    # Формат: "До 10 апреля"
    match = re.search(r'до\s+(\d{1,2})\s+([а-я]+)', text, re.IGNORECASE)
    if match:
        day, month_name = match.groups()
        month = MONTHS.get(month_name)
        if month:
            year = datetime.now().year
            if month < datetime.now().month:
                year += 1
            try:
                ev_date = datetime(year, month, int(day))
                return ev_date.strftime("%Y-%m-%d"), None
            except ValueError:
                pass
    
    return None, None


def parse_date_from_text_partywall(text: str) -> tuple[str | None, str | None]:
    """Парсит дату из формата @partywall_minsk"""
    text = text.lower()
    
    match = re.search(r'(пятница|суббота|воскресенье)\s+(\d{1,2})\.(\d{1,2})', text, re.IGNORECASE)
    if match:
        day_name, d, m = match.groups()
        try:
            d_int, m_int = int(d), int(m)
            year = datetime.now().year
            if m_int < datetime.now().month:
                year += 1
            ev_date = datetime(year, m_int, d_int)
            return ev_date.strftime("%Y-%m-%d"), None
        except ValueError:
            return None, None
    
    return None, None


# ==================== ПАРСЕР ДЛЯ @partywall_minsk ====================

def parse_age_restriction(text: str) -> str | None:
    match = re.search(r'(1[268]\+|6\+|0\+)', text)
    return match.group(1) if match else None


def parse_hashtags(text: str) -> list:
    return re.findall(r'#([\wа-яё]+)', text, re.IGNORECASE)


def get_details_from_hashtags(hashtags: list) -> str:
    if not hashtags:
        return ""
    
    formats = []
    for tag in hashtags:
        tag_lower = tag.lower()
        if tag_lower in HASHTAG_TO_FORMAT:
            formats.append(HASHTAG_TO_FORMAT[tag_lower])
    
    if formats:
        unique = []
        for f in formats:
            if f not in unique:
                unique.append(f)
        if len(unique) == 1:
            return unique[0]
        return f"{', '.join(unique)}"
    
    return "вечеринка"


def parse_event_block_partywall(block: str, default_date: str | None = None, all_links: dict = None) -> dict | None:
    """Парсит блок события в формате @partywall_minsk"""
    lines = [l.strip() for l in block.split('\n') if l.strip()]
    if len(lines) < 3:
        return None
    
    # Время
    time_match = re.search(r'(\d{1,2}):(\d{2})', lines[0])
    if not time_match:
        return None
    show_time = f"{time_match.group(1).zfill(2)}:{time_match.group(2)}"
    
    # Название
    title = ""
    for i, line in enumerate(lines):
        if line == 'Кто:' or line == 'Кто :':
            if i + 1 < len(lines):
                title = lines[i + 1]
                break
    
    if not title:
        who_line = [l for l in lines if l.startswith('Кто:') or l.startswith('Кто :')]
        if who_line:
            title = who_line[0].replace('Кто:', '').replace('Кто :', '').strip()
    
    # Ссылка
    url = None
    if all_links and title:
        if title in all_links:
            url = all_links[title]
    
    # Место
    place = ""
    for i, line in enumerate(lines):
        if line == 'Где:' or line == 'Где :':
            if i + 1 < len(lines):
                place = normalize_place(lines[i + 1])
                break
    
    if not place:
        where_line = [l for l in lines if l.startswith('Где:') or l.startswith('Где :')]
        if where_line:
            place = normalize_place(where_line[0].replace('Где:', '').replace('Где :', '').strip())
    
    # Адрес
    address = ""
    for i, line in enumerate(lines):
        if line == 'Где:' or line == 'Где :':
            if i + 2 < len(lines):
                next_line = lines[i + 2]
                if not next_line.startswith(('Вход:', '18+', '#', 'http')):
                    address = clean_address(next_line)
                    break
    
    if not address:
        for line in lines:
            if re.search(r'(ул\.|пр-т|проспект|площадь|пер\.|бульвар)', line, re.IGNORECASE):
                address = clean_address(line)
                break
    
    # Цена
    price_line = [l for l in lines if l.startswith(('Вход:', 'Вход :'))]
    price = ""
    if price_line:
        price_text = price_line[0].replace('Вход:', '').replace('Вход :', '').strip()
        if 'FREE' in price_text.upper() or 'free' in price_text.lower():
            price = "Бесплатно"
        elif '/' in price_text:
            min_price = price_text.split('/')[0].strip()
            price = f"от {min_price} руб"
        elif price_text:
            price = normalize_price(price_text)
    
    # Возраст и хэштеги
    full_text = ' '.join(lines)
    age_restriction = parse_age_restriction(full_text)
    hashtags = parse_hashtags(full_text)
    hashtags_str = ' '.join([f"#{h}" for h in hashtags]) if hashtags else ""
    details = get_details_from_hashtags(hashtags)
    
    description_parts = []
    if hashtags_str:
        description_parts.append(hashtags_str)
    if age_restriction:
        description_parts.append(f"🔞 {age_restriction}")
    description = "\n".join(description_parts) if description_parts else ""
    
    # Дата
    event_date = default_date
    if not event_date:
        for line in lines:
            date_str, _ = parse_date_from_text_partywall(line)
            if date_str:
                event_date = date_str
                break
    
    if not event_date:
        return None
    
    # Проверка даты
    try:
        ev_date = datetime.strptime(event_date, "%Y-%m-%d").date()
        if ev_date < date.today() or ev_date > date.today() + timedelta(days=180):
            return None
    except ValueError:
        return None
    
    return {
        "title": title[:100] if title else "",
        "details": details[:200] if details else "",
        "event_date": event_date,
        "show_time": show_time,
        "place": place,
        "location": address,
        "price": price,
        "category": "party",
        "description": description,
        "source_url": url,
    }


def extract_events_from_post_partywall(post: dict) -> list:
    """Извлекает события из поста @partywall_minsk"""
    events = []
    text = post["text"]
    soup = post["soup"]
    lines = text.split('\n')
    
    all_links = {}
    for a in soup.find_all("a"):
        href = a.get("href")
        link_text = a.get_text(strip=True)
        if href and 't.me/partywall' not in href and 't.me/s/partywall' not in href:
            all_links[link_text] = href
    
    current_date = None
    current_block = []
    in_block = False
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        date_match = re.search(r'(пятница|суббота|воскресенье)\s+(\d{1,2})\.(\d{1,2})', line, re.IGNORECASE)
        if date_match:
            if current_block and current_date:
                ev = parse_event_block_partywall('\n'.join(current_block), current_date, all_links)
                if ev:
                    events.append(ev)
            current_block = []
            date_str, _ = parse_date_from_text_partywall(line)
            current_date = date_str
            in_block = True
            continue
        
        if re.match(r'^\d{1,2}:\d{2}$', line):
            if current_block and current_date:
                ev = parse_event_block_partywall('\n'.join(current_block), current_date, all_links)
                if ev:
                    events.append(ev)
            current_block = [line]
            in_block = True
        elif in_block:
            current_block.append(line)
    
    if current_block and current_date:
        ev = parse_event_block_partywall('\n'.join(current_block), current_date, all_links)
        if ev:
            events.append(ev)
    
    return events


# ==================== ПАРСЕР ДЛЯ @minsk_kuda ====================

def parse_event_kuda(post: dict) -> dict | None:
    """Парсит один пост канала @minsk_kuda"""
    text = post["text"]
    lines = text.split('\n')
    
    # 1. Заголовок — первая непустая строка, не начинающаяся с эмодзи
    title = ""
    for line in lines:
        line = line.strip()
        if line and not line.startswith(('⏰', '📍', '💵', '🎫', '✅', '🔹', '—', '📅')):
            title = line
            break
    
    if not title or len(title) < 3:
        return None
    
    # 2. Время и дата
    event_date = None
    show_time = None
    for line in lines:
        if '⏰' in line:
            date_str, time_str = parse_date_from_text_kuda(line)
            if date_str:
                event_date = date_str
                show_time = time_str
                break
    
    if not event_date:
        return None
    
    # Проверка даты
    try:
        ev_date = datetime.strptime(event_date, "%Y-%m-%d").date()
        if ev_date < date.today() or ev_date > date.today() + timedelta(days=180):
            return None
    except ValueError:
        return None
    
    # 3. Место и адрес
    place = ""
    address = ""
    for line in lines:
        if '📍' in line:
            place_raw = line.replace('📍', '').strip()
            if ',' in place_raw:
                parts = place_raw.split(',', 1)
                place = normalize_place(parts[0].strip())
                address = clean_address(parts[1].strip())
            else:
                place = normalize_place(place_raw)
            break
    
    # 4. Цена
    price = ""
    for line in lines:
        if '💵' in line:
            price_text = line.replace('💵', '').strip()
            if 'свободный' in price_text.lower() or 'free' in price_text.lower():
                price = "Бесплатно"
            elif price_text:
                price = normalize_price(price_text)
            break
    
    # 5. Ссылка
    url = None
    for line in lines:
        if 'https://' in line:
            url = parse_url_from_text(line)
            if url:
                break
    
    # 6. Описание — всё, что не вошло в другие поля
    description_lines = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith(('⏰', '📍', '💵', '🎫', '✅')):
            if line != title:
                description_lines.append(line)
    
    description = '\n'.join(description_lines[:500])[:500]
    
    # 7. Детали (формат события) — из первых строк описания или заголовка
    details = ""
    if title:
        # Определяем категорию из заголовка
        if 'спектакль' in title.lower() or 'театр' in title.lower():
            details = "театральное событие"
        elif 'выставк' in title.lower() or 'экспозиц' in title.lower():
            details = "выставка"
        elif 'концерт' in title.lower() or 'музык' in title.lower():
            details = "концерт"
        elif 'лекц' in title.lower() or 'семинар' in title.lower():
            details = "лекция"
        elif 'маркет' in title.lower() or 'ярмарк' in title.lower():
            details = "маркет"
        else:
            details = title[:100]
    
    return {
        "title": title[:100],
        "details": details[:200],
        "description": description[:500],
        "event_date": event_date,
        "show_time": show_time or "",
        "place": place,
        "location": address,
        "price": price,
        "category": "other",  # можно определить по ключевым словам
        "source_url": url or post["post_url"],
    }


def extract_events_from_post_kuda(post: dict) -> list:
    """Извлекает события из поста @minsk_kuda (один пост = одно событие)"""
    ev = parse_event_kuda(post)
    return [ev] if ev else []


# ==================== ОБЩИЕ ФУНКЦИИ ====================

def fetch_channel_posts(channel: str) -> list:
    """Читает публичный канал через t.me/s/..."""
    base_url = f"https://t.me/s/{channel}"
    posts = []
    before_id = None
    
    for page in range(10):
        url = f"{base_url}?before={before_id}" if before_id else base_url
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Ошибка загрузки @{channel}: {e}")
            break
        
        soup = BeautifulSoup(resp.text, "lxml")
        wraps = soup.find_all("div", class_="tgme_widget_message_wrap")
        if not wraps:
            break
        
        logger.info(f"  стр.{page+1}: {len(wraps)} постов")
        
        min_id = None
        for wrap in wraps:
            time_tag = wrap.find("time")
            if not time_tag or not time_tag.get("datetime"):
                continue
            
            datetime_str = time_tag["datetime"]
            if datetime_str.endswith('Z'):
                datetime_str = datetime_str.replace('Z', '+00:00')
            try:
                post_dt = datetime.fromisoformat(datetime_str)
            except:
                continue
            
            since_dt = datetime.now().replace(tzinfo=post_dt.tzinfo) - timedelta(days=7)
            if post_dt < since_dt:
                continue
            
            msg_div = wrap.find("div", class_="tgme_widget_message")
            data_post = msg_div.get("data-post", "") if msg_div else ""
            post_id = data_post.split("/")[-1] if "/" in data_post else None
            
            if post_id and post_id.isdigit():
                pid = int(post_id)
                min_id = pid if min_id is None else min(min_id, pid)
            
            text_div = wrap.find("div", class_="tgme_widget_message_text")
            if not text_div:
                continue
            
            text_div_copy = text_div.__copy__()
            text = text_div.get_text(separator="\n", strip=True)
            
            if not text or len(text) < 50:
                continue
            
            posts.append({
                "text": text,
                "soup": text_div_copy,
                "date": post_dt,
                "post_url": f"https://t.me/{channel}/{post_id}" if post_id else base_url,
            })
        
        if min_id is None:
            break
        before_id = min_id
        time.sleep(1.5)
    
    logger.info(f"  Загружено {len(posts)} постов")
    return posts


def load_existing_index(source_name: str) -> dict:
    """Загружает индекс существующих событий из других источников"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT title, event_date, place, show_time 
        FROM events 
        WHERE source_name != ?
    """, (source_name,))
    rows = cursor.fetchall()
    conn.close()
    
    index = {}
    for title, ev_date, place, show_time in rows:
        norm_title = normalize_title(title)
        norm_place = normalize_place(place) if place else ""
        index.setdefault(ev_date, []).append(
            (norm_title, norm_place, show_time or "")
        )
    logger.info(f"📋 Индекс дублей (другие источники): {sum(len(v) for v in index.values())} событий")
    return index


def is_duplicate(title: str, event_date: str, place: str, show_time: str, index: dict) -> bool:
    if not title or not event_date:
        return False
    
    norm_title = normalize_title(title)
    norm_place = normalize_place(place) if place else ""
    candidates = index.get(event_date, [])
    
    for ex_title, ex_place, ex_time in candidates:
        if norm_title == ex_title:
            return True
        if norm_place and ex_place and show_time and ex_time:
            if norm_place == ex_place and show_time == ex_time:
                return True
    
    return False


def save_event(event: dict, source_name: str, post_url: str) -> bool:
    """Сохраняет событие в БД"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        final_url = event.get("source_url") or post_url
        
        cursor.execute("""
            INSERT INTO events
              (title, details, description, event_date, show_time,
               place, location, price, category, source_name, source_url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            event["title"],
            event.get("details", ""),
            event.get("description", ""),
            event["event_date"],
            event.get("show_time", ""),
            event.get("place", ""),
            event.get("location", ""),
            event.get("price", ""),
            event.get("category", "other"),
            source_name,
            final_url,
        ))
        added = cursor.rowcount > 0
        conn.commit()
        return added
    except Exception as e:
        logger.error(f"Ошибка сохранения '{event.get('title')}': {e}")
        return False
    finally:
        conn.close()


def clean_old_events(source_name: str):
    """Удаляет все старые события этого источника"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM events WHERE source_name = ?", (source_name,))
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted:
        logger.info(f"🗑 Удалено {deleted} старых записей {source_name}")


def run_for_channel(channel: str):
    """Запускает парсер для указанного канала"""
    channel_type = detect_channel_type(channel)
    source_name = f"tg_{channel}" if channel_type == "partywall" else f"tg_{channel}"
    
    logger.info("=" * 55)
    logger.info(f"📱 TELEGRAM ПАРСЕР: @{channel} (тип: {channel_type})")
    logger.info("=" * 55)
    
    clean_old_events(source_name)
    index = load_existing_index(source_name)
    posts = fetch_channel_posts(channel)
    
    stats = {"posts": 0, "events_found": 0, "saved": 0, "duplicates": 0}
    
    for post in posts:
        stats["posts"] += 1
        
        if channel_type == "partywall":
            events = extract_events_from_post_partywall(post)
        else:
            events = extract_events_from_post_kuda(post)
        
        if not events:
            continue
        
        stats["events_found"] += len(events)
        seen_in_post = set()
        
        for event in events:
            post_key = (event["title"], event["event_date"], event.get("show_time", ""))
            if post_key in seen_in_post:
                continue
            seen_in_post.add(post_key)
            
            if is_duplicate(
                event["title"], event["event_date"],
                event.get("place", ""), event.get("show_time", ""),
                index
            ):
                stats["duplicates"] += 1
                continue
            
            if save_event(event, source_name, post["post_url"]):
                stats["saved"] += 1
                logger.info(
                    f"  ✅ {event['event_date']} {event['show_time']:5} | "
                    f"{event['title'][:30]:30} | {event.get('place', '')[:20]} | "
                    f"{event.get('price', '')}"
                )
                norm_title = normalize_title(event["title"])
                norm_place = normalize_place(event.get("place", ""))
                index.setdefault(event["event_date"], []).append(
                    (norm_title, norm_place, event.get("show_time", ""))
                )
    
    logger.info("=" * 55)
    logger.info(
        f"📊 Итого: постов {stats['posts']}, "
        f"событий {stats['events_found']}, "
        f"сохранено {stats['saved']}, "
        f"дублей {stats['duplicates']}"
    )
    print(f"RESULT:tg@{channel}:{stats['events_found']}:{stats['saved']}")
    
    return stats["saved"]


def run():
    """Запускает парсеры для всех каналов"""
    channels = ["partywall_minsk", "minsk_kuda"]
    
    total_saved = 0
    for channel in channels:
        saved = run_for_channel(channel)
        total_saved += saved
    
    logger.info(f"📊 ВСЕГО СОХРАНЕНО: {total_saved}")
    return total_saved


if __name__ == "__main__":
    run()
