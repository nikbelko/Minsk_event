#!/usr/bin/env python3
# telegram_parser.py
# Парсер публичных Telegram-каналов через t.me/s/ (без авторизации)
# + Claude API для извлечения событий из текста постов
#
# Переменные окружения (Railway):
#   ANTHROPIC_API_KEY  — для парсинга текста постов
#   DB_PATH            — путь к SQLite (по умолчанию /data/events_final.db)

import os
import re
import json
import sqlite3
import logging
import time
from datetime import datetime, date, timedelta

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ── Конфиг ───────────────────────────────────────────────────────────────────

DB_PATH       = os.getenv("DB_PATH", "/data/events_final.db")
GEMINI_KEY    = os.getenv("GEMINI_API_KEY", "")
SOURCE_NAME   = "telegram"
DAYS_BACK     = 7

CHANNELS = [
    "minsk_kuda",
    "Minsk_kudaGo",
    "gdevibe",
    "ominske",
    "deti_minsk",
    "mafia_meets",
]

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
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
}


# ── Парсинг HTML t.me/s/ ──────────────────────────────────────────────────────

def fetch_channel_posts(username: str) -> list:
    """
    Читает публичный канал через t.me/s/username.
    Поддерживает пагинацию через ?before=MSG_ID.
    Возвращает [{text, date, url, post_id}, ...]
    """
    base_url = f"https://t.me/s/{username}"
    # Используем timezone-aware datetime для корректного сравнения
    from datetime import timezone
    since_dt = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    posts = []
    before_id = None

    for page in range(10):
        url = f"{base_url}?before={before_id}" if before_id else base_url
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Ошибка загрузки @{username}: {e}")
            break

        soup = BeautifulSoup(resp.text, "lxml")
        wraps = soup.find_all("div", class_="tgme_widget_message_wrap")
        if not wraps:
            break

        logger.info(f"  @{username} стр.{page+1}: {len(wraps)} постов")
        # Логируем дату первого/последнего поста для диагностики
        times = [w.find("time") for w in wraps if w.find("time")]
        if times:
            logger.info(f"  Последний пост: {times[0].get('datetime','?')} | Старейший: {times[-1].get('datetime','?')}")

        min_id = None

        for wrap in wraps:
            time_tag = wrap.find("time")
            if not time_tag or not time_tag.get("datetime"):
                continue
            try:
                # Сохраняем timezone из поста для корректного сравнения
                post_dt = datetime.fromisoformat(
                    time_tag["datetime"].replace("Z", "+00:00")
                )
            except ValueError:
                continue


            if post_dt < since_dt:
                continue  # пропускаем старые посты, но продолжаем итерацию

            msg_div = wrap.find("div", class_="tgme_widget_message")
            data_post = msg_div.get("data-post", "") if msg_div else ""
            post_id = data_post.split("/")[-1] if "/" in data_post else None

            if post_id and post_id.isdigit():
                pid = int(post_id)
                min_id = pid if min_id is None else min(min_id, pid)

            text_div = wrap.find("div", class_="tgme_widget_message_text")
            if not text_div:
                continue
            text = text_div.get_text(separator="\n", strip=True)
            if not text or len(text) < 30:
                continue

            posts.append({
                "text":    text,
                "date":    post_dt,
                "url":     f"https://t.me/{username}/{post_id}" if post_id else base_url,
                "post_id": post_id,
            })

        if min_id is None:
            break

        before_id = min_id
        time.sleep(1)

    logger.info(f"  @{username}: итого {len(posts)} постов")
    return posts


# ── Claude API ────────────────────────────────────────────────────────────────

CLAUDE_SYSTEM = """Ты парсер афиши Минска. Из текста поста Telegram-канала извлеки ВСЕ события.

Верни ТОЛЬКО валидный JSON-массив без markdown и пояснений:
[
  {
    "title": "название события",
    "event_date": "YYYY-MM-DD",
    "show_time": "HH:MM",
    "place": "название площадки",
    "price": "цена или пустая строка",
    "category": "concert/theater/exhibition/kids/party/sport/free/other"
  }
]

Если пост не содержит конкретных событий — верни [].

Правила:
- Только будущие даты. Прошедшие или без даты — пропускай
- Только Минск. Другой город — пропускай
- show_time: HH:MM или ""
- price: "от N BYN" / "N BYN" / "Бесплатно" / ""
- Афиша на неделю — каждое событие отдельной записью
- category: concert=концерт, theater=спектакль, exhibition=выставка,
  kids=детское, party=вечеринка/клуб, sport=спорт, free=бесплатное, other=остальное"""


def call_gemini(post_text: str, today_str: str) -> list:
    if not GEMINI_KEY:
        logger.error("GEMINI_API_KEY не задан")
        return []

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"gemini-2.0-flash:generateContent?key={GEMINI_KEY}"
    )
    prompt = CLAUDE_SYSTEM + f"\n\nСегодня: {today_str}\n\nТекст поста:\n{post_text[:3000]}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": 1000, "temperature": 0.1},
    }

    # Retry при 429: ждём 15/30/60 сек
    for attempt in range(4):
        try:
            resp = requests.post(
                url,
                headers={"content-type": "application/json"},
                json=payload,
                timeout=30,
            )
            if resp.status_code == 429:
                wait = 15 * (2 ** attempt)  # 15, 30, 60, 120 сек
                logger.warning(f"Gemini 429 — ждём {wait} сек (попытка {attempt+1}/4)")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            raw = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            raw = re.sub(r"```json\s*|\s*```", "", raw).strip()
            result = json.loads(raw)
            if isinstance(result, dict):
                return [result] if result.get("title") else []
            if isinstance(result, list):
                return [r for r in result if r.get("title")]
            return []
        except json.JSONDecodeError as e:
            logger.warning(f"Gemini невалидный JSON: {e}")
            return []
        except Exception as e:
            logger.warning(f"Ошибка Gemini API: {e}")
            return []

    logger.error("Gemini: исчерпаны попытки после 429")
    return []


# ── БД ────────────────────────────────────────────────────────────────────────

def normalize_title(title: str) -> str:
    if not title:
        return ""
    norm = re.sub(r"[«»\"'`]", "", title.lower())
    norm = re.sub(r"[^\w\s\-&]", "", norm)
    return re.sub(r"\s+", " ", norm).strip()


def load_existing_index() -> dict:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT title, event_date, place, show_time FROM events WHERE source_name != ?",
        (SOURCE_NAME,),
    )
    index = {}
    for title, ev_date, place, show_time in cursor.fetchall():
        index.setdefault(ev_date, []).append(
            (normalize_title(title), place or "", show_time or "")
        )
    conn.close()
    logger.info(f"📋 Индекс дублей: {sum(len(v) for v in index.values())} событий")
    return index


def is_duplicate(title: str, event_date: str, place: str,
                 show_time: str, index: dict) -> bool:
    if not title or not event_date:
        return False
    norm = normalize_title(title)
    for norm_ex, ex_place, ex_time in index.get(event_date, []):
        if place and show_time and ex_place == place and ex_time == show_time:
            return True
        if norm and norm == norm_ex:
            return True
    return False


def clean_old_events():
    today = date.today().isoformat()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM events WHERE source_name = ? AND event_date < ?",
        (SOURCE_NAME, today),
    )
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    if deleted:
        logger.info(f"🗑 Удалено устаревших: {deleted}")


def save_event(event: dict) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO events
              (title, details, description, event_date, show_time,
               place, location, price, category, source_name, source_url)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            event["title"], "",
            event.get("description", ""),
            event["event_date"],
            event.get("show_time", ""),
            event.get("place", ""),
            "Минск",
            event.get("price", ""),
            event.get("category", "other"),
            SOURCE_NAME,
            event.get("source_url", ""),
        ))
        added = cursor.rowcount > 0
        conn.commit()
        return added
    except Exception as e:
        logger.error(f"Ошибка сохранения '{event.get('title')}': {e}")
        return False
    finally:
        conn.close()


# ── Обработка одного канала ───────────────────────────────────────────────────

def process_channel(username: str, index: dict, today_str: str) -> dict:
    stats = {"posts": 0, "saved": 0, "duplicates": 0, "skipped": 0}

    posts = fetch_channel_posts(username)
    if not posts:
        logger.warning(f"@{username}: постов не найдено")
        return stats

    for post in posts:
        stats["posts"] += 1
        events_in_post = call_gemini(post["text"], today_str)

        if not events_in_post:
            stats["skipped"] += 1
            continue

        description = post["text"][:300] + ("..." if len(post["text"]) > 300 else "")

        for result in events_in_post:
            title      = (result.get("title") or "").strip()
            event_date = (result.get("event_date") or "").strip()
            if not title or not event_date:
                continue

            try:
                ev_dt = date.fromisoformat(event_date)
                if ev_dt < date.today() or ev_dt > date.today() + timedelta(days=180):
                    continue
            except ValueError:
                continue

            show_time = (result.get("show_time") or "").strip()
            place     = (result.get("place") or "").strip()
            price     = (result.get("price") or "").strip()
            category  = result.get("category", "other")

            if is_duplicate(title, event_date, place, show_time, index):
                stats["duplicates"] += 1
                continue

            if save_event({
                "title": title, "description": description,
                "event_date": event_date, "show_time": show_time,
                "place": place, "price": price,
                "category": category, "source_url": post["url"],
            }):
                stats["saved"] += 1
                logger.info(
                    f"  ✅ {event_date} {show_time:5} | {title[:35]:35} | {place[:20]}"
                )
                index.setdefault(event_date, []).append(
                    (normalize_title(title), place, show_time)
                )
            else:
                stats["duplicates"] += 1

        time.sleep(4)  # Gemini free tier: 15 req/min → пауза 4 сек

    return stats


# ── Запуск ────────────────────────────────────────────────────────────────────

def run():
    logger.info("=" * 55)
    logger.info("📱 TELEGRAM ПАРСЕР (t.me/s/)")
    logger.info("=" * 55)
    logger.info(f"📂 DB_PATH = {DB_PATH}")
    logger.info(f"🔑 GEMINI_KEY = {'OK' if GEMINI_KEY else 'НЕ ЗАДАН'}")

    if not GEMINI_KEY:
        logger.error("Не задан GEMINI_API_KEY в env")
        return

    clean_old_events()
    index     = load_existing_index()
    today_str = date.today().isoformat()
    total     = {"posts": 0, "saved": 0, "duplicates": 0}

    for username in CHANNELS:
        logger.info(f"\n── @{username} ──")
        stats = process_channel(username, index, today_str)
        for k in total:
            total[k] += stats.get(k, 0)
        logger.info(
            f"  Постов: {stats['posts']} | "
            f"Сохранено: {stats['saved']} | "
            f"Дублей: {stats['duplicates']} | "
            f"Пропущено: {stats['skipped']}"
        )
        print(f"RESULT:tg@{username}:{stats['posts']}:{stats['saved']}")
        time.sleep(2)

    logger.info("=" * 55)
    logger.info(
        f"📊 Итого: постов {total['posts']}, "
        f"сохранено {total['saved']}, дублей {total['duplicates']}"
    )


if __name__ == "__main__":
    run()
