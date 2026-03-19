#!/usr/bin/env python3
# bycard_parser.py
# Парсер для bycard.by — театры Минска
#
# Стратегия:
#   1. Загружаем /objects/minsk/1 → список всех театров (id + название)
#   2. Для каждого театра загружаем /objects/minsk/1/{id} → парсим NUXT sessions
#   3. Из NUXT: название спектакля, дата, время, цена — точные, не диапазоны

import os
import re
import json
import sqlite3
import logging
import time
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from normalizer import (
    normalize_place, normalize_title, is_future_date,
    is_minsk_event, titles_are_similar,
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
        logging.FileHandler("bycard_parser.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Константы ─────────────────────────────────────────────────────────────────
BASE_URL     = "https://bycard.by"
SOURCE_NAME  = "bycard.by"
THEATRES_URL = f"{BASE_URL}/objects/minsk/1"
MAX_DAYS     = 90

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


# ── HTTP ──────────────────────────────────────────────────────────────────────

def fetch_page(url: str, retries: int = 3) -> Optional[str]:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            r.encoding = "utf-8"
            logger.info(f"  Загружено {url} ({len(r.text)} симв.)")
            return r.text
        except Exception as e:
            logger.warning(f"  Попытка {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2 * (attempt + 1))
    return None


# ── NUXT декодер ──────────────────────────────────────────────────────────────

def decode_nuxt(html: str) -> dict:
    """Декодирует window.__NUXT__ → маппинг {имя_переменной: значение}."""
    soup = BeautifulSoup(html, "lxml")
    for s in soup.find_all("script"):
        t = s.string or ""
        if "__NUXT__" not in t:
            continue
        params_m = re.search(r'\(function\(([^)]+)\)\{', t)
        vals_m   = re.search(r'\}\((.+)\)\);?\s*$', t, re.DOTALL)
        if not params_m or not vals_m:
            continue

        names    = [n.strip() for n in params_m.group(1).split(",")]
        vals_raw = vals_m.group(1)
        vals = []
        depth = 0; in_str = False; cur = []; i = 0
        while i < len(vals_raw):
            c = vals_raw[i]
            if c == '"' and (i == 0 or vals_raw[i-1] != "\\"):
                in_str = not in_str; cur.append(c)
            elif not in_str and c in "([{":
                depth += 1; cur.append(c)
            elif not in_str and c in ")]}":
                depth -= 1; cur.append(c)
            elif not in_str and c == "," and depth == 0:
                vals.append("".join(cur).strip()); cur = []
            else:
                cur.append(c)
            i += 1
        if cur:
            vals.append("".join(cur).strip())
        return dict(zip(names, vals))
    return {}


def resolve(var: str, var_map: dict) -> str:
    """Разыменовывает переменную из NUXT маппинга."""
    v = var_map.get(var, var)
    if isinstance(v, str) and v.startswith('"'):
        return v.strip('"').replace('\\"', '"').replace("\\\\", "\\")
    return v


# ── Список театров ─────────────────────────────────────────────────────────────

def fetch_theatre_list(html: str) -> list[dict]:
    """
    Парсит /objects/minsk/1.
    Возвращает [{"id": "56", "name": "...", "url": "..."}, ...]
    """
    soup = BeautifulSoup(html, "lxml")
    theatres = []
    seen_ids: set = set()

    for a in soup.find_all("a", href=re.compile(r"/objects/minsk/1/\d+")):
        href = a.get("href", "")
        m = re.search(r"/objects/minsk/1/(\d+)", href)
        if not m:
            continue
        tid = m.group(1)
        if tid in seen_ids:
            continue
        seen_ids.add(tid)

        # Название — ищем в дочерних элементах
        name = ""
        for cls in ["object-card__title", "object__name", "card__title",
                    "tagTitle", "title", "head", "name"]:
            el = a.find(class_=cls)
            if el:
                name = el.get_text(strip=True)
                break
        if not name:
            # Берём весь текст ссылки, убираем лишнее
            name = re.sub(r'\s+', ' ', a.get_text(strip=True))[:80]

        if not name or len(name) < 3:
            continue
        if not is_minsk_event(name):
            continue

        url = BASE_URL + href if href.startswith("/") else href
        theatres.append({"id": tid, "name": name, "url": url})

    # Fallback: ищем ID театров через ссылки в тексте страницы
    if not theatres:
        all_ids = re.findall(r'/objects/minsk/1/(\d+)', html)
        for tid in sorted(set(all_ids)):
            if tid not in seen_ids:
                seen_ids.add(tid)
                theatres.append({
                    "id": tid,
                    "name": f"Театр #{tid}",
                    "url": f"{BASE_URL}/objects/minsk/1/{tid}",
                })

    logger.info(f"Театров найдено: {len(theatres)}")
    return theatres


# ── Парсинг страницы театра ────────────────────────────────────────────────────

def parse_theatre_page(html: str, venue_fallback: str) -> list[dict]:
    """
    Парсит NUXT sessions со страницы /objects/minsk/1/{id}.

    NUXT sessions структура:
      {id:N, performanceId:VAR, name:VAR, timeSpending:VAR,
       timeSpendingStopsale:VAR, isSaleOpen:VAR, isBooking:VAR,
       minPrice:VAR, maxPrice:VAR, type:VAR, ...}

    isSaleOpen=true  → билеты есть
    isSaleOpen=false → нет билетов (всё равно сохраняем, цену ставим из минцены)
    """
    soup = BeautifulSoup(html, "lxml")
    var_map = decode_nuxt(html)

    if not var_map:
        logger.warning(f"  NUXT не декодирован для {venue_fallback!r}")
        return []

    # Venue и адрес из JSON-LD Place
    place    = venue_fallback
    location = "Минск"
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            d = json.loads(s.string or "")
            if d.get("@type") == "Place":
                ld_name = d.get("name", "")
                if ld_name:
                    place = normalize_place(ld_name) or ld_name
                addr = d.get("address", {})
                street = addr.get("streetAddress", "")
                if street:
                    # Убираем "Минск, " из начала — уже есть в location города
                    location = re.sub(r'^[Мм]инск,?\s*', '', street).strip() or street
                break
        except Exception:
            continue

    # canonical URL
    canonical = soup.find("link", rel="canonical")
    page_url = canonical.get("href", "") if canonical else ""

    events = []
    seen: set = set()

    for s_tag in soup.find_all("script"):
        t = s_tag.string or ""
        if "__NUXT__" not in t:
            continue

        # Более гибкий regex — id может быть числом или сжатой переменной (bs, bl)
        # timeSpendingStopsale может быть числом или переменной — \w+ покрывает оба
        sessions_raw = re.findall(
            r'\{id:(\w+),performanceId:(\w+),name:(\w+),timeSpending:(\w+),'
            r'timeSpendingStopsale:\w+,isSaleOpen:(\w+),isBooking:\w+,'
            r'minPrice:(\w+),maxPrice:(\w+)',
            t
        )

        for sid, perf_id, name_var, ts_var, sale_var, min_p_var, max_p_var in sessions_raw:
            title    = resolve(name_var, var_map)
            ts_raw   = resolve(ts_var, var_map)
            min_p    = resolve(min_p_var, var_map)
            max_p    = resolve(max_p_var, var_map)
            is_sale  = resolve(sale_var, var_map)   # "true" / "false"

            if not title or len(title) < 2:
                continue

            # Дата и время из unix timestamp
            try:
                dt = datetime.fromtimestamp(int(ts_raw))
                event_date = dt.strftime("%Y-%m-%d")
                show_time  = dt.strftime("%H:%M")
            except Exception:
                continue

            if not is_future_date(event_date, MAX_DAYS):
                continue

            # Цена: если билетов нет — пишем "Нет билетов"
            price = _format_price(min_p, max_p)
            if is_sale != "true":
                price = "Нет билетов"

            # source_url: страница события
            perf_resolved = resolve(perf_id, var_map)
            if perf_resolved.isdigit():
                source_url = f"{BASE_URL}/afisha/minsk/theatre/{perf_resolved}"
            else:
                source_url = page_url or THEATRES_URL

            key = (normalize_title(title), event_date, show_time)
            if key in seen:
                continue
            seen.add(key)

            events.append({
                "title":       title,
                "details":     "",
                "description": "",
                "event_date":  event_date,
                "show_time":   show_time,
                "place":       place,
                "location":    location,
                "price":       price,
                "category":    "theater",
                "source_url":  source_url,
                "source_name": SOURCE_NAME,
                "_is_sale":    is_sale == "true",
            })

        break  # обрабатываем только первый NUXT скрипт

    with_tickets    = sum(1 for e in events if e.get("_is_sale"))
    without_tickets = len(events) - with_tickets
    logger.info(f"  [{place}]: {len(events)} сеансов "
                f"(с билетами: {with_tickets}, без: {without_tickets})")

    # Убираем служебное поле перед возвратом
    for e in events:
        e.pop("_is_sale", None)

    return events


def _format_price(min_p: str, max_p: str) -> str:
    """
    Форматирует цену из NUXT.
    Значения: строка "20.00" (рубли) или число 2000 (копейки, делим на 100).
    """
    def to_rub(v: str) -> Optional[float]:
        try:
            f = float(v)
            if f > 100 and "." not in str(v):
                f = f / 100  # копейки → рубли
            return f
        except Exception:
            return None

    min_f = to_rub(min_p)
    max_f = to_rub(max_p)

    if min_f is None:
        return ""
    if max_f is None or max_f == min_f:
        return f"{min_f:g} руб"
    if min_f == 0:
        return f"до {max_f:g} руб"
    return f"от {min_f:g} руб"


# ── БД ────────────────────────────────────────────────────────────────────────

def load_existing_index() -> dict:
    """Загружает события из других источников для проверки дублей."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT title, event_date, place FROM events WHERE source_name != ?",
            (SOURCE_NAME,)
        )
        rows = cursor.fetchall()
        conn.close()
    except Exception as e:
        logger.warning(f"load_existing_index: {e}")
        return {}

    index: dict = {}
    for title, ev_date, place in rows:
        norm = normalize_title(title)
        index.setdefault(ev_date, []).append((norm, place or ""))
    logger.info(f"Индекс: {sum(len(v) for v in index.values())} событий из других источников")
    return index


def is_duplicate(ev: dict, index: dict) -> bool:
    """True если событие уже есть от другого источника."""
    candidates = index.get(ev["event_date"], [])
    if not candidates:
        return False
    norm = normalize_title(ev["title"])
    if not norm:
        return False
    for norm_ex, _ in candidates:
        if norm == norm_ex:
            return True
        if norm_ex and titles_are_similar(norm, norm_ex, threshold=0.88):
            return True
    return False


def save_events(events: list[dict]) -> int:
    """Удаляет старые записи bycard, сохраняет новые уникальные."""
    if not events:
        logger.info("Нет событий для сохранения")
        return 0

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Удаляем ВСЕ старые записи bycard (как ticketpro/bezkassira)
        cursor.execute("DELETE FROM events WHERE source_name = ?", (SOURCE_NAME,))
        deleted = cursor.rowcount
        logger.info(f"🗑️ Удалено старых записей bycard: {deleted}")

        # Дедупликация внутри текущего запуска по (title, date, time, place)
        seen: set = set()
        unique_events = []
        for ev in events:
            key = (normalize_title(ev["title"]), ev["event_date"], ev["show_time"], ev["place"])
            if key not in seen:
                seen.add(key)
                unique_events.append(ev)

        saved = 0
        for ev in unique_events:
            try:
                cursor.execute("""
                    INSERT INTO events
                        (title, details, description, event_date, show_time,
                         place, location, price, category,
                         source_url, source_name)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    ev["title"], ev["details"], ev["description"],
                    ev["event_date"], ev["show_time"],
                    ev["place"], ev["location"],
                    ev["price"], ev["category"],
                    ev["source_url"], ev["source_name"],
                ))
                saved += 1
            except Exception as e:
                logger.error(f"  Ошибка вставки {ev['title']!r}: {e}")

        conn.commit()
        conn.close()
        logger.info(f"💾 Сохранено: {saved}")
        return saved

    except Exception as e:
        logger.error(f"save_events: {e}")
        return 0


# ── Главный запуск ─────────────────────────────────────────────────────────────

def run():
    logger.info("=" * 60)
    logger.info("🎭 BYCARD парсер запущен")
    logger.info("=" * 60)

    # Загружаем индекс ДО парсинга — там уже есть relax/ticketpro/bezkassira
    index = load_existing_index()

    # Шаг 1: список театров
    logger.info(f"Загружаю список театров: {THEATRES_URL}")
    theatres_html = fetch_page(THEATRES_URL)
    if not theatres_html:
        logger.error("Не удалось загрузить список театров")
        print("RESULT:Bycard театры:0:0")
        return 0

    theatres = fetch_theatre_list(theatres_html)
    if not theatres:
        logger.error("Список театров пуст")
        print("RESULT:Bycard театры:0:0")
        return 0

    logger.info(f"Театров для обработки: {len(theatres)}")

    # Шаг 2: для каждого театра — парсим сеансы
    all_events = []
    for theatre in theatres:
        logger.info(f"\n▶ {theatre['name']} ({theatre['url']})")
        html = fetch_page(theatre["url"])
        if not html:
            logger.warning("  Не удалось загрузить страницу театра")
            time.sleep(1)
            continue

        events = parse_theatre_page(html, theatre["name"])
        all_events.extend(events)
        time.sleep(0.5)

    total_found = len(all_events)
    logger.info(f"\nВсего найдено сеансов: {total_found}")

    # Фильтрация дублей с другими источниками
    unique, dup = [], 0
    for ev in all_events:
        if is_duplicate(ev, index):
            logger.debug(f"  Дубль с другим источником: {ev['title']} {ev['event_date']}")
            dup += 1
        else:
            unique.append(ev)
            # Добавляем в индекс чтобы следующие театры тоже видели
            norm = normalize_title(ev["title"])
            index.setdefault(ev["event_date"], []).append((norm, ev["place"] or ""))

    logger.info(f"Дублей с другими источниками: {dup}")

    # Сохраняем (удаляет старые bycard + вставляет новые)
    saved = save_events(unique)

    logger.info(f"Итог: найдено {total_found}, дублей {dup}, сохранено {saved}")
    logger.info("=" * 60)
    print(f"RESULT:Bycard театры:{total_found}:{saved}")
    return saved


# ── Точка входа ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        import glob
        files = (
            glob.glob("/mnt/user-data/uploads/*Дом*.html")
            + glob.glob("/mnt/user-data/uploads/*bycard*.html")
            + glob.glob("/mnt/user-data/uploads/*Литера*.html")
        )
        if not files:
            print("HTML файл не найден")
            sys.exit(1)
        html_file = files[0]
        print(f"Тест на файле: {html_file}\n")
        with open(html_file, encoding="utf-8") as f:
            html = f.read()
        events = parse_theatre_page(html, "Тестовый театр")
        print(f"Сеансов: {len(events)}\n")
        for e in sorted(events, key=lambda x: (x["event_date"], x["show_time"])):
            print(f"  {e['event_date']} {e['show_time']}  {e['title'][:40]:40s}  {e['place'][:20]:20s}  {e['price']}")

    elif len(sys.argv) > 1 and sys.argv[1] == "dump":
        print(f"Загружаю список театров: {THEATRES_URL}")
        html = fetch_page(THEATRES_URL)
        if html:
            with open("dump_bycard_objects.html", "w", encoding="utf-8") as f:
                f.write(html)
            theatres = fetch_theatre_list(html)
            print(f"Театров: {len(theatres)}")
            for t in theatres[:15]:
                print(f"  {t}")
        else:
            print("Ошибка загрузки")

    else:
        run()
