#!/usr/bin/env python3
# api.py
# FastAPI backend для MinskDvizh — читает ту же SQLite БД что и бот

import os
import sqlite3
import httpx
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional
from collections import defaultdict

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Конфиг ──────────────────────────────────────────────────────────────────

DB_PATH      = os.getenv("DB_PATH", "/data/events_final.db")
MINSK_TZ     = timezone(timedelta(hours=3))
FRONTEND_URL = os.getenv("FRONTEND_URL", "*")
BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_ID     = int(os.getenv("ADMIN_ID", "502917728"))

app = FastAPI(
    title="MinskDvizh API",
    description="API афиши Минска",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── БД ──────────────────────────────────────────────────────────────────────

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def row_to_dict(row) -> dict:
    return dict(row)


# ── Pydantic схемы ───────────────────────────────────────────────────────────

class Event(BaseModel):
    id: int
    title: str
    details: Optional[str] = ""
    description: Optional[str] = ""
    event_date: str
    show_time: Optional[str] = ""
    place: Optional[str] = ""
    location: Optional[str] = "Минск"
    price: Optional[str] = ""
    category: str
    source_url: Optional[str] = ""
    source_name: Optional[str] = ""


class EventsResponse(BaseModel):
    total: int
    page: int
    per_page: int
    events: list[Event]


class CategoryCounts(BaseModel):
    cinema: int = 0
    concert: int = 0
    theater: int = 0
    exhibition: int = 0
    kids: int = 0
    sport: int = 0
    party: int = 0
    free: int = 0
    excursion: int = 0
    market: int = 0
    masterclass: int = 0
    boardgames: int = 0
    broadcast: int = 0
    education: int = 0
    other: int = 0


# ── Вспомогательные функции ──────────────────────────────────────────────────

def now_minsk() -> datetime:
    return datetime.now(MINSK_TZ)


def today_str() -> str:
    return now_minsk().strftime("%Y-%m-%d")


def now_time_str() -> str:
    return now_minsk().strftime("%H:%M")


def get_weekend_dates() -> tuple[str, str]:
    today = now_minsk()
    days_until_saturday = (5 - today.weekday()) % 7 or 7
    saturday = today + timedelta(days=days_until_saturday)
    sunday = saturday + timedelta(days=1)
    return saturday.strftime("%Y-%m-%d"), sunday.strftime("%Y-%m-%d")


def fetch_events(
    where_clauses: list[str],
    params: list,
    order: str = "event_date, show_time, title",
    limit: int = 500,
) -> list[dict]:
    where = " AND ".join(where_clauses) if where_clauses else "1=1"
    sql = f"""
        SELECT id, title, details, description, event_date, show_time,
               place, location, price, category, source_url, source_name
        FROM events
        WHERE {where}
        ORDER BY {order}
        LIMIT {limit}
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        return [row_to_dict(r) for r in cursor.fetchall()]


def paginate(items: list, page: int, per_page: int) -> tuple[list, int]:
    total = len(items)
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end], total


# ── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "time": now_minsk().isoformat()}


# ── Категории ────────────────────────────────────────────────────────────────

@app.get("/api/categories/counts", response_model=CategoryCounts)
def categories_counts():
    """Количество актуальных событий по каждой категории."""
    today = today_str()
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT category, COUNT(*) as cnt FROM events WHERE event_date >= ? GROUP BY category",
            (today,)
        )
        data = {row["category"]: row["cnt"] for row in cursor.fetchall()}
    return CategoryCounts(**data)


# ── Даты с событиями (для календаря) ────────────────────────────────────────

@app.get("/api/calendar/dates")
def calendar_dates(
    category: Optional[str] = Query(None),
    months_ahead: int = Query(3, ge=1, le=12),
):
    """Список дат у которых есть события (для подсветки в календаре)."""
    today = today_str()
    until = (now_minsk() + timedelta(days=30 * months_ahead)).strftime("%Y-%m-%d")
    params: list = [today, until]
    where = ["event_date >= ?", "event_date <= ?"]
    if category and category != "all":
        where.append("category = ?")
        params.append(category)
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT DISTINCT event_date FROM events WHERE {' AND '.join(where)} ORDER BY event_date",
            params,
        )
        dates = [row["event_date"] for row in cursor.fetchall()]
    return {"dates": dates}


# ── Последнее обновление ─────────────────────────────────────────────────────

@app.get("/api/last-updated")
def last_updated():
    """Когда последний раз обновлялась база (по минимальной дате добавления)."""
    # Простая эвристика — самая свежая запись в БД
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT MAX(created_at) as ts FROM events")
            row = cursor.fetchone()
            ts = row["ts"] if row and row["ts"] else None
        except Exception:
            ts = None
    return {"last_updated": ts, "schedule": "ежедневно в 06:00"}


# ── Основной эндпоинт событий ────────────────────────────────────────────────

@app.get("/api/events", response_model=EventsResponse)
def get_events(
    category: Optional[str] = Query(None, description="Категория: cinema/concert/theater/..."),
    date: Optional[str]     = Query(None, description="Конкретная дата YYYY-MM-DD"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str]   = Query(None),
    search: Optional[str]    = Query(None, description="Поиск по названию/месту"),
    page: int                = Query(1, ge=1),
    per_page: int            = Query(10, ge=1, le=500),
):
    today = today_str()
    now_t = now_time_str()

    where: list[str] = []
    params: list = []

    # Дата
    if date:
        where.append("event_date = ?")
        params.append(date)
        # Для сегодня — фильтруем прошедшие сеансы
        if date == today:
            where.append("(show_time = '' OR show_time IS NULL OR show_time > ?)")
            params.append(now_t)
    elif date_from and date_to:
        where.append("event_date BETWEEN ? AND ?")
        params.extend([date_from, date_to])
    elif date_from:
        where.append("event_date >= ?")
        params.append(date_from)
    else:
        # По умолчанию — только будущие
        where.append("event_date >= ?")
        params.append(today)
        where.append("(event_date > ? OR show_time = '' OR show_time IS NULL OR show_time > ?)")
        params.extend([today, now_t])

    # Категория
    if category and category != "all":
        where.append("category = ?")
        params.append(category)

    # Поиск
    if search and len(search.strip()) >= 2:
        q = search.strip()
        current_year = now_minsk().year

        # Попытка распарсить как дату (дд.мм или дд.мм.гггг) — как в боте
        import re as _re
        date_match = _re.match(r'^(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?$', q)
        if date_match:
            day, month = date_match.group(1).zfill(2), date_match.group(2).zfill(2)
            year = date_match.group(3) or str(current_year)
            search_date = f"{year}-{month}-{day}"
            where.append("event_date = ?")
            params.append(search_date)
        else:
            ql = q.lower()
            where.append("(LOWER(title) LIKE ? OR LOWER(place) LIKE ? OR LOWER(details) LIKE ? OR LOWER(description) LIKE ? OR category LIKE ?)")
            params.extend([f"%{ql}%", f"%{ql}%", f"%{ql}%", f"%{ql}%", f"%{ql}%"])

    events = fetch_events(where, params)
    page_events, total = paginate(events, page, per_page)

    return EventsResponse(
        total=total,
        page=page,
        per_page=per_page,
        events=[Event(**e) for e in page_events],
    )


# ── Шорткаты ─────────────────────────────────────────────────────────────────

@app.get("/api/events/today", response_model=EventsResponse)
def events_today(
    category: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=500),
):
    today = today_str()
    now_t = now_time_str()
    where = [
        "event_date = ?",
        "(show_time = '' OR show_time IS NULL OR show_time > ?)",
    ]
    params: list = [today, now_t]
    if category and category != "all":
        where.append("category = ?")
        params.append(category)
    events = fetch_events(where, params)
    page_events, total = paginate(events, page, per_page)
    return EventsResponse(total=total, page=page, per_page=per_page,
                          events=[Event(**e) for e in page_events])


@app.get("/api/events/tomorrow", response_model=EventsResponse)
def events_tomorrow(
    category: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=500),
):
    tomorrow = (now_minsk() + timedelta(days=1)).strftime("%Y-%m-%d")
    where = ["event_date = ?"]
    params: list = [tomorrow]
    if category and category != "all":
        where.append("category = ?")
        params.append(category)
    events = fetch_events(where, params)
    page_events, total = paginate(events, page, per_page)
    return EventsResponse(total=total, page=page, per_page=per_page,
                          events=[Event(**e) for e in page_events])


@app.get("/api/events/weekend", response_model=EventsResponse)
def events_weekend(
    category: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=500),
):
    saturday, sunday = get_weekend_dates()
    where = ["event_date IN (?, ?)"]
    params: list = [saturday, sunday]
    if category and category != "all":
        where.append("category = ?")
        params.append(category)
    events = fetch_events(where, params)
    page_events, total = paginate(events, page, per_page)
    return EventsResponse(total=total, page=page, per_page=per_page,
                          events=[Event(**e) for e in page_events])


@app.get("/api/events/upcoming", response_model=EventsResponse)
def events_upcoming(
    category: Optional[str] = Query(None),
    days: int = Query(30, ge=1, le=90),
    page: int = Query(1, ge=1),
    per_page: int = Query(10, ge=1, le=500),
):
    today = today_str()
    now_t = now_time_str()
    until = (now_minsk() + timedelta(days=days)).strftime("%Y-%m-%d")
    where = [
        "event_date BETWEEN ? AND ?",
        "(event_date > ? OR show_time = '' OR show_time IS NULL OR show_time > ?)",
    ]
    params: list = [today, until, today, now_t]
    if category and category != "all":
        where.append("category = ?")
        params.append(category)
    events = fetch_events(where, params)
    page_events, total = paginate(events, page, per_page)
    return EventsResponse(total=total, page=page, per_page=per_page,
                          events=[Event(**e) for e in page_events])


# ── Сабмит события от пользователя сайта ────────────────────────────────────

class EventSubmit(BaseModel):
    title: str
    category: str
    event_date: str
    event_date_to: Optional[str] = None   # для периода
    show_time: Optional[str] = ""
    place: str
    address: Optional[str] = ""
    price: Optional[str] = ""
    details: Optional[str] = ""
    description: Optional[str] = ""
    source_url: Optional[str] = ""


def _dates_in_range(date_from: str, date_to: str) -> list[str]:
    """Возвращает список дат включительно от date_from до date_to."""
    from datetime import date as _date
    start = _date.fromisoformat(date_from)
    end = _date.fromisoformat(date_to)
    result = []
    current = start
    while current <= end:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return result


@app.post("/api/events/submit")
def submit_event(event: EventSubmit):
    """Принимает событие → сохраняет в pending_events (одна запись или по одной на каждый день периода)."""
    combined_description = ""
    if event.details:
        combined_description += f"[Формат: {event.details}]"
    if event.description:
        combined_description += (" " if combined_description else "") + event.description

    # Список дат для вставки
    if event.event_date_to and event.event_date_to > event.event_date:
        dates = _dates_in_range(event.event_date, event.event_date_to)
    else:
        dates = [event.event_date]

    try:
        pending_ids = []
        with get_db() as conn:
            cursor = conn.cursor()
            for d in dates:
                cursor.execute("""
                    INSERT INTO pending_events
                        (user_id, username, first_name, title, event_date, show_time,
                         place, address, category, description, price, source_url,
                         status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    0, "web_user", "Web",
                    event.title, d, event.show_time or "",
                    event.place, event.address or "",
                    event.category, combined_description,
                    event.price or "", event.source_url or "",
                    "pending",
                    datetime.now(MINSK_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                ))
                pending_ids.append(cursor.lastrowid)
            conn.commit()
        pending_id = pending_ids[0]
        days_info = f" ({len(dates)} дней)" if len(dates) > 1 else ""

        # Уведомление админу в Telegram
        if BOT_TOKEN and ADMIN_ID:
            date_info = event.event_date
            if event.event_date_to and event.event_date_to > event.event_date:
                date_info = f"{event.event_date} → {event.event_date_to}{days_info}"
            lines = [
                "🌐 <b>Новое событие с сайта</b>",
                f"📌 <b>{event.title}</b>",
                f"🗂 Категория: {event.category}",
            ]
            if event.details:
                lines.append(f"📝 Формат: {event.details}")
            lines += [
                f"📅 Дата: {date_info}" + (f" ⏰ {event.show_time}" if event.show_time else ""),
                f"🏢 Место: {event.place}" + (f", {event.address}" if event.address else ""),
            ]
            if event.price:
                lines.append(f"💰 Цена: {event.price}")
            if event.description:
                lines.append(f"📝 {event.description[:200]}")
            if event.source_url:
                lines.append(f"🔗 {event.source_url}")
            lines.append(f"\n<i>ID в очереди: #{pending_id}</i>")
            text = "\n".join(lines)
            try:
                with httpx.Client(timeout=5) as client:
                    client.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        json={
                            "chat_id": ADMIN_ID,
                            "text": text,
                            "parse_mode": "HTML",
                            "reply_markup": {
                                "inline_keyboard": [[
                                    {"text": "✅ Одобрить", "callback_data": f"mod_approve_{pending_id}"},
                                    {"text": "❌ Отклонить", "callback_data": f"mod_reject_{pending_id}"},
                                ]]
                            }
                        }
                    )
            except Exception:
                pass  # не блокируем ответ если TG недоступен

        return {"ok": True, "message": "Событие отправлено на модерацию"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Webapp ping — трекинг открытий Mini App ─────────────────────────────────

class WebappPingRequest(BaseModel):
    user_id: Optional[int] = None
    username: Optional[str] = None
    first_name: Optional[str] = None

@app.post("/api/webapp-ping")
def webapp_ping(req: WebappPingRequest):
    """Сайт вызывает при открытии — логируем для статистики."""
    try:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO user_stats (user_id, username, first_name, action, detail, created_at) VALUES (?,?,?,?,?,?)",
                (req.user_id or 0, req.username or "", req.first_name or "",
                 "webapp_ping", "web",
                 datetime.now(MINSK_TZ).strftime("%Y-%m-%d %H:%M:%S"))
            )
            conn.commit()
    except Exception:
        pass
    return {"ok": True}


# ── Одно событие ─────────────────────────────────────────────────────────────

@app.get("/api/events/{event_id}", response_model=Event)
def get_event(event_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT id, title, details, description, event_date, show_time,
                      place, location, price, category, source_url, source_name
               FROM events WHERE id = ?""",
            (event_id,)
        )
        row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Event not found")
    return Event(**row_to_dict(row))
