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
        q = search.strip().lower()
        where.append("(LOWER(title) LIKE ? OR LOWER(place) LIKE ? OR LOWER(details) LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])

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
    show_time: Optional[str] = ""
    place: str
    address: Optional[str] = ""
    price: Optional[str] = ""
    description: Optional[str] = ""
    source_url: Optional[str] = ""


@app.post("/api/events/submit")
def submit_event(event: EventSubmit):
    """Принимает событие от пользователя сайта → сохраняет в pending_events."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO pending_events
                    (user_id, username, first_name, title, event_date, show_time,
                     place, address, category, description, price, source_url,
                     status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                0, "web_user", "Web",
                event.title, event.event_date, event.show_time or "",
                event.place, event.address or "",
                event.category, event.description or "",
                event.price or "", event.source_url or "",
                "pending",
                datetime.now(MINSK_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            ))
            conn.commit()
            pending_id = cursor.lastrowid

        # Уведомление админу в Telegram
        if BOT_TOKEN and ADMIN_ID:
            lines = [
                "🌐 <b>Новое событие с сайта</b>",
                f"📌 <b>{event.title}</b>",
                f"🗂 Категория: {event.category}",
                f"📅 Дата: {event.event_date}" + (f" ⏰ {event.show_time}" if event.show_time else ""),
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
