#!/usr/bin/env python3
# bot_enhanced.py
# Бот-афиша Минска

import logging
import os
import re
import sqlite3
from contextlib import contextmanager
from collections import defaultdict
from datetime import datetime, timedelta

from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from telegram.constants import ParseMode
from telegram.ext import PreCheckoutQueryHandler

# ---------------------- Конфиг и логирование ----------------------

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_NAME = os.getenv("DB_PATH", "/data/events_final.db")  # Volume path
ADMIN_ID = 502917728

DONATION_ENABLED = True
DONATION_SUGGESTIONS = [10, 50, 100, 500]
DONATION_CURRENCY = "XTR"  # Telegram Stars

PER_PAGE = 10
SEARCH_MULTIPLIER = 3

CATEGORY_EMOJI = {
    "cinema": "🎬",
    "concert": "🎵",
    "theater": "🎭",
    "exhibition": "🖼️",
    "kids": "🧸",
    "sport": "⚽",
    "free": "🆓",
}

CATEGORY_NAMES = {
    "cinema": "🎬 Кино",
    "concert": "🎵 Концерты",
    "theater": "🎭 Театр",
    "exhibition": "🖼️ Выставки",
    "kids": "🧸 Детям",
    "sport": "⚽ Спорт",
    "free": "🆓 Бесплатно",
}

# ---------------------- Работа с БД ----------------------


@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    # Создаём директорию если не существует (первый запуск на Volume)
    db_dir = os.path.dirname(DB_NAME)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER,
                category TEXT,
                date_type TEXT,
                PRIMARY KEY (user_id, category, date_type)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                action TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_stats_user_id ON user_stats(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_stats_created_at ON user_stats(created_at)")
        conn.commit()


def log_user_action(user_id: int, username: str | None, first_name: str | None, action: str, detail: str | None = None):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO user_stats (user_id, username, first_name, action, detail, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, username, first_name, action, detail, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Ошибка логирования: {e}")


def get_stats_data() -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_stats")
        total_users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM user_stats")
        total_actions = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_stats WHERE created_at LIKE ?", (f"{today}%",))
        users_today = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM user_stats WHERE created_at LIKE ?", (f"{today}%",))
        actions_today = cursor.fetchone()[0]
        cursor.execute("""
            SELECT DATE(created_at) as day, COUNT(*) as cnt, COUNT(DISTINCT user_id) as users
            FROM user_stats
            WHERE created_at >= DATE('now', '-7 days')
            GROUP BY day ORDER BY day DESC
        """)
        daily_activity = cursor.fetchall()
        cursor.execute("SELECT action, COUNT(*) as cnt FROM user_stats GROUP BY action ORDER BY cnt DESC LIMIT 10")
        top_actions = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) FROM events WHERE event_date >= ?", (today,))
        events_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM subscriptions")
        subscribers_count = cursor.fetchone()[0]
        return {
            "total_users": total_users,
            "total_actions": total_actions,
            "users_today": users_today,
            "actions_today": actions_today,
            "daily_activity": daily_activity,
            "top_actions": top_actions,
            "events_count": events_count,
            "subscribers_count": subscribers_count,
        }


def get_events_count_by_category() -> dict:
    """Реальное кол-во актуальных событий по категориям (для /about)."""
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT category, COUNT(*) as cnt FROM events WHERE event_date >= ? GROUP BY category",
            (today,),
        )
        return {row["category"]: row["cnt"] for row in cursor.fetchall()}


def search_events_by_title(query: str, limit: int = 20):
    today = datetime.now(MINSK_TZ).strftime("%Y-%m-%d")
    q = query.lower()
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, title, details, description, event_date, show_time,
                   place, location, price, category, source_url
            FROM events
            WHERE (LOWER(title) LIKE ? OR LOWER(details) LIKE ? OR LOWER(place) LIKE ?)
              AND event_date >= ?
            ORDER BY event_date, show_time, title
            LIMIT ?
        """, (f"%{q}%", f"%{q}%", f"%{q}%", today, limit * SEARCH_MULTIPLIER))
        return cursor.fetchall()


def search_events_by_date_raw(date_str: str):
    current_year = datetime.now().year
    date_str = date_str.strip()
    if re.match(r"^\d{1,2}\.\d{1,2}\.\d{4}$", date_str):
        day, month, year = date_str.split(".")
    elif re.match(r"^\d{1,2}\.\d{1,2}$", date_str):
        day, month = date_str.split(".")
        year = str(current_year)
    else:
        return None, None, "неверный_формат"
    day, month = day.zfill(2), month.zfill(2)
    search_date = f"{year}-{month}-{day}"
    formatted_date = f"{day}.{month}.{year}"
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, title, details, description, event_date, show_time,
                   place, location, price, category, source_url
            FROM events WHERE event_date = ? ORDER BY show_time, title LIMIT 300
        """, (search_date,))
        events = cursor.fetchall()
    return (events, formatted_date, "найдены") if events else ([], formatted_date, "нет_событий")


def get_events_by_date_and_category(target_date: datetime, category: str | None = None):
    """События на дату. Для сегодня фильтрует прошедшие сеансы."""
    date_str = target_date.strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        query = """
            SELECT id, title, details, description, event_date, show_time,
                   place, location, price, category, source_url
            FROM events WHERE event_date = ?
        """
        params = [date_str]
        if category and category != "all":
            query += " AND category = ?"
            params.append(category)
        if date_str == today_str:
            query += " AND (show_time = '' OR show_time IS NULL OR show_time > ?)"
            params.append(datetime.now().strftime("%H:%M"))
        query += " ORDER BY show_time, title"
        cursor.execute(query, params)
        return cursor.fetchall()


def get_upcoming_events(limit: int = 20, category: str | None = None):
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if category and category != "all":
            cursor.execute("""
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events WHERE event_date >= ? AND category = ?
                ORDER BY event_date, show_time, title LIMIT ?
            """, (today, category, limit * SEARCH_MULTIPLIER))
        else:
            cursor.execute("""
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events WHERE event_date >= ?
                ORDER BY event_date, show_time, title LIMIT ?
            """, (today, limit * SEARCH_MULTIPLIER))
        return cursor.fetchall()


def get_weekend_events(category: str | None = None):
    today = datetime.now()
    days_until_saturday = (5 - today.weekday()) % 7 or 7
    saturday = today + timedelta(days=days_until_saturday)
    sunday = saturday + timedelta(days=1)
    saturday_str, sunday_str = saturday.strftime("%Y-%m-%d"), sunday.strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if category and category != "all":
            cursor.execute("""
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events WHERE event_date IN (?, ?) AND category = ?
                ORDER BY event_date, show_time, title
            """, (saturday_str, sunday_str, category))
        else:
            cursor.execute("""
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events WHERE event_date IN (?, ?)
                ORDER BY event_date, show_time, title
            """, (saturday_str, sunday_str))
        return cursor.fetchall(), saturday, sunday


def filter_events_by_category(events, category: str):
    return [e for e in events if e["category"] == category]


def add_subscription(user_id: int, category: str, date_type: str):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO subscriptions (user_id, category, date_type) VALUES (?, ?, ?)",
            (user_id, category, date_type),
        )
        conn.commit()


def remove_subscription(user_id: int, category: str, date_type: str):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM subscriptions WHERE user_id = ? AND category = ? AND date_type = ?",
                       (user_id, category, date_type))
        conn.commit()


def get_user_subscriptions(user_id: int):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT category, date_type FROM subscriptions WHERE user_id = ?", (user_id,))
        return cursor.fetchall()


def get_all_subscribers() -> dict:
    """Возвращает {(category, date_type): [user_id, ...]}."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, category, date_type FROM subscriptions")
        result = defaultdict(list)
        for row in cursor.fetchall():
            result[(row["category"], row["date_type"])].append(row["user_id"])
        return result


# ---------------------- Форматирование ----------------------


def format_event_text(event) -> str:
    text = f"🎉 **{event['title']}**"
    if event["details"]:
        details = event["details"][:177] + "..." if len(event["details"]) > 180 else event["details"]
        text += f"\n📝 {details}"
    if event["event_date"]:
        text += f"\n📅 {datetime.strptime(event['event_date'], '%Y-%m-%d').strftime('%d.%m.%Y')}"
    if event["show_time"]:
        text += f" ⏰ {event['show_time']}"
    if event["place"] and event["place"] != "Кинотеатр":
        text += f"\n🏢 {event['place']}"
    if event["price"]:
        text += f"\n💰 {event['price']}"
    if event["category"]:
        text += f"\n{CATEGORY_EMOJI.get(event['category'], '📌')} {event['category'].capitalize()}"
    return text


def group_cinema_events(events):
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for event in events:
        if event["category"] == "cinema":
            grouped[event["title"]][event["event_date"]][event["place"]].append(
                {"time": event["show_time"], "details": event["details"]}
            )
    return grouped


def format_grouped_cinema_events(grouped):
    result = []
    for title, dates in grouped.items():
        for date, cinemas in dates.items():
            first_cinema = next(iter(cinemas.values()))
            details = first_cinema[0]["details"] if first_cinema else ""
            text = f"🎬 **{title}**"
            if details:
                details = details[:177] + "..." if len(details) > 180 else details
                text += f"\n🎭 {details}"
            text += f"\n📅 {datetime.strptime(date, '%Y-%m-%d').strftime('%d.%m.%Y')}"
            for place, seances in cinemas.items():
                times = [s["time"] for s in seances if s["time"]]
                if times:
                    text += f"\n   ⏰ {', '.join(times)} — {place}"
            result.append(text)
    return result


# ---------------------- Пагинация + категории ----------------------


def set_pagination(context: ContextTypes.DEFAULT_TYPE, events, title: str, date_info: str | None = None):
    context.user_data["pagination"] = {
        "events": list(events), "page": 0, "per_page": PER_PAGE,
        "title": title, "date_info": date_info,
    }


def build_page_keyboard(data: dict):
    """Клавиатура: фильтры категорий + навигация ◀ 1/5 ▶."""
    events = data["events"]
    page = data["page"]
    per_page = data["per_page"]
    total = len(events)
    max_page = max(0, (total - 1) // per_page)
    keyboard = []
    category_counts = defaultdict(int)
    for e in events:
        if e["category"]: category_counts[e["category"]] += 1
    if len(category_counts) > 1:
        row = []
        for cat_key, cat_name in CATEGORY_NAMES.items():
            if cat_key in category_counts:
                row.append(InlineKeyboardButton(f"{cat_name} ({category_counts[cat_key]})", callback_data=f"filter_{cat_key}"))
                if len(row) == 2: keyboard.append(row); row = []
        if row: keyboard.append(row)
    if max_page > 0:
        keyboard.append([
            InlineKeyboardButton("◀️", callback_data="page_prev") if page > 0 else InlineKeyboardButton(" ", callback_data="page_noop"),
            InlineKeyboardButton(f"{page + 1}/{max_page + 1}", callback_data="page_noop"),
            InlineKeyboardButton("▶️", callback_data="page_next") if page < max_page else InlineKeyboardButton(" ", callback_data="page_noop"),
        ])
    return InlineKeyboardMarkup(keyboard) if keyboard else None
async def show_page(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("pagination")
    if not data:
        msg = "Данные не найдены. Попробуйте запрос заново."
        if isinstance(update_or_query, Update):
            await update_or_query.message.reply_text(msg)
        else:
            await update_or_query.answer(msg, show_alert=True)
        return
    events, page, per_page = data["events"], data["page"], data["per_page"]
    total = len(events)
    if total == 0:
        msg = "😕 Событий не найдено."
        if isinstance(update_or_query, Update):
            await update_or_query.message.reply_text(msg)
        else:
            await update_or_query.answer()
            await update_or_query.message.reply_text(msg)
        return
    max_page = (total - 1) // per_page
    page = max(0, min(page, max_page))
    data["page"] = page
    chunk = events[page * per_page:(page + 1) * per_page]
    if isinstance(update_or_query, Update):
        await update_or_query.message.chat.send_action(action="typing")
        send = update_or_query.message.reply_text
    else:
        await update_or_query.answer()
        send = update_or_query.message.reply_text
    lines = []
    if data.get("title"): lines.append(data["title"])
    if data.get("date_info"): lines.append(data["date_info"])
    lines.append(f"Найдено: {total} | Стр. {page + 1}/{max_page + 1}")
    lines.append("")
    cinema_events = [e for e in chunk if e["category"] == "cinema"]
    other_events = [e for e in chunk if e["category"] != "cinema"]
    if cinema_events:
        for t in format_grouped_cinema_events(group_cinema_events(cinema_events)):
            lines.append(t); lines.append("🔗 afisha.relax.by/kino/minsk/"); lines.append("")
    for event in other_events:
        lines.append(format_event_text(event))
        url = event["source_url"]
        if url: lines.append(f"🔗 {url}")
        lines.append("")
    text = "\n".join(lines).strip()
    keyboard = build_page_keyboard(data)
    if len(text) <= 4000:
        await send(text, reply_markup=keyboard, parse_mode="Markdown", disable_web_page_preview=True)
    else:
        await send(f"{data.get('title', '')}\nНайдено: {total} | Стр. {page + 1}/{max_page + 1}", parse_mode="Markdown")
        all_items = []
        if cinema_events:
            for t in format_grouped_cinema_events(group_cinema_events(cinema_events)):
                all_items.append((t, "https://afisha.relax.by/kino/minsk/"))
        for event in other_events:
            all_items.append((format_event_text(event), event["source_url"] or ""))
        for idx, (item_text, url) in enumerate(all_items):
            is_last = idx == len(all_items) - 1
            await send(item_text + (f"\n🔗 {url}" if url else ""),
                       reply_markup=keyboard if is_last else None,
                       parse_mode="Markdown", disable_web_page_preview=True)


# ---------------------- Календарь ----------------------


def build_calendar_keyboard(year: int, month: int, available_dates: set) -> InlineKeyboardMarkup:
    import calendar as cal_module
    MONTH_NAMES = ["", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                   "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]
    keyboard = [
        [InlineKeyboardButton("◀", callback_data=f"cal_prev_{year}_{month}"),
         InlineKeyboardButton(f"{MONTH_NAMES[month]} {year}", callback_data="page_noop"),
         InlineKeyboardButton("▶", callback_data=f"cal_next_{year}_{month}")],
        [InlineKeyboardButton(d, callback_data="page_noop") for d in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]]
    ]
    for week in cal_module.monthcalendar(year, month):
        row = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="page_noop"))
            else:
                date_str = f"{year}-{month:02d}-{day:02d}"
                if date_str in available_dates:
                    row.append(InlineKeyboardButton(str(day), callback_data=f"cal_day_{year}_{month}_{day}"))
                else:
                    row.append(InlineKeyboardButton(f"·{day}", callback_data="page_noop"))
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)


def get_available_dates() -> set:
    today = datetime.now(MINSK_TZ).strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT event_date FROM events WHERE event_date >= ?", (today,))
        return {row["event_date"] for row in cursor.fetchall()}


async def show_calendar(update_or_query, context: ContextTypes.DEFAULT_TYPE, year: int = None, month: int = None):
    now = datetime.now(MINSK_TZ)
    year = year or now.year
    month = month or now.month
    keyboard = build_calendar_keyboard(year, month, get_available_dates())
    text = "🗓 Выберите дату (активны даты с событиями):"
    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_text(text, reply_markup=keyboard)
    else:
        await update_or_query.answer()
        try:
            await update_or_query.edit_message_text(text, reply_markup=keyboard)
        except Exception:
            await update_or_query.message.reply_text(text, reply_markup=keyboard)


# ---------------------- UI-хелперы ----------------------


def get_reply_main_menu():
    return ReplyKeyboardMarkup([
        ["📅 Сегодня", "📆 Завтра"],
        ["⏰ Ближайшие", "🎉 Выходные"],
        ["🗓 Календарь", "🎯 Категории"],
        ["ℹ️ О проекте", "⭐ Поддержать"],
    ], resize_keyboard=True)


async def show_main_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE | None = None, send_method=None):
    text = "🎉 **Главное меню**\n\nВыберите действие:"
    kwargs = {"reply_markup": get_reply_main_menu(), "parse_mode": "Markdown"}
    if send_method:
        await send_method(text, **kwargs)
    else:
        await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)


async def show_categories_menu(query, context: ContextTypes.DEFAULT_TYPE):
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("🎬 Кино", callback_data="cat_cinema"), InlineKeyboardButton("🎵 Концерты", callback_data="cat_concert")],
        [InlineKeyboardButton("🎭 Театр", callback_data="cat_theater"), InlineKeyboardButton("🖼️ Выставки", callback_data="cat_exhibition")],
        [InlineKeyboardButton("🧸 Детям", callback_data="cat_kids"), InlineKeyboardButton("⚽ Спорт", callback_data="cat_sport")],
        [InlineKeyboardButton("🆓 Бесплатно", callback_data="cat_free"), InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")],
    ]
    await query.edit_message_text("🎯 **Выберите категорию:**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_date_options(update_or_query, category_name: str):
    display_name = CATEGORY_NAMES.get(category_name, category_name)
    keyboard = [
        [InlineKeyboardButton("📅 Сегодня", callback_data=f"date_today_{category_name}"),
         InlineKeyboardButton("📆 Завтра", callback_data=f"date_tomorrow_{category_name}")],
        [InlineKeyboardButton("⏰ Ближайшие", callback_data=f"date_upcoming_{category_name}"),
         InlineKeyboardButton("🎉 Выходные", callback_data=f"date_weekend_{category_name}")],
        [InlineKeyboardButton("◀️ Назад к категориям", callback_data="show_categories")],
    ]
    text = f"📌 **{display_name}**\n\nВыберите дату для поиска:"
    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await update_or_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ---------------------- Подписки ----------------------


async def send_subscription_prompt(query_or_update, category: str, date_type: str):
    display_name = CATEGORY_NAMES.get(category, category)
    dt_names = {"today": "на сегодня", "tomorrow": "на завтра", "upcoming": "на ближайшие дни", "weekend": "на выходные"}
    keyboard = [[InlineKeyboardButton("🔔 Подписаться", callback_data=f"sub_{category}_{date_type}")]]
    text = f"🔔 Подписаться на {display_name} {dt_names.get(date_type, '')}?"
    send = query_or_update.message.reply_text if isinstance(query_or_update, Update) else query_or_update.message.reply_text
    await send(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_subscriptions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    subs = get_user_subscriptions(user_id)
    if not subs:
        await update.message.reply_text("У вас пока нет активных подписок 🔔\n\nПодписаться можно через меню 🎯 Категории")
        return
    dt_names = {"today": "на сегодня", "tomorrow": "на завтра", "upcoming": "на ближайшие дни", "weekend": "на выходные"}
    keyboard = []
    lines = ["🔔 Ваши подписки:"]
    for s in subs:
        cat_name = CATEGORY_NAMES.get(s["category"], s["category"])
        dt_name = dt_names.get(s["date_type"], s["date_type"])
        lines.append(f"• {cat_name} {dt_name}")
        keyboard.append([InlineKeyboardButton(f"🔕 Отписаться: {cat_name} {dt_name}",
                                               callback_data=f"unsub_{s['category']}_{s['date_type']}")])
    await update.message.reply_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(keyboard))
async def send_subscriptions_digest(bot, date_type: str):
    """Рассылает дайджест подписчикам после обновления парсеров."""
    logger.info(f"📬 Рассылка дайджеста: {date_type}")
    subscribers = get_all_subscribers()
    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    sent_count, error_count = 0, 0

    categories_with_subs = {cat for (cat, dt) in subscribers.keys() if dt == date_type}

    for category in categories_with_subs:
        user_ids = subscribers.get((category, date_type), [])
        if not user_ids:
            continue

        if date_type == "today":
            events = get_events_by_date_and_category(today, category)
            period_label = f"сегодня ({today.strftime('%d.%m')})"
        elif date_type == "tomorrow":
            events = get_events_by_date_and_category(tomorrow, category)
            period_label = f"завтра ({tomorrow.strftime('%d.%m')})"
        elif date_type == "upcoming":
            events = get_upcoming_events(limit=10, category=category)
            period_label = "ближайшие дни"
        elif date_type == "weekend":
            events, saturday, sunday = get_weekend_events(category=category)
            period_label = f"выходные ({saturday.strftime('%d.%m')}–{sunday.strftime('%d.%m')})"
        else:
            continue

        if not events:
            continue

        display_name = CATEGORY_NAMES.get(category, category)
        preview = list(events)[:5]
        lines = [f"🔔 **{display_name} на {period_label}** — {len(events)} событий\n"]

        if category == "cinema":
            for text in format_grouped_cinema_events(group_cinema_events(preview)):
                lines.append(text + "\n")
        else:
            for event in preview:
                lines.append(format_event_text(event) + "\n")

        if len(events) > 5:
            lines.append(f"_...и ещё {len(events) - 5} событий. Откройте бот для просмотра всех._")

        message_text = "\n".join(lines)

        unsubscribe_keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔕 Отписаться", callback_data=f"unsub_{category}_{date_type}")
        ]])
        for user_id in user_ids:
            try:
                await bot.send_message(chat_id=user_id, text=message_text,
                                       reply_markup=unsubscribe_keyboard,
                                       parse_mode="Markdown", disable_web_page_preview=True)
                sent_count += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                error_count += 1
                logger.warning(f"Не удалось отправить подписчику {user_id}: {e}")

    logger.info(f"📬 Рассылка завершена: отправлено {sent_count}, ошибок {error_count}")
    return sent_count, error_count


# ---------------------- Статистика ----------------------


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Нет доступа.")
        return
    
    stats = get_stats_data()
    
    lines = [
        "<b>📊 СТАТИСТИКА БОТА</b>",
        "",
        f"👥 Всего пользователей: <b>{stats['total_users']}</b>",
        f"📨 Всего запросов: <b>{stats['total_actions']}</b>",
        f"🟢 Пользователей сегодня: <b>{stats['users_today']}</b>",
        f"📬 Запросов сегодня: <b>{stats['actions_today']}</b>",
        f"🔔 Подписчиков: <b>{stats['subscribers_count']}</b>",
        f"🗂 Событий в базе: <b>{stats['events_count']}</b>",
        "",
        "<b>📅 Активность за 7 дней:</b>",
    ]
    
    for row in stats["daily_activity"]:
        lines.append(f"  {row['day']} — {row['cnt']} запр. {row['users']} польз.")
    
    lines.extend([
        "",
        "<b>🔝 Топ действий:</b>"
    ])
    
    for row in stats["top_actions"]:
        import html
        lines.append(f"  {html.escape(str(row['action']))} — {row['cnt']}")
    
    await update.message.reply_text(
        "\n".join(lines), 
        parse_mode="HTML"
    )


# ---------------------- Планировщик парсеров ----------------------


async def update_parsers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручной запуск парсеров (только для администратора)."""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Нет доступа.")
        return
    
    await update.message.reply_text("🔄 **Обновление афиши...**\nЗапускаю парсеры, ~1-2 минуты.", parse_mode="Markdown")
    
    try:
        process = await asyncio.create_subprocess_exec(
            "python", "run_all_parsers.py",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
        elapsed = (datetime.now() - update.message.date.replace(tzinfo=None)).total_seconds()
        
        if process.returncode == 0:
            output = stdout.decode("utf-8", errors="replace")
            
            # Собираем статистику по парсерам
            parsers_stats = {
                'total': 0,
                'success': 0,
                'failed': 0
            }
            
            # Список для хранения строк с результатами
            result_lines = []
            
            # Парсим вывод построчно
            lines = output.split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Убираем логи (время и уровень)
                clean = re.sub(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ - [\w.]+ - \w+ - ", "", line)
                clean = re.sub(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ - \w+ - ", "", clean)
                
                # Ищем строки с запуском парсеров
                if "▶️ Запуск" in clean:
                    parsers_stats['total'] += 1
                    result_lines.append(clean)
                
                # Ищем успешные завершения
                elif "✅" in clean and "завершен успешно" in clean:
                    parsers_stats['success'] += 1
                    result_lines.append(clean)
                
                # Ищем ошибки
                elif "❌" in clean and ("ошибкой" in clean.lower() or "превысил" in clean.lower() or "ошибка" in clean.lower()):
                    parsers_stats['failed'] += 1
                    result_lines.append(clean)
                
                # Показываем статистику парсеров (добавлено новых и т.д.)
                elif any(marker in clean for marker in ["✅ Добавлено", "📊 Результаты", "📊 Всего найдено"]):
                    # Убираем дубликаты (первые 3 строки с результатами)
                    if len(result_lines) < 20:  # Лимит на количество строк
                        result_lines.append(clean)
                
                # Показываем важные предупреждения
                elif "⚠️" in clean:
                    result_lines.append(clean)
            
            # Убираем дубликаты строк
            seen = set()
            unique_lines = []
            for line in result_lines:
                if line not in seen:
                    seen.add(line)
                    unique_lines.append(line)
            
            # Формируем ответ
            response = [
                f"✅ Обновление завершено! ⏱ {elapsed:.0f} сек",
                f"📊 Статистика: запущено {parsers_stats['total']}, ✅ {parsers_stats['success']}, ❌ {parsers_stats['failed']}",
                "",
                "📋 Детали:"
            ]
            response.extend(unique_lines[:15])
            if len(unique_lines) > 15:
                response.append(f"...и ещё {len(unique_lines) - 15} строк")
            await update.message.reply_text("\n".join(response))
            
        else:
            error_msg = stderr.decode()[:500] if stderr else "неизвестная ошибка"
            await update.message.reply_text(
                f"❌ **Ошибка при выполнении парсеров**\n\n```\n{error_msg}\n```", 
                parse_mode="Markdown"
            )
            
    except asyncio.TimeoutError:
        await update.message.reply_text("⏰ Превышено время ожидания (5 мин).", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"💥 **Ошибка**: `{e}`", parse_mode="Markdown")


async def run_parsers_job(bot=None):
    """Запускает парсеры по расписанию, отправляет отчёт и рассылает дайджест."""
    logger.info("⏰ Запуск парсеров по расписанию...")
    start_time = datetime.now()
    try:
        process = await asyncio.create_subprocess_exec(
            "python", "run_all_parsers.py",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=600)
        elapsed = (datetime.now() - start_time).total_seconds()
        if process.returncode == 0:
            output = stdout.decode()
            results = [
                line.strip() for line in output.split('\n')
                if ('✅' in line or '❌' in line) and ('добавлено' in line.lower() or 'ошибка' in line.lower())
            ]
            logger.info(f"✅ Парсеры завершены за {elapsed:.0f} сек")
            if bot:
                await _send_parser_report(bot, results, elapsed)
                sent, errors = await send_subscriptions_digest(bot, "today")
                logger.info(f"📬 Дайджест отправлен: {sent} польз., {errors} ошибок")
        else:
            error_msg = stderr.decode()[:300] if stderr else "неизвестная ошибка"
            logger.error(f"❌ Парсеры упали: {error_msg}")
            if bot:
                await bot.send_message(chat_id=ADMIN_ID, text=f"❌ **Ошибка парсеров**\n\n```\n{error_msg}\n```", parse_mode="Markdown")
    except asyncio.TimeoutError:
        logger.error("⏰ Таймаут парсеров (10 мин)")
        if bot:
            await bot.send_message(chat_id=ADMIN_ID, text="⏰ **Таймаут** парсеров (>10 мин)", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"💥 Ошибка: {e}")
        if bot:
            await bot.send_message(chat_id=ADMIN_ID, text=f"💥 **Критическая ошибка**: {e}", parse_mode="Markdown")


async def _send_parser_report(bot, results: list, elapsed: float):
    lines = [
        "🤖 **Отчёт о запуске парсеров**",
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')} | ⏱ {elapsed:.0f} сек", "",
    ]
    lines.extend(results or ["ℹ️ Нет данных о результатах"])
    success = sum(1 for r in results if '✅' in r)
    failed = sum(1 for r in results if '❌' in r)
    lines.extend(["", f"📊 Итого: ✅ {success} | ❌ {failed}"])
    try:
        await bot.send_message(chat_id=ADMIN_ID, text="\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Не удалось отправить отчёт: {e}")


def setup_scheduler(application):
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_parsers_job,
        trigger=CronTrigger(hour=3, minute=0),  # UTC = 6:00 Минск
        kwargs={"bot": application.bot},
        id="daily_parsers",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("⏰ Планировщик запущен. Парсеры + рассылка ежедневно в 6:00 (Минск)")


# ---------------------- Донат ----------------------


def _build_donate_keyboard():
    keyboard, row = [], []
    for amount in DONATION_SUGGESTIONS:
        row.append(InlineKeyboardButton(f"⭐ {amount} Stars", callback_data=f"donate_{amount}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    return keyboard


DONATE_TEXT = (
    "🌟 **Поддержать проект**\n\n"
    "Если вам нравится бот и вы хотите поддержать его развитие, "
    "вы можете отправить донат в Telegram Stars.\n\n"
    "Выберите сумму ниже или отправьте команду\n"
    "`/donate <сумма>` (например, `/donate 150`)"
)


async def donate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "donate_menu")
    await update.message.reply_text(DONATE_TEXT, reply_markup=InlineKeyboardMarkup(_build_donate_keyboard()), parse_mode=ParseMode.MARKDOWN)


async def custom_donate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    try:
        if not context.args or len(context.args) != 1:
            await update.message.reply_text("❌ Используйте: `/donate <сумма>`\nНапример: `/donate 150`", parse_mode=ParseMode.MARKDOWN)
            return
        amount = int(context.args[0])
        if amount < 10:
            await update.message.reply_text("❌ Минимальная сумма — 10 Stars")
            return
        if amount > 2500:
            await update.message.reply_text("❌ Максимальная сумма — 2500 Stars")
            return
        log_user_action(user.id, user.username, user.first_name, "donate_custom", str(amount))
        await send_star_invoice(update, context, amount)
    except ValueError:
        await update.message.reply_text("❌ Введите число. Пример: `/donate 150`", parse_mode=ParseMode.MARKDOWN)


async def send_star_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE, amount: int):
    chat_id = update.callback_query.message.chat_id if update.callback_query else update.message.chat_id
    await context.bot.send_invoice(
        chat_id=chat_id,
        title="Поддержка бота",
        description=f"Благодарим за поддержку! Вы отправляете {amount} Telegram Stars.",
        payload=f"donation_{amount}_{datetime.now().timestamp()}",
        provider_token="",
        currency=DONATION_CURRENCY,
        prices=[LabeledPrice(label=f"Stars {amount}", amount=amount)],
        need_name=False, need_phone_number=False, need_email=False,
        need_shipping_address=False, is_flexible=False,
    )


async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    if query.invoice_payload.startswith("donation_"):
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message="Что-то пошло не так")


async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    amount = update.message.successful_payment.total_amount
    log_user_action(user.id, user.username, user.first_name, "donate_success", str(amount))
    await update.message.reply_text(
        f"✅ **Спасибо за поддержку!**\n\nВы отправили {amount} ⭐ Stars.\nВаша помощь очень ценится! 🙏",
        parse_mode=ParseMode.MARKDOWN,
    )
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=(f"💰 **Получен донат!**\n\nОт: {user.first_name}\n"
              f"Username: @{user.username or 'нет'}\nID: `{user.id}`\nСумма: {amount} ⭐ Stars"),
        parse_mode=ParseMode.MARKDOWN,
    )


# ---------------------- Хендлеры сообщений ----------------------


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "start")
    await update.message.reply_text(
        f"🎉 Привет, {user.first_name}!\n\n"
        "Я бот-афиша Минска. Помогу найти интересные события в городе.\n\n"
        "🔍 **Как искать:**\n"
        "• Отправьте **название** события (например: «концерт», «Дельфин»)\n"
        "• Или **дату** в формате ДД.ММ или ДД.ММ.ГГГГ\n\n"
        "Используйте кнопки для быстрого поиска 👇",
        reply_markup=get_reply_main_menu(), parse_mode="Markdown",
    )


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "cmd_today")
    today = datetime.now()
    events = get_events_by_date_and_category(today)
    set_pagination(context, events, f"📅 **События на {today.strftime('%d.%m.%Y')}:**")
    await show_page(update, context)


async def about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "about")
    counts = get_events_count_by_category()
    total_events = sum(counts.values())
    cat_lines = [
        f"  {CATEGORY_NAMES[cat]} — {cnt}"
        for cat, cnt in counts.items()
        if cat in CATEGORY_NAMES and cnt > 0
    ]
    text = (
        "🌟 **MinskDvizh** — твой гид по событиям Минска!\n\n"
        "📅 **О проекте:**\n"
        "Собираем данные из разных источников и обновляем афишу каждое утро.\n\n"
        "🎯 **Актуальные события:**\n"
        + "\n".join(cat_lines) + "\n"
        f"\n📊 Всего событий: **{total_events}**\n\n"
        "🔍 **Как пользоваться:**\n"
        "• Отправь **название** события или **дату** (ДД.ММ)\n"
        "• Или используй кнопки меню\n\n"
        "💼 **Добавить мероприятие / сотрудничество:**\n"
        "📱 @i354444\n\n"
        "⭐ Если бот полезен — поддержи проект!\n\n"
        "#minskdvizh #афишаминск #минск"
    )
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Поддержать", callback_data="show_donate")]]),
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )


async def search_by_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    user = update.effective_user
    if len(query) < 3:
        await update.message.reply_text("🔍 Введите минимум 3 символа.")
        return
    log_user_action(user.id, user.username, user.first_name, "search_title", query)
    await update.message.chat.send_action(action="typing")
    events = search_events_by_title(query)
    if events:
        set_pagination(context, events, f"🔍 **Результаты: «{query}»**")
        await show_page(update, context)
    else:
        await update.message.reply_text(f"По запросу «{query}» ничего не найдено.", parse_mode="Markdown")


async def search_by_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    date_text = update.message.text.strip()
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "search_date", date_text)
    result, formatted_date, status = search_events_by_date_raw(date_text)
    if status == "неверный_формат":
        await update.message.reply_text(
            f"📅 Не удалось распознать дату «{date_text}».\nФормат: ДД.ММ или ДД.ММ.ГГГГ",
            parse_mode="Markdown",
        )
    elif status == "нет_событий":
        await update.message.reply_text(f"📅 Событий на {formatted_date} не найдено.")
    elif status == "найдены":
        set_pagination(context, result, f"📅 **События на {formatted_date}:**")
        await show_page(update, context)
    else:
        await update.message.reply_text("❌ Ошибка при поиске. Попробуйте позже.")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.effective_user

    if text == "⭐ Поддержать":
        log_user_action(user.id, user.username, user.first_name, "donate_menu_button")
        await donate_command(update, context)
        return
    if text == "ℹ️ О проекте":
        log_user_action(user.id, user.username, user.first_name, "about_button")
        await about(update, context)
        return
    if text == "📅 Сегодня":
        log_user_action(user.id, user.username, user.first_name, "menu_today")
        today = datetime.now()
        events = get_events_by_date_and_category(today)
        set_pagination(context, events, f"📅 **События на {today.strftime('%d.%m.%Y')}:**")
        await show_page(update, context)
        return
    if text == "📆 Завтра":
        log_user_action(user.id, user.username, user.first_name, "menu_tomorrow")
        tomorrow = datetime.now() + timedelta(days=1)
        events = get_events_by_date_and_category(tomorrow)
        set_pagination(context, events, f"📆 **События на {tomorrow.strftime('%d.%m.%Y')}:**")
        await show_page(update, context)
        return
    if text == "🎉 Выходные":
        log_user_action(user.id, user.username, user.first_name, "menu_weekend")
        events, saturday, sunday = get_weekend_events()
        set_pagination(context, events, f"🎉 **Выходные ({saturday.strftime('%d.%m')}–{sunday.strftime('%d.%m')}):**")
        await show_page(update, context)
        return
    if text == "⏰ Ближайшие":
        log_user_action(user.id, user.username, user.first_name, "menu_upcoming")
        events = get_upcoming_events(limit=100)
        if events:
            set_pagination(context, events, "⏰ **Ближайшие события:**")
            await show_page(update, context)
        else:
            await update.message.reply_text("😕 Ближайших событий не найдено.")
        return
    if text == "🗓 Календарь":
        log_user_action(user.id, user.username, user.first_name, "menu_calendar")
        await show_calendar(update, context)
        return
    if text == "🎯 Категории":
        log_user_action(user.id, user.username, user.first_name, "menu_categories")
        await update.message.reply_text(
            "🎯 **Выберите категорию:**",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎬 Кино", callback_data="cat_cinema"), InlineKeyboardButton("🎵 Концерты", callback_data="cat_concert")],
                [InlineKeyboardButton("🎭 Театр", callback_data="cat_theater"), InlineKeyboardButton("🖼️ Выставки", callback_data="cat_exhibition")],
                [InlineKeyboardButton("🧸 Детям", callback_data="cat_kids"), InlineKeyboardButton("⚽ Спорт", callback_data="cat_sport")],
                [InlineKeyboardButton("🆓 Бесплатно", callback_data="cat_free"), InlineKeyboardButton("◀️ Назад", callback_data="back_to_main")],
            ]),
            parse_mode="Markdown",
        )
        return
    if re.match(r"^\d{1,2}\.\d{1,2}(\.\d{2,4})?$", text):
        await search_by_date(update, context)
    else:
        await search_by_title(update, context)


# ---------------------- Хендлер кнопок ----------------------


async def handle_filter_buttons(query, context: ContextTypes.DEFAULT_TYPE, category: str):
    data = context.user_data.get("pagination")
    if not data:
        await query.answer("Устарело. Попробуйте снова.")
        return
    user = query.from_user
    log_user_action(user.id, user.username, user.first_name, "filter_category", category)
    filtered = data["events"] if category == "all" else filter_events_by_category(data["events"], category)
    set_pagination(context, filtered, data["title"], date_info=data["date_info"])
    await show_page(query, context)


async def handle_date_category_buttons(query, context: ContextTypes.DEFAULT_TYPE, date_type: str, category: str):
    user = query.from_user
    log_user_action(user.id, user.username, user.first_name, f"cat_{category}_{date_type}")
    display_name = CATEGORY_NAMES.get(category, category)
    if date_type == "today":
        today = datetime.now()
        events = get_events_by_date_and_category(today, category)
        set_pagination(context, events, f"📅 **{display_name} на {today.strftime('%d.%m.%Y')}:**")
        await show_page(query, context)
        await send_subscription_prompt(query, category, "today")
    elif date_type == "tomorrow":
        tomorrow = datetime.now() + timedelta(days=1)
        events = get_events_by_date_and_category(tomorrow, category)
        set_pagination(context, events, f"📆 **{display_name} на {tomorrow.strftime('%d.%m.%Y')}:**")
        await show_page(query, context)
        await send_subscription_prompt(query, category, "tomorrow")
    elif date_type == "upcoming":
        events = get_upcoming_events(limit=100, category=category)
        if events:
            set_pagination(context, events, f"⏰ **Ближайшие {display_name}:**")
            await show_page(query, context)
            await send_subscription_prompt(query, category, "upcoming")
        else:
            await query.edit_message_text(f"😕 Ближайших событий в категории {display_name} не найдено.", parse_mode="Markdown")
    elif date_type == "weekend":
        events, saturday, sunday = get_weekend_events(category=category)
        set_pagination(context, events, f"🎉 **{display_name} на выходные ({saturday.strftime('%d.%m')}–{sunday.strftime('%d.%m')}):**")
        await show_page(query, context)
        await send_subscription_prompt(query, category, "weekend")


async def handle_simple_buttons(query, context: ContextTypes.DEFAULT_TYPE, data: str):
    chat_id = query.message.chat_id
    user = query.from_user
    if data == "today":
        log_user_action(user.id, user.username, user.first_name, "btn_today")
        today = datetime.now()
        events = get_events_by_date_and_category(today)
        set_pagination(context, events, f"📅 **События на {today.strftime('%d.%m.%Y')}:**")
        await show_page(query, context)
    elif data == "tomorrow":
        log_user_action(user.id, user.username, user.first_name, "btn_tomorrow")
        tomorrow = datetime.now() + timedelta(days=1)
        events = get_events_by_date_and_category(tomorrow)
        set_pagination(context, events, f"📆 **События на {tomorrow.strftime('%d.%m.%Y')}:**")
        await show_page(query, context)
    elif data == "weekend":
        log_user_action(user.id, user.username, user.first_name, "btn_weekend")
        events, saturday, sunday = get_weekend_events()
        set_pagination(context, events, f"🎉 **Выходные ({saturday.strftime('%d.%m')}–{sunday.strftime('%d.%m')}):**")
        await show_page(query, context)
    elif data == "soon":
        log_user_action(user.id, user.username, user.first_name, "btn_upcoming")
        events = get_upcoming_events(limit=100)
        if events:
            set_pagination(context, events, "⏰ **Ближайшие события:**")
            await show_page(query, context)
        else:
            await query.edit_message_text("😕 Ближайших событий не найдено.", parse_mode="Markdown")
    elif data == "calendar":
        await show_calendar(query, context)
    elif data == "show_categories":
        await show_categories_menu(query, context)
    elif data == "back_to_main":
        await show_main_menu(chat_id, context, query.message.reply_text)
    elif data.startswith("cat_"):
        category = data.replace("cat_", "")
        log_user_action(user.id, user.username, user.first_name, "open_category", category)
        await show_date_options(query, category)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if data == "show_donate":
        await query.answer()
        user = query.from_user
        log_user_action(user.id, user.username, user.first_name, "donate_menu")
        await query.message.reply_text(DONATE_TEXT, reply_markup=InlineKeyboardMarkup(_build_donate_keyboard()), parse_mode=ParseMode.MARKDOWN)
        return
    if data.startswith("donate_"):
        await query.answer()
        await send_star_invoice(update, context, int(data.replace("donate_", "")))
        return
    if data.startswith("filter_"):
        await handle_filter_buttons(query, context, data.replace("filter_", ""))
        return
    if data.startswith("date_"):
        parts = data.split("_")
        await handle_date_category_buttons(query, context, parts[1], parts[2])
        return
    if data == "page_noop":
        await query.answer()
        return
    if data == "page_prev":
        if "pagination" in context.user_data:
            context.user_data["pagination"]["page"] = max(0, context.user_data["pagination"]["page"] - 1)
        await show_page(query, context)
        return
    if data == "page_next":
        if "pagination" in context.user_data:
            context.user_data["pagination"]["page"] += 1
        await show_page(query, context)
        return
    if data.startswith("sub_"):
        _, category, date_type = data.split("_", 2)
        user = query.from_user
        add_subscription(user.id, category, date_type)
        log_user_action(user.id, user.username, user.first_name, "subscribe", f"{category}_{date_type}")
        try:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔕 Отписаться", callback_data=f"unsub_{category}_{date_type}")
            ]]))
        except Exception: pass
        await query.answer("Подписка оформлена 🔔", show_alert=False)
        return
    if data.startswith("unsub_"):
        _, category, date_type = data.split("_", 2)
        user = query.from_user
        remove_subscription(user.id, category, date_type)
        log_user_action(user.id, user.username, user.first_name, "unsubscribe", f"{category}_{date_type}")
        try:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔔 Подписаться", callback_data=f"sub_{category}_{date_type}")
            ]]))
        except Exception: pass
        await query.answer("Подписка отменена 🔕", show_alert=False)
        return
    if data.startswith("cal_"):
        parts = data.split("_")
        action = parts[1]
        year, month = int(parts[2]), int(parts[3])
        if action == "prev":
            month -= 1
            if month < 1: month = 12; year -= 1
        elif action == "next":
            month += 1
            if month > 12: month = 1; year += 1
        elif action == "day":
            day = int(parts[4])
            date_obj = datetime(year, month, day, tzinfo=MINSK_TZ)
            user = query.from_user
            log_user_action(user.id, user.username, user.first_name, "calendar_day", f"{day:02d}.{month:02d}.{year}")
            events = get_events_by_date_and_category(date_obj)
            if events:
                set_pagination(context, events, f"📅 События на {day:02d}.{month:02d}.{year}:")
                await show_page(query, context)
            else:
                await query.answer(f"На {day:02d}.{month:02d}.{year} событий нет", show_alert=True)
            return
        await show_calendar(query, context, year, month)
        return
    await handle_simple_buttons(query, context, data)


# ---------------------- main ----------------------


def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в окружении")

    init_db()

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("today", today_command))
    application.add_handler(CommandHandler("subs", show_subscriptions))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("update", update_parsers))
    application.add_handler(CommandHandler("donate", custom_donate))
    application.add_handler(CommandHandler("support", donate_command))
    application.add_handler(CommandHandler("about", about))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    setup_scheduler(application)

    logger.info("🚀 Бот запущен")
    application.run_polling()


if __name__ == "__main__":
    main()
