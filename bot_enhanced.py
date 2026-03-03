#!/usr/bin/env python3
# bot_enhanced.py
# Бот-афиша Минска с запуском run_all_parsers.py по расписанию

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
from telegram import LabeledPrice
from telegram.ext import PreCheckoutQueryHandler, ShippingQueryHandler

# ---------------------- Конфиг и логирование ----------------------

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_NAME = "events_final.db"
ADMIN_ID = 502917728

DONATION_ENABLED = True
DONATION_SUGGESTIONS = [10, 50, 100, 500]  # Варианты доната в звёздах
DONATION_CURRENCY = "XTR"  # XTR = Telegram Stars

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
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER,
                category TEXT,
                date_type TEXT,
                PRIMARY KEY (user_id, category, date_type)
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT,
                action TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL
            )
        """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_stats_user_id ON user_stats(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_stats_created_at ON user_stats(created_at)")
        conn.commit()


def log_user_action(user_id: int, username: str | None, first_name: str | None, action: str, detail: str | None = None):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO user_stats (user_id, username, first_name, action, detail, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (user_id, username, first_name, action, detail, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Ошибка логирования действия: {e}")


def get_stats_data() -> dict:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_stats")
        total_users = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM user_stats")
        total_actions = cursor.fetchone()[0]
        today = datetime.now().strftime("%Y-%m-%d")
        cursor.execute("SELECT COUNT(DISTINCT user_id) FROM user_stats WHERE created_at LIKE ?", (f"{today}%",))
        users_today = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM user_stats WHERE created_at LIKE ?", (f"{today}%",))
        actions_today = cursor.fetchone()[0]
        cursor.execute(
            """
            SELECT DATE(created_at) as day, COUNT(*) as cnt, COUNT(DISTINCT user_id) as users
            FROM user_stats
            WHERE created_at >= DATE('now', '-7 days')
            GROUP BY day
            ORDER BY day DESC
            """
        )
        daily_activity = cursor.fetchall()
        cursor.execute("SELECT action, COUNT(*) as cnt FROM user_stats GROUP BY action ORDER BY cnt DESC LIMIT 10")
        top_actions = cursor.fetchall()
        return {
            "total_users": total_users,
            "total_actions": total_actions,
            "users_today": users_today,
            "actions_today": actions_today,
            "daily_activity": daily_activity,
            "top_actions": top_actions,
        }


def search_events_by_title(query: str, limit: int = 20):
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, title, details, description, event_date, show_time,
                   place, location, price, category, source_url
            FROM events 
            WHERE title LIKE ? COLLATE NOCASE AND event_date >= ?
            ORDER BY event_date, show_time, title 
            LIMIT ?
        """,
            (f"%{query}%", today, limit * SEARCH_MULTIPLIER),
        )
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
    day = day.zfill(2)
    month = month.zfill(2)
    search_date = f"{year}-{month}-{day}"
    formatted_date = f"{day}.{month}.{year}"
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, title, details, description, event_date, show_time,
                   place, location, price, category, source_url
            FROM events 
            WHERE event_date = ?
            ORDER BY show_time, title 
            LIMIT 300
        """,
            (search_date,),
        )
        events = cursor.fetchall()
    if events:
        return events, formatted_date, "найдены"
    else:
        return [], formatted_date, "нет_событий"


def get_events_by_date_and_category(target_date: datetime, category: str | None = None):
    """Получает события на конкретную дату с учётом времени (для сегодня)"""
    date_str = target_date.strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Базовый запрос
        query = """
            SELECT id, title, details, description, event_date, show_time,
                   place, location, price, category, source_url
            FROM events 
            WHERE event_date = ?
        """
        params = [date_str]
        
        # Добавляем фильтр по категории если нужно
        if category and category != "all":
            query += " AND category = ?"
            params.append(category)
        
        # Если это сегодня - фильтруем по времени
        if date_str == today_str:
            current_time = datetime.now().strftime("%H:%M")
            query += """ AND (
                show_time = \'\'
                OR show_time > ?
            )"""
            params.append(current_time)
        
        query += " ORDER BY show_time, title"
        
        cursor.execute(query, params)
        return cursor.fetchall()

def get_upcoming_events(limit: int = 20, category: str | None = None):
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if category and category != "all":
            cursor.execute(
                """
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events 
                WHERE event_date >= ? AND category = ?
                ORDER BY event_date, show_time, title 
                LIMIT ?
            """,
                (today, category, limit * SEARCH_MULTIPLIER),
            )
        else:
            cursor.execute(
                """
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events 
                WHERE event_date >= ? 
                ORDER BY event_date, show_time, title 
                LIMIT ?
            """,
                (today, limit * SEARCH_MULTIPLIER),
            )
        return cursor.fetchall()


def get_weekend_events(category: str | None = None):
    today = datetime.now()
    days_until_saturday = (5 - today.weekday()) % 7
    if days_until_saturday == 0:
        days_until_saturday = 7
    saturday = today + timedelta(days=days_until_saturday)
    sunday = saturday + timedelta(days=1)
    saturday_str = saturday.strftime("%Y-%m-%d")
    sunday_str = sunday.strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if category and category != "all":
            cursor.execute(
                """
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events 
                WHERE event_date IN (?, ?) AND category = ?
                ORDER BY event_date, show_time, title
            """,
                (saturday_str, sunday_str, category),
            )
        else:
            cursor.execute(
                """
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events 
                WHERE event_date IN (?, ?)
                ORDER BY event_date, show_time, title
            """,
                (saturday_str, sunday_str),
            )
        events = cursor.fetchall()
    return events, saturday, sunday


def filter_events_by_category(events, category: str):
    return [e for e in events if e["category"] == category]


def add_subscription(user_id: int, category: str, date_type: str):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO subscriptions (user_id, category, date_type)
            VALUES (?, ?, ?)
        """,
            (user_id, category, date_type),
        )
        conn.commit()


def get_user_subscriptions(user_id: int):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT category, date_type
            FROM subscriptions
            WHERE user_id = ?
        """,
            (user_id,),
        )
        return cursor.fetchall()


# ---------------------- Форматирование ----------------------


def format_event_text(event) -> str:
    text = f"🎉 **{event['title']}**"
    if event["details"]:
        details = event["details"]
        if len(details) > 180:
            details = details[:177] + "..."
        text += f"\n📝 {details}"
    if event["event_date"]:
        date_obj = datetime.strptime(event["event_date"], "%Y-%m-%d")
        formatted_date = date_obj.strftime("%d.%m.%Y")
        text += f"\n📅 {formatted_date}"
    if event["show_time"]:
        text += f" ⏰ {event['show_time']}"
    if event["place"] and event["place"] != "Кинотеатр":
        text += f"\n🏢 {event['place']}"
    if event["price"]:
        text += f"\n💰 {event['price']}"
    if event["category"]:
        emoji = CATEGORY_EMOJI.get(event["category"], "📌")
        text += f"\n{emoji} {event['category'].capitalize()}"
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
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            formatted_date = date_obj.strftime("%d.%m.%Y")
            first_cinema = next(iter(cinemas.values()))
            details = first_cinema[0]["details"] if first_cinema else ""
            text = f"🎬 **{title}**"
            if details:
                if len(details) > 180:
                    details = details[:177] + "..."
                text += f"\n🎭 {details}"
            text += f"\n📅 {formatted_date}"
            for place, seances in cinemas.items():
                times = [s["time"] for s in seances if s["time"]]
                if not times:
                    continue
                times_str = ", ".join(times)
                text += f"\n   ⏰ {times_str} — {place}"
            result.append(text)
    return result


# ---------------------- Пагинация + категории ----------------------


def set_pagination(context: ContextTypes.DEFAULT_TYPE, events, title: str, date_info: str | None = None):
    context.user_data["pagination"] = {
        "events": list(events),
        "page": 0,
        "per_page": PER_PAGE,
        "title": title,
        "date_info": date_info,
    }


async def show_category_filter(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("pagination")
    if not data:
        return
    events = data["events"]
    if len(events) <= PER_PAGE:
        return
    category_counts = defaultdict(int)
    for e in events:
        if e["category"]:
            category_counts[e["category"]] += 1
    keyboard = []
    row = []
    category_buttons = {
        "cinema": ("🎬 Кино", "cinema"),
        "concert": ("🎵 Концерты", "concert"),
        "theater": ("🎭 Театр", "theater"),
        "exhibition": ("🖼️ Выставки", "exhibition"),
        "kids": ("🧸 Детям", "kids"),
        "sport": ("⚽ Спорт", "sport"),
        "free": ("🆓 Бесплатно", "free"),
    }
    for cat_key, (cat_name, cat_value) in category_buttons.items():
        if cat_key in category_counts:
            count = category_counts[cat_key]
            button_text = f"{cat_name} ({count})"
            row.append(InlineKeyboardButton(button_text, callback_data=f"filter_{cat_key}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("📋 Показать все", callback_data="filter_all")])
    total = len(events)
    text = f"📊 Найдено всего: {total} событий\nПоказаны первые {PER_PAGE}. Выберите категорию для просмотра всех:"
    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_page(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("pagination")
    if not data:
        if isinstance(update_or_query, Update):
            await update_or_query.message.reply_text("Данные для пагинации не найдены. Попробуйте запрос заново.")
        else:
            await update_or_query.answer("Данные для пагинации не найдены. Попробуйте запрос заново.", show_alert=True)
        return
    events = data["events"]
    page = data["page"]
    per_page = data["per_page"]
    title = data["title"]
    date_info = data["date_info"]
    total = len(events)
    if total == 0:
        if isinstance(update_or_query, Update):
            send_method = update_or_query.message.reply_text
        else:
            send_method = update_or_query.message.reply_text
        await send_method("😕 Событий не найдено.", parse_mode="Markdown")
        return
    max_page = (total - 1) // per_page
    if page < 0:
        page = 0
    if page > max_page:
        page = max_page
    data["page"] = page
    start = page * per_page
    end = start + per_page
    chunk = events[start:end]
    if isinstance(update_or_query, Update):
        message = update_or_query.message
        await message.chat.send_action(action="typing")
        send_method = message.reply_text
    else:
        query = update_or_query
        await query.answer()
        send_method = query.message.reply_text
    header_lines = []
    if title:
        header_lines.append(title)
    if date_info:
        header_lines.append(f"{date_info}")
    header_lines.append(f"Страница {page + 1} из {max_page + 1} (показано {len(chunk)} из {total})")
    header_text = "\n".join(header_lines)
    await send_method(header_text, parse_mode="Markdown")
    cinema_events = [e for e in chunk if e["category"] == "cinema"]
    other_events = [e for e in chunk if e["category"] != "cinema"]
    if cinema_events:
        grouped = group_cinema_events(cinema_events)
        formatted = format_grouped_cinema_events(grouped)
        for text in formatted:
            await send_method(
                f"{text}\n\n🔗 [Подробнее](https://afisha.relax.by/kino/minsk/)",
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
    for event in other_events:
        text = format_event_text(event)
        url = event["source_url"]
        await send_method(
            f"{text}\n\n🔗 [Подробнее]({url})",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    keyboard = []
    if page < max_page:
        keyboard.append([InlineKeyboardButton("➡️ Далее", callback_data="page_next")])
    if keyboard:
        await send_method("Навигация по страницам:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    if page == 0 and total > per_page:
        await show_category_filter(update_or_query, context)


# ---------------------- UI-хелперы ----------------------


def get_reply_main_menu():
    keyboard = [
        ["📅 Сегодня", "📆 Завтра"],
        ["⏰ Ближайшие", "🎉 Выходные"],
        ["📋 Все события", "🎯 Категории"],
        ["ℹ️ О проекте", "⭐ Поддержать"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def show_main_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE | None = None, send_method=None):
    text = "🎉 **Главное меню**\n\nВыберите действие:"
    reply_markup = get_reply_main_menu()
    if send_method:
        await send_method(text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode="Markdown")


async def show_categories_menu(query, context: ContextTypes.DEFAULT_TYPE):
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("🎬 Кино", callback_data="cat_cinema"), InlineKeyboardButton("🎵 Концерты", callback_data="cat_concert")],
        [InlineKeyboardButton("🎭 Театр", callback_data="cat_theater"), InlineKeyboardButton("🖼️ Выставки", callback_data="cat_exhibition")],
        [InlineKeyboardButton("🧸 Детям", callback_data="cat_kids"), InlineKeyboardButton("⚽ Спорт", callback_data="cat_sport")],
        [InlineKeyboardButton("🆓 Бесплатно", callback_data="cat_free"), InlineKeyboardButton("◀️ Назад в главное меню", callback_data="back_to_main")],
    ]
    await query.edit_message_text("🎯 **Выберите категорию:**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_date_options(update_or_query, category_name: str):
    keyboard = [
        [InlineKeyboardButton("📅 Сегодня", callback_data=f"date_today_{category_name}"), InlineKeyboardButton("📆 Завтра", callback_data=f"date_tomorrow_{category_name}")],
        [InlineKeyboardButton("⏰ Ближайшие", callback_data=f"date_upcoming_{category_name}"), InlineKeyboardButton("🎉 Выходные", callback_data=f"date_weekend_{category_name}")],
        [InlineKeyboardButton("◀️ Назад к категориям", callback_data="show_categories")],
    ]
    display_name = CATEGORY_NAMES.get(category_name, category_name)
    text = f"📌 **{display_name}**\n\nВыберите дату для поиска:"
    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await update_or_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ---------------------- Подписки ----------------------


async def send_subscription_prompt(query_or_update, category: str, date_type: str):
    display_name = CATEGORY_NAMES.get(category, category)
    date_type_names = {"today": "на сегодня", "tomorrow": "на завтра", "upcoming": "на ближайшие дни", "weekend": "на выходные"}
    dt_name = date_type_names.get(date_type, "")
    text = f"🔔 Подписаться на {display_name} {dt_name}?"
    keyboard = [[InlineKeyboardButton("🔔 Подписаться", callback_data=f"sub_{category}_{date_type}")]]
    if isinstance(query_or_update, Update):
        await query_or_update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await query_or_update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_subscriptions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    subs = get_user_subscriptions(user_id)
    if not subs:
        await update.message.reply_text("У вас пока нет активных подписок 🔔", parse_mode="Markdown")
        return
    lines = ["🔔 Ваши подписки:"]
    date_type_names = {"today": "на сегодня", "tomorrow": "на завтра", "upcoming": "на ближайшие дни", "weekend": "на выходные"}
    for sub in subs:
        cat = sub["category"]
        dt = sub["date_type"]
        cat_name = CATEGORY_NAMES.get(cat, cat)
        dt_name = date_type_names.get(dt, dt)
        lines.append(f"• {cat_name} {dt_name}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


# ---------------------- Статистика (только для админа) ----------------------


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Нет доступа.")
        return
    stats = get_stats_data()
    lines = [
        "📊 СТАТИСТИКА БОТА",
        "",
        f"👥 Всего пользователей: {stats['total_users']}",
        f"📨 Всего запросов: {stats['total_actions']}",
        f"🟢 Пользователей сегодня: {stats['users_today']}",
        f"📬 Запросов сегодня: {stats['actions_today']}",
        "",
        "📅 Активность за 7 дней:"
    ]
    for row in stats["daily_activity"]:
        lines.append(f"  {row['day']} — {row['cnt']} запр., {row['users']} польз.")
    lines.append("")
    lines.append("🔝 Топ действий:")
    for row in stats["top_actions"]:
        lines.append(f"  {row['action']} — {row['cnt']}")
    await update.message.reply_text("\n".join(lines))


# ---------------------- Планировщик парсеров ----------------------

async def update_parsers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручной запуск всех парсеров (только для администратора)"""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Нет доступа.")
        return
    
    await update.message.reply_text(
        "🔄 **Обновление афиши...**\n"
        "Запускаю все парсеры. Это может занять 1-2 минуты.",
        parse_mode="Markdown"
    )
    
    try:
        process = await asyncio.create_subprocess_exec(
            "python", "run_all_parsers.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
        
        elapsed = (datetime.now() - update.message.date.replace(tzinfo=None)).total_seconds()
        
        if process.returncode == 0:
            output = stdout.decode()
            
            # Собираем результаты (только понятные строки)
            results = []
            lines = output.split('\n')
            
            success_count = 0
            failed_count = 0
            
            for line in lines:
                # Пропускаем строки с INFO и технической информацией
                if 'INFO -' in line:
                    continue
                # Оставляем строки с эмодзи и результатами
                if '✅' in line or '❌' in line or '📊' in line:
                    # Очищаем от технической информации
                    clean_line = line.strip()
                    # Если есть дата и время в начале, убираем их
                    if re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', clean_line):
                        # Убираем первые 20 символов (дата+время)
                        parts = clean_line.split(' - ', 1)
                        if len(parts) > 1:
                            clean_line = parts[1]
                        else:
                            clean_line = clean_line[20:]
                    
                    if '✅' in line:
                        success_count += 1
                    elif '❌' in line:
                        failed_count += 1
                    
                    results.append(clean_line)
            
            # Формируем ответ
            response = [
                "✅ **Обновление завершено!**",
                f"⏱ Время выполнения: {elapsed:.0f} сек",
                ""
            ]
            
            if results:
                response.extend(results[:15])  # Показываем первые 15 результатов
                if len(results) > 15:
                    response.append(f"... и ещё {len(results) - 15} результатов")
            else:
                response.append("ℹ️ Нет данных о результатах")
            
            response.append("")
            response.append(f"📊 Итог: ✅ {success_count} успешно | ❌ {failed_count} ошибок")
            
            await update.message.reply_text(
                "\n".join(response),
                parse_mode="Markdown"
            )
        else:
            error_msg = stderr.decode()[:500] if stderr else "неизвестная ошибка"
            await update.message.reply_text(
                f"❌ **Ошибка при обновлении**\n\n```\n{error_msg}\n```",
                parse_mode="Markdown"
            )
            
    except asyncio.TimeoutError:
        await update.message.reply_text(
            "⏰ **Превышено время ожидания**\n"
            "Парсеры выполнялись дольше 5 минут.",
            parse_mode="Markdown"
        )
    except Exception as e:
        await update.message.reply_text(
            f"💥 **Критическая ошибка**:\n```\n{str(e)}\n```",
            parse_mode="Markdown"
        )

async def run_parsers_job(bot=None):
    """Запускает все парсеры по расписанию и отправляет отчёт админу."""
    logger.info("⏰ Запуск парсеров по расписанию...")
    start_time = datetime.now()

    # Список парсеров для отчёта
    parser_names = [
        "🎬 Кино (Relax)",
        "🎭 Театр (Relax)",
        "🎵 Концерты (Relax)",
        "🖼️ Выставки (Relax)",
        "🧸 Детям (Relax)",
        "🎫 Ticketpro"
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            "python", "run_all_parsers.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=600)

        elapsed = (datetime.now() - start_time).total_seconds()

        if process.returncode == 0:
            output = stdout.decode()
            logger.info(f"✅ run_all_parsers.py завершен за {elapsed:.0f} сек")
            
            # Собираем статистику по каждому парсеру
            results = []
            lines = output.split('\n')
            
            for line in lines:
                # Ищем строки с результатами парсеров
                if '✅' in line and ('добавлено' in line.lower() or 'сохранено' in line.lower()):
                    results.append(line.strip())
                elif '❌' in line and ('ошибка' in line.lower() or 'упал' in line.lower()):
                    results.append(line.strip())
            
            # Если не нашли строк с результатами, добавляем заглушку
            if not results:
                for i, name in enumerate(parser_names, 1):
                    if f"Парсер {i}" in output:
                        results.append(f"{name}: выполнен")
            
            if bot:
                await _send_parser_report(bot, results, elapsed)
        else:
            error_msg = stderr.decode()[:300] if stderr else "неизвестная ошибка"
            logger.error(f"❌ run_all_parsers.py упал: {error_msg}")
            if bot:
                await bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"❌ **Ошибка запуска парсеров**\n\n```\n{error_msg}\n```",
                    parse_mode="Markdown",
                )

    except asyncio.TimeoutError:
        logger.error("⏰ run_all_parsers.py превысил время ожидания (10 мин)")
        if bot:
            await bot.send_message(
                chat_id=ADMIN_ID,
                text="⏰ **Таймаут** при запуске парсеров (больше 10 минут)",
                parse_mode="Markdown",
            )
    except Exception as e:
        logger.error(f"💥 Ошибка при запуске парсеров: {e}")
        if bot:
            await bot.send_message(
                chat_id=ADMIN_ID,
                text=f"💥 **Критическая ошибка**: {e}",
                parse_mode="Markdown",
            )


async def _send_parser_report(bot, results: list, elapsed: float):
    """Отправляет отчёт о работе парсеров админу."""
    lines = [
        "🤖 **Отчёт о запуске парсеров**",
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')} | ⏱ {elapsed:.0f} сек",
        "",
    ]
    
    if results:
        lines.extend(results)
    else:
        lines.append("❌ Нет данных о работе парсеров")
    
    # Подсчитываем успешные и ошибочные
    success = sum(1 for r in results if '✅' in r)
    failed = sum(1 for r in results if '❌' in r)
    
    lines.append("")
    lines.append(f"📊 Итого: ✅ {success} | ❌ {failed}")

    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text="\n".join(lines),
            parse_mode="Markdown",
        )
        logger.info("📨 Отчёт отправлен админу")
    except Exception as e:
        logger.error(f"Не удалось отправить отчёт: {e}")


def setup_scheduler(application):
    """Настраивает планировщик задач."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_parsers_job,
        trigger=CronTrigger(hour=3, minute=0),  # UTC = 6:00 Минск
        kwargs={"bot": application.bot},
        id="daily_parsers",
        name="Run all parsers daily at 6:00 Minsk time",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("⏰ Планировщик запущен. Парсеры будут выполняться ежедневно в 6:00 (Минск)")


# ---------------------- Хендлеры сообщений ----------------------


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "start")
    welcome_text = f"""
🎉 Привет, {user.first_name}!

Я бот-афиша Минска. Помогу найти интересные события в городе.

🔍 **Как искать:**
• Просто отправьте **название** события (например: "концерт", "выставка", "Дельфин")
• Или отправьте **дату** в формате ДД.ММ или ДД.ММ.ГГГГ (например: 25.02 или 25.02.2026)

Используйте кнопки для быстрого поиска 👇
"""
    await update.message.reply_text(welcome_text, reply_markup=get_reply_main_menu(), parse_mode="Markdown")


async def search_by_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()
    user = update.effective_user
    if len(query) < 3:
        await update.message.reply_text("🔍 **Поиск по названию**\n\nВведите минимум 3 символа для поиска.", parse_mode="Markdown")
        return
    log_user_action(user.id, user.username, user.first_name, "search_title", query)
    await update.message.chat.send_action(action="typing")
    events = search_events_by_title(query)
    if events:
        title = f"🔍 **Результаты поиска по запросу '{query}':**"
        set_pagination(context, events, title, date_info=None)
        await show_page(update, context)
    else:
        await update.message.reply_text(f"🔍 **Поиск по запросу '{query}'**\n\n😕 Ничего не найдено.", parse_mode="Markdown")


async def search_by_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    date_text = update.message.text.strip()
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "search_date", date_text)
    result, formatted_date, status = search_events_by_date_raw(date_text)
    if status == "неверный_формат":
        await update.message.reply_text(
            f"📅 **Поиск по дате**\n\nНе удалось распознать дату '{date_text}'.\n\n"
            "Введите дату в формате:\n• ДД.ММ.ГГГГ (например, 25.02.2026)\n• ДД.ММ (например, 25.02)",
            parse_mode="Markdown",
        )
    elif status == "нет_событий":
        await update.message.reply_text(
            f"📅 **Событий на {formatted_date} не найдено.**\n\n"
            "Попробуйте другую дату или воспользуйтесь поиском по названию.",
            parse_mode="Markdown",
        )
    elif status == "найдены":
        title = f"📅 **События на {formatted_date}:**"
        set_pagination(context, result, title, date_info=None)
        await show_page(update, context)
    else:
        await update.message.reply_text("❌ Произошла ошибка при поиске. Попробуйте позже.", parse_mode="Markdown")


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
        title = f"📅 **События на {today.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(update, context)
        return
    if text == "📆 Завтра":
        log_user_action(user.id, user.username, user.first_name, "menu_tomorrow")
        tomorrow = datetime.now() + timedelta(days=1)
        events = get_events_by_date_and_category(tomorrow)
        title = f"📆 **События на {tomorrow.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(update, context)
        return
    if text == "🎉 Выходные":
        log_user_action(user.id, user.username, user.first_name, "menu_weekend")
        events, saturday, sunday = get_weekend_events()
        title = f"🎉 **Выходные ({saturday.strftime('%d.%m')}-{sunday.strftime('%d.%m')}):**"
        set_pagination(context, events, title, date_info=None)
        await show_page(update, context)
        return
    if text == "⏰ Ближайшие":
        log_user_action(user.id, user.username, user.first_name, "menu_upcoming")
        events = get_upcoming_events(limit=100)
        if events:
            title = "⏰ **Ближайшие события:**"
            set_pagination(context, events, title, date_info=None)
            await show_page(update, context)
        else:
            await update.message.reply_text("😕 Ближайших событий не найдено.", parse_mode="Markdown")
        return
    if text == "📋 Все события":
        log_user_action(user.id, user.username, user.first_name, "menu_all")
        events = get_upcoming_events(limit=300)
        if events:
            title = "📋 **Все события:**"
            set_pagination(context, events, title, date_info=None)
            await show_page(update, context)
        else:
            await update.message.reply_text("😕 Событий не найдено.", parse_mode="Markdown")
        return
    if text == "🎯 Категории":
        log_user_action(user.id, user.username, user.first_name, "menu_categories")
        await update.message.reply_text(
            "🎯 **Выберите категорию:**",
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🎬 Кино", callback_data="cat_cinema"), InlineKeyboardButton("🎵 Концерты", callback_data="cat_concert")],
                    [InlineKeyboardButton("🎭 Театр", callback_data="cat_theater"), InlineKeyboardButton("🖼️ Выставки", callback_data="cat_exhibition")],
                    [InlineKeyboardButton("🧸 Детям", callback_data="cat_kids"), InlineKeyboardButton("⚽ Спорт", callback_data="cat_sport")],
                    [InlineKeyboardButton("🆓 Бесплатно", callback_data="cat_free"), InlineKeyboardButton("◀️ Назад в главное меню", callback_data="back_to_main")],
                ]
            ),
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
        await query.answer("Результаты поиска устарели. Попробуйте снова.")
        return
    user = query.from_user
    log_user_action(user.id, user.username, user.first_name, "filter_category", category)
    all_events = data["events"]
    filtered_events = all_events if category == "all" else filter_events_by_category(all_events, category)
    set_pagination(context, filtered_events, data["title"], date_info=data["date_info"])
    await show_page(query, context)


async def handle_date_category_buttons(query, context: ContextTypes.DEFAULT_TYPE, date_type: str, category: str):
    user = query.from_user
    log_user_action(user.id, user.username, user.first_name, f"cat_{category}_{date_type}")
    display_name = CATEGORY_NAMES.get(category, category)
    if date_type == "today":
        today = datetime.now()
        events = get_events_by_date_and_category(today, category)
        title = f"📅 **{display_name} на {today.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)
        await send_subscription_prompt(query, category, "today")
    elif date_type == "tomorrow":
        tomorrow = datetime.now() + timedelta(days=1)
        events = get_events_by_date_and_category(tomorrow, category)
        title = f"📆 **{display_name} на {tomorrow.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)
        await send_subscription_prompt(query, category, "tomorrow")
    elif date_type == "upcoming":
        events = get_upcoming_events(limit=100, category=category)
        if events:
            title = f"⏰ **Ближайшие {display_name}:**"
            set_pagination(context, events, title, date_info=None)
            await show_page(query, context)
            await send_subscription_prompt(query, category, "upcoming")
        else:
            await query.edit_message_text(f"😕 Ближайших событий в категории {display_name} не найдено.", parse_mode="Markdown")
    elif date_type == "weekend":
        events, saturday, sunday = get_weekend_events(category=category)
        title = f"🎉 **{display_name} на выходные ({saturday.strftime('%d.%m')}-{sunday.strftime('%d.%m')}):**"
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)
        await send_subscription_prompt(query, category, "weekend")


async def handle_simple_buttons(query, context: ContextTypes.DEFAULT_TYPE, data: str):
    chat_id = query.message.chat_id
    user = query.from_user
    if data == "today":
        log_user_action(user.id, user.username, user.first_name, "btn_today")
        today = datetime.now()
        events = get_events_by_date_and_category(today)
        title = f"📅 **События на {today.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)
    elif data == "tomorrow":
        log_user_action(user.id, user.username, user.first_name, "btn_tomorrow")
        tomorrow = datetime.now() + timedelta(days=1)
        events = get_events_by_date_and_category(tomorrow)
        title = f"📆 **События на {tomorrow.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)
    elif data == "weekend":
        log_user_action(user.id, user.username, user.first_name, "btn_weekend")
        events, saturday, sunday = get_weekend_events()
        title = f"🎉 **Выходные ({saturday.strftime('%d.%m')}-{sunday.strftime('%d.%m')}):**"
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)
    elif data == "soon":
        log_user_action(user.id, user.username, user.first_name, "btn_upcoming")
        events = get_upcoming_events(limit=100)
        if events:
            title = "⏰ **Ближайшие события:**"
            set_pagination(context, events, title, date_info=None)
            await show_page(query, context)
        else:
            await query.edit_message_text("😕 Ближайших событий не найдено.", parse_mode="Markdown")
    elif data == "all":
        log_user_action(user.id, user.username, user.first_name, "btn_all")
        events = get_upcoming_events(limit=300)
        if events:
            title = "📋 **Все события:**"
            set_pagination(context, events, title, date_info=None)
            await show_page(query, context)
        else:
            await query.edit_message_text("😕 Событий не найдено.", parse_mode="Markdown")
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
    
    # Обработка кнопок доната (новые)
    
    # Обработка кнопки "Поддержать" из about
    if data == "show_donate":
        await donate_command(update, context)
        return
    if data.startswith("donate_"):
        amount = int(data.replace("donate_", ""))
        await query.answer()
        await send_star_invoice(update, context, amount)
        return
    
    # Обработка фильтров
    if data.startswith("filter_"):
        category = data.replace("filter_", "")
        await handle_filter_buttons(query, context, category)
        return
    
    # Обработка дат
    if data.startswith("date_"):
        parts = data.split("_")
        date_type = parts[1]
        category = parts[2]
        await handle_date_category_buttons(query, context, date_type, category)
        return
    
    # Обработка пагинации
    if data == "page_next":
        if "pagination" in context.user_data:
            context.user_data["pagination"]["page"] += 1
        await show_page(query, context)
        return
    
    # Обработка подписок
    if data.startswith("sub_"):
        _, category, date_type = data.split("_", 2)
        user = query.from_user
        add_subscription(user.id, category, date_type)
        log_user_action(user.id, user.username, user.first_name, "subscribe", f"{category}_{date_type}")
        await query.answer("Подписка оформлена 🔔", show_alert=False)
        return
    
    # Все остальные кнопки
    await handle_simple_buttons(query, context, data)
    
# ---------------------- Донат ----------------------

async def donate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает варианты доната"""
    # Если это callback запрос, используем message из него
    if update.callback_query:
        message = update.callback_query.message
    else:
        message = update.message
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "donate_menu")
    
    keyboard = []
    row = []
    
    for amount in DONATION_SUGGESTIONS:
        button = InlineKeyboardButton(
            f"⭐ {amount} Stars", 
            callback_data=f"donate_{amount}"
        )
        row.append(button)
        if len(row) == 2:
            keyboard.append(row)
            row = []
    
    if row:
        keyboard.append(row)
    
    text = (
        "🌟 **Поддержать проект**\n\n"
        "Если вам нравится бот и вы хотите поддержать его развитие, "
        "вы можете отправить донат в Telegram Stars.\n\n"
        "Выберите сумму ниже или отправьте команду\n"
        "`/donate <сумма>` (например, `/donate 150`)"
    )
    
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def custom_donate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает команду /donate <сумма>"""
    user = update.effective_user
    
    try:
        # Проверяем, есть ли аргумент
        if not context.args or len(context.args) != 1:
            await update.message.reply_text(
                "❌ Используйте: `/donate <сумма>`\n"
                "Например: `/donate 150`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        amount = int(context.args[0])
        
        if amount < 10:
            await update.message.reply_text("❌ Минимальная сумма доната — 10 Stars")
            return
        
        if amount > 2500:
            await update.message.reply_text("❌ Максимальная сумма доната — 2500 Stars")
            return
        
        log_user_action(user.id, user.username, user.first_name, "donate_custom", str(amount))
        await send_star_invoice(update, context, amount)
        
    except ValueError:
        await update.message.reply_text(
            "❌ Пожалуйста, введите число.\n"
            "Пример: `/donate 150`",
            parse_mode=ParseMode.MARKDOWN
        )



async def about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Информация о боте и контакты"""
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "about")
    
    text = """
🌟 **MinskDvizh** — твой гид по событиям Минска!

📅 **О проекте:**
Этот бот создан, чтобы помочь тебе находить самые интересные мероприятия в городе. Мы собираем данные из разных источников и обновляем афишу каждое утро.
Что будет дальше? Скоро узнаете!

🎯 **Что умеет бот:**
• 🎬 **Кино** — расписание всех кинотеатров
• 🎵 **Концерты** — живые выступления
• 🎭 **Театр** — спектакли и премьеры
• 🖼️ **Выставки** — искусство и культура
• 🧸 **Детям** — мероприятия для детей
• ⚽ **Спорт** — спортивные события

🔍 **Как пользоваться:**
• Просто отправь **название** события
• Или введи **дату** в формате ДД.ММ
• Или используй кнопки для быстрого поиска

📊 **Статистика:**
• 2700+ событий в базе
• 6 категорий мероприятий
• Новинки каждое утро

💼 **Сотрудничество:**
Хотите добавить свое мероприятие?
📱 По всем вопросам: @i354444

⭐ **Поддержать проект:**
Если бот оказался полезным, вы можете поддержать его развитие, нажав кнопку "Поддержать" в главном меню или командой /donate

#minskdvizh #афишаминск #минск #событияминск #концертыминск 
    """
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐ Поддержать", callback_data="show_donate")],
    ])
    
    await update.message.reply_text(
        text,
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True
    )


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /today - события на сегодня"""
    user = update.effective_user
    log_user_action(user.id, user.username, user.first_name, "command_today")
    
    today = datetime.now()
    events = get_events_by_date_and_category(today)
    title = f"📅 **События на {today.strftime('%d.%m.%Y')}:**"
    set_pagination(context, events, title, date_info=None)
    await show_page(update, context)
async def send_star_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE, amount: int):
    """Отправляет инвойс для оплаты Stars"""
    title = "Поддержка бота"
    description = f"Благодарим вас за поддержку проекта! Вы отправляете {amount} Telegram Stars."
    payload = f"donation_{amount}_{datetime.now().timestamp()}"
    currency = DONATION_CURRENCY
    prices = [LabeledPrice(label=f"⭐ {amount} Stars", amount=amount)]
    
    if update.callback_query:
        chat_id = update.callback_query.message.chat_id
    else:
        chat_id = update.message.chat_id
    
    await context.bot.send_invoice(
        chat_id=chat_id,
        title=title,
        description=description,
        payload=payload,
        provider_token="",  # Для Stars оставляем пустым
        currency=currency,
        prices=prices,
        need_name=False,
        need_phone_number=False,
        need_email=False,
        need_shipping_address=False,
        is_flexible=False,
    )

async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверка перед оплатой"""
    query = update.pre_checkout_query
    if query.invoice_payload.startswith("donation_"):
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message="Что-то пошло не так")

async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка успешной оплаты"""
    user = update.effective_user
    payment = update.message.successful_payment
    
    amount = payment.total_amount
    
    log_user_action(user.id, user.username, user.first_name, "donate_success", str(amount))
    
    # Отправляем благодарность пользователю
    await update.message.reply_text(
        f"✅ **Спасибо за поддержку!**\n\n"
        f"Вы отправили {amount} ⭐ Stars.\n"
        f"Ваша помощь очень ценится и помогает развивать бота! 🙏",
        parse_mode=ParseMode.MARKDOWN
    )
    
    # Отправляем уведомление админу
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=(
            f"💰 **Получен донат!**\n\n"
            f"От: {user.first_name}\n"
            f"Username: @{user.username if user.username else 'нет'}\n"
            f"ID: `{user.id}`\n"
            f"Сумма: {amount} ⭐ Stars"
        ),
        parse_mode=ParseMode.MARKDOWN
    )

# ---------------------- main ----------------------


def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в окружении")

    init_db()

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("subs", show_subscriptions))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("update", update_parsers))
    application.add_handler(CommandHandler("today", today_command))
    application.add_handler(CommandHandler("donate", custom_donate))
    application.add_handler(CommandHandler("support", donate_command))
    application.add_handler(CommandHandler("about", about))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))
    
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    setup_scheduler(application)

    logger.info("🚀 Бот запущен с поддержкой донатов ⭐")
    application.run_polling()


if __name__ == "__main__":
    main()
