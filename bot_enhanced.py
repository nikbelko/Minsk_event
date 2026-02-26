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
import subprocess
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ---------------------- Конфиг и логирование ----------------------

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_NAME = "events_final.db"

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
        conn.commit()


def search_events_by_title(query: str, limit: int = 20):
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, title, details, description, event_date, show_time,
                   place, location, price, category, source_url
            FROM events 
            WHERE title LIKE ? AND event_date >= ?
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
    date_str = target_date.strftime("%Y-%m-%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        if category and category != "all":
            cursor.execute(
                """
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events 
                WHERE event_date = ? AND category = ?
                ORDER BY show_time, title
            """,
                (date_str, category),
            )
        else:
            cursor.execute(
                """
                SELECT id, title, details, description, event_date, show_time,
                       place, location, price, category, source_url
                FROM events 
                WHERE event_date = ? 
                ORDER BY show_time, title
            """,
                (date_str,),
            )
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


def set_pagination(
    context: ContextTypes.DEFAULT_TYPE,
    events,
    title: str,
    date_info: str | None = None,
):
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
            row.append(
                InlineKeyboardButton(button_text, callback_data=f"filter_{cat_key}")
            )
            if len(row) == 2:
                keyboard.append(row)
                row = []

    if row:
        keyboard.append(row)

    keyboard.append(
        [InlineKeyboardButton("📋 Показать все", callback_data="filter_all")]
    )

    total = len(events)
    text = (
        f"📊 Найдено всего: {total} событий\n"
        f"Показаны первые {PER_PAGE}. Выберите категорию для просмотра всех:"
    )

    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )
    else:
        await update_or_query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )


async def show_page(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("pagination")
    if not data:
        if isinstance(update_or_query, Update):
            await update_or_query.message.reply_text(
                "Данные для пагинации не найдены. Попробуйте запрос заново."
            )
        else:
            await update_or_query.answer(
                "Данные для пагинации не найдены. Попробуйте запрос заново.",
                show_alert=True,
            )
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
    header_lines.append(
        f"Страница {page + 1} из {max_page + 1} (показано {len(chunk)} из {total})"
    )
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
        keyboard.append(
            [InlineKeyboardButton("➡️ Далее", callback_data="page_next")]
        )

    if keyboard:
        await send_method(
            "Навигация по страницам:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )

    if page == 0 and total > per_page:
        await show_category_filter(update_or_query, context)


# ---------------------- UI-хелперы ----------------------


def get_reply_main_menu():
    keyboard = [
        ["📅 Сегодня", "📆 Завтра"],
        ["⏰ Ближайшие", "🎉 Выходные"],
        ["📋 Все события", "🎯 Категории"],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def show_main_menu(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE | None = None,
    send_method=None,
):
    text = "🎉 **Главное меню**\n\nВыберите действие:"
    reply_markup = get_reply_main_menu()

    if send_method:
        await send_method(
            text,
            reply_markup=reply_markup,
            parse_mode="Markdown",
        )
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode="Markdown",
        )


async def show_categories_menu(query, context: ContextTypes.DEFAULT_TYPE):
    await query.answer()

    keyboard = [
        [
            InlineKeyboardButton("🎬 Кино", callback_data="cat_cinema"),
            InlineKeyboardButton("🎵 Концерты", callback_data="cat_concert"),
        ],
        [
            InlineKeyboardButton("🎭 Театр", callback_data="cat_theater"),
            InlineKeyboardButton("🖼️ Выставки", callback_data="cat_exhibition"),
        ],
        [
            InlineKeyboardButton("🧸 Детям", callback_data="cat_kids"),
            InlineKeyboardButton("⚽ Спорт", callback_data="cat_sport"),
        ],
        [
            InlineKeyboardButton("🆓 Бесплатно", callback_data="cat_free"),
            InlineKeyboardButton(
                "◀️ Назад в главное меню", callback_data="back_to_main"
            ),
        ],
    ]

    await query.edit_message_text(
        "🎯 **Выберите категорию:**",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
    )


async def show_date_options(update_or_query, category_name: str):
    keyboard = [
        [
            InlineKeyboardButton(
                "📅 Сегодня", callback_data=f"date_today_{category_name}"
            ),
            InlineKeyboardButton(
                "📆 Завтра", callback_data=f"date_tomorrow_{category_name}"
            ),
        ],
        [
            InlineKeyboardButton(
                "⏰ Ближайшие", callback_data=f"date_upcoming_{category_name}"
            ),
            InlineKeyboardButton(
                "🎉 Выходные", callback_data=f"date_weekend_{category_name}"
            ),
        ],
        [
            InlineKeyboardButton(
                "◀️ Назад к категориям", callback_data="show_categories"
            )
        ],
    ]

    display_name = CATEGORY_NAMES.get(category_name, category_name)
    text = f"📌 **{display_name}**\n\nВыберите дату для поиска:"

    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )
    else:
        await update_or_query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )


# ---------------------- Подписки ----------------------


async def send_subscription_prompt(
    query_or_update, category: str, date_type: str
):
    display_name = CATEGORY_NAMES.get(category, category)

    date_type_names = {
        "today": "на сегодня",
        "tomorrow": "на завтра",
        "upcoming": "на ближайшие дни",
        "weekend": "на выходные",
    }
    dt_name = date_type_names.get(date_type, "")

    text = f"🔔 Подписаться на {display_name} {dt_name}?"

    keyboard = [
        [
            InlineKeyboardButton(
                "🔔 Подписаться",
                callback_data=f"sub_{category}_{date_type}",
            )
        ]
    ]

    if isinstance(query_or_update, Update):
        await query_or_update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )
    else:
        await query_or_update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown",
        )


async def show_subscriptions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    subs = get_user_subscriptions(user_id)

    if not subs:
        await update.message.reply_text(
            "У вас пока нет активных подписок 🔔",
            parse_mode="Markdown",
        )
        return

    lines = ["🔔 Ваши подписки:"]
    date_type_names = {
        "today": "на сегодня",
        "tomorrow": "на завтра",
        "upcoming": "на ближайшие дни",
        "weekend": "на выходные",
    }

    for sub in subs:
        cat = sub["category"]
        dt = sub["date_type"]
        cat_name = CATEGORY_NAMES.get(cat, cat)
        dt_name = date_type_names.get(dt, dt)
        lines.append(f"• {cat_name} {dt_name}")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
    )

# ---------------------- Планировщик парсеров ----------------------

async def run_parsers_job():
    """Запускает все парсеры по расписанию"""
    logger.info("⏰ Запуск парсеров по расписанию...")
    
    try:
        # Запускаем run_all_parsers.py как отдельный процесс
        process = await asyncio.create_subprocess_exec(
            'python', 'run_all_parsers.py',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            logger.info(f"✅ Парсеры успешно завершены")
            if stdout:
                # Логируем только последние строки, чтобы не засорять
                output = stdout.decode().strip().split('\n')
                last_lines = output[-5:] if len(output) > 5 else output
                for line in last_lines:
                    if line.strip():
                        logger.info(f"   {line}")
        else:
            logger.error(f"❌ Ошибка при запуске парсеров (код {process.returncode})")
            if stderr:
                logger.error(f"Ошибка:\n{stderr.decode()}")
                
    except Exception as e:
        logger.error(f"💥 Критическая ошибка при запуске парсеров: {e}")

def setup_scheduler(application):
    """Настраивает планировщик задач"""
    scheduler = AsyncIOScheduler()
    
    # Запуск каждый день в 6:00 утра по Минску (3:00 UTC)
    scheduler.add_job(
        run_parsers_job,
        trigger=CronTrigger(hour=3, minute=0),  # UTC
        id='daily_parsers',
        name='Run all parsers daily at 6:00 Minsk time',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("⏰ Планировщик запущен. Парсеры будут выполняться ежедневно в 6:00 (Минск)")

async def manual_run_parsers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручной запуск парсеров (команда /run_parsers)"""
    await update.message.reply_text("🔄 Запускаю парсеры...")
    
    try:
        process = await asyncio.create_subprocess_exec(
            'python', 'run_all_parsers.py',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            result_text = "✅ Парсеры завершены успешно\n\n"
            if stdout:
                output = stdout.decode().strip().split('\n')
                last_lines = output[-10:] if len(output) > 10 else output
                result_text += "```\n" + "\n".join(last_lines) + "\n```"
            await update.message.reply_text(result_text, parse_mode="Markdown")
        else:
            error_text = f"❌ Ошибка при запуске парсеров (код {process.returncode})\n"
            if stderr:
                error_text += f"```\n{stderr.decode()}\n```"
            await update.message.reply_text(error_text, parse_mode="Markdown")
            
    except Exception as e:
        await update.message.reply_text(f"💥 Ошибка: {e}")

        
# ---------------------- Хендлеры сообщений ----------------------


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    welcome_text = f"""
🎉 Привет, {user.first_name}!

Я бот-афиша Минска. Помогу найти интересные события в городе.

🔍 **Как искать:**
• Просто отправьте **название** события (например: "концерт", "выставка", "Дельфин")
• Или отправьте **дату** в формате ДД.ММ или ДД.ММ.ГГГГ (например: 25.02 или 25.02.2026)

Используйте кнопки для быстрого поиска 👇
"""

    reply_markup = get_reply_main_menu()

    await update.message.reply_text(
        welcome_text,
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )


async def search_by_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.strip()

    if len(query) < 3:
        await update.message.reply_text(
            "🔍 **Поиск по названию**\n\nВведите минимум 3 символа для поиска.",
            parse_mode="Markdown",
        )
        return

    await update.message.chat.send_action(action="typing")
    events = search_events_by_title(query)

    if events:
        title = f"🔍 **Результаты поиска по запросу '{query}':**"
        set_pagination(context, events, title, date_info=None)
        await show_page(update, context)
    else:
        await update.message.reply_text(
            f"🔍 **Поиск по запросу '{query}'**\n\n😕 Ничего не найдено.",
            parse_mode="Markdown",
        )


async def search_by_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    date_text = update.message.text.strip()

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
        date_info = None  # чтобы не дублировать дату
        set_pagination(context, result, title, date_info=date_info)
        await show_page(update, context)
    else:
        await update.message.reply_text(
            "❌ Произошла ошибка при поиске. Попробуйте позже.",
            parse_mode="Markdown",
        )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    # обработка reply-клавиатуры
    if text == "📅 Сегодня":
        today = datetime.now()
        events = get_events_by_date_and_category(today)
        title = f"📅 **События на {today.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(update, context)
        return

    if text == "📆 Завтра":
        tomorrow = datetime.now() + timedelta(days=1)
        events = get_events_by_date_and_category(tomorrow)
        title = f"📆 **События на {tomorrow.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(update, context)
        return

    if text == "🎉 Выходные":
        events, saturday, sunday = get_weekend_events()
        title = (
            f"🎉 **Выходные "
            f"({saturday.strftime('%d.%m')}-{sunday.strftime('%d.%m')}):**"
        )
        set_pagination(context, events, title, date_info=None)
        await show_page(update, context)
        return

    if text == "⏰ Ближайшие":
        events = get_upcoming_events(limit=100)
        if events:
            title = "⏰ **Ближайшие события:**"
            set_pagination(context, events, title, date_info=None)
            await show_page(update, context)
        else:
            await update.message.reply_text(
                "😕 Ближайших событий не найдено.",
                parse_mode="Markdown",
            )
        return

    if text == "📋 Все события":
        events = get_upcoming_events(limit=300)
        if events:
            title = "📋 **Все события:**"
            set_pagination(context, events, title, date_info=None)
            await show_page(update, context)
        else:
            await update.message.reply_text(
                "😕 Событий не найдено.",
                parse_mode="Markdown",
            )
        return

    if text == "🎯 Категории":
        # просто покажем inline-меню категорий
        await update.message.reply_text(
            "🎯 **Выберите категорию:**",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("🎬 Кино", callback_data="cat_cinema"),
                        InlineKeyboardButton(
                            "🎵 Концерты", callback_data="cat_concert"
                        ),
                    ],
                    [
                        InlineKeyboardButton("🎭 Театр", callback_data="cat_theater"),
                        InlineKeyboardButton(
                            "🖼️ Выставки", callback_data="cat_exhibition"
                        ),
                    ],
                    [
                        InlineKeyboardButton("🧸 Детям", callback_data="cat_kids"),
                        InlineKeyboardButton("⚽ Спорт", callback_data="cat_sport"),
                    ],
                    [
                        InlineKeyboardButton("🆓 Бесплатно", callback_data="cat_free"),
                        InlineKeyboardButton(
                            "◀️ Назад в главное меню",
                            callback_data="back_to_main",
                        ),
                    ],
                ]
            ),
            parse_mode="Markdown",
        )
        return

    # если это дата
    if re.match(r"^\d{1,2}\.\d{1,2}(\.\d{2,4})?$", text):
        await search_by_date(update, context)
    else:
        await search_by_title(update, context)


# ---------------------- Хендлер кнопок ----------------------


async def handle_filter_buttons(
    query, context: ContextTypes.DEFAULT_TYPE, category: str
):
    data = context.user_data.get("pagination")
    if not data:
        await query.answer("Результаты поиска устарели. Попробуйте снова.")
        return

    all_events = data["events"]

    if category == "all":
        filtered_events = all_events
    else:
        filtered_events = filter_events_by_category(all_events, category)

    title = data["title"]
    date_info = data["date_info"]

    set_pagination(context, filtered_events, title, date_info=date_info)
    await show_page(query, context)


async def handle_date_category_buttons(
    query, context: ContextTypes.DEFAULT_TYPE, date_type: str, category: str
):
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
            await query.edit_message_text(
                f"😕 Ближайших событий в категории {display_name} не найдено.",
                parse_mode="Markdown",
            )

    elif date_type == "weekend":
        events, saturday, sunday = get_weekend_events(category=category)
        title = (
            f"🎉 **{display_name} на выходные "
            f"({saturday.strftime('%d.%m')}-{sunday.strftime('%d.%m')}):**"
        )
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)
        await send_subscription_prompt(query, category, "weekend")


async def handle_simple_buttons(
    query, context: ContextTypes.DEFAULT_TYPE, data: str
):
    chat_id = query.message.chat_id

    if data == "today":
        today = datetime.now()
        events = get_events_by_date_and_category(today)
        title = f"📅 **События на {today.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)

    elif data == "tomorrow":
        tomorrow = datetime.now() + timedelta(days=1)
        events = get_events_by_date_and_category(tomorrow)
        title = f"📆 **События на {tomorrow.strftime('%d.%m.%Y')}:**"
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)

    elif data == "weekend":
        events, saturday, sunday = get_weekend_events()
        title = (
            f"🎉 **Выходные "
            f"({saturday.strftime('%d.%m')}-{sunday.strftime('%d.%m')}):**"
        )
        set_pagination(context, events, title, date_info=None)
        await show_page(query, context)

    elif data == "soon":
        events = get_upcoming_events(limit=100)
        if events:
            title = "⏰ **Ближайшие события:**"
            set_pagination(context, events, title, date_info=None)
            await show_page(query, context)
        else:
            await query.edit_message_text(
                "😕 Ближайших событий не найдено.",
                parse_mode="Markdown",
            )

    elif data == "all":
        events = get_upcoming_events(limit=300)
        if events:
            title = "📋 **Все события:**"
            set_pagination(context, events, title, date_info=None)
            await show_page(query, context)
        else:
            await query.edit_message_text(
                "😕 Событий не найдено.",
                parse_mode="Markdown",
            )

    elif data == "show_categories":
        await show_categories_menu(query, context)

    elif data == "back_to_main":
        await show_main_menu(
            chat_id, context, query.message.reply_text
        )

    elif data.startswith("cat_"):
        category = data.replace("cat_", "")
        await show_date_options(query, category)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if data.startswith("filter_"):
        category = data.replace("filter_", "")
        await handle_filter_buttons(query, context, category)
        return

    if data.startswith("date_"):
        parts = data.split("_")
        date_type = parts[1]
        category = parts[2]
        await handle_date_category_buttons(query, context, date_type, category)
        return

    if data == "page_next":
        if "pagination" in context.user_data:
            context.user_data["pagination"]["page"] += 1
        await show_page(query, context)
        return

    if data.startswith("sub_"):
        _, category, date_type = data.split("_", 2)
        user_id = query.from_user.id
        add_subscription(user_id, category, date_type)
        await query.answer("Подписка оформлена 🔔", show_alert=False)
        return

    await handle_simple_buttons(query, context, data)


# ---------------------- main ----------------------


def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в окружении")

    init_db()

    application = Application.builder().token(TOKEN).build()

    # Добавляем хендлеры
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("subs", show_subscriptions))
    application.add_handler(CommandHandler("run_parsers", manual_run_parsers))  # Новая команда
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    # Настраиваем планировщик
    setup_scheduler(application)

    logger.info("🚀 Бот запущен с ежедневным запуском парсеров в 6:00")
    application.run_polling()


if __name__ == "__main__":
    main()

