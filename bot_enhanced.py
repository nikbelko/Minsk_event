import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sqlite3
import re
import asyncio
from telegram import Bot
from collections import defaultdict

load_dotenv()

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
DB_NAME = 'events_final.db'

def get_db_connection():
    """Создает подключение к базе"""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def search_events_by_title(query, limit=20):
    """Поиск событий по названию"""
    conn = get_db_connection()
    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    
    cursor.execute("""
        SELECT title, details, description, event_date, place, location, price, category, source_url, show_time 
        FROM events 
        WHERE title LIKE ? AND event_date >= ?
        ORDER BY event_date, show_time, title 
        LIMIT ?
    """, (f'%{query}%', today, limit * 3))
    
    events = cursor.fetchall()
    conn.close()
    return events

def search_events_by_date(date_str):
    """Поиск событий по дате"""
    conn = get_db_connection()
    cursor = conn.cursor()
    current_year = datetime.now().year
    
    date_str = date_str.strip()
    
    try:
        # Формат ДД.ММ.ГГГГ
        if re.match(r'^\d{1,2}\.\d{1,2}\.\d{4}$', date_str):
            day, month, year = date_str.split('.')
            day = day.zfill(2)
            month = month.zfill(2)
            search_date = f"{year}-{month}-{day}"
            formatted_date = f"{day}.{month}.{year}"
        # Формат ДД.ММ
        elif re.match(r'^\d{1,2}\.\d{1,2}$', date_str):
            day, month = date_str.split('.')
            day = day.zfill(2)
            month = month.zfill(2)
            search_date = f"{current_year}-{month}-{day}"
            formatted_date = f"{day}.{month}.{current_year}"
        else:
            conn.close()
            return None, None, "неверный_формат"
        
        cursor.execute("""
            SELECT title, details, description, event_date, place, location, price, category, source_url, show_time 
            FROM events 
            WHERE event_date = ?
            ORDER BY show_time, title 
            LIMIT 100
        """, (search_date,))
        
        events = cursor.fetchall()
        conn.close()
        
        if events:
            return events, formatted_date, "найдены"
        else:
            return [], formatted_date, "нет_событий"
            
    except Exception as e:
        conn.close()
        return None, None, "ошибка"

def group_cinema_events(events):
    """Группирует сеансы кино по фильмам и датам"""
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    for event in events:
        if event['category'] == 'cinema':
            key = (event['title'], event['event_date'], event['place'])
            grouped[key[0]][key[1]][key[2]].append({
                'time': event['show_time'],
                'details': event['details']
            })
    
    return grouped

def format_grouped_cinema_events(grouped, limit=10):
    """Форматирует сгруппированные события кино для вывода"""
    result = []
    count = 0
    
    for title, dates in grouped.items():
        if count >= limit:
            break
        
        for date, cinemas in dates.items():
            if count >= limit:
                break
            
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%d.%m.%Y')
            
            first_cinema = next(iter(cinemas.values()))
            details = first_cinema[0]['details'] if first_cinema else ''
            
            text = f"🎬 **{title}**"
            if details:
                text += f"\n🎭 {details}"
            text += f"\n📅 {formatted_date}"
            
            for place, seances in cinemas.items():
                times = [s['time'] for s in seances]
                times_str = ', '.join(times)
                text += f"\n   ⏰ {times_str} — {place}"
            
            result.append(text)
            count += 1
    
    return result

def get_events_by_date_and_category(target_date, category=None):
    """Получает события на конкретную дату с опциональной категорией"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    date_str = target_date.strftime('%Y-%m-%d')
    
    if category and category != 'all':
        cursor.execute("""
            SELECT title, details, description, event_date, place, location, price, category, source_url, show_time 
            FROM events 
            WHERE event_date = ? AND category = ?
            ORDER BY show_time, title
        """, (date_str, category))
    else:
        cursor.execute("""
            SELECT title, details, description, event_date, place, location, price, category, source_url, show_time 
            FROM events 
            WHERE event_date = ? 
            ORDER BY show_time, title
        """, (date_str,))
    
    events = cursor.fetchall()
    conn.close()
    return events

def get_upcoming_events(limit=20, category=None):
    """Получает ближайшие события с опциональной категорией"""
    conn = get_db_connection()
    cursor = conn.cursor()
    today = datetime.now().strftime('%Y-%m-%d')
    
    if category and category != 'all':
        cursor.execute("""
            SELECT title, details, description, event_date, place, location, price, category, source_url, show_time 
            FROM events 
            WHERE event_date >= ? AND category = ?
            ORDER BY event_date, show_time, title 
            LIMIT ?
        """, (today, category, limit * 3))
    else:
        cursor.execute("""
            SELECT title, details, description, event_date, place, location, price, category, source_url, show_time 
            FROM events 
            WHERE event_date >= ? 
            ORDER BY event_date, show_time, title 
            LIMIT ?
        """, (today, limit * 3))
    
    events = cursor.fetchall()
    conn.close()
    return events

def get_weekend_events(category=None):
    """Получает события на ближайшие выходные с опциональной категорией"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    today = datetime.now()
    
    days_until_saturday = (5 - today.weekday()) % 7
    if days_until_saturday == 0:
        days_until_saturday = 7
    
    saturday = today + timedelta(days=days_until_saturday)
    sunday = saturday + timedelta(days=1)
    
    saturday_str = saturday.strftime('%Y-%m-%d')
    sunday_str = sunday.strftime('%Y-%m-%d')
    
    if category and category != 'all':
        cursor.execute("""
            SELECT title, details, description, event_date, place, location, price, category, source_url, show_time 
            FROM events 
            WHERE event_date IN (?, ?) AND category = ?
            ORDER BY event_date, show_time, title
        """, (saturday_str, sunday_str, category))
    else:
        cursor.execute("""
            SELECT title, details, description, event_date, place, location, price, category, source_url, show_time 
            FROM events 
            WHERE event_date IN (?, ?)
            ORDER BY event_date, show_time, title
        """, (saturday_str, sunday_str))
    
    events = cursor.fetchall()
    conn.close()
    return events, saturday, sunday

def format_event_text(event):
    """Форматирует событие для вывода (для не-кино)"""
    text = f"🎉 **{event['title']}**"
    
    if event['details']:
        text += f"\n📝 {event['details']}"
    
    if event['event_date']:
        date_obj = datetime.strptime(event['event_date'], '%Y-%m-%d')
        formatted_date = date_obj.strftime('%d.%m.%Y')
        text += f"\n📅 {formatted_date}"
    
    if event['show_time']:
        text += f" ⏰ {event['show_time']}"
    
    if event['place'] and event['place'] != 'Кинотеатр':
        text += f"\n🏢 {event['place']}"
    
    if event['price']:
        text += f"\n💰 {event['price']}"
    
    if event['category']:
        category_emoji = {
            'cinema': '🎬',
            'concert': '🎵',
            'theater': '🎭',
            'exhibition': '🖼️',
            'kids': '🧸',
            'sport': '⚽',
            'free': '🆓'
        }
        emoji = category_emoji.get(event['category'], '📌')
        text += f"\n{emoji} {event['category'].capitalize()}"
    
    return text

async def show_events_and_menu(update_or_query, events, title, limit=10):
    """Показывает события и затем возвращает главное меню"""
    
    if isinstance(update_or_query, Update):
        message = update_or_query.message
        await message.chat.send_action(action="typing")
        send_method = message.reply_text
        chat_id = message.chat_id
    else:
        query = update_or_query
        await query.answer()
        send_method = query.message.reply_text
        chat_id = query.message.chat_id
    
    if not events:
        await send_method(
            f"{title}\n\n😕 Событий не найдено.",
            parse_mode='Markdown'
        )
    else:
        # Разделяем кино и другие события
        cinema_events = [e for e in events if e['category'] == 'cinema']
        other_events = [e for e in events if e['category'] != 'cinema']
        
        await send_method(
            f"{title}\n\n📊 Найдено: {len(events)}",
            parse_mode='Markdown'
        )
        
        # Показываем сгруппированное кино
        if cinema_events:
            grouped = group_cinema_events(cinema_events)
            formatted = format_grouped_cinema_events(grouped, limit)
            
            for text in formatted:
                await send_method(
                    f"{text}\n\n🔗 [Подробнее](https://afisha.relax.by/kino/minsk/)",
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
        
        # Показываем остальные события без группировки
        for event in other_events[:limit]:
            text = format_event_text(event)
            url = event['source_url']
            
            await send_method(
                f"{text}\n\n🔗 [Подробнее]({url})",
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
    
    # После выдачи результатов показываем главное меню
    await show_main_menu(chat_id, context=None, send_method=send_method)

async def show_main_menu(chat_id, context=None, send_method=None):
    """Показывает главное меню"""
    keyboard = [
        [InlineKeyboardButton("📅 Сегодня", callback_data="today"),
         InlineKeyboardButton("📆 Завтра", callback_data="tomorrow")],
        [InlineKeyboardButton("⏰ Ближайшие", callback_data="soon"),
         InlineKeyboardButton("🎉 Выходные", callback_data="weekend")],
        [InlineKeyboardButton("📋 Все события", callback_data="all"),
         InlineKeyboardButton("🎯 Категории", callback_data="show_categories")]
    ]
    
    menu_text = "🎉 **Главное меню**\n\nВыберите действие:"
    
    if send_method:
        await send_method(
            menu_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        # Если send_method не передан, используем контекст бота
        await context.bot.send_message(
            chat_id=chat_id,
            text=menu_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def search_by_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик поиска по названию"""
    query = update.message.text.strip()
    
    if len(query) < 3:
        await update.message.reply_text(
            "🔍 **Поиск по названию**\n\nВведите минимум 3 символа для поиска.",
            parse_mode='Markdown'
        )
        # Все равно показываем меню
        await show_main_menu(update.message.chat_id, context)
        return
    
    await update.message.chat.send_action(action="typing")
    events = search_events_by_title(query)
    
    if events:
        await show_events_and_menu(update, events, f"🔍 **Результаты поиска по запросу '{query}':**", limit=10)
    else:
        await update.message.reply_text(
            f"🔍 **Поиск по запросу '{query}'**\n\n😕 Ничего не найдено.",
            parse_mode='Markdown'
        )
        await show_main_menu(update.message.chat_id, context)

async def search_by_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик поиска по дате"""
    date_text = update.message.text.strip()
    
    result, formatted_date, status = search_events_by_date(date_text)
    
    if status == "неверный_формат":
        await update.message.reply_text(
            f"📅 **Поиск по дате**\n\nНе удалось распознать дату '{date_text}'.\n\n"
            "Введите дату в формате:\n• ДД.ММ.ГГГГ (например, 25.02.2026)\n• ДД.ММ (например, 25.02)",
            parse_mode='Markdown'
        )
        await show_main_menu(update.message.chat_id, context)
    elif status == "нет_событий":
        await update.message.reply_text(
            f"📅 **Событий на {formatted_date} не найдено.**\n\n"
            "Попробуйте другую дату или воспользуйтесь поиском по названию.",
            parse_mode='Markdown'
        )
        await show_main_menu(update.message.chat_id, context)
    elif status == "найдены":
        await show_events_and_menu(update, result, f"📅 **События на {formatted_date}:**")
    else:
        await update.message.reply_text(
            "❌ Произошла ошибка при поиске. Попробуйте позже.",
            parse_mode='Markdown'
        )
        await show_main_menu(update.message.chat_id, context)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений - определяет, что ищет пользователь"""
    text = update.message.text.strip()
    
    # Проверяем, похоже ли на дату
    if re.match(r'^\d{1,2}\.\d{1,2}(\.\d{2,4})?$', text):
        await search_by_date(update, context)
    else:
        await search_by_title(update, context)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню"""
    user = update.effective_user
    
    welcome_text = f"""
🎉 Привет, {user.first_name}!

Я бот-афиша Минска. Помогу найти интересные события в городе.

🔍 **Как искать:**
• Просто отправьте **название** события (например: "концерт", "выставка", "Дельфин")
• Или отправьте **дату** в формате ДД.ММ или ДД.ММ.ГГГГ (например: 25.02 или 25.02.2026)

Используйте кнопки для быстрого поиска 👇
    """
    
    keyboard = [
        [InlineKeyboardButton("📅 Сегодня", callback_data="today"),
         InlineKeyboardButton("📆 Завтра", callback_data="tomorrow")],
        [InlineKeyboardButton("⏰ Ближайшие", callback_data="soon"),
         InlineKeyboardButton("🎉 Выходные", callback_data="weekend")],
        [InlineKeyboardButton("📋 Все события", callback_data="all"),
         InlineKeyboardButton("🎯 Категории", callback_data="show_categories")]
    ]
    
    await update.message.reply_text(
        welcome_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def show_categories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню категорий"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("🎬 Кино", callback_data="cat_cinema"),
         InlineKeyboardButton("🎵 Концерты", callback_data="cat_concert")],
        [InlineKeyboardButton("🎭 Театр", callback_data="cat_theater"),
         InlineKeyboardButton("🖼️ Выставки", callback_data="cat_exhibition")],
        [InlineKeyboardButton("🧸 Детям", callback_data="cat_kids"),
         InlineKeyboardButton("⚽ Спорт", callback_data="cat_sport")],
        [InlineKeyboardButton("🆓 Бесплатно", callback_data="cat_free"),
         InlineKeyboardButton("◀️ Назад в главное меню", callback_data="back_to_main")]
    ]
    
    await query.edit_message_text(
        "🎯 **Выберите категорию:**",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def show_date_options(update_or_query, category_name):
    """Показывает меню выбора даты после выбора категории"""
    keyboard = [
        [InlineKeyboardButton("📅 Сегодня", callback_data=f"date_today_{category_name}"),
         InlineKeyboardButton("📆 Завтра", callback_data=f"date_tomorrow_{category_name}")],
        [InlineKeyboardButton("⏰ Ближайшие", callback_data=f"date_upcoming_{category_name}"),
         InlineKeyboardButton("🎉 Выходные", callback_data=f"date_weekend_{category_name}")],
        [InlineKeyboardButton("◀️ Назад к категориям", callback_data="show_categories")]
    ]
    
    category_names = {
        'cinema': '🎬 Кино',
        'concert': '🎵 Концерты',
        'theater': '🎭 Театр',
        'exhibition': '🖼️ Выставки',
        'kids': '🧸 Детям',
        'sport': '⚽ Спорт',
        'free': '🆓 Бесплатно'
    }
    
    display_name = category_names.get(category_name, category_name)
    
    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_text(
            f"📌 **{display_name}**\n\nВыберите дату для поиска:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await update_or_query.edit_message_text(
            f"📌 **{display_name}**\n\nВыберите дату для поиска:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )

async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в главное меню"""
    query = update.callback_query
    await query.answer()
    
    await show_main_menu(query.message.chat_id, context, query.message.reply_text)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на кнопки"""
    query = update.callback_query
    data = query.data
    
    # Обработка комбинированных кнопок (категория + дата)
    if data.startswith('date_'):
        parts = data.split('_')
        date_type = parts[1]  # today, tomorrow, upcoming, weekend
        category = parts[2]    # cinema, concert, etc.
        
        if date_type == 'today':
            today = datetime.now()
            events = get_events_by_date_and_category(today, category)
            await show_events_and_menu(query, events, f"📅 **{category_names.get(category, category)} на {today.strftime('%d.%m.%Y')}:**")
        
        elif date_type == 'tomorrow':
            tomorrow = datetime.now() + timedelta(days=1)
            events = get_events_by_date_and_category(tomorrow, category)
            await show_events_and_menu(query, events, f"📆 **{category_names.get(category, category)} на {tomorrow.strftime('%d.%m.%Y')}:**")
        
        elif date_type == 'upcoming':
            events = get_upcoming_events(limit=20, category=category)
            if events:
                today = datetime.now().strftime('%Y-%m-%d')
                tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
                today_count = sum(1 for e in events if e['event_date'] == today)
                tomorrow_count = sum(1 for e in events if e['event_date'] == tomorrow)
                title = f"⏰ **Ближайшие {category_names.get(category, category)}:**\n\n📅 Сегодня: {today_count}\n📆 Завтра: {tomorrow_count}\n📊 Всего: {len(events)}"
            else:
                title = f"⏰ **Ближайшие {category_names.get(category, category)}:**"
            await show_events_and_menu(query, events, title)
        
        elif date_type == 'weekend':
            events, saturday, sunday = get_weekend_events(category=category)
            title = f"🎉 **{category_names.get(category, category)} на выходные ({saturday.strftime('%d.%m')}-{sunday.strftime('%d.%m')}):**"
            await show_events_and_menu(query, events, title)
        
        return
    
    # Обычные кнопки
    if data == "today":
        today = datetime.now()
        events = get_events_by_date_and_category(today)
        await show_events_and_menu(query, events, f"📅 **События на {today.strftime('%d.%m.%Y')}:**")
    
    elif data == "tomorrow":
        tomorrow = datetime.now() + timedelta(days=1)
        events = get_events_by_date_and_category(tomorrow)
        await show_events_and_menu(query, events, f"📆 **События на {tomorrow.strftime('%d.%m.%Y')}:**")
    
    elif data == "weekend":
        events, saturday, sunday = get_weekend_events()
        title = f"🎉 **Выходные ({saturday.strftime('%d.%m')}-{sunday.strftime('%d.%m')}):**"
        await show_events_and_menu(query, events, title)
    
    elif data == "soon":
        events = get_upcoming_events(limit=20)
        if events:
            today = datetime.now().strftime('%Y-%m-%d')
            tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
            today_count = sum(1 for e in events if e['event_date'] == today)
            tomorrow_count = sum(1 for e in events if e['event_date'] == tomorrow)
            title = f"⏰ **Ближайшие события:**\n\n📅 Сегодня: {today_count}\n📆 Завтра: {tomorrow_count}\n📊 Всего: {len(events)}"
        else:
            title = "⏰ **Ближайшие события:**"
        await show_events_and_menu(query, events, title)
    
    elif data == "all":
        events = get_upcoming_events(limit=20)
        await show_events_and_menu(query, events, "📋 **Все события:**")
    
    elif data == "show_categories":
        await show_categories(update, context)
    
    elif data == "back_to_main":
        await back_to_main(update, context)
    
    elif data.startswith("cat_"):
        category = data.replace("cat_", "")
        await show_date_options(query, category)

# Словарь названий категорий
category_names = {
    'cinema': '🎬 Кино',
    'concert': '🎵 Концерты',
    'theater': '🎭 Театр',
    'exhibition': '🖼️ Выставки',
    'kids': '🧸 Детям',
    'sport': '⚽ Спорт',
    'free': '🆓 Бесплатно'
}

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Помощь"""
    help_text = """
📚 **Как пользоваться ботом**

🔍 **Поиск:**
• Просто отправьте **название** события (например: "концерт", "Дельфин", "выставка")
• Или отправьте **дату** в формате ДД.ММ или ДД.ММ.ГГГГ (например: 25.02 или 25.02.2026)

🎯 **Кнопки:**
📅 Сегодня - события на сегодня
📆 Завтра - события на завтра
⏰ Ближайшие - все ближайшие события
🎉 Выходные - события на субботу и воскресенье
📋 Все события - все события в базе
🎯 Категории - выбрать по категории

**Новая функция:** Выберите категорию, а затем дату!
Например: Категории → Кино → Сегодня

📍 Данные собираются с relax.by
🔄 Обновляются автоматически
    """
    
    await update.message.reply_text(help_text, parse_mode='Markdown')

def main():
    bot = Bot(TOKEN)
    asyncio.run(bot.delete_webhook(drop_pending_updates=True))
    logger.info("✅ Вебхуки очищены")
    
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    logger.info("🚀 Бот запущен с автоматическим возвратом в меню после каждого поиска")
    app.run_polling()

if __name__ == '__main__':
    main()
