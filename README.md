# MinskDvizh Bot

Telegram-бот и backend для афиши событий в Минске.

Проект собирает события из нескольких источников, нормализует их, сохраняет в SQLite и отдаёт данные в двух каналах:

- Telegram-бот для пользователей и администратора
- FastAPI API для web-приложения и внешних интеграций

## Что умеет проект

- парсить события из `relax.by`, `Ticketpro`, `Bycard`, `BezKassira` и дополнительных источников
- складывать события в единую SQLite-базу
- фильтровать события по датам, категориям и поисковому запросу
- показывать афишу в Telegram-боте
- принимать пользовательские события на модерацию
- поддерживать подписки и flash-подписки
- публиковать события в Telegram-канал
- отдавать данные через HTTP API для фронтенда

## Архитектура

Основные части проекта:

- [`bot_enhanced.py`] — основной Telegram-бот, команды, inline-режим, админка, модерация, подписки, платежи, планировщик
- [`api.py`] — FastAPI backend для событий, календаря, подписок и отправки событий пользователями
- [`start.py`] — запуск webhook-бота и API в одном `asyncio`-процессе
- [`run_all_parsers.py`] — последовательный запуск всех парсеров и постобработка бесплатных событий
- [`normalizer.py`] — нормализация, дедупликация и обработка событий
- парсеры: [`relax_parser.py`], [`ticketpro_parser.py`], [`bycard_parser.py`], [`bezkassira_parser.py`]

Поток данных:

1. Парсеры собирают события из внешних источников.
2. Данные нормализуются и сохраняются в SQLite.
3. Бот читает события из БД и показывает их пользователю.
4. API отдаёт эти же данные фронтенду и web app.

## Стек

- Python 3.12
- `python-telegram-bot`
- FastAPI + Uvicorn
- SQLite
- APScheduler
- BeautifulSoup + lxml
- `httpx`

## Структура данных

Основная рабочая база — SQLite-файл.

В проекте используются таблицы:

- `events` — основная афиша
- `pending_events` — пользовательские события на модерации
- `subscriptions` — подписки пользователей
- `flash_subscriptions` — быстрые подписки на поиск
- `user_stats` — действия пользователей для статистики

## Требования

- Python 3.12+
- `pip`
- локальный SQLite
- для части парсеров может понадобиться браузерный драйвер

## Установка

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Что используется в коде:

- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота
- `DB_PATH` — путь к SQLite-базе
- `WEB_APP_URL` — ссылка на Telegram Web App / сайт
- `FRONTEND_URL` — origin фронтенда
- `WEBHOOK_URL` — публичный URL backend-сервиса
- `PORT`, `API_PORT` — порт для запуска API/webhook
- `CHANNEL_ID` — канал для публикации событий
- `ADMIN_ID` — Telegram ID администратора


## Основные команды бота

В коде зарегистрированы, в том числе:

- `/start`
- `/today`
- `/subs`
- `/about`
- `/app`
- `/support`
- `/donate`

Админские команды:

- `/admin`
- `/pending`
- `/stats`
- `/ustats`
- `/update`
- `/download_db`
- `/post_channel`
- `/template`


