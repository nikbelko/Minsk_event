#!/usr/bin/env python3
# start.py — бот (webhook) + API в одном asyncio процессе

import asyncio
import logging
import os

import uvicorn
from telegram.ext import Application

logger = logging.getLogger(__name__)

# ── Конфиг ───────────────────────────────────────────────────────────────────

TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")          # https://your-service.railway.app
PORT        = int(os.getenv("PORT", 8000))
API_PORT    = int(os.getenv("API_PORT", PORT))       # Railway даёт один PORT

# Путь для вебхука — случайный суффикс для безопасности
WEBHOOK_PATH = f"/webhook/{TOKEN}"


async def run_bot_webhook(application: Application):
    """Запускает бота через webhook внутри существующего event loop."""
    await application.initialize()
    await application.start()

    if WEBHOOK_URL:
        webhook_full_url = WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
        await application.bot.set_webhook(
            url=webhook_full_url,
            allowed_updates=["message", "callback_query", "pre_checkout_query"],
            drop_pending_updates=True,
        )
        logger.info(f"✅ Webhook установлен: {webhook_full_url}")
    else:
        logger.warning("⚠️ WEBHOOK_URL не задан — бот без webhook (только API)")

    return application


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Импортируем после настройки логирования
    from bot_enhanced import build_application, setup_scheduler
    from api import app as fastapi_app

    # Строим приложение бота
    application = build_application()
    setup_scheduler(application)

    # Монтируем webhook-роут в FastAPI
    if WEBHOOK_URL:
        from telegram import Update
        from fastapi import Request
        from fastapi.responses import Response

        @fastapi_app.post(WEBHOOK_PATH)
        async def telegram_webhook(request: Request):
            data = await request.json()
            update = Update.de_json(data, application.bot)
            await application.process_update(update)
            return Response(status_code=200)

        logger.info(f"✅ Webhook роут добавлен: POST {WEBHOOK_PATH}")

    # Запускаем бота
    await run_bot_webhook(application)

    # Запускаем uvicorn (API + webhook в одном порту)
    config = uvicorn.Config(
        app=fastapi_app,
        host="0.0.0.0",
        port=API_PORT,
        log_level="warning",    # меньше шума в логах
        access_log=False,       # экономим I/O
    )
    server = uvicorn.Server(config)

    logger.info(f"🚀 Запуск на порту {API_PORT}")

    try:
        await server.serve()
    finally:
        await application.stop()
        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
