#!/usr/bin/env python3
# run_all_parsers.py
# Последовательный запуск всех парсеров

import os
import sys
import subprocess
import logging
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "/data/events_final.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("parser_cron.log"),
    ],
)
logger = logging.getLogger(__name__)

# Все Relax-парсеры через единый relax_parser.py, Ticketpro отдельно
PARSERS = [
    ("relax_parser.py kino",        "🎬 Кино (Relax)"),
    ("relax_parser.py theatre",     "🎭 Театр (Relax)"),
    ("relax_parser.py concert",     "🎵 Концерты (Relax)"),
    ("relax_parser.py exhibition",  "🖼️ Выставки (Relax)"),
    ("relax_parser.py kids",        "🧸 Детям (Relax)"),
    ("ticketpro_parser.py",         "🎫 Ticketpro"),
]


def run_parser(cmd: str, parser_name: str) -> bool:
    try:
        logger.info(f"▶️ Запуск {parser_name} ({cmd})...")
        result = subprocess.run(
            [sys.executable] + cmd.split(),
            capture_output=True, text=True, timeout=600,
        )
        if result.returncode == 0:
            logger.info(f"✅ {parser_name} завершён успешно")
            if result.stdout:
                for line in result.stdout.strip().split("\n"):
                    if any(w in line for w in ["✅", "❌", "📊", "🧹", "⚠️", "Добавлено", "Найдено"]):
                        logger.info(f"   {line.strip()}")
            return True
        else:
            logger.error(f"❌ {parser_name} завершился с ошибкой (код {result.returncode})")
            if result.stderr:
                for line in result.stderr.strip().split("\n")[-5:]:
                    if line.strip():
                        logger.error(f"   {line}")
            return False
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {parser_name} превысил время ожидания (10 мин)")
        return False
    except Exception as e:
        logger.error(f"💥 Ошибка при запуске {parser_name}: {e}")
        return False


def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ВСЕХ ПАРСЕРОВ")
    logger.info(f"Старт: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    success = failed = 0
    for cmd, name in PARSERS:
        logger.info("-" * 40)
        if run_parser(cmd, name):
            success += 1
        else:
            failed += 1
        logger.info("-" * 40)

    duration = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("📊 ИТОГИ")
    logger.info(f"✅ Успешно:  {success}")
    logger.info(f"❌ Ошибки:   {failed}")
    logger.info(f"📦 Всего:    {success + failed}")
    logger.info(f"⏱️  Время:    {duration:.1f} сек")
    logger.info("=" * 60)
    logger.info(f"PARSER_STATS:{success}:{failed}:{success + failed}")
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())

