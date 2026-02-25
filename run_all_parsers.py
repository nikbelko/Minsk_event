#!/usr/bin/env python3
# run_all_parsers.py
# Скрипт для последовательного запуска всех парсеров

import subprocess
import logging
import sys
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('parser_cron.log')
    ]
)

logger = logging.getLogger(__name__)

# Список парсеров в порядке запуска
PARSERS = [
    "relax_kino_live.py",      # 🎬 Кино
    "relax_theatre_parser.py",  # 🎭 Театр
    "relax_concert_parser.py",  # 🎵 Концерты
    "relax_exhibition_parser.py", # 🖼️ Выставки
    "relax_kids_parser.py"      # 🧸 Детям
]

def run_parser(parser_name):
    """Запускает один парсер и возвращает результат"""
    try:
        logger.info(f"▶️ Запуск {parser_name}...")
        
        # Запускаем парсер и захватываем вывод
        result = subprocess.run(
            [sys.executable, parser_name],
            capture_output=True,
            text=True,
            timeout=300  # 5 минут на каждый парсер
        )
        
        # Проверяем результат
        if result.returncode == 0:
            logger.info(f"✅ {parser_name} завершен успешно")
            if result.stdout:
                logger.debug(f"Вывод: {result.stdout[:200]}...")
            return True
        else:
            logger.error(f"❌ {parser_name} завершился с ошибкой (код {result.returncode})")
            if result.stderr:
                logger.error(f"Ошибка: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {parser_name} превысил время ожидания (5 минут)")
        return False
    except Exception as e:
        logger.error(f"💥 Ошибка при запуске {parser_name}: {e}")
        return False

def main():
    """Главная функция"""
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ВСЕХ ПАРСЕРОВ")
    logger.info(f"Время старта: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    results = {
        'success': 0,
        'failed': 0,
        'total': len(PARSERS)
    }
    
    for parser in PARSERS:
        logger.info("-" * 40)
        if run_parser(parser):
            results['success'] += 1
        else:
            results['failed'] += 1
        logger.info("-" * 40)
    
    # Итоги
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("=" * 60)
    logger.info("📊 ИТОГИ ЗАПУСКА")
    logger.info(f"✅ Успешно: {results['success']}")
    logger.info(f"❌ С ошибками: {results['failed']}")
    logger.info(f"📦 Всего парсеров: {results['total']}")
    logger.info(f"⏱️  Время выполнения: {duration:.1f} сек")
    logger.info("=" * 60)
    
    # Возвращаем код ошибки, если были проблемы
    return 1 if results['failed'] > 0 else 0

if __name__ == "__main__":
    sys.exit(main())
