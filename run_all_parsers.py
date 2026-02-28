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
    ("relax_kino_live.py", "🎬 Кино (Relax)"),
    ("relax_theatre_parser.py", "🎭 Театр (Relax)"),
    ("relax_concert_parser.py", "🎵 Концерты (Relax)"),
    ("relax_exhibition_parser.py", "🖼️ Выставки (Relax)"),
    ("relax_kids_parser.py", "🧸 Детям (Relax)"),
    ("ticketpro_parser.py", "🎫 Ticketpro (все категории)"),  # Telegram пока убран
]

def run_parser(parser_file, parser_name):
    """Запускает один парсер и возвращает результат"""
    try:
        logger.info(f"▶️ Запуск {parser_name} ({parser_file})...")
        
        # Запускаем парсер и захватываем вывод
        result = subprocess.run(
            [sys.executable, parser_file],
            capture_output=True,
            text=True,
            timeout=600  # 10 минут на каждый парсер (Ticketpro может быть долгим)
        )
        
        # Проверяем результат
        if result.returncode == 0:
            logger.info(f"✅ {parser_name} завершен успешно")
            # Показываем последние строки вывода
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                # Ищем строки со статистикой
                stats_lines = []
                for line in lines:
                    if any(word in line.lower() for word in 
                          ['статистика', 'найдено', 'сохранено', 'дубликат', 'минск', 
                           '✅', '📊', 'страниц', 'отфильтровано']):
                        stats_lines.append(line)
                
                # Показываем последние 10 строк статистики
                for line in stats_lines[-10:]:
                    if line.strip():
                        logger.info(f"   {line}")
            return True
        else:
            logger.error(f"❌ {parser_name} завершился с ошибкой (код {result.returncode})")
            if result.stderr:
                # Показываем последние строки ошибки
                error_lines = result.stderr.strip().split('\n')[-5:]
                for line in error_lines:
                    if line.strip():
                        logger.error(f"   {line}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {parser_name} превысил время ожидания (10 минут)")
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
    
    # Проверяем наличие Ticketpro парсера
    try:
        import ticketpro_parser
        logger.info("✅ Ticketpro парсер найден")
    except ImportError:
        logger.warning("⚠️ Ticketpro парсер не найден, будет пропущен")
    
    results = {
        'success': 0,
        'failed': 0,
        'total': len(PARSERS)
    }
    
    for parser_file, parser_name in PARSERS:
        logger.info("-" * 40)
        if run_parser(parser_file, parser_name):
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
