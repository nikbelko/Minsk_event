#!/usr/bin/env python3
import subprocess
import logging
import sys
import requests
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('parser_cron.log')
    ]
)

logger = logging.getLogger(__name__)

PARSERS = [
    ("relax_kino_live.py", "🎬 Кино"),
    ("relax_theatre_parser.py", "🎭 Театр"),
    ("relax_concert_parser.py", "🎵 Концерты"),
    ("relax_exhibition_parser.py", "🖼️ Выставки"),
    ("relax_kids_parser.py", "🧸 Детям")
]

def check_site_availability():
    urls = [
        "https://afisha.relax.by",
        "https://afisha.relax.by/kino/minsk/",
        "https://afisha.relax.by/theatre/minsk/"
    ]
    
    logger.info("🌐 Проверка доступности сайта relax.by...")
    for url in urls:
        try:
            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            if response.status_code == 200:
                logger.info(f"✅ {url} - доступен ({len(response.text)} символов)")
            else:
                logger.error(f"❌ {url} - код ответа {response.status_code}")
        except Exception as e:
            logger.error(f"❌ {url} - ошибка: {e}")

def run_parser(parser_file, parser_name):
    try:
        logger.info(f"▶️ Запуск {parser_name} ({parser_file})...")
        result = subprocess.run(
            [sys.executable, parser_file],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {parser_name} завершен успешно")
            # Показываем последние 5 строк вывода парсера
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                last_lines = lines[-5:] if len(lines) > 5 else lines
                for line in last_lines:
                    if line.strip():
                        logger.info(f"   {line}")
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
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ВСЕХ ПАРСЕРОВ")
    logger.info(f"Время старта: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    check_site_availability()
    
    results = {'success': 0, 'failed': 0}
    
    for parser_file, parser_name in PARSERS:
        logger.info("-" * 40)
        if run_parser(parser_file, parser_name):
            results['success'] += 1
        else:
            results['failed'] += 1
    
    logger.info("=" * 60)
    logger.info("📊 ИТОГИ ЗАПУСКА")
    logger.info(f"✅ Успешно: {results['success']}")
    logger.info(f"❌ С ошибками: {results['failed']}")
    logger.info(f"⏱️  Время выполнения: {(datetime.now() - start_time).total_seconds():.1f} сек")
    logger.info("=" * 60)
    
    return 1 if results['failed'] > 0 else 0

if __name__ == "__main__":
    sys.exit(main())
