#!/usr/bin/env python3
# run_all_parsers.py
# Последовательный запуск всех парсеров с обработкой бесплатных событий

import os
import sys
import subprocess
import logging
import json
import sqlite3
from datetime import datetime

# Импортируем функцию из обновлённого нормализатора
try:
    from normalizer import mark_free_duplicates, apply_kids_pass
    _NORMALIZER_OK = True
except ImportError:
    _NORMALIZER_OK = False
    def mark_free_duplicates(relax_events, free_events):
        return relax_events + free_events
    def apply_kids_pass(kids_events, conn):
        return {"marked": 0, "added": 0}

from config import DB_PATH, MINSK_TZ
from parser_state import (
    init_parser_source_state,
    record_successful_parse,
    record_always_parse_success,
)

try:
    from daytime_update import CHECK_FNS, MIN_SANE_COUNT
    _DAYTIME_AVAILABLE = True
except ImportError:
    _DAYTIME_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("parser_cron.log", encoding='utf-8'),
    ],
)
logger = logging.getLogger(__name__)

if not _DAYTIME_AVAILABLE:
    logger.warning("daytime_update unavailable — parser_source_state will NOT be synced after nightly parse")

# Mapping: parser command → source_key for parser_source_state sync.
# relax_parser.py free is intentionally omitted — free pass is not a baseline source.
CMD_TO_SOURCE_KEY: dict[str, str] = {
    "relax_parser.py theatre":    "relax.by:theatre",
    "relax_parser.py concert":    "relax.by:concert",
    "relax_parser.py exhibition": "relax.by:exhibition",
    "relax_parser.py party":      "relax.by:party",
    "relax_parser.py kino":       "relax.by:kino",
    "bycard_parser.py":           "bycard.by",
    "ticketpro_parser.py":        "ticketpro.by",
    "bezkassira_parser.py":       "bezkassira.by",
}

# These sources have no fingerprint check — only metadata is written to state.
ALWAYS_PARSE_KEYS = {"ticketpro.by", "bezkassira.by"}


def _sync_parser_state(source_key: str, now_iso: str):
    """
    After a successful full parse, update parser_source_state so daytime checks
    have an accurate baseline to compare against (last_successful_hash/count).

    For checkable sources (relax categories, bycard): runs the fingerprint check
    post-parse and writes last_seen_* + last_successful_* if sane.
    For always-parse sources (ticketpro, bezkassira): writes metadata only.
    """
    if not _DAYTIME_AVAILABLE:
        return

    if source_key in ALWAYS_PARSE_KEYS:
        record_always_parse_success(source_key, now_iso)
        logger.info(f"   📍 {source_key}: state updated (fallback_full_parse)")
        return

    check_fn = CHECK_FNS.get(source_key)
    if not check_fn:
        logger.warning(f"   ⚠️ {source_key}: no check function — state not synced")
        return

    try:
        fp = check_fn()
    except Exception as e:
        logger.warning(f"   ⚠️ {source_key}: post-parse check failed ({e}) — baseline NOT updated")
        return

    min_count = MIN_SANE_COUNT.get(source_key, 1)
    written = record_successful_parse(source_key, fp, now_iso, min_sane_count=min_count)
    if written:
        logger.info(
            f"   📍 {source_key}: baseline updated "
            f"(count={fp['count']}, hash={fp.get('hash', '')[:8]}…)"
        )
    else:
        logger.warning(
            f"   ⚠️ {source_key}: post-parse check returned insane result "
            f"(count={fp.get('count')}, status={fp.get('status')}) — baseline NOT updated"
        )


# Категории парсеров с указанием, относятся ли они к бесплатным событиям
PARSERS = [
    # Обычные парсеры
    ("relax_parser.py theatre",     "🎭 Театр (Relax)",      False,  False),
    ("relax_parser.py concert",     "🎵 Концерты (Relax)",   False,  False),
    ("relax_parser.py exhibition",  "🖼️ Выставки (Relax)",   False,  False),
    ("relax_parser.py party",       "🎉 Вечеринки (Relax)",  False,  False),
    ("relax_parser.py kino",        "🎬 Кино (Relax)",       False,  False),
    ("ticketpro_parser.py",         "🎫 Ticketpro",          False,  False),
    ("bezkassira_parser.py",        "🎟 BezKassira",         False,  False),
    ("bycard_parser.py",            "🎭 Bycard",             False,  False),

    # JSON-парсеры (возвращают EVENTS_JSON, не сохраняют напрямую)
    ("relax_parser.py free",        "🆓 Бесплатно (Relax)",  True,   False),
    ("relax_parser.py kids",        "🧸 Детям (Relax)",      False,  True),   # is_kids pass
]


def run_parser(cmd: str, parser_name: str) -> tuple[bool, list[str], list[dict]]:
    """
    Запускает парсер и возвращает:
        success: bool - успешно ли завершился
        result_lines: list[str] - строки RESULT:... для отчёта
        events: list[dict] - спарсенные события (только от free-парсеров)
    """
    events = []
    try:
        logger.info(f"▶️ Запуск {parser_name} ({cmd})...")
        result = subprocess.run(
            [sys.executable] + cmd.split(),
            capture_output=True, text=True, timeout=900,  # Увеличил таймаут до 15 минут
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {parser_name} завершён успешно")
            result_lines = []
            
            if result.stdout:
                for line in result.stdout.strip().split("\n"):
                    stripped = line.strip()
                    
                    # Логируем информационные строки
                    if any(w in stripped for w in ["✅", "❌", "📊", "🧹", "⚠️", "Добавлено", "Найдено"]):
                        logger.info(f"   {stripped}")
                    
                    # Собираем RESULT строки для отчёта
                    if stripped.startswith("RESULT:"):
                        result_lines.append(stripped)
                    
                    # Собираем JSON с бесплатными событиями
                    if stripped.startswith("EVENTS_JSON:"):
                        try:
                            events_json = stripped[len("EVENTS_JSON:"):]
                            events_data = json.loads(events_json)
                            events.extend(events_data)
                            logger.info(f"   📦 Получено {len(events_data)} событий")
                        except Exception as e:
                            logger.error(f"   ❌ Ошибка парсинга JSON событий: {e}")
            
            return True, result_lines, events
        else:
            logger.error(f"❌ {parser_name} завершился с ошибкой (код {result.returncode})")
            if result.stderr:
                for line in result.stderr.strip().split("\n")[-5:]:
                    if line.strip():
                        logger.error(f"   {line}")
            return False, [], []
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {parser_name} превысил время ожидания (15 мин)")
        return False, [], []
    except Exception as e:
        logger.error(f"💥 Ошибка при запуске {parser_name}: {e}")
        return False, [], []


def load_events_from_db() -> list:
    """
    Загружает все события из БД для обработки бесплатных дубликатов.
    """
    events = []
    try:
        if not os.path.exists(DB_PATH):
            logger.warning(f"⚠️ БД {DB_PATH} не найдена")
            return []
        
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Загружаем все события
        cursor.execute("""
            SELECT title, details, description, event_date, show_time,
                   place, location, price, category, source_url, source_name
            FROM events
            WHERE source_name = 'relax.by'
        """)
        
        rows = cursor.fetchall()
        for row in rows:
            events.append({
                "title": row["title"],
                "details": row["details"] or "",
                "description": row["description"] or "",
                "event_date": row["event_date"],
                "show_time": row["show_time"] or "",
                "place": row["place"] or "",
                "location": row["location"] or "Минск",
                "price": row["price"] or "",
                "category": row["category"] or "other",
                "source_url": row["source_url"] or "",
                "source_name": row["source_name"] or "",
            })
        
        conn.close()
        logger.info(f"📚 Загружено {len(events)} событий из БД")
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки из БД: {e}")
    
    return events


def update_events_in_db(events: list) -> int:
    """
    Обновляет цены событий в БД (только для бесплатных).
    """
    if not events:
        return 0
    
    updated = 0
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        for event in events:
            # Обновляем только если цена изменилась на "Бесплатно"
            if event.get("price") == "Бесплатно" and event.get("_from_free_section"):
                cursor.execute("""
                    UPDATE events 
                    SET price = ?
                    WHERE title = ? AND event_date = ? AND place = ?
                """, (
                    "Бесплатно",
                    event["title"],
                    event["event_date"],
                    event["place"]
                ))
                if cursor.rowcount > 0:
                    updated += 1
        
        conn.commit()
        conn.close()
        logger.info(f"🔄 Обновлено цен в БД: {updated}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка обновления БД: {e}")
    
    return updated


def main():
    start_time = datetime.now()
    now_iso = datetime.now(MINSK_TZ).isoformat()
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ВСЕХ ПАРСЕРОВ")
    logger.info(f"Старт: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    init_parser_source_state()

    success = failed = 0
    all_results: list[str] = []
    parser_status: list[tuple] = []  # (name, ok, result_lines)

    # Собираем JSON-события отдельно
    free_events = []
    kids_events = []
    kids_parser_ok = False  # True только если kids-парсер отработал без ошибок

    # Запускаем все парсеры
    for cmd, name, is_free, is_kids in PARSERS:
        logger.info("-" * 40)
        ok, result_lines, events = run_parser(cmd, name)

        if ok:
            success += 1
            if is_free:
                free_events.extend(events)
                logger.info(f"   📦 Бесплатных событий получено: {len(events)}")
            elif is_kids:
                kids_events.extend(events)
                kids_parser_ok = True
                logger.info(f"   📦 Kids событий получено: {len(events)}")
            else:
                source_key = CMD_TO_SOURCE_KEY.get(cmd)
                if source_key:
                    _sync_parser_state(source_key, now_iso)
        else:
            failed += 1

        all_results.extend(result_lines)
        parser_status.append((name, ok, result_lines))
        logger.info("-" * 40)

    # Загружаем все события из БД
    logger.info("=" * 40)
    logger.info("📚 ЗАГРУЗКА СОБЫТИЙ ИЗ БД")
    relax_events = load_events_from_db()
    
    logger.info(f"📊 Загружено из БД: {len(relax_events)}")
    logger.info(f"🆓 Бесплатных событий из JSON: {len(free_events)}")

    # Обрабатываем бесплатные события
    logger.info("=" * 40)
    logger.info("🔄 ОБРАБОТКА БЕСПЛАТНЫХ СОБЫТИЙ")
    
    if free_events:
        final_events = mark_free_duplicates(relax_events, free_events)
        logger.info(f"📦 Событий после обработки: {len(final_events)}")
        free_count = sum(1 for e in final_events if e.get("price") == "Бесплатно")
        logger.info(f"🆓 Из них бесплатных: {free_count}")
        updated = update_events_in_db(final_events)
        logger.info(f"💾 Обновлено в БД: {updated}")
    else:
        logger.info("ℹ️ Нет бесплатных событий для обработки")

    # Kids pass — проставляем is_kids=1 и сохраняем уникальные kids-события
    logger.info("=" * 40)
    logger.info("🧸 ОБРАБОТКА KIDS (is_kids маркер)")
    kids_stats: dict = {"marked": 0, "added": 0}
    if kids_parser_ok:
        # Вызываем apply_kids_pass даже при пустом списке: функция очищает stale is_kids
        # и synthetic relax.by/kids строки, что нужно для корректного full rescan.
        try:
            with sqlite3.connect(DB_PATH) as conn:
                kids_stats = apply_kids_pass(kids_events, conn)
            logger.info(f"🧸 is_kids=1: {kids_stats['marked']} событий; добавлено уникальных: {kids_stats['added']}")
        except Exception as e:
            logger.error(f"❌ Ошибка kids pass: {e}")
    else:
        logger.info("ℹ️ Kids парсер не выполнился успешно — stale state сохранён")

    duration = (datetime.now() - start_time).total_seconds()
    logger.info("=" * 60)
    logger.info("📊 ИТОГИ")
    logger.info(f"✅ Успешно:  {success}")
    logger.info(f"❌ Ошибки:   {failed}")
    logger.info(f"📦 Всего:    {success + failed}")
    logger.info(f"⏱️  Время:    {duration:.1f} сек")
    logger.info("=" * 60)

    # Машиночитаемый отчёт для бота
    report = {
        "success": success,
        "failed": failed,
        "duration": round(duration, 1),
        "kids_stats": kids_stats,
        "parsers": [
            {
                "name": name,
                "ok": ok,
                "results": result_lines,
            }
            for name, ok, result_lines in parser_status
        ],
        "all_results": all_results,
    }
    print(f"PARSER_REPORT:{json.dumps(report, ensure_ascii=False)}")
    
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
