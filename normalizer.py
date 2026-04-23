#!/usr/bin/env python3
# normalizer.py
# Единая нормализация для всех парсеров: ticketpro, bezkassira, relax

import re
from datetime import datetime
from typing import Optional, Tuple
import logging

# Настраиваем логгер для модуля
logger = logging.getLogger(__name__)

# ── Словарь нормализации площадок ────────────────────────────────────────────
# Ключи — любые варианты написания (строчные, без кавычек),
# значения — каноническое название
PLACE_ALIASES: dict[str, str] = {
    # КЗ Минск
    "кз минск":                        "КЗ Минск",
    "кз \"минск\"":                     "КЗ Минск",
    "концертный зал минск":            "КЗ Минск",
    "концертный зал «минск»":          "КЗ Минск",

    # Дворец спорта
    "дворец спорта":                   "Дворец спорта",
    "дворец спорта, 4":                "Дворец спорта",

    # Белгосфилармония
    "белгосфилармония":                    "Белорусская государственная филармония",
    "белорусская государственная филармония": "Белорусская государственная филармония",
    "филармония":                          "Белорусская государственная филармония",
    "государственная филармония":          "Белорусская государственная филармония",
    "белорусская филармония":              "Белорусская государственная филармония",
    "концертный зал белгосфилармонии":     "Белорусская государственная филармония",
    "малый зал имени г.ширмы":             "Белорусская государственная филармония",
    "филармония малый зал имени г.щирмы":  "Белорусская государственная филармония",
    "концертный зал малый зал имени г. ширмы": "Белорусская государственная филармония",
    "кз белгосфилармонии":                 "Белорусская государственная филармония",
    "музыкальная гостиная":                "Белорусская государственная филармония",

    # Молодёжный театр
    "молодёжный театр":                "Молодёжный театр",
    "молодежный театр":                "Молодёжный театр",

    # Молодёжный театр эстрады
    "молодёжный театр эстрады":        "Молодёжный театр эстрады",
    "молодежный театр эстрады":        "Молодёжный театр эстрады",
    "театр эстрады":                   "Молодёжный театр эстрады",

    # Дворец Республики
    "дворец республики":               "Дворец Республики",
    "гу дворец республики":            "Дворец Республики",

    # Дворец Профсоюзов
    "дворец профсоюзов":               "Дворец Профсоюзов",
    "республиканский дворец культуры": "Дворец Профсоюзов",
    "дк профсоюзов":                   "Дворец Профсоюзов",
    "республиканский дворец культуры профсоюзов": "Дворец Профсоюзов",

    # Центральный дом офицеров
    "центральный дом офицеров":        "Центральный дом офицеров",
    "дом офицеров":                    "Центральный дом офицеров",

    # Дом литератора
    "дом литератора":                  "Дом литератора",

    # Музыкальный театр
    "музыкальный театр":               "Музыкальный театр",

    # ТЮЗ
    "театр юного зрителя":             "ТЮЗ",
    "тюз":                             "ТЮЗ",

    # Большой театр
    "большой театр беларуси":          "Большой театр",
    "большой театр":                   "Большой театр",
    "государственный академический большой театр": "Большой театр",
    "театр оперы и балета":            "Большой театр",

    # Театры
    "театр им. горького":              "Театр им. Горького",
    "театр имени горького":            "Театр им. Горького",
    "театр им. янки купалы":           "Театр им. Янки Купалы",
    "театр им я.купалы":               "Театр им. Янки Купалы",
    "театр им. я.купалы":              "Театр им. Янки Купалы",
    "театр имени янки купалы":         "Театр им. Янки Купалы",
    "театр им. янки купалы":           "Театр им. Янки Купалы",
    "купаловский театр":               "Театр им. Янки Купалы",
    "купаловский":                     "Театр им. Янки Купалы",
    "национальный академический театр имени янки купалы": "Театр им. Янки Купалы",
    "нацыянальны акадэмiчны тэатр iмя янкi купалы": "Театр им. Янки Купалы",
    "театр сатиры и юмора":            "Театр сатиры и юмора",
    "театр сатиры":                    "Театр сатиры и юмора",
    "театр-студия киноактера":         "Театр-студия киноактёра",
    "театр киноактера":                "Театр-студия киноактёра",
    "новый драматический театр":       "Новый драматический театр",
    "ртбд":                            "Республиканский театр белорусской драматургии",

    # Falcon Club
    "falcon club":                     "Falcon Club Arena",
    "falcon club arena":               "Falcon Club Arena",

    # Prime Hall
    "prime hall":                      "Prime Hall",
    "прайм холл":                      "Prime Hall",

    # Re:Public
    "re:public":                       "Club Re:Public",
    "club re:public":                  "Club Re:Public",
    "клуб ре: паблик":                 "Club Re:Public",
    "ре: паблик":                      "Club Re:Public",
    "re: public":                      "Club Re:Public",
    "club re: public":                 "Club Re:Public",
    "клуб ре:паблик":                  "Club Re:Public",
    "ре:паблик":                       "Club Re:Public",

    # DoZari
    "dozari":                          "DoZari Club",
    "dozari club":                     "DoZari Club",

    # 375 Place
    "клуб 375":                        "375 Place",
    "place 375":                       "375 Place",
    "375":                             "375 Place",

    # Astro club
    "astro club":                       "Astro Club",
    "астро клуб":                       "Astro Club",
    "клуб астро":                       "Astro Club",

    # Особняк
    "особняк":                        "Особняк Гастропаб",

    # Минск-Арена
    "мкск минск-арена":                "Минск-Арена",
    "мкск «минск-арена»":              "Минск-Арена",
    "минск-арена":                     "Минск-Арена",
    "minsk-arena":                     "Минск-Арена",
    "Минск арена":                     "Минск-Арена",
    "минск арена":                     "Минск-Арена",

    # Чижовка-Арена
    "чижовка-арена":                   "Чижовка-Арена",
    "гксу Чижовка арена":              "Чижовка-Арена",
    "Чижовка арена":                   "Чижовка-Арена",
    "чижовка арена":                   "Чижовка-Арена",

    # ДК МАЗ
    "дк маз":                          "ДК МАЗ",
    "дворец культуры маз":             "ДК МАЗ",
    "дворец культуры мтз":             "ДК МТЗ",

    # ДК Железнодорожников
    "дк железнодорожников":            "ДК Железнодорожников",

    # ДК Ветеранов
    "дк «ветеранов»":                  "ДК Ветеранов",
    "дк \"ветеранов\"":                "ДК Ветеранов",
    "дк ветеранов":                    "ДК Ветеранов",
    "дворец культуры ветеранов":       "ДК Ветеранов",
    "дворец ветеранов":                "ДК Ветеранов",

    # Верхний город
    "верхний город":                   "Концертный зал Верхний город",
    "концертный зал верхний город":    "Концертный зал Верхний город",

    # Национальная библиотека
    "национальная библиотека":         "Национальная библиотека",

    # Дворец искусств
    "дворец искусств":                 "Дворец искусств",

    # Национальный художественный музей
    "национальный художественный музей": "Национальный художественный музей",
    "художественный музей":            "Национальный художественный музей",

    # Музей ВОВ
    "музей великой отечественной войны": "Музей истории ВОВ",
    "музей истории вов":               "Музей истории ВОВ",

    # Галерея Ў
    "галерея ў":                       "Галерея Ў",

    # Виктория Олимп
    "виктория олимп":                    "Виктория Олимп",
    "отель виктория олимп":              "Виктория Олимп",
    "виктория олимп отель":              "Виктория Олимп",
    "виктория олимп спа":                "Виктория Олимп",
    "конгресс-зал барселона - виктория олимп отель": "Виктория Олимп",
    "виктория олимп отель (конгресс-зал барселона)": "Виктория Олимп",
    "виктория олимп отель (конгресс зал барселона)": "Виктория Олимп",
    "виктория олимп отель конгресс зал барселона": "Виктория Олимп",

    # Цирк
    "белгосцирк":                      "Белгосцирк",
    "цирк":                            "Белгосцирк",

    # Клубы
    "руки вверх! бар":                 "Руки Вверх Бар",
    "пространство эхо":                "Пространство ЭХО",
    "эхо place":                       "Пространство ЭХО",
    "эхо двор":                        "Пространство ЭХО",
    "эхо":                             "Пространство ЭХО",
    "пространство цех":                "Пространство ЦЕХ",
    "цех":                             "Пространство ЦЕХ",
    "клуб 58":                         "Club 58",
    "клуб58":                          "Club 58",
    "club 58":                         "Club 58",
    "club58":                         "Club 58",
    "58club":                         "Club 58",
    "58 club":                         "Club 58",
    "58клуб":                         "Club 58",
    "58 клуб":                         "Club 58",
}

# ── Список городов для исключения (не-минские события) ───────────────────────
OTHER_CITIES = [
    "гомель", "gomel", "витебск", "vitebsk", "могилев", "mogilev",
    "гродно", "grodno", "брест", "brest", "бобруйск", "bobruisk",
    "солигорск", "soligorsk", "орша", "orsha", "пинск", "pinsk",
    "лида", "lida", "новополоцк", "novopolotsk", "молодечно", "molodechno",
    "кобрин", "kobrin", "жодино", "zhodino", "речица", "rechitsa",
    "берёза", "bereza", "мозырь", "mozyr", "борисов", "borisov",
    "барановичи", "baranovichi", "несвиж", "nesvizh", "дзержинск", "dzerzhinsk",
    "пружаны", "pruzhany", "гродна", "гродненская", "слоним", "slonim",
    "волковыск", "volkovysk", "слуцк", "slutsk", "светлогорск", "svetlogorsk",
    "полоцк", "polotsk", "областная филармония", "областной дворец",
    # Дополнительные райцентры
    "горки", "gorki", "горки", "бгсха",
    "лепель", "lepel", "чечерск", "chechersk", "рогачёв", "rogachev",
    "жлобин", "zhlobin", "калинковичи", "kalinkovichi", "туров", "turov",
    "кричев", "krichev", "климовичи", "klimovichi", "костюковичи",
    "хойники", "khoyniki", "наровля", "narovlya", "ветка", "vetka",
    "г.горки", "г. горки", "г. ширмы", "ширмы", "ширма", "иваново", "каменюки", "строчицы"
]

# ── Месяцы ───────────────────────────────────────────────────────────────────
MONTHS_RU: dict[str, str] = {
    "января":   "01", "февраля":  "02", "марта":    "03", "апреля":   "04",
    "мая":      "05", "июня":     "06", "июля":     "07", "августа":  "08",
    "сентября": "09", "октября":  "10", "ноября":   "11", "декабря":  "12",
}

MONTHS_RU_INT: dict[str, int] = {k: int(v) for k, v in MONTHS_RU.items()}


# ═══════════════════════════════════════════════════════════════════════════════
#  МЕСТО
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_place(place: str, known_venues: list | None = None) -> str:
    """
    Приводит название площадки к каноническому виду.

    1. Если передан known_venues (relax-стиль) — сначала ищет точное вхождение
       канонического названия из списка (приоритет перед общим словарём).
    2. Убирает «Минск,» / «г. Минск,» в начале и адресные части.
    3. Убирает кавычки всех видов.
    4. Ищет совпадение в PLACE_ALIASES (точное → подстрока alias→cleaned).
    5. Если не нашёл — возвращает очищенную строку.
    """
    if not place:
        return ""

    # Проверяем known_venues до любой очистки — relax передаёт канонические имена напрямую.
    # После матча прогоняем через PLACE_ALIASES чтобы получить канонический вариант
    # (например "Филармония" → "Белорусская государственная филармония").
    if known_venues:
        place_lower = place.lower()
        for venue in known_venues:
            if venue.lower() in place_lower:
                # Ищем канонический вариант в PLACE_ALIASES
                venue_lower = venue.lower()
                for alias, canonical in PLACE_ALIASES.items():
                    alias_clean = re.sub(r'[«»"„"]', "", alias).strip()
                    if alias_clean == venue_lower or alias_clean in venue_lower:
                        return canonical
                # Не нашли в словаре — возвращаем как есть из known_venues
                return venue

    # Убираем «Минск,» в начале
    cleaned = re.sub(r"^Минск,\s*", "", place)
    cleaned = re.sub(r"^г\.\s*Минск,\s*", "", cleaned)

    # Убираем адресные части (для relax-стиля)
    # Паттерны требуют пробел после сокращения чтобы не зацепить «культуры», «переулок» и т.п.
    cleaned = re.sub(r"\bул\.\s+\w+", "", cleaned)
    cleaned = re.sub(r"\bпр-?т\.\s+\w+", "", cleaned)
    cleaned = re.sub(r"\bпл\.\s+\w+", "", cleaned)
    cleaned = re.sub(r"\bпер\.\s+\w+", "", cleaned)

    # Убираем кавычки
    cleaned = re.sub(r'[«»"„"]', "", cleaned)

    # Убираем лишние пробелы
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if not cleaned or len(cleaned) < 3:
        return place.strip()

    # Ищем в словаре по очищенной строке (без кавычек)
    # Два прохода: сначала точное совпадение, потом подстрока alias→cleaned.
    # Обратное вхождение (cleaned→alias) намеренно исключено:
    # "Большой театр" не должен матчить алиас "большой театр беларуси".
    cleaned_lower = cleaned.lower()

    # Проход 1: точное совпадение
    for alias, canonical in PLACE_ALIASES.items():
        alias_clean = re.sub(r'[«»"„"]', "", alias).strip()
        if alias_clean == cleaned_lower:
            return canonical

    # Проход 2: алиас является подстрокой входной строки
    # (например, "мкск минск-арена" найдёт "мкск минск-арена, трибуна б")
    for alias, canonical in PLACE_ALIASES.items():
        alias_clean = re.sub(r'[«»"„"]', "", alias).strip()
        if alias_clean in cleaned_lower:
            return canonical

    return cleaned


def is_minsk_event(place_text: str) -> bool:
    """Возвращает False, если место явно из другого города Беларуси."""
    if not place_text:
        return False
    place_lower = place_text.lower()
    for city in OTHER_CITIES:
        if city in place_lower:
            return False
    return True


# ═══════════════════════════════════════════════════════════════════════════════
#  ЗАГОЛОВОК
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_title(title: str) -> str:
    """
    Нормализует название события для сравнения дублей.
    Убирает вводные слова, кавычки, знаки препинания.
    """
    if not title:
        return ""

    norm = title.lower()

    # Убираем типичные вводные слова в начале
    norm = re.sub(
        r"^(концерт|концертная\s+программа|спектакль|шоу|выступление|концерт группы|группа|"
        r"юбилейный\s+концерт|сольный\s+концерт|гала-концерт|"
        r"праздничный\s+концерт|отчетный\s+концерт|"
        r"эстрадный\s+караоке-спектакль)\s+",
        "", norm,
    )
    # Убираем в конце
    norm = re.sub(r"\s+(концерт|спектакль|шоу|программа|фестиваль)$", "", norm)

    # Кавычки
    norm = re.sub(r'[«»"\'`„”“’‘′]', "", norm)

    # Точки в конце / многоточия
    norm = re.sub(r"\.+$", "", norm)
    norm = re.sub(r"\.{2,}", "", norm)

    # «и» / «&» → единый разделитель
    norm = re.sub(r"\s+и\s+", " & ", norm)
    norm = re.sub(r"&", " & ", norm)

    # Дефисы и тире
    norm = re.sub(r"[—–-]", "-", norm)

    # Оставляем только буквы, цифры, пробелы, дефис, амперсанд
    norm = re.sub(r"[^\w\s\-&]", "", norm)

    # Лишние пробелы
    norm = re.sub(r"\s+", " ", norm).strip()

    return norm


# ═══════════════════════════════════════════════════════════════════════════════
#  ЦЕНА (обновлённая с флагом бесплатной секции)
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_price(raw: str, from_free_section: bool = False) -> str:
    """
    Приводит строку цены к единому формату.
    
    Args:
        raw: исходная строка цены
        from_free_section: событие найдено в разделе "бесплатно" на сайте
    """
    if not raw and from_free_section:
        return "Бесплатно"
    
    if not raw:
        return ""

    text = raw.strip()

    # Бесплатно — включая "0 BYN", "0.00 BYN"
    if re.search(r"(бесплатно|вход\s*свободный|free)", text, re.IGNORECASE):
        return "Бесплатно"

    # Числовая цена
    m = re.search(r"(от\s*)?([\d.,]+)\s*(руб|р\.|byn|₽|byr)?", text, re.IGNORECASE)
    if m:
        prefix = "от " if m.group(1) else ""
        amount = m.group(2).replace(",", ".")
        # Убираем .0 / .00
        try:
            f = float(amount)
            if f == 0:
                return "Бесплатно"
            amount = f"{f:g}"   # 60.00→60, 15.50→15.5, 15.75→15.75
        except ValueError:
            pass
        return f"{prefix}{amount} руб"

    return text


def format_price_from_offers(offers: dict, from_free_section: bool = False) -> str:
    """
    Форматирует цену из JSON-LD offers (bezkassira-стиль).
    
    Args:
        offers: словарь с ценами из JSON-LD
        from_free_section: событие найдено в разделе "бесплатно"
    """
    if not offers and from_free_section:
        return "Бесплатно"
    
    if not offers:
        return ""
    
    # Важно: используем get с None-дефолтом и явную проверку на None,
    # иначе price=0 (falsy) будет заменён lowPrice
    price_val = offers.get("price")
    if price_val is None:
        price_val = offers.get("lowPrice")
    if price_val is None and from_free_section:
        return "Бесплатно"
    if price_val is None:
        return ""
    
    try:
        price_val = float(price_val)
    except (TypeError, ValueError):
        return "Бесплатно" if from_free_section else ""
    
    if price_val == 0:
        return "Бесплатно"
    
    # Убираем лишние нули: 60.00 → 60, 15.50 → 15.5
    amount = f"{price_val:g}"
    high_val = offers.get("highPrice")
    if high_val is not None:
        try:
            if float(high_val) != price_val:
                return f"от {amount} руб"
        except (TypeError, ValueError):
            pass
    return f"{amount} руб"


# ═══════════════════════════════════════════════════════════════════════════════
#  НОВЫЕ ФУНКЦИИ ДЛЯ ОБРАБОТКИ БЕСПЛАТНЫХ СОБЫТИЙ
# ═══════════════════════════════════════════════════════════════════════════════

def mark_free_duplicates(relax_events: list, free_events: list) -> list:
    """
    Проходит по списку бесплатных событий, находит их дубликаты в основном списке
    и проставляет им цену "Бесплатно".
    
    Args:
        all_events: список всех спарсенных событий (кроме free)
        free_events: список событий из free-секции
    
    Returns:
        Обновлённый список всех событий (события из free_events НЕ добавляются,
        а только проставляют цену существующим событиям)
    """
    if not free_events:
        return relax_events
    
    # Строим индекс бесплатных событий для быстрого поиска
    free_index = {}
    for i, fe in enumerate(free_events):
        key = (
            normalize_title(fe.get("title", "")),
            fe.get("event_date", ""),
            normalize_place(fe.get("place", ""))
        )
        free_index[key] = fe
    
    # Проходим по основным событиям и ищем совпадения
    result = []
    marked_count = 0
    processed_free_keys = set()
    
    for event in relax_events:
        key = (
            normalize_title(event.get("title", "")),
            event.get("event_date", ""),
            normalize_place(event.get("place", ""))
        )
        
        if key in free_index:
            # Нашли дубликат из free-секции — проставляем бесплатную цену
            marked_event = event.copy()
            marked_event["price"] = "Бесплатно"
            marked_event["_from_free_section"] = True
            result.append(marked_event)
            marked_count += 1
            processed_free_keys.add(key)
            logger.debug(f"🏷️ Проставлено 'Бесплатно' для: {event.get('title')} (категория: {event.get('category')})")
        else:
            result.append(event)
    
    if marked_count > 0:
        logger.info(f"🏷️ Проставлено 'Бесплатно' для {marked_count} событий из free-секции")
    
    # ВАЖНО: события из free_events НЕ добавляем в результат!
    # Они использовались только для проставления цены существующим событиям
    if len(free_events) - marked_count > 0:
        logger.info(f"ℹ️ {len(free_events) - marked_count} бесплатных событий не нашли дубликатов (пропущены)")
    
    return result


def apply_kids_pass(kids_events: list, conn) -> dict:
    """
    Обрабатывает события из relax.by/kids/:
    - Сбрасывает is_kids=0 для всех событий в БД (полный ресcan).
    - Для каждого kids-события ищет совпадение в БД по source_url или (title, event_date).
      Нашли → UPDATE is_kids=1.
      Не нашли → INSERT как уникальное детское событие (category='kids', is_kids=1).
    Returns: {'marked': int, 'added': int}
    """
    # Always clean up stale state regardless of whether kids_events is empty:
    # - remove synthetic rows inserted by a previous kids pass (unique circus/zoo events)
    # - reset is_kids flag on all remaining events
    conn.execute("DELETE FROM events WHERE source_name = 'relax.by' AND category = 'kids'")
    conn.execute("UPDATE events SET is_kids = 0 WHERE source_name != 'user_submitted'")

    if not kids_events:
        conn.commit()
        return {"marked": 0, "added": 0}

    marked = 0
    added = 0

    for ev in kids_events:
        source_url = (ev.get("source_url") or "").strip()
        title = (ev.get("title") or "").strip()
        event_date = (ev.get("event_date") or "").strip()
        place = (ev.get("place") or "").strip()

        found_ids = []

        if source_url:
            rows = conn.execute(
                "SELECT id FROM events WHERE source_url = ?", (source_url,)
            ).fetchall()
            found_ids = [r[0] for r in rows]

        if not found_ids and title and event_date:
            rows = conn.execute(
                "SELECT id FROM events WHERE LOWER(title) = ? AND event_date = ?",
                (title.lower(), event_date)
            ).fetchall()
            found_ids = [r[0] for r in rows]

        if found_ids:
            placeholders = ",".join("?" * len(found_ids))
            conn.execute(
                f"UPDATE events SET is_kids = 1 WHERE id IN ({placeholders})",
                found_ids,
            )
            marked += len(found_ids)
        elif title and event_date:
            conn.execute(
                """INSERT INTO events
                   (title, details, description, event_date, show_time, end_time,
                    place, location, price, category, source_url, source_name, is_kids)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'kids', ?, 'relax.by', 1)""",
                (
                    title,
                    ev.get("details") or "",
                    ev.get("description") or "",
                    event_date,
                    ev.get("show_time") or "",
                    ev.get("end_time") or "",
                    place,
                    ev.get("location") or "Минск",
                    ev.get("price") or "",
                    source_url,
                ),
            )
            added += 1

    conn.commit()
    logger.info(f"🧸 Kids pass: is_kids=1 проставлено для {marked} событий, добавлено уникальных: {added}")
    return {"marked": marked, "added": added}


def is_likely_free(event: dict) -> bool:
    """
    Проверяет, является ли событие вероятно бесплатным.
    Используется для статистики и отладки.
    """
    price = event.get("price", "")
    if not price:
        return False
    
    price_lower = price.lower()
    return "бесплатно" in price_lower or price == "0" or event.get("_from_free_section")


# ═══════════════════════════════════════════════════════════════════════════════
#  ДАТА И ВРЕМЯ (без изменений)
# ═══════════════════════════════════════════════════════════════════════════════

def parse_iso_datetime(iso: str) -> tuple[str, str]:
    """
    '2026-03-06T18:00:00+03:00' → ('2026-03-06', '18:00')
    При отсутствии времени возвращает ('2026-03-06', '').
    """
    try:
        dt = datetime.fromisoformat(iso)
        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%H:%M") if (dt.hour or dt.minute) else ""
        return date_str, time_str
    except Exception:
        return "", ""


def parse_text_date(text: str, current_year: Optional[int] = None) -> str:
    """
    '6 марта 2026'        → '2026-03-06'
    '7 — 8 марта 2026'    → '2026-03-07'  (берём первую дату)
    '14 мая'              → '2026-05-14'  (год подставляется автоматически)

    Если год в тексте не указан и дата уже прошла — берём следующий год.
    """
    if not text:
        return ""

    text = text.strip()

    # Убираем «до NN месяца» → берём первую дату диапазона
    text = re.sub(r"\s*[—–-]\s*\d+\s+", " ", text)

    # Вариант с годом
    m = re.search(r"(\d{1,2})\s+([а-яё]+)\s+(\d{4})", text, re.IGNORECASE)
    if m:
        day, mon_ru, year = m.group(1), m.group(2).lower(), m.group(3)
        mon = MONTHS_RU.get(mon_ru)
        if mon:
            return f"{year}-{mon}-{day.zfill(2)}"

    # Вариант без года
    m = re.search(r"(\d{1,2})\s+([а-яё]+)", text, re.IGNORECASE)
    if m:
        day, mon_ru = m.group(1), m.group(2).lower()
        mon_int = MONTHS_RU_INT.get(mon_ru)
        if mon_int:
            today = datetime.now()
            year = current_year or today.year
            # Если дата уже прошла — следующий год
            if mon_int < today.month or (mon_int == today.month and int(day) < today.day):
                year += 1
            return f"{year}-{mon_int:02d}-{int(day):02d}"

    return ""


def extract_time(text: str) -> str:
    """
    Извлекает время из произвольной строки.
    '19:00', 'в 19.00', 'начало в 20:00', '1900' и т.д.
    """
    for pattern in [
        r"начало\s*в\s*(\d{1,2}[:.]\d{2})",
        r"в\s*(\d{1,2}[:.]\d{2})",
        r"(\d{2}[:.]\d{2})",
        r"(\d{1,2}[:.]\d{2})\s*ч",
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            t = m.group(1).replace(".", ":")
            # «1900» → «19:00» (без разделителя)
            if len(t) == 4 and ":" not in t:
                t = t[:2] + ":" + t[2:]
            return t
    return ""


# ═══════════════════════════════════════════════════════════════════════════════
#  ФИЛЬТРАЦИЯ ПО ДАТЕ
# ═══════════════════════════════════════════════════════════════════════════════

def is_future_date(date_str: str, max_days: int = 180) -> bool:
    """
    Возвращает True, если дата в будущем и не дальше max_days от сегодня.

    Используется для фильтрации устаревших и слишком далёких событий.
    При невалидной строке возвращает False.
    """
    if not date_str:
        return False
    try:
        from datetime import date, timedelta
        ev_dt = date.fromisoformat(date_str)
        today = date.today()
        return today <= ev_dt <= today + timedelta(days=max_days)
    except ValueError:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  ДЕДУПЛИКАЦИЯ (обновлённая с использованием новых функций)
# ═══════════════════════════════════════════════════════════════════════════════

def titles_are_similar(title_a: str, title_b: str, threshold: float = 0.82) -> bool:
    """
    Быстрое сравнение нормализованных заголовков.
    Сначала проверяет точное совпадение, потом — через коэффициент Жакара
    на уровне слов (без тяжёлых зависимостей).
    """
    a = normalize_title(title_a)
    b = normalize_title(title_b)
    if not a or not b:
        return False
    if a == b:
        return True

    # Жакар по словам
    set_a = set(a.split())
    set_b = set(b.split())
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    if union == 0:
        return False
    return (intersection / union) >= threshold
