import os
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import re
import time
import logging
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("theatre_parser.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class RelaxTheatreParser:
    def __init__(self, db_path=os.getenv(\"DB_PATH\", \"/data/events_final.db\")):
        self.db_path = db_path
        self.base_url = 'https://afisha.relax.by'
        self.theatre_url = f'{self.base_url}/theatre/minsk/'
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        })
        
        self.months = {
            'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
            'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
        }
        self.current_year = datetime.now().year
        
        # Список известных театров Минска
        self.known_theatres = [
            'Молодёжный театр', 'Молодежный театр',
            'Молодёжный театр эстрады', 'Молодежный театр эстрады',
            'Театр им. Горького', 'Театр имени Горького',
            'Театр им. Янки Купалы', 'Театр имени Янки Купалы', 'Купаловский',
            'Театр оперы и балета', 'Большой театр',
            'Театр сатиры и юмора', 'Театр сатиры',
            'Театр-студия киноактера', 'Театр киноактера',
            'Новый драматический театр', 'Новый театр',
            'Театр юного зрителя', 'ТЮЗ',
            'Дворец Республики',
            'Дворец Профсоюзов',
            'Белорусская государственная филармония', 'Филармония',
            'Центральный Дом офицеров', 'Дом офицеров',
            'Музыкальный театр',
            'Дом литератора',
            'SKYLINE Cinema',
            'mooon в ТРЦ Dana Mall',
            'Центральный'
        ]

    def fetch_page(self, url, retries=3):
        for attempt in range(retries):
            try:
                logging.info(f"Попытка {attempt + 1}/{retries} загрузить {url}")
                response = self.session.get(url, timeout=30)
                response.encoding = 'utf-8'
                if response.status_code == 200:
                    logging.info(f"Страница загружена ({len(response.text)} символов)")
                    return response.text
                else:
                    logging.warning(f"Код ответа: {response.status_code}")
            except Exception as e:
                logging.warning(f"Ошибка загрузки: {e}")
            if attempt < retries - 1:
                time.sleep(5)
        return None

    def parse_date_from_header(self, header_text):
        if not header_text:
            return None
        
        header_text = header_text.strip().lower()
        match = re.search(r'(\d{1,2})\s+([а-я]+)', header_text)
        if not match:
            return None
            
        day = int(match.group(1))
        month_name = match.group(2)
        month = self.months.get(month_name)
        if not month:
            return None
        
        today = datetime.now()
        year = self.current_year
        if month < today.month:
            year += 1
        elif month == today.month and day < today.day:
            year += 1
            
        return f"{year}-{month:02d}-{day:02d}"

    def normalize_place(self, place):
        """Нормализует название места, убирая мусор"""
        if not place:
            return None
        
        # Проверяем, есть ли известный театр в тексте
        place_lower = place.lower()
        for theatre in self.known_theatres:
            if theatre.lower() in place_lower:
                return theatre
        
        # Если не нашли, но это явно не "Театр" по умолчанию
        if place == "Театр" or len(place) < 3:
            return None
        
        # Просто чистим текст
        place = re.sub(r'ул\.?\s*\w+', '', place)
        place = re.sub(r'пр-?т\.?\s*\w+', '', place)
        place = re.sub(r'пл\.?\s*\w+', '', place)
        place = re.sub(r'\s+', ' ', place).strip()
        
        return place if len(place) > 3 else None

    def extract_time(self, text):
        """Извлекает время из текста"""
        time_patterns = [
            r'(\d{2}[:\.]\d{2})',  # 19:00 или 19.00
            r'в\s*(\d{1,2}[:\.]\d{2})',  # в 19:00
            r'начало\s*в\s*(\d{1,2}[:\.]\d{2})',  # начало в 19:00
            r'(\d{1,2}[:\.]\d{2})\s*ч',  # 19:00 ч
        ]
        
        for pattern in time_patterns:
            match = re.search(pattern, text)
            if match:
                time_str = match.group(1)
                time_str = time_str.replace('.', ':')
                if len(time_str) == 4:  # 1900 -> 19:00
                    time_str = time_str[:2] + ':' + time_str[2:]
                return time_str
        return ""

    def extract_price(self, text):
        """Извлекает цену из текста"""
        price_patterns = [
            r'(от\s*\d+[\.,]?\d*\s*руб)',
            r'(\d+[\.,]?\d*\s*руб)',
            r'(\d+[\.,]?\d*\s*р\.)',
            r'(\d+[\.,]?\d*\s*₽)',
        ]
        
        for pattern in price_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        return ""

    def parse_page(self, url):
        """Парсит страницу и собирает спектакли"""
        # Используем словарь для объединения информации из разных блоков
        event_dict = {}
        
        html = self.fetch_page(url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Находим все блоки со спектаклями
        event_blocks = soup.find_all('div', class_=re.compile(r'event|schema|item'))
        if not event_blocks:
            event_blocks = soup.find_all('a', href=re.compile(r'/event/|/theatre/'))
        
        logging.info(f"Найдено блоков событий: {len(event_blocks)}")
        
        # Переменные для хранения последнего театра
        last_place = None
        last_location = "Минск"
        last_date = None
        
        for block in event_blocks:
            try:
                # Ищем название спектакля
                title_elem = None
                if block.name == 'a':
                    title_elem = block
                else:
                    title_elem = block.find('a', href=re.compile(r'/event/|/theatre/'))
                
                if not title_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                if not title or len(title) < 3:
                    continue
                
                if title in ['Купить билет', 'Подробнее', 'Афиша']:
                    continue
                
                # Ищем дату
                event_date = None
                parent = block.find_parent()
                for _ in range(5):
                    if not parent:
                        break
                    date_header = parent.find_previous('h5')
                    if date_header:
                        event_date = self.parse_date_from_header(date_header.get_text())
                        if event_date:
                            break
                    parent = parent.parent
                
                if not event_date:
                    continue
                
                # Если дата изменилась, сбрасываем последний театр
                if last_date != event_date:
                    last_place = None
                    last_location = "Минск"
                    last_date = event_date
                
                # Получаем текст блока
                block_text = block.get_text()
                
                # Извлекаем время и цену
                show_time = self.extract_time(block_text)
                price = self.extract_price(block_text)
                
                # Ищем место (театр) в текущем блоке
                place = None
                place_elem = block.find(['a', 'span', 'div'], class_=re.compile(r'place|theatre|location'))
                if place_elem:
                    place_text = place_elem.get_text(strip=True)
                    place = self.normalize_place(place_text)
                
                # Если нашли театр в этом блоке, запоминаем его
                if place:
                    last_place = place
                    # Ищем адрес
                    location_elem = block.find(['span', 'div'], class_=re.compile(r'address|street|metro'))
                    if location_elem:
                        last_location = location_elem.get_text(strip=True)
                    logging.info(f"  🏛️  Текущий театр: {last_place} ({last_location})")
                
                # Если театр не нашли, используем последний
                if not place and last_place:
                    place = last_place
                    location = last_location
                else:
                    # Если всё ещё нет театра, пропускаем
                    continue
                
                # Ищем детали (жанр)
                details = ""
                details_elem = block.find(['div', 'span'], class_=re.compile(r'genre|dscr|desc|type'))
                if details_elem:
                    details = details_elem.get_text(strip=True)
                
                # Создаем ключ ТОЛЬКО по названию и дате (без времени)
                event_key = f"{title}_{event_date}_{place}"
                
                # Если такое событие уже есть, обновляем информацию
                if event_key in event_dict:
                    existing = event_dict[event_key]
                    # Приоритет: если есть время - оставляем, если нет - добавляем
                    if show_time and not existing['show_time']:
                        existing['show_time'] = show_time
                    if price and not existing['price']:
                        existing['price'] = price
                    if details and not existing['details']:
                        existing['details'] = details
                else:
                    # Новое событие
                    event_dict[event_key] = {
                        'title': title,
                        'details': details,
                        'event_date': event_date,
                        'show_time': show_time,
                        'place': place,
                        'location': last_location,
                        'price': price,
                        'source_url': title_elem.get('href', '') if title_elem.get('href', '').startswith('http') else self.base_url + title_elem.get('href', ''),
                    }
                    
            except Exception as e:
                logging.error(f"Ошибка при обработке блока: {e}")
                continue
        
        # Преобразуем словарь в список событий
        events = []
        for event_key, event_data in event_dict.items():
            # Формируем описание
            description = f"🎭 {event_data['title']}"
            if event_data['details']:
                description += f"\n📖 {event_data['details']}"
            if event_data['location']:
                description += f"\n📍 {event_data['location']}"
            if event_data['price']:
                description += f"\n💰 {event_data['price']}"
            
            event = {
                'title': event_data['title'],
                'details': event_data['details'],
                'description': description,
                'event_date': event_data['event_date'],
                'show_time': event_data['show_time'],
                'place': event_data['place'],
                'location': event_data['location'],
                'price': event_data['price'],
                'category': 'theater',  # ИЗМЕНЕНО: 'Театр' -> 'theater'
                'source_url': event_data['source_url'],
                'source_name': 'relax.by/theatre'
            }
            events.append(event)
            
            # Красивый вывод в лог
            time_display = event_data['show_time'] if event_data['show_time'] else "     "
            price_display = event_data['price'] if event_data['price'] else "без цены"
            logging.info(f"  ✅ {event_data['event_date']} | {time_display:5} | {event_data['title'][:25]:25} | {event_data['place'][:20]:20} | {price_display}")
        
        logging.info(f"Всего найдено уникальных спектаклей: {len(events)}")
        return events

    def save_events(self, events):
        if not events:
            logging.info("Нет событий для сохранения")
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Очищаем старые записи театра (с категорией 'theater')
        cursor.execute("DELETE FROM events WHERE category='theater'")
        deleted = cursor.rowcount
        logging.info(f"Удалено старых записей: {deleted}")
        
        # Добавляем новые
        new_count = 0
        for event in events:
            try:
                cursor.execute("""
                    INSERT INTO events (
                        title, details, description, event_date, show_time,
                        place, location, price, category, source_url, source_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event['title'], event['details'], event['description'],
                    event['event_date'], event['show_time'], event['place'],
                    event['location'], event['price'], event['category'],
                    event['source_url'], event['source_name']
                ))
                new_count += 1
            except Exception as e:
                logging.error(f"Ошибка сохранения: {e}")
        
        conn.commit()
        conn.close()
        
        logging.info(f"Добавлено новых: {new_count}")
        return new_count

    def run(self):
        logging.info("=" * 60)
        logging.info("🎭 ПАРСЕР ТЕАТРА")
        logging.info("=" * 60)
        
        events = self.parse_page(self.theatre_url)
        
        if events:
            saved = self.save_events(events)
            logging.info(f"Итого: найдено {len(events)}, сохранено {saved}")
            # Добавляем print для run_all_parsers.py
            print(f"   🧹 Очищено {len(events)} старых записей театра")
            print(f"   📊 Результаты:")
            print(f"      ✅ Добавлено новых спектаклей: {saved}")
        else:
            logging.warning("События не найдены")
            print(f"   ⚠️ Событий театра не найдено")
        
        logging.info("=" * 60)

if __name__ == "__main__":
    parser = RelaxTheatreParser()
    parser.run()
