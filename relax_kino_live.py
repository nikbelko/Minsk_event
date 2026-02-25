import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import re
import time

def setup_database():
    """Подключается к базе events_final.db"""
    conn = sqlite3.connect('events_final.db')
    return conn

def clear_cinema_data(conn):
    """Очищает все записи кино из базы"""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM events WHERE category='cinema'")
    deleted = cursor.rowcount
    conn.commit()
    print(f"🧹 Очищено {deleted} старых записей кино")
    return deleted

def fetch_page(url, retries=3):
    """Загружает страницу с сайта"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    for attempt in range(retries):
        try:
            print(f"  🌐 Попытка {attempt + 1}/{retries}...")
            response = requests.get(url, headers=headers, timeout=30)
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                print(f"  ✅ Страница загружена ({len(response.text)} символов)")
                return response.text
            else:
                print(f"  ⚠️ Код ответа: {response.status_code}")
                
        except Exception as e:
            print(f"  ⚠️ Ошибка: {e}")
        
        if attempt < retries - 1:
            time.sleep(5)
    
    return None

def parse_date(date_str):
    """Преобразует дату из формата MM/DD/YYYY в YYYY-MM-DD"""
    if not date_str:
        return None
    
    match = re.search(r'(\d{2})\/(\d{2})\/(\d{4})', date_str)
    if match:
        month, day, year = match.groups()
        return f"{year}-{month}-{day}"
    
    return None

def extract_cinema_info(cinema_block):
    """Извлекает информацию о кинотеатре"""
    place = None
    location = None
    
    name_elem = cinema_block.find('a', class_='js-schedule__place-link')
    if name_elem:
        place = name_elem.get_text(strip=True)
    
    addr_elem = cinema_block.find('span', class_='schedule__place-link')
    if addr_elem:
        location = addr_elem.get_text(strip=True)
    
    return place, location

def extract_movie_info(block, current_date, cinema_place, cinema_location):
    """Извлекает информацию о фильме из блока"""
    movies = []
    
    try:
        if 'schedule__item' not in block.get('class', []) or 'table_by_place' not in block.get('class', []):
            return movies
        
        title_elem = block.find('a', class_='js-schedule__event-link')
        if not title_elem:
            return movies
        
        title = title_elem.get_text(strip=True)
        if not title or len(title) < 3:
            return movies
        
        # Пропускаем служебные записи
        skip_titles = ['Вся афиша', 'Кино', 'Спектакли', 'Квесты', 'Концерты', 
                      'События', 'Выставки', 'Детская афиша', 'Вечеринки', 'Stand Up',
                      'Популярное', 'Сегодня', 'Завтра', 'Премьеры', 'Кинотеатры', 
                      'Фильмы', 'Афиша', 'Экскурсии', 'Обучение', 'Спорт', 'Хоккей', 
                      'Бесплатные мероприятия']
        
        if title in skip_titles:
            return movies
        
        source_url = title_elem.get('href', '')
        if source_url and not source_url.startswith('http'):
            source_url = 'https://afisha.relax.by' + source_url
        
        details_elem = block.find('a', class_='schedule__event-dscr')
        details = details_elem.get_text(strip=True) if details_elem else ''
        
        seance_blocks = block.find_all('div', class_='schedule__seance')
        
        for seance in seance_blocks:
            time_elem = seance.find('a', class_='schedule__seance-time')
            if not time_elem:
                continue
            
            show_time = time_elem.get_text(strip=True)
            
            # Пропускаем записи без времени
            if not show_time:
                continue
            
            price_elem = seance.find('span', class_='seance-price')
            price = price_elem.get_text(strip=True) if price_elem else None
            
            date_from_attr = time_elem.get('data-date-format', '')
            event_date = parse_date(date_from_attr) or current_date
            
            # Формируем описание
            description = f"🎬 **{title}**"
            if details:
                description += f"\n🎭 {details}"
            if cinema_location:
                description += f"\n📍 {cinema_location}"
            if price:
                description += f"\n💰 {price}"
            
            movie_data = {
                'title': title,
                'details': details,
                'description': description,
                'event_date': event_date,
                'show_time': show_time,
                'place': cinema_place,
                'location': cinema_location,
                'price': price,
                'category': 'cinema',
                'source_url': source_url,
                'source_name': 'relax.by/kino'
            }
            movies.append(movie_data)
            print(f"      ✅ {title} - {show_time} ({cinema_place})")
            
    except Exception as e:
        print(f"      ⚠️ Ошибка: {e}")
    
    return movies

def parse_kino_page(html_content, current_date):
    """Парсит HTML страницы кино"""
    soup = BeautifulSoup(html_content, 'lxml')
    all_movies = []
    
    cinema_blocks = soup.find_all('div', class_='schedule__place--fill')
    cinema_blocks.extend(soup.find_all('div', class_='schedule__place--empty'))
    
    print(f"📦 Найдено кинотеатров: {len(cinema_blocks)}")
    
    cinema_count = 0
    for cinema_block in cinema_blocks:
        place, location = extract_cinema_info(cinema_block)
        
        if not place or place == 'Кинотеатр':
            continue
        
        cinema_count += 1
        print(f"\n  {cinema_count}. 🎦 {place}")
        if location:
            print(f"     📍 {location}")
        
        current = cinema_block.find_next()
        
        while current:
            if current.name == 'div' and ('schedule__place--fill' in current.get('class', []) or 'schedule__place--empty' in current.get('class', [])):
                break
            
            if current.name == 'div' and 'schedule__item' in current.get('class', []) and 'table_by_place' in current.get('class', []):
                movies = extract_movie_info(current, current_date, place, location)
                all_movies.extend(movies)
            
            current = current.find_next()
    
    return all_movies

def save_movies_to_db(conn, movies):
    """Сохраняет фильмы в базу данных"""
    cursor = conn.cursor()
    new_count = 0
    duplicate_count = 0
    
    for movie in movies:
        # Проверяем уникальность
        cursor.execute("""
            SELECT id FROM events 
            WHERE title = ? AND event_date = ? AND show_time = ? AND place = ?
        """, (movie['title'], movie['event_date'], movie['show_time'], movie['place']))
        
        if cursor.fetchone():
            duplicate_count += 1
            continue
        
        cursor.execute("""
            INSERT INTO events 
            (title, details, description, event_date, show_time, place, location, price, category, source_url, source_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            movie['title'],
            movie['details'],
            movie['description'],
            movie['event_date'],
            movie['show_time'],
            movie['place'],
            movie['location'],
            movie['price'],
            'cinema',
            movie['source_url'],
            'relax.by/kino'
        ))
        new_count += 1
    
    conn.commit()
    return new_count, duplicate_count

def main():
    print("=" * 60)
    print("🎬 ПАРСЕР КИНО (С АВТОМАТИЧЕСКОЙ ОЧИСТКОЙ)")
    print("=" * 60)
    
    url = 'https://afisha.relax.by/kino/minsk/'
    
    print(f"\n🌐 Загружаем страницу: {url}")
    html_content = fetch_page(url)
    
    if not html_content:
        print("❌ Не удалось загрузить страницу")
        return
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    movies = parse_kino_page(html_content, current_date)
    
    print(f"\n📊 Всего найдено сеансов фильмов: {len(movies)}")
    
    if movies:
        place_stats = {}
        for movie in movies:
            place_stats[movie['place']] = place_stats.get(movie['place'], 0) + 1
        
        print(f"\n📋 Статистика по кинотеатрам:")
        for place, count in sorted(place_stats.items())[:10]:
            print(f"   • {place}: {count} сеансов")
        
        # Сохраняем в базу
        conn = setup_database()
        
        # Сначала очищаем старые записи
        clear_cinema_data(conn)
        
        # Затем добавляем новые
        new, duplicates = save_movies_to_db(conn, movies)
        conn.close()
        
        print(f"\n📊 Результаты:")
        print(f"   ✅ Добавлено новых сеансов: {new}")
        print(f"   🔄 Дубликатов (пропущено): {duplicates}")
    else:
        print("❌ Фильмы не найдены")

if __name__ == '__main__':
    main()
