import sqlite3
import os
import tempfile

import api


def build_db(path: str):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE users (user_id INTEGER PRIMARY KEY, username TEXT DEFAULT '', first_name TEXT DEFAULT '', telegram_username TEXT DEFAULT '', created_at TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL DEFAULT '')")
    conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, title TEXT, event_date TEXT, place TEXT, category TEXT, show_time TEXT, source_name TEXT)")
    conn.execute("CREATE TABLE event_ticket_posts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, event_id INTEGER NOT NULL DEFAULT 0, event_key TEXT NOT NULL DEFAULT '', post_type TEXT NOT NULL, qty INTEGER NOT NULL DEFAULT 1, price_text TEXT DEFAULT '', note TEXT DEFAULT '', status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    conn.execute("CREATE TABLE ticket_match_notifications (id INTEGER PRIMARY KEY AUTOINCREMENT, recipient_user_id INTEGER NOT NULL, event_key TEXT NOT NULL, source_user_id INTEGER NOT NULL, source_post_id INTEGER NOT NULL, match_type TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(recipient_user_id, event_key, source_user_id, source_post_id, match_type))")
    conn.commit()
    return conn


def test_new_sell_notifies_only_buyers_and_deduplicates():
    fd, db_path = tempfile.mkstemp(prefix="ticket_match_", suffix=".db")
    os.close(fd)
    api.DB_PATH = db_path
    conn = build_db(db_path)

    conn.execute("INSERT INTO users(user_id, first_name) VALUES (10, 'Buyer 1')")
    conn.execute("INSERT INTO users(user_id, first_name) VALUES (20, 'Buyer 2')")
    conn.execute("INSERT INTO users(user_id, first_name) VALUES (30, 'Seller 1')")
    conn.execute("INSERT INTO users(user_id, first_name) VALUES (40, 'Seller 2')")
    conn.execute("INSERT INTO events(id, title, event_date, place, category, show_time, source_name) VALUES (1, 'Test show', '2026-09-01', 'Hall 1', 'concert', '20:00', 'test')")

    event_key = 'other:Test show:Hall 1'
    conn.execute("INSERT INTO event_ticket_posts(user_id, event_id, event_key, post_type, qty, created_at, updated_at) VALUES (10, 1, ?, 'buy', 2, '2026-08-22 10:00:00', '2026-08-22 10:00:00')", (event_key,))
    conn.execute("INSERT INTO event_ticket_posts(user_id, event_id, event_key, post_type, qty, created_at, updated_at) VALUES (20, 1, ?, 'buy', 1, '2026-08-22 10:05:00', '2026-08-22 10:05:00')", (event_key,))
    conn.execute("INSERT INTO event_ticket_posts(user_id, event_id, event_key, post_type, qty, created_at, updated_at) VALUES (30, 1, ?, 'sell', 1, '2026-08-22 10:10:00', '2026-08-22 10:10:00')", (event_key,))
    conn.commit()

    matches = api._find_ticket_matches_for_post(conn, event_key, 40, 'sell', 999)
    assert [row["user_id"] for row in matches] == [10, 20], matches
    assert all(row["user_id"] != 30 for row in matches), matches
    assert all(row["user_id"] != 40 for row in matches), matches

    api._record_ticket_match_notifications_for_post(conn, event_key, 40, 'sell', 999)
    rows = conn.execute("SELECT recipient_user_id FROM ticket_match_notifications ORDER BY recipient_user_id").fetchall()
    assert [r[0] for r in rows] == [10, 20], rows

    api._record_ticket_match_notifications_for_post(conn, event_key, 40, 'sell', 999)
    total = conn.execute("SELECT COUNT(*) FROM ticket_match_notifications").fetchone()[0]
    assert total == 2, total

    conn.close()
    os.remove(db_path)


if __name__ == '__main__':
    test_new_sell_notifies_only_buyers_and_deduplicates()
    print('ticket match notifications ok')
