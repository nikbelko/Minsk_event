import os
import sqlite3

import importlib.util
from collections import Counter


MODULE_PATH = "/Users/nikbelko/minskdvizh/bot_enhanced.py"


def load_module(db_path: str):
    spec = importlib.util.spec_from_file_location("bot_enhanced", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.DB_NAME = db_path
    return mod


def create_rating_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE event_ratings (user_id INTEGER, event_id INTEGER, event_key TEXT, score INTEGER, created_at TEXT, updated_at TEXT)"
    )
    rows = [
        (1, 101, "other:Буря:Клуб", 5, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        (2, 102, "other:Буря:Клуб", 5, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        (3, 103, "other:Буря:Клуб", 4, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        (4, 104, "other:Котлета:Парк", 2, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
    ]
    conn.executemany(
        "INSERT INTO event_ratings (user_id, event_id, event_key, score, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def test_helper_prioritizes_highly_rated_recurring_event():
    db_path = "/tmp/minskdvizh_test_subscription_digest.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    create_rating_db(db_path)
    mod = load_module(db_path)

    events = [
        {"title": "Котлета", "place": "Парк", "category": "concert", "event_date": "2026-01-03", "show_time": "19:00", "price": "50 BYN"},
        {"title": "Буря", "place": "Клуб", "category": "concert", "event_date": "2026-01-03", "show_time": "20:00", "price": "80 BYN"},
        {"title": "Буря", "place": "Клуб", "category": "concert", "event_date": "2026-01-04", "show_time": "21:00", "price": "80 BYN"},
    ]

    ranked = mod._rank_events_for_helper(events, seed="unit-test")
    assert ranked[0]["title"] == "Буря", ranked
    assert ranked[1]["title"] == "Котлета", ranked


def test_helper_respects_category_and_total_limits():
    db_path = "/tmp/minskdvizh_test_helper_limits.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    mod = load_module(db_path)

    events = []
    for idx in range(12):
        events.append({
            "title": f"concert-{idx}",
            "place": "Hall",
            "category": "concert",
            "event_date": "2026-01-03",
            "show_time": f"{10 + idx}:00",
            "price": "50 BYN",
        })
    for idx in range(12):
        events.append({
            "title": f"theater-{idx}",
            "place": "Theatre",
            "category": "theater",
            "event_date": "2026-01-04",
            "show_time": f"{11 + idx}:00",
            "price": "70 BYN",
        })

    selected = mod._select_helper_events(events, seed="limit-check", limit=6, max_per_category=2)
    counts = Counter(event["category"] for event in selected)

    assert len(selected) <= 6
    assert max(counts.values()) <= 2


def test_helper_removes_duplicate_events_before_selection():
    db_path = "/tmp/minskdvizh_test_helper_dedupe.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    mod = load_module(db_path)

    events = [
        {"title": "Шум", "place": "Клуб", "category": "party", "event_date": "2026-01-10", "show_time": "20:00", "price": "50 BYN"},
        {"title": "Шум", "place": "Клуб", "category": "party", "event_date": "2026-01-10", "show_time": "21:00", "price": "50 BYN"},
        {"title": "Шум", "place": "Клуб", "category": "party", "event_date": "2026-01-11", "show_time": "20:00", "price": "50 BYN"},
        {"title": "Тихо", "place": "Парк", "category": "party", "event_date": "2026-01-12", "show_time": "19:00", "price": "0 BYN"},
    ]

    selected = mod._select_helper_events(events, seed="dedupe-check", limit=10, max_per_category=10)
    titles = [event["title"] for event in selected]
    assert titles.count("Шум") == 1, selected
    assert "Тихо" in titles
