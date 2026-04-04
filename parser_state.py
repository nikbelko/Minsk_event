#!/usr/bin/env python3
"""
SQLite state management for parser source fingerprints.

Two sets of hash/count fields:
  - last_seen_*       → updated on every non-error check (for change detection)
  - last_successful_* → updated only when check succeeded AND count > MIN_SANE_COUNT
                        (used as stable baseline; never overwritten with suspicious data)
"""
import sqlite3
from typing import Optional
from config import DB_PATH


def init_parser_source_state():
    """Create parser_source_state table if not exists. Idempotent."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS parser_source_state (
                source_name           TEXT PRIMARY KEY,
                last_seen_count       INTEGER DEFAULT 0,
                last_seen_hash        TEXT    DEFAULT '',
                last_successful_count INTEGER DEFAULT 0,
                last_successful_hash  TEXT    DEFAULT '',
                last_checked_at       TEXT    DEFAULT '',
                last_changed_at       TEXT    DEFAULT '',
                last_parse_status     TEXT    DEFAULT '',
                last_parse_mode       TEXT    DEFAULT '',
                last_parse_details    TEXT    DEFAULT ''
            )
        """)
        # Migrate existing tables that are missing the new columns (safe ALTER TABLE)
        existing_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(parser_source_state)").fetchall()
        }
        for col, definition in [
            ("last_successful_count", "INTEGER DEFAULT 0"),
            ("last_successful_hash",  "TEXT DEFAULT ''"),
            ("last_parse_error_hash", "TEXT DEFAULT ''"),
            ("last_parse_error_at",   "TEXT DEFAULT ''"),
        ]:
            if col not in existing_cols:
                conn.execute(
                    f"ALTER TABLE parser_source_state ADD COLUMN {col} {definition}"
                )
        conn.commit()


def get_parser_source_state(source_name: str) -> Optional[dict]:
    """Return current state row for a source, or None if not found."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM parser_source_state WHERE source_name = ?",
            (source_name,),
        ).fetchone()
        return dict(row) if row else None


def update_parser_source_state(source_name: str, **kwargs):
    """Upsert state for a source. Pass only the fields you want to update."""
    if not kwargs:
        return
    with sqlite3.connect(DB_PATH) as conn:
        exists = conn.execute(
            "SELECT 1 FROM parser_source_state WHERE source_name = ?",
            (source_name,),
        ).fetchone()
        if exists:
            sets = ", ".join(f"{k} = ?" for k in kwargs)
            conn.execute(
                f"UPDATE parser_source_state SET {sets} WHERE source_name = ?",
                [*kwargs.values(), source_name],
            )
        else:
            all_data = {"source_name": source_name, **kwargs}
            cols = ", ".join(all_data.keys())
            placeholders = ", ".join("?" * len(all_data))
            conn.execute(
                f"INSERT INTO parser_source_state ({cols}) VALUES ({placeholders})",
                list(all_data.values()),
            )
        conn.commit()
