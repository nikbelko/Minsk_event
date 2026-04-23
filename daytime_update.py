#!/usr/bin/env python3
"""
Daytime lightweight source check + conditional full parse.

Strategy:
  - relax.by  → per-category cheap fingerprint check → parse only changed categories
                → always run free-events pass after any relax category was parsed
  - bycard.by → cheap fingerprint check → full parse only if changed
  - ticketpro.by, bezkassira.by → always full parse (fallback_full_parse)

Fingerprint quality:
  - relax.by per category: href|date|show_time per seance (listing page only, no detail pages)
  - bycard.by: performanceId|date|time per NUXT session (resolved via decode_nuxt)

Safety rules:
  - last_seen_hash/count updated only on non-error checks with count > MIN_SANE_COUNT
  - last_successful_hash/count updated only when above + parse_ok=True
  - If check returns error or suspiciously empty → do NOT overwrite baseline; mark status=error

Output: DAYTIME_REPORT:<json> on stdout for bot_enhanced.py to capture.
"""

import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime

import sqlite3

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DB_PATH, MINSK_TZ  # noqa: E402
from parser_state import (  # noqa: E402
    init_parser_source_state,
    get_parser_source_state,
    update_parser_source_state,
)

try:
    from normalizer import mark_free_duplicates as _mark_free_duplicates
    from normalizer import apply_kids_pass as _apply_kids_pass
    _NORMALIZER_AVAILABLE = True
except ImportError:
    _NORMALIZER_AVAILABLE = False

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "daytime_update.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

if not _NORMALIZER_AVAILABLE:
    log.warning("normalizer import failed — free-events price update disabled")

# ── Config ────────────────────────────────────────────────────────────────────

# If full parse failed for a given fingerprint, don't retry it more often than this.
PARSE_ERROR_COOLDOWN_HOURS = 6

# relax.by per-category config: source_key → (listing_url, parser_cmd, human_label)
# source_key is used as the parser_source_state primary key, so each category
# has its own independent fingerprint/baseline.
RELAX_CATEGORIES: dict[str, tuple[str, str, str]] = {
    "relax.by:theatre":    ("https://afisha.relax.by/theatre/minsk/",  "relax_parser.py theatre",    "🎭 Театр (Relax)"),
    "relax.by:concert":    ("https://afisha.relax.by/conserts/minsk/", "relax_parser.py concert",    "🎵 Концерты (Relax)"),
    "relax.by:exhibition": ("https://afisha.relax.by/expo/minsk/",     "relax_parser.py exhibition", "🖼️ Выставки (Relax)"),
    "relax.by:party":      ("https://afisha.relax.by/clubs/minsk/",    "relax_parser.py party",      "🎉 Вечеринки (Relax)"),
    "relax.by:kino":       ("https://afisha.relax.by/kino/minsk/",     "relax_parser.py kino",       "🎬 Кино (Relax)"),
}

# Free-events pass must run after ANY relax category was parsed.
# It marks free events across all relax categories — skipping it loses "Бесплатно" labels.
RELAX_FREE_CMD   = "relax_parser.py free"
RELAX_FREE_LABEL = "🆓 Бесплатно (Relax)"

# Minimum number of items a check must return to be considered sane.
MIN_SANE_COUNT: dict[str, int] = {
    "relax.by:theatre":    3,
    "relax.by:concert":    3,
    "relax.by:exhibition": 3,
    "relax.by:party":      3,
    "relax.by:kino":       5,   # kino always has many sessions
    "bycard.by":           3,
}

# Sources with cheap fingerprint check (relax per-category + bycard)
CHECKABLE_SOURCES = list(RELAX_CATEGORIES.keys()) + ["bycard.by"]

# Sources that always run full parse (no cheap check implemented)
ALWAYS_PARSE_SOURCES = ["ticketpro.by", "bezkassira.by"]

# Maps source_key → single parser (cmd, label).
# Relax free pass is NOT here — it's triggered separately after any category parse.
SOURCE_PARSERS: dict[str, tuple[str, str]] = {
    **{key: (cmd, label) for key, (_, cmd, label) in RELAX_CATEGORIES.items()},
    "bycard.by":    ("bycard_parser.py",    "🎭 Bycard"),
    "ticketpro.by": ("ticketpro_parser.py", "🎫 Ticketpro"),
    "bezkassira.by":("bezkassira_parser.py","🎟 BezKassira"),
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
}
REQUEST_TIMEOUT = 25

# ── Fingerprint helpers ───────────────────────────────────────────────────────


def _sha256(items: list[str]) -> str:
    """SHA-256 of sorted joined items, truncated to 16 chars."""
    content = "|".join(sorted(items))
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def _parse_text_date(text: str) -> str:
    """
    Extract YYYY-MM-DD from relax h5 date text.
    Handles formats: '29 марта', '29 марта 2026', '29.03', '29.03.2026'.
    Returns '' on failure.
    """
    MONTHS = {
        "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
        "мая": 5, "июня": 6, "июля": 7, "августа": 8,
        "сентября": 9, "октября": 10, "ноября": 11, "декября": 12,
    }
    text = text.strip().lower()
    year = datetime.now(MINSK_TZ).year

    # "29 марта 2026" or "29 марта"
    m = re.search(r'(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?', text)
    if m:
        day = int(m.group(1))
        mon = MONTHS.get(m.group(2), 0)
        if m.group(3):
            year = int(m.group(3))
        if mon:
            try:
                return datetime(year, mon, day).strftime("%Y-%m-%d")
            except ValueError:
                return ""

    # "29.03.2026" or "29.03"
    m2 = re.search(r'(\d{1,2})\.(\d{2})(?:\.(\d{4}))?', text)
    if m2:
        day, mon2 = int(m2.group(1)), int(m2.group(2))
        if m2.group(3):
            year = int(m2.group(3))
        try:
            return datetime(year, mon2, day).strftime("%Y-%m-%d")
        except ValueError:
            return ""

    return ""


# ── relax.by cheap check (per category) ──────────────────────────────────────


def _check_relax_category(source_key: str, url: str) -> dict:
    """
    Fetch ONE relax listing page and return its fingerprint.
    Fingerprint key per seance: href|date|show_time
      href      — most stable unique key (event URL, available on listing page)
      date      — from schedule__list > h5 (available on listing page)
      show_time — from a/span.schedule__seance-time (available on listing page)
    Together they catch: new events, removed events, rescheduled events.
    No detail pages fetched.
    """
    cat = source_key.split(":")[-1]
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        keys: list[str] = []
        for day_block in soup.find_all("div", class_="schedule__list"):
            h5 = day_block.find("h5")
            date_str = _parse_text_date(h5.get_text()) if h5 else ""

            for movie_item in day_block.find_all("div", class_="schedule__table--movie__item"):
                item = movie_item.find("div", class_="schedule__item")
                if not item:
                    continue
                title_a = item.find("a", class_="js-schedule__event-link")
                if not title_a:
                    continue
                href = title_a.get("href", "").strip()
                if not href:
                    continue

                seances = item.find_all("div", class_="schedule__seance") or [item]
                for seance in seances:
                    time_elem = (
                        seance.find("a", class_="schedule__seance-time")
                        or seance.find("span", class_="schedule__seance-time")
                    )
                    show_time = ""
                    if time_elem:
                        raw = time_elem.get_text(strip=True)
                        if re.match(r"^\d{1,2}:\d{2}$", raw):
                            show_time = raw
                    keys.append(f"{href}|{date_str}|{show_time}")

        count = len(keys)
        log.info(f"  relax/{cat}: {count} seances")
        return {
            "source_name": source_key,
            "count":  count,
            "hash":   _sha256(keys) if keys else "",
            "details": f"{cat}={count}",
            "status": "ok" if count > 0 else "error",
        }
    except Exception as e:
        log.warning(f"  relax/{cat} error: {e}")
        return {
            "source_name": source_key,
            "count": 0,
            "hash":  "",
            "details": str(e),
            "status": "error",
        }


def _make_relax_check(source_key: str, url: str):
    """Factory to avoid late-binding closure bug in list comprehensions."""
    def _check():
        return _check_relax_category(source_key, url)
    return _check


# ── bycard.by cheap check ─────────────────────────────────────────────────────

BYCARD_BASE_URL   = "https://bycard.by"
BYCARD_LISTING_URL = "https://bycard.by/objects/minsk/1"

# Import decode_nuxt/resolve from bycard_parser (same directory, always available).
# No fallback: if import fails, bycard check returns error → skipped_due_to_error.
# A raw-regex fallback would return NUXT variable names (bs, bl, …) instead of
# resolved performanceId values — a different key format that causes false "changed"
# whenever the code switches between paths.
try:
    from bycard_parser import decode_nuxt as _decode_nuxt, resolve as _resolve
    _BYCARD_NUXT_AVAILABLE = True
except ImportError:
    _BYCARD_NUXT_AVAILABLE = False
    log.warning("bycard_parser import failed — bycard fingerprint check disabled")


def _extract_bycard_keys_from_html(html: str) -> list[str]:
    """
    Extract performanceId|date|time keys from a bycard venue page using
    decode_nuxt + resolve (same logic as full parser).

    Returns [] if NUXT cannot be decoded — caller treats venue as contributing
    0 keys (venue_errors counter increments, may trigger skipped_due_to_error
    via MIN_SANE_COUNT check).
    """
    if not _BYCARD_NUXT_AVAILABLE:
        return []

    var_map = _decode_nuxt(html)
    if not var_map:
        # decode_nuxt failed for this page — return nothing, not a guessed fallback.
        return []

    keys: list[str] = []
    seen: set[str] = set()
    for s_tag in BeautifulSoup(html, "html.parser").find_all("script"):
        t = s_tag.string or ""
        if "__NUXT__" not in t:
            continue
        sessions_raw = re.findall(
            r'\{id:(\w+),performanceId:(\w+),name:\w+,timeSpending:(\w+),',
            t,
        )
        for _sid, perf_var, ts_var in sessions_raw:
            perf_id = _resolve(perf_var, var_map)
            ts_raw  = _resolve(ts_var, var_map)
            try:
                dt = datetime.fromtimestamp(int(ts_raw))
                date_str = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M")
            except (ValueError, TypeError, OSError):
                date_str = ts_raw
                time_str = ""

            key = f"{perf_id}|{date_str}|{time_str}"
            if key not in seen:
                seen.add(key)
                keys.append(key)
        break  # first NUXT script only
    return keys


def check_bycard_fingerprint() -> dict:
    """
    Fetch bycard venue listing → collect venue hrefs → per venue extract
    performanceId|date|time from NUXT state.
    """
    if not _BYCARD_NUXT_AVAILABLE:
        return {
            "source_name": "bycard.by",
            "count": 0,
            "hash": "",
            "details": "bycard_parser import unavailable",
            "status": "error",
        }
    try:
        resp = requests.get(BYCARD_LISTING_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        venue_hrefs: list[str] = []
        seen_ids: set[str] = set()
        for a in soup.find_all("a", href=re.compile(r"/objects/minsk/1/\d+")):
            href = a.get("href", "")
            m = re.search(r"/objects/minsk/1/(\d+)", href)
            if m and m.group(1) not in seen_ids:
                seen_ids.add(m.group(1))
                venue_hrefs.append(href)

        if not venue_hrefs:
            log.warning("bycard: no venue links found on listing page")
            return {
                "source_name": "bycard.by",
                "count": 0,
                "hash": "",
                "details": "no venue links found",
                "status": "error",
            }

        all_keys: list[str] = []
        venue_errors = 0
        for href in venue_hrefs:
            try:
                url = BYCARD_BASE_URL + href
                r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                keys = _extract_bycard_keys_from_html(r.text)
                all_keys.extend(keys)
            except Exception as e:
                log.warning(f"  bycard venue {href} error: {e}")
                venue_errors += 1

        log.info(
            f"  bycard: {len(venue_hrefs)} venues ({venue_errors} errors), "
            f"{len(all_keys)} session keys"
        )

        all_failed = venue_errors == len(venue_hrefs)
        status = "error" if all_failed else "ok"

        return {
            "source_name": "bycard.by",
            "count": len(all_keys),
            "hash": _sha256(all_keys) if all_keys else "",
            "details": f"venues={len(venue_hrefs)}, sessions={len(all_keys)}, errors={venue_errors}",
            "status": status,
        }

    except Exception as e:
        log.error(f"bycard check failed: {e}")
        return {
            "source_name": "bycard.by",
            "count": 0,
            "hash": "",
            "details": str(e),
            "status": "error",
        }


CHECK_FNS = {
    **{key: _make_relax_check(key, url) for key, (url, _, _label) in RELAX_CATEGORIES.items()},
    "bycard.by": check_bycard_fingerprint,
}

# ── Action status vocabulary ──────────────────────────────────────────────────
# unchanged          - check ok, fingerprint same as baseline
# changed            - check ok, fingerprint differs → full parse launched
# error              - check failed or returned suspiciously empty result
# fallback_full_parse - no check implemented; always-parse source
# skipped_due_to_error - check errored; parse was NOT launched


# ── Parser runner ─────────────────────────────────────────────────────────────

def run_parser(cmd: str, label: str) -> tuple[bool, list[str]]:
    """Run one parser script as subprocess. Returns (success, RESULT: lines)."""
    full_cmd = [sys.executable] + cmd.split()
    try:
        result = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            timeout=900,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        result_lines = [
            line for line in result.stdout.splitlines()
            if line.startswith("RESULT:")
        ]
        return result.returncode == 0, result_lines
    except subprocess.TimeoutExpired:
        log.error(f"Parser {label} timed out")
        return False, []
    except Exception as e:
        log.error(f"Parser {label} exception: {e}")
        return False, []


def run_single_parser(source_key: str) -> dict:
    """Run the one parser command for source_key. Returns result dict."""
    cmd, label = SOURCE_PARSERS[source_key]
    t0 = time.time()
    ok, lines = run_parser(cmd, label)
    elapsed = round(time.time() - t0, 1)
    log.info(f"  {label}: {'✅' if ok else '❌'} in {elapsed}s")
    return {"label": label, "ok": ok, "results": lines, "elapsed": elapsed}


# ── State update helpers ──────────────────────────────────────────────────────

def _is_sane(source_name: str, count: int, fp_hash: str) -> bool:
    """
    Return True if a check result is considered sane and can overwrite the baseline.
    Criteria: status==ok, count > min threshold, hash non-empty.
    """
    min_count = MIN_SANE_COUNT.get(source_name, 1)
    return count > min_count and bool(fp_hash)


def _record_check(source_name: str, now_iso: str, fp: dict, action: str):
    """Update parser_source_state after a check. Only writes to last_seen_* if sane."""
    count    = fp.get("count", 0)
    fp_hash  = fp.get("hash", "")
    details  = fp.get("details", "")
    sane     = fp.get("status") == "ok" and _is_sane(source_name, count, fp_hash)

    fields: dict = {
        "last_checked_at":   now_iso,
        "last_parse_status": action,
        "last_parse_mode":   "check",
        "last_parse_details": details,
    }
    if sane:
        fields["last_seen_count"] = count
        fields["last_seen_hash"]  = fp_hash

    update_parser_source_state(source_name, **fields)


def _record_parse(source_name: str, now_iso: str, fp: dict, parse_ok: bool):
    """Update parser_source_state after a real full-parse attempt.

    Field update rules:
      last_seen_*         — always updated when sane (we observed this state)
      last_successful_*   — only when parse_ok=True
      last_parse_error_*  — only when parse_ok=False (real parse attempt that failed)
                            NOT updated by cooldown/skip/check-errors — those never
                            call this function, so the contract is enforced structurally.
    """
    count   = fp.get("count", 0)
    fp_hash = fp.get("hash", "")
    sane    = fp.get("status") == "ok" and _is_sane(source_name, count, fp_hash)

    parse_status = "changed" if parse_ok else "parse_error"

    fields: dict = {
        "last_checked_at":    now_iso,
        "last_changed_at":    now_iso,
        "last_parse_status":  parse_status,
        "last_parse_mode":    "full",
        "last_parse_details": fp.get("details", ""),
    }
    if sane:
        fields["last_seen_count"] = count
        fields["last_seen_hash"]  = fp_hash
        if parse_ok:
            fields["last_successful_count"] = count
            fields["last_successful_hash"]  = fp_hash
        else:
            # Only a real parse failure updates last_parse_error_*.
            # This keeps the cooldown window honest: it only extends on actual attempts.
            fields["last_parse_error_hash"] = fp_hash
            fields["last_parse_error_at"]   = now_iso

    update_parser_source_state(source_name, **fields)


# ── Free-pass post-processing helpers ────────────────────────────────────────

def _load_relax_events_from_db() -> list[dict]:
    """Load all relax.by events from DB for free-duplicate matching."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT title, details, description, event_date, show_time, "
                "place, location, price, category, source_url, source_name "
                "FROM events WHERE source_name = 'relax.by'"
            ).fetchall()
        events = [dict(r) for r in rows]
        log.info(f"  free-pass: loaded {len(events)} relax.by events from DB")
        return events
    except Exception as e:
        log.error(f"  free-pass: DB load failed: {e}")
        return []


def _update_free_prices_in_db(events: list[dict]) -> int:
    """Write 'Бесплатно' price for events marked as free by mark_free_duplicates."""
    if not events:
        return 0
    updated = 0
    try:
        with sqlite3.connect(DB_PATH) as conn:
            for ev in events:
                if ev.get("price") == "Бесплатно" and ev.get("_from_free_section"):
                    cur = conn.execute(
                        "UPDATE events SET price = ? WHERE title = ? AND event_date = ? AND place = ?",
                        ("Бесплатно", ev["title"], ev["event_date"], ev["place"]),
                    )
                    updated += cur.rowcount
            conn.commit()
        log.info(f"  free-pass: updated {updated} event prices to 'Бесплатно'")
    except Exception as e:
        log.error(f"  free-pass: DB update failed: {e}")
    return updated


def run_free_pass() -> dict:
    """
    Run relax_parser.py free subprocess, capture EVENTS_JSON output,
    then call mark_free_duplicates + update DB prices.

    Returns result dict (compatible with regular parse_result format) with
    an extra 'free_updated' key for the report.
    """
    t0 = time.time()
    full_cmd = [sys.executable] + RELAX_FREE_CMD.split()
    try:
        proc = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            timeout=900,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        result_lines: list[str] = []
        free_events:  list[dict] = []

        for line in proc.stdout.splitlines():
            s = line.strip()
            if s.startswith("RESULT:"):
                result_lines.append(s)
            elif s.startswith("EVENTS_JSON:"):
                try:
                    free_events.extend(json.loads(s[len("EVENTS_JSON:"):]))
                except Exception as je:
                    log.error(f"  free-pass: EVENTS_JSON parse error: {je}")

        ok = proc.returncode == 0
        elapsed = round(time.time() - t0, 1)
        log.info(f"  {RELAX_FREE_LABEL}: {'✅' if ok else '❌'} in {elapsed}s, "
                 f"{len(free_events)} free events received")

        free_updated = 0
        if ok and free_events and _NORMALIZER_AVAILABLE:
            relax_db = _load_relax_events_from_db()
            if relax_db:
                merged = _mark_free_duplicates(relax_db, free_events)
                free_updated = _update_free_prices_in_db(merged)
        elif free_events and not _NORMALIZER_AVAILABLE:
            log.warning("  free-pass: normalizer unavailable — price update skipped")

        return {
            "label":        RELAX_FREE_LABEL,
            "ok":           ok,
            "results":      result_lines,
            "elapsed":      elapsed,
            "free_updated": free_updated,
        }
    except subprocess.TimeoutExpired:
        log.error(f"  free-pass: timed out")
        return {"label": RELAX_FREE_LABEL, "ok": False, "results": [], "elapsed": round(time.time() - t0, 1), "free_updated": 0}
    except Exception as e:
        log.error(f"  free-pass: exception: {e}")
        return {"label": RELAX_FREE_LABEL, "ok": False, "results": [], "elapsed": round(time.time() - t0, 1), "free_updated": 0}


RELAX_KIDS_CMD   = "relax_parser.py kids"
RELAX_KIDS_LABEL = "🧸 Детям (Relax)"


def run_kids_pass() -> dict:
    """
    Run relax_parser.py kids subprocess, capture EVENTS_JSON output,
    then call apply_kids_pass() to mark is_kids=1 and save unique kids events.
    """
    t0 = time.time()
    full_cmd = [sys.executable] + RELAX_KIDS_CMD.split()
    try:
        proc = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            timeout=900,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )
        kids_events: list[dict] = []
        result_lines: list[str] = []

        for line in proc.stdout.splitlines():
            s = line.strip()
            if s.startswith("RESULT:"):
                result_lines.append(s)
            elif s.startswith("EVENTS_JSON:"):
                try:
                    kids_events.extend(json.loads(s[len("EVENTS_JSON:"):]))
                except Exception as je:
                    log.error(f"  kids-pass: EVENTS_JSON parse error: {je}")

        ok = proc.returncode == 0
        elapsed = round(time.time() - t0, 1)
        log.info(f"  {RELAX_KIDS_LABEL}: {'✅' if ok else '❌'} in {elapsed}s, "
                 f"{len(kids_events)} kids events received")

        kids_marked = 0
        kids_added = 0
        if ok and _NORMALIZER_AVAILABLE:
            try:
                with sqlite3.connect(DB_PATH) as conn:
                    stats = _apply_kids_pass(kids_events, conn)
                    kids_marked = stats.get("marked", 0)
                    kids_added  = stats.get("added", 0)
            except Exception as e:
                log.error(f"  kids-pass: apply_kids_pass failed: {e}")
        elif not _NORMALIZER_AVAILABLE:
            log.warning("  kids-pass: normalizer unavailable — is_kids update skipped")

        return {
            "label":        RELAX_KIDS_LABEL,
            "ok":           ok,
            "results":      result_lines,
            "elapsed":      elapsed,
            "kids_marked":  kids_marked,
            "kids_added":   kids_added,
        }
    except subprocess.TimeoutExpired:
        log.error(f"  kids-pass: timed out")
        return {"label": RELAX_KIDS_LABEL, "ok": False, "results": [], "elapsed": round(time.time() - t0, 1)}
    except Exception as e:
        log.error(f"  kids-pass: exception: {e}")
        return {"label": RELAX_KIDS_LABEL, "ok": False, "results": [], "elapsed": round(time.time() - t0, 1)}


# ── Main orchestrator ─────────────────────────────────────────────────────────

def main():
    t_start = time.time()
    now = datetime.now(MINSK_TZ)
    log.info(f"=== Daytime update started {now.strftime('%Y-%m-%d %H:%M')} ===")

    init_parser_source_state()

    summary: dict = {
        "started_at": now.isoformat(),
        "sources": [],
    }
    now_iso = now.isoformat()
    relax_any_parsed = False  # tracks whether any relax category ran its parser

    # ── 1. Sources with cheap fingerprint check ──────────────────────────────
    for source_name in CHECKABLE_SOURCES:
        t0 = time.time()
        log.info(f"[{source_name}] lightweight check...")

        check_fn = CHECK_FNS.get(source_name)
        if not check_fn:
            log.warning(f"[{source_name}] no check function — skipping")
            continue

        # Run check, catch unexpected exceptions (transport failure, crash, etc.)
        # → status: skipped_due_to_error (critical failure, check didn't complete)
        try:
            fp = check_fn()
        except Exception as e:
            log.error(f"[{source_name}] check exception: {e}")
            update_parser_source_state(
                source_name,
                last_checked_at=now_iso,
                last_parse_status="skipped_due_to_error",
                last_parse_mode="check",
                last_parse_details=f"exception: {e}",
            )
            summary["sources"].append({
                "name": source_name,
                "action": "skipped_due_to_error",
                "details": str(e),
            })
            continue

        count   = fp.get("count", 0)
        fp_hash = fp.get("hash", "")
        details = fp.get("details", "")
        status  = fp.get("status", "error")

        # Sanity check — don't trust suspiciously empty results
        sane = status == "ok" and _is_sane(source_name, count, fp_hash)

        if not sane:
            # Check completed but result is invalid/insane/unsupported
            # → status: error (check ran, result can't be trusted)
            reason = (
                f"count={count} below threshold or hash empty"
                if status == "ok" else f"check status={status}: {details}"
            )
            log.warning(f"[{source_name}] insane/error result ({reason}) — skipping parse, NOT updating baseline")
            update_parser_source_state(
                source_name,
                last_checked_at=now_iso,
                last_parse_status="error",
                last_parse_mode="check",
                last_parse_details=reason,
            )
            summary["sources"].append({
                "name":   source_name,
                "action": "error",
                "details": reason,
                "count":  count,
            })
            continue

        # Compare with last_successful_* only — the stable baseline.
        # Never fall back to last_seen_* here: if a previous full parse failed,
        # last_seen_hash already has the "new" fingerprint but last_successful_hash
        # still has the old one → we must keep detecting "changed" until parse succeeds.
        prev = get_parser_source_state(source_name)
        if prev:
            baseline_hash  = prev.get("last_successful_hash") or ""
            baseline_count = prev.get("last_successful_count") or 0
        else:
            baseline_hash  = ""
            baseline_count = 0

        changed = (fp_hash != baseline_hash) or (count != baseline_count)

        log.info(
            f"[{source_name}] count={count} hash={fp_hash} "
            f"baseline_count={baseline_count} baseline_hash={baseline_hash} "
            f"changed={changed}"
        )

        if not changed:
            log.info(f"[{source_name}] unchanged — skipping parse")
            _record_check(source_name, now_iso, fp, action="unchanged")
            summary["sources"].append({
                "name":   source_name,
                "action": "unchanged",
                "count":  count,
                "hash":   fp_hash,
            })
            continue

        # Changed → check cooldown before launching full parse.
        # If this exact fingerprint already caused a parse_error within the cooldown
        # window, skip the parse to avoid a noise loop (broken source / broken parser).
        if prev:
            err_hash = prev.get("last_parse_error_hash") or ""
            err_at   = prev.get("last_parse_error_at") or ""
            if err_hash == fp_hash and err_at:
                try:
                    err_dt  = datetime.fromisoformat(err_at)
                    age_h   = (now - err_dt).total_seconds() / 3600
                    if age_h < PARSE_ERROR_COOLDOWN_HOURS:
                        log.warning(
                            f"[{source_name}] CHANGED but same fingerprint failed parse "
                            f"{age_h:.1f}h ago (cooldown={PARSE_ERROR_COOLDOWN_HOURS}h) — skipping"
                        )
                        # Still advance last_seen_* — source is genuinely in this state.
                        # Do NOT touch last_successful_* or last_parse_error_*.
                        update_parser_source_state(
                            source_name,
                            last_checked_at=now_iso,
                            last_seen_count=count,
                            last_seen_hash=fp_hash,
                            last_parse_status="parse_error_cooldown",
                            last_parse_mode="check",
                            last_parse_details=f"cooldown active, last_error={err_at}",
                        )
                        summary["sources"].append({
                            "name":       source_name,
                            "action":     "parse_error_cooldown",
                            "count":      count,
                            "hash":       fp_hash,
                            "error_age_h": round(age_h, 1),
                        })
                        continue
                except (ValueError, TypeError):
                    pass  # malformed timestamp — proceed with parse

        log.info(f"[{source_name}] CHANGED (count {baseline_count}→{count}) → running parser")
        parse_result = run_single_parser(source_name)
        elapsed_src  = round(time.time() - t0, 1)
        parse_ok     = parse_result["ok"]

        if source_name in RELAX_CATEGORIES:
            relax_any_parsed = True

        _record_parse(source_name, now_iso, fp, parse_ok)
        summary["sources"].append({
            "name":          source_name,
            "action":        "changed" if parse_ok else "parse_error",
            "count":         count,
            "hash":          fp_hash,
            "parse_results": [parse_result],
            "elapsed":       elapsed_src,
        })

    # ── 2. Free-events pass — runs after ANY relax category was parsed ────────
    # relax_parser.py free cross-checks all relax categories and marks free events.
    # Must run whenever at least one category was updated; skipping it drops
    # "Бесплатно" labels from events that are free but priced in other categories.
    if relax_any_parsed:
        log.info("[relax.by:free] running free-events pass")
        free_result = run_free_pass()
        summary["sources"].append({
            "name":          "relax.by:free",
            "action":        "free_pass",
            "parse_results": [free_result],
            "elapsed":       free_result["elapsed"],
            "free_updated":  free_result.get("free_updated", 0),
        })

        log.info("[relax.by:kids] running kids-pass (is_kids marker)")
        kids_result = run_kids_pass()
        summary["sources"].append({
            "name":          "relax.by:kids",
            "action":        "kids_pass",
            "parse_results": [kids_result],
            "elapsed":       kids_result["elapsed"],
            "kids_marked":   kids_result.get("kids_marked", 0),
            "kids_added":    kids_result.get("kids_added", 0),
        })

    # ── 3. Always-parse sources ──────────────────────────────────────────────
    for source_name in ALWAYS_PARSE_SOURCES:
        t0 = time.time()
        log.info(f"[{source_name}] fallback_full_parse (no check implemented)")
        parse_result = run_single_parser(source_name)
        elapsed_src  = round(time.time() - t0, 1)

        update_parser_source_state(
            source_name,
            last_checked_at=now_iso,
            last_parse_status="fallback_full_parse",
            last_parse_mode="full",
            last_parse_details=f"always-parse, elapsed={elapsed_src}s",
        )
        summary["sources"].append({
            "name":          source_name,
            "action":        "fallback_full_parse",
            "parse_results": [parse_result],
            "elapsed":       elapsed_src,
        })

    summary["duration"] = round(time.time() - t_start, 1)
    log.info(f"=== Daytime update finished in {summary['duration']}s ===")
    print(f"DAYTIME_REPORT:{json.dumps(summary, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
