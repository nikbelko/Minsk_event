#!/usr/bin/env python3
"""
Scenario tests for daytime_update state machine.
Covers exactly the 4 pre-merge scenarios without HTTP or subprocess.

Run: python test_daytime_scenarios.py
"""

import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call, patch

# ── Patch DB and subprocess before importing the module ───────────────────────
# daytime_update imports parser_state which calls sqlite3 at import time (init),
# so we patch the whole module-level functions.

import importlib

MINSK_TZ = timezone(timedelta(hours=3))

# We'll reload with patches in setUp via direct patching of the functions after import.


SOURCE_KEY = "relax.by:theatre"  # representative relax category


def _make_fp(hash_val: str, count: int = 50, status: str = "ok") -> dict:
    return {"source_name": SOURCE_KEY, "count": count, "hash": hash_val,
            "details": "test", "status": status}


def _make_prev(
    successful_hash: str = "",
    successful_count: int = 0,
    seen_hash: str = "",
    seen_count: int = 0,
    error_hash: str = "",
    error_at: str = "",
) -> dict:
    return {
        "source_name":           SOURCE_KEY,
        "last_successful_hash":  successful_hash,
        "last_successful_count": successful_count,
        "last_seen_hash":        seen_hash,
        "last_seen_count":       seen_count,
        "last_parse_error_hash": error_hash,
        "last_parse_error_at":   error_at,
        "last_parse_status":     "",
        "last_parse_mode":       "",
        "last_parse_details":    "",
        "last_checked_at":       "",
        "last_changed_at":       "",
    }


class TestDaytimeScenarios(unittest.TestCase):

    def setUp(self):
        # Import with heavy side-effects suppressed
        with patch("parser_state.init_parser_source_state"), \
             patch("parser_state.get_parser_source_state"), \
             patch("parser_state.update_parser_source_state"):
            import daytime_update as du
            self.du = du

        # Fix "now" so cooldown arithmetic is deterministic
        self.now = datetime(2026, 4, 5, 13, 0, 0, tzinfo=MINSK_TZ)
        self.now_iso = self.now.isoformat()

    # ── helpers ───────────────────────────────────────────────────────────────

    def _run_changed_branch(self, fp, prev_state, parse_results):
        """
        Simulate the 'changed' branch of the orchestrator for relax.by.
        Patches: get/update state, run_source_parsers, datetime.now, check fn.
        Returns the recorded update_parser_source_state calls as a list of kwargs dicts.
        """
        du = self.du
        recorded: list[dict] = []

        def fake_update(source_name, **kwargs):
            recorded.append({"source_name": source_name, **kwargs})

        # run_single_parser returns a single dict; wrap parse_results[0] for the mock
        single_result = parse_results[0] if parse_results else {"ok": True, "results": [], "elapsed": 0}
        with patch.object(du, "get_parser_source_state", return_value=prev_state), \
             patch.object(du, "update_parser_source_state", side_effect=fake_update), \
             patch.object(du, "run_single_parser", return_value=single_result), \
             patch.object(du, "run_parser", return_value=(True, [])), \
             patch.object(du, "init_parser_source_state"), \
             patch.object(du, "CHECK_FNS", {SOURCE_KEY: lambda: fp}), \
             patch.object(du, "CHECKABLE_SOURCES", [SOURCE_KEY]), \
             patch.object(du, "ALWAYS_PARSE_SOURCES", []), \
             patch("daytime_update.datetime") as mock_dt:

            mock_dt.now.return_value = self.now
            mock_dt.fromisoformat.side_effect = datetime.fromisoformat  # real parsing

            du.main()

        return recorded

    # ── Scenario 1: changed → parse ok ───────────────────────────────────────

    def test_scenario1_changed_parse_ok(self):
        """changed → parse ok: last_seen_*, last_successful_* updated; status=changed"""
        fp = _make_fp("newhash")
        prev = _make_prev(successful_hash="oldhash", successful_count=40,
                          seen_hash="oldhash", seen_count=40)
        parse_ok_results = [{"ok": True, "results": [], "elapsed": 1.0, "label": "test"}]

        calls = self._run_changed_branch(fp, prev, parse_ok_results)

        # Find the parse-result write (last_parse_mode=full)
        full_write = next(c for c in calls if c.get("last_parse_mode") == "full")

        self.assertEqual(full_write["last_parse_status"], "changed")
        self.assertEqual(full_write["last_seen_hash"],        "newhash")
        self.assertEqual(full_write["last_seen_count"],       50)
        self.assertEqual(full_write["last_successful_hash"],  "newhash")
        self.assertEqual(full_write["last_successful_count"], 50)
        self.assertNotIn("last_parse_error_hash", full_write)
        self.assertNotIn("last_parse_error_at",   full_write)

    # ── Scenario 2: changed → parse error ────────────────────────────────────

    def test_scenario2_changed_parse_error(self):
        """changed → parse error: last_seen_* updated; last_successful_* NOT touched;
        last_parse_error_* updated; status=parse_error"""
        fp = _make_fp("newhash")
        prev = _make_prev(successful_hash="oldhash", successful_count=40)
        parse_fail_results = [{"ok": False, "results": [], "elapsed": 1.0, "label": "test"}]

        calls = self._run_changed_branch(fp, prev, parse_fail_results)

        full_write = next(c for c in calls if c.get("last_parse_mode") == "full")

        self.assertEqual(full_write["last_parse_status"], "parse_error")
        # last_seen_* updated
        self.assertEqual(full_write["last_seen_hash"],  "newhash")
        self.assertEqual(full_write["last_seen_count"], 50)
        # last_successful_* NOT in this write
        self.assertNotIn("last_successful_hash",  full_write)
        self.assertNotIn("last_successful_count", full_write)
        # last_parse_error_* updated
        self.assertEqual(full_write["last_parse_error_hash"], "newhash")
        self.assertIn("last_parse_error_at", full_write)

    # ── Scenario 3: same failing hash < 6h → cooldown ────────────────────────

    def test_scenario3_cooldown_active(self):
        """Same hash that failed parse < 6h ago: parse NOT launched;
        last_seen_* updated; last_parse_error_* NOT changed; status=parse_error_cooldown"""
        du = self.du
        fp = _make_fp("badhash")
        error_at = (self.now - timedelta(hours=2)).isoformat()  # 2h ago < 6h cooldown
        prev = _make_prev(
            successful_hash="oldhash", successful_count=40,
            error_hash="badhash", error_at=error_at,
        )

        recorded: list[dict] = []
        parse_called = []

        def fake_update(source_name, **kwargs):
            recorded.append({"source_name": source_name, **kwargs})

        def fake_parse(source_name):
            parse_called.append(source_name)
            return {"ok": True, "results": [], "elapsed": 1.0, "label": "test"}

        with patch.object(du, "get_parser_source_state", return_value=prev), \
             patch.object(du, "update_parser_source_state", side_effect=fake_update), \
             patch.object(du, "run_single_parser", side_effect=fake_parse), \
             patch.object(du, "run_parser", return_value=(True, [])), \
             patch.object(du, "init_parser_source_state"), \
             patch.object(du, "CHECK_FNS", {SOURCE_KEY: lambda: fp}), \
             patch.object(du, "CHECKABLE_SOURCES", [SOURCE_KEY]), \
             patch.object(du, "ALWAYS_PARSE_SOURCES", []), \
             patch("daytime_update.datetime") as mock_dt:

            mock_dt.now.return_value = self.now
            mock_dt.fromisoformat.side_effect = datetime.fromisoformat

            du.main()

        # parse must NOT have been called
        self.assertEqual(parse_called, [], "run_source_parsers must not be called during cooldown")

        # find the cooldown write
        cooldown_write = next(
            (c for c in recorded if c.get("last_parse_status") == "parse_error_cooldown"),
            None,
        )
        self.assertIsNotNone(cooldown_write, "Expected a parse_error_cooldown write")

        # last_seen_* updated
        self.assertEqual(cooldown_write["last_seen_hash"],  "badhash")
        self.assertEqual(cooldown_write["last_seen_count"], 50)

        # last_parse_error_* must NOT appear in cooldown write
        self.assertNotIn("last_parse_error_hash", cooldown_write)
        self.assertNotIn("last_parse_error_at",   cooldown_write)

    # ── Scenario 4: new hash after previous parse error ───────────────────────

    def test_scenario4_new_hash_bypasses_cooldown(self):
        """Different hash from the one that failed: cooldown does NOT block;
        full parse launches immediately"""
        du = self.du
        fp = _make_fp("newdifferenthash")
        error_at = (self.now - timedelta(hours=1)).isoformat()  # very recent error
        prev = _make_prev(
            successful_hash="oldhash", successful_count=40,
            error_hash="badhash",       # different from fp hash!
            error_at=error_at,
        )
        parse_called = []

        def fake_parse(source_name):
            parse_called.append(source_name)
            return {"ok": True, "results": [], "elapsed": 1.0, "label": "test"}

        with patch.object(du, "get_parser_source_state", return_value=prev), \
             patch.object(du, "update_parser_source_state"), \
             patch.object(du, "run_single_parser", side_effect=fake_parse), \
             patch.object(du, "run_parser", return_value=(True, [])), \
             patch.object(du, "init_parser_source_state"), \
             patch.object(du, "CHECK_FNS", {SOURCE_KEY: lambda: fp}), \
             patch.object(du, "CHECKABLE_SOURCES", [SOURCE_KEY]), \
             patch.object(du, "ALWAYS_PARSE_SOURCES", []), \
             patch("daytime_update.datetime") as mock_dt:

            mock_dt.now.return_value = self.now
            mock_dt.fromisoformat.side_effect = datetime.fromisoformat

            du.main()

        self.assertIn(SOURCE_KEY, parse_called,
                      "run_single_parser must be called for a new fingerprint hash")


if __name__ == "__main__":
    # Run with verbose output so each scenario is clearly labelled
    loader = unittest.TestLoader()
    loader.sortTestMethodsUsing = None  # preserve definition order
    suite = loader.loadTestsFromTestCase(TestDaytimeScenarios)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
