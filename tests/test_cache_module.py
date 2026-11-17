import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

import workspace_engine
from workspace_analytics.cache import (
    load_versioned_result,
    load_versioned_results_by_prefix,
    result_cache_status,
    semantic_source_version,
    store_versioned_result,
)


class VersionedResultCacheTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Path(self.tempdir.name) / "market.sqlite3"
        connection = sqlite3.connect(self.db)
        connection.executescript(
            """CREATE TABLE candles(
                   session_date TEXT, retrieved_at TEXT, close REAL
               );
               CREATE TABLE unrelated(note TEXT);"""
        )
        connection.execute(
            "INSERT INTO candles VALUES ('2026-11-13','2026-11-13T12:00:00Z',100)"
        )
        connection.commit()
        connection.close()
        self.specification = ((self.db, (("candles", "session_date", "retrieved_at"),)),)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_cached_payload_round_trips_and_tracks_hits(self):
        version = semantic_source_version(self.specification, {"calendar_date": "2026-11-17"})
        payload = {"ok": True, "contract_version": "example-v1", "value": 7}
        store_versioned_result(self.db, "example", "example-v1", version, payload)
        self.assertEqual(
            load_versioned_result(self.db, "example", "example-v1", version), payload
        )
        status = result_cache_status(self.db)
        self.assertEqual(status["entry_count"], 1)
        self.assertEqual(status["entries"][0]["hit_count"], 1)

    def test_cache_writes_and_unrelated_tables_do_not_change_source_version(self):
        before = semantic_source_version(self.specification)
        store_versioned_result(self.db, "example", "example-v1", before, {"ok": True})
        connection = sqlite3.connect(self.db)
        connection.execute("INSERT INTO unrelated VALUES ('ignore')")
        connection.commit()
        connection.close()
        self.assertEqual(semantic_source_version(self.specification), before)

    def test_new_eod_source_version_replaces_only_matching_contract_key(self):
        first = semantic_source_version(self.specification)
        store_versioned_result(self.db, "dashboard", "overview-v1", first, {"value": 1})
        store_versioned_result(self.db, "other", "other-v1", first, {"value": 9})
        connection = sqlite3.connect(self.db)
        connection.execute(
            "INSERT INTO candles VALUES ('2026-11-14','2026-11-14T12:00:00Z',101)"
        )
        connection.commit()
        connection.close()
        second = semantic_source_version(self.specification)
        self.assertNotEqual(first, second)
        store_versioned_result(self.db, "dashboard", "overview-v1", second, {"value": 2})
        self.assertIsNone(load_versioned_result(self.db, "dashboard", "overview-v1", first))
        self.assertEqual(load_versioned_result(self.db, "dashboard", "overview-v1", second), {"value": 2})
        self.assertEqual(load_versioned_result(self.db, "other", "other-v1", first), {"value": 9})

    def test_stable_research_wrapper_builds_once_per_source_version(self):
        connection = sqlite3.connect(self.db)
        connection.executescript(
            """CREATE TABLE instruments(updated_at TEXT);
               CREATE TABLE workspace_snapshots(as_of_date TEXT,created_at TEXT);"""
        )
        connection.execute("INSERT INTO instruments VALUES ('2026-11-13T12:00:00Z')")
        connection.execute("INSERT INTO workspace_snapshots VALUES ('2026-11-13','2026-11-13T12:00:00Z')")
        connection.commit()
        connection.close()
        builder = Mock(return_value={"ok": True, "rows": [{"symbol": "TEST"}]})
        first = workspace_engine._stable_stock_research_contract(
            self.db, "research", "research-v1", builder,
        )
        second = workspace_engine._stable_stock_research_contract(
            self.db, "research", "research-v1", builder,
        )
        self.assertEqual(first, second)
        builder.assert_called_once_with()

        connection = sqlite3.connect(self.db)
        connection.execute(
            "INSERT INTO candles VALUES ('2026-11-14','2026-11-14T12:00:00Z',101)"
        )
        connection.commit()
        connection.close()
        workspace_engine._stable_stock_research_contract(
            self.db, "research", "research-v1", builder,
        )
        self.assertEqual(builder.call_count, 2)

    def test_prefix_loader_hydrates_only_matching_source_family(self):
        version = semantic_source_version(self.specification)
        store_versioned_result(
            self.db, "universe_oos:first", "oos-cache-v1", version,
            {"engine": "first", "evidence": {"AAA": {"status": "ready"}}},
        )
        store_versioned_result(
            self.db, "universe_oos:second", "oos-cache-v1", version,
            {"engine": "second", "evidence": {}},
        )
        store_versioned_result(
            self.db, "other", "other-v1", version, {"value": 1},
        )
        loaded = load_versioned_results_by_prefix(
            self.db, "universe_oos:", "oos-cache-v1", version,
        )
        self.assertEqual(set(loaded), {"universe_oos:first", "universe_oos:second"})
        self.assertEqual(loaded["universe_oos:first"]["engine"], "first")
