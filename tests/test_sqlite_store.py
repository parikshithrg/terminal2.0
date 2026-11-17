import sqlite3
import tempfile
import unittest
from pathlib import Path

from workspace_analytics.sqlite_store import (
    SQLITE_BUSY_TIMEOUT_MS,
    connect_database,
    connection_policy,
)


class SQLiteStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "workspace.sqlite3"

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_write_connections_share_wal_and_timeout_policy(self) -> None:
        connection = connect_database(self.database)
        try:
            policy = connection_policy(connection)
            self.assertEqual(policy["journal_mode"], "wal")
            self.assertEqual(policy["busy_timeout_ms"], SQLITE_BUSY_TIMEOUT_MS)
            self.assertTrue(policy["foreign_keys"])
            self.assertEqual(policy["synchronous"], 1)  # SQLite NORMAL
            self.assertFalse(policy["query_only"])
            connection.execute("CREATE TABLE observations(id INTEGER PRIMARY KEY, value TEXT)")
            connection.execute("INSERT INTO observations(value) VALUES ('stored')")
            connection.commit()
        finally:
            connection.close()

        reopened = connect_database(self.database)
        try:
            self.assertEqual(reopened.execute("SELECT value FROM observations").fetchone()[0], "stored")
            self.assertEqual(connection_policy(reopened)["journal_mode"], "wal")
        finally:
            reopened.close()

    def test_wal_reader_is_not_blocked_by_uncommitted_writer(self) -> None:
        writer = connect_database(self.database)
        writer.execute("CREATE TABLE observations(id INTEGER PRIMARY KEY, value TEXT)")
        writer.execute("INSERT INTO observations(value) VALUES ('committed')")
        writer.commit()
        reader = connect_database(self.database, read_only=True)
        try:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute("INSERT INTO observations(value) VALUES ('pending')")
            rows = reader.execute("SELECT value FROM observations ORDER BY id").fetchall()
            self.assertEqual([row[0] for row in rows], ["committed"])
            self.assertTrue(connection_policy(reader)["query_only"])
            with self.assertRaises(sqlite3.OperationalError):
                reader.execute("INSERT INTO observations(value) VALUES ('blocked')")
        finally:
            writer.rollback()
            reader.close()
            writer.close()

    def test_point_in_time_indexes_match_research_lookup_shapes(self) -> None:
        from workspace_engine import _ensure_workspace_query_indexes

        connection = connect_database(self.database)
        try:
            connection.executescript(
                """
                CREATE TABLE instruments(instrument_id TEXT PRIMARY KEY, kind TEXT);
                CREATE TABLE universe_classification_snapshots(
                    index_name TEXT, snapshot_date TEXT, symbol TEXT,
                    PRIMARY KEY(index_name, snapshot_date, symbol)
                );
                CREATE TABLE universe_membership_events(
                    index_name TEXT, symbol TEXT, action TEXT, effective_date TEXT,
                    PRIMARY KEY(index_name, symbol, action, effective_date)
                );
                CREATE TABLE symbol_history(
                    lineage_key TEXT PRIMARY KEY, instrument_id TEXT, effective_date TEXT
                );
                """
            )
            _ensure_workspace_query_indexes(connection)
            indexes = {
                row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'"
                )
            }
            self.assertTrue({
                "instruments_kind_id_idx",
                "universe_classification_symbol_date_idx",
                "universe_membership_symbol_date_idx",
                "symbol_history_instrument_date_idx",
                "symbol_history_effective_date_idx",
            }.issubset(indexes))
            plan = " ".join(
                str(value)
                for row in connection.execute(
                    """EXPLAIN QUERY PLAN
                       SELECT snapshot_date FROM universe_classification_snapshots
                       WHERE index_name='NIFTY 500' AND symbol=? AND snapshot_date<=?
                       ORDER BY snapshot_date DESC LIMIT 1""",
                    ("ABC", "2026-11-17"),
                )
                for value in row
            )
            self.assertIn("universe_classification_symbol_date_idx", plan)
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
