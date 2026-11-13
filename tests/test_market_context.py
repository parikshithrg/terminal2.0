import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from workspace_engine import _breadth, _relative_strength


class CurrentBreadthUniverseTests(unittest.TestCase):
    def test_excludes_removed_stocks_from_current_nifty500_breadth(self):
        with tempfile.TemporaryDirectory() as folder:
            market_db = Path(folder) / "market.sqlite3"
            snapshot_db = Path(folder) / "snapshots.sqlite3"
            connection = sqlite3.connect(market_db)
            connection.row_factory = sqlite3.Row
            connection.executescript(
                """
                CREATE TABLE instruments (
                    instrument_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL
                );
                CREATE TABLE candles (
                    instrument_id TEXT NOT NULL,
                    session_date TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL,
                    PRIMARY KEY(instrument_id, session_date)
                );
                """
            )
            for symbol in ("A", "B", "REMOVED"):
                connection.execute("INSERT INTO instruments VALUES (?, 'stock')", (f"stock:{symbol}",))
            start = date(2026, 1, 1)
            for symbol, sessions in (("A", 220), ("B", 60), ("REMOVED", 220)):
                for index in range(sessions):
                    close = 100 + index
                    connection.execute(
                        "INSERT INTO candles VALUES (?,?,?,?,?,?,?)",
                        (f"stock:{symbol}", (start + timedelta(days=index)).isoformat(),
                         close, close, close, close, 1000),
                    )
            connection.commit()

            snapshots = sqlite3.connect(snapshot_db)
            snapshots.execute(
                """CREATE TABLE index_membership_snapshots (
                    index_name TEXT, snapshot_date TEXT, symbol TEXT
                )"""
            )
            snapshots.executemany(
                "INSERT INTO index_membership_snapshots VALUES ('NIFTY 500','2026-11-12',?)",
                [("A",), ("B",)],
            )
            snapshots.commit()
            snapshots.close()

            result = _breadth(connection, snapshot_db)
            connection.close()

        self.assertEqual(result["official_count"], 2)
        self.assertEqual(result["universe_count"], 2)
        self.assertEqual(result["registered_history_count"], 3)
        self.assertEqual(result["excluded_noncurrent_count"], 1)
        self.assertEqual(result["evaluated_50dma"], 2)
        self.assertEqual(result["evaluated_200dma"], 1)
        self.assertEqual(result["evaluated"], 1)
        self.assertEqual(result["universe_contract"], "latest_official_nifty500_snapshot")


class RelativeStrengthTests(unittest.TestCase):
    def test_reports_top_bottom_three_ytd_and_pull_date(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "market.sqlite3"
            connection = sqlite3.connect(db)
            connection.row_factory = sqlite3.Row
            connection.executescript(
                """
                CREATE TABLE instruments (
                    instrument_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    display_name TEXT NOT NULL
                );
                CREATE TABLE candles (
                    instrument_id TEXT NOT NULL,
                    session_date TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL,
                    retrieved_at TEXT NOT NULL,
                    PRIMARY KEY(instrument_id, session_date)
                );
                """
            )
            names = ["Nifty 50", "Index A", "Index B", "Index C", "Index D", "Index E", "Index F", "India VIX"]
            slopes = [1.0, 2.2, 1.8, 1.4, 0.7, 0.2, -0.3, 4.0]
            sessions = [date(2025, 12, 31)] + [date(2026, 1, 1) + timedelta(days=day) for day in range(25)]
            for name, slope in zip(names, slopes):
                instrument_id = f"index:{name}"
                connection.execute("INSERT INTO instruments VALUES (?, 'index', ?)", (instrument_id, name))
                for index, session in enumerate(sessions):
                    close = 100.0 + slope * index
                    connection.execute(
                        "INSERT INTO candles VALUES (?,?,?,?,?,?,?,?)",
                        (
                            instrument_id, session.isoformat(), close, close, close, close, 1000,
                            "2026-01-27T05:30:00+00:00",
                        ),
                    )
            connection.commit()

            result = _relative_strength(connection)
            connection.close()

        self.assertEqual(len(result["indices"]["leaders"]), 3)
        self.assertEqual(len(result["indices"]["laggards"]), 3)
        self.assertNotIn("India VIX", [row["name"] for row in result["indices"]["leaders"]])
        self.assertEqual(result["indices"]["leaders"][0]["name"], "Index A")
        self.assertEqual(result["indices"]["laggards"][0]["name"], "Index F")
        self.assertIsNotNone(result["indices"]["leaders"][0]["ytd_return_pct"])
        self.assertEqual(result["as_of_date"], "2026-01-25")
        self.assertEqual(result["retrieved_at"], "2026-01-27T05:30:00+00:00")
        self.assertIsNotNone(result["benchmark"]["ytd_return_pct"])


if __name__ == "__main__":
    unittest.main()
