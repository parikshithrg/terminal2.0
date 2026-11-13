import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from workspace_engine import _global_risk_snapshot, refresh_global_risk


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class GlobalRiskRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "market.sqlite3"
        connection = sqlite3.connect(self.db)
        connection.executescript(
            """
            CREATE TABLE global_risk_observations (
                observation_date TEXT NOT NULL,
                metric_key TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT NOT NULL,
                source_series TEXT NOT NULL,
                source TEXT NOT NULL,
                retrieved_at TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                PRIMARY KEY(observation_date, metric_key)
            );
            CREATE TABLE workspace_source_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_key TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                status TEXT NOT NULL,
                records_written INTEGER NOT NULL,
                detail TEXT
            );
            """
        )
        connection.close()

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def _get(*_args, **_kwargs):
        final = date.today() - timedelta(days=1)
        sessions = [final - timedelta(days=offset) for offset in range(30, -1, -1)]
        timestamps = [int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp()) for day in sessions]
        closes = [100.0 + index for index in range(len(sessions))]
        return _Response({
            "chart": {
                "result": [{
                    "timestamp": timestamps,
                    "indicators": {"quote": [{"close": closes}]},
                }],
                "error": None,
            }
        })

    def test_refresh_stores_all_core_series_and_changes(self):
        with patch("workspace_engine.requests.get", side_effect=self._get):
            payload = refresh_global_risk(self.db)

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "successful")
        self.assertEqual(payload["records_written"], 12)
        self.assertTrue(payload["global_risk"]["available"])
        self.assertEqual(payload["global_risk"]["coverage"]["core_available"], 4)
        self.assertIn("brent_crude", payload["global_risk"]["metrics"])
        self.assertIsNotNone(payload["global_risk"]["metrics"]["brent_crude"]["twenty_session_change_pct"])

        connection = sqlite3.connect(self.db)
        connection.row_factory = sqlite3.Row
        snapshot = _global_risk_snapshot(connection)
        run = connection.execute("SELECT * FROM workspace_source_runs").fetchone()
        connection.close()
        self.assertTrue(snapshot["available"])
        self.assertEqual(run["source_key"], "global_risk_eod")
        self.assertEqual(run["status"], "successful")


if __name__ == "__main__":
    unittest.main()
