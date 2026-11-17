import tempfile
import unittest
from pathlib import Path

from workspace_analytics.jobs import (
    finish_job,
    job_status,
    list_jobs,
    start_job,
    update_job,
)


class BackgroundJobTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "jobs.sqlite3"

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_progress_is_persistent_and_observable(self) -> None:
        started = start_job(
            self.database,
            "oos-family:abc",
            "full_history_oos_strategy_family",
            "abc",
            total_units=10,
            completed_units=2,
            detail={"completed_engines": ["sma", "ema"]},
        )
        self.assertEqual(started["status"], "running")
        self.assertEqual(started["progress_pct"], 20.0)
        self.assertFalse(started["terminal"])

        updated = update_job(
            self.database,
            "oos-family:abc",
            completed_units=5,
            current_stage="donchian_breakout",
            detail={"completed_engines": ["sma", "ema", "tsm", "triple", "rsi"]},
        )
        self.assertEqual(updated["progress_pct"], 50.0)
        self.assertEqual(updated["current_stage"], "donchian_breakout")
        self.assertEqual(job_status(self.database, "oos-family:abc")["completed_units"], 5)

        finished = finish_job(self.database, "oos-family:abc")
        self.assertEqual(finished["status"], "complete")
        self.assertEqual(finished["progress_pct"], 100.0)
        self.assertTrue(finished["terminal"])
        self.assertIsNotNone(finished["completed_at"])
        jobs = list_jobs(self.database)
        self.assertEqual(jobs["count"], 1)
        self.assertEqual(jobs["jobs"][0]["job_id"], "oos-family:abc")

    def test_terminal_errors_and_bounds_are_explicit(self) -> None:
        start_job(
            self.database,
            "oos-family:def",
            "full_history_oos_strategy_family",
            "def",
            total_units=3,
        )
        update_job(
            self.database,
            "oos-family:def",
            completed_units=1,
            current_stage="ema_crossover",
            errors={"moving_average_crossover": "gated"},
        )
        failed = finish_job(
            self.database,
            "oos-family:def",
            status="failed",
            errors={"background_job": "stopped"},
        )
        self.assertEqual(failed["progress_pct"], 33.3)
        self.assertEqual(failed["errors"]["moving_average_crossover"], "gated")
        self.assertEqual(failed["errors"]["background_job"], "stopped")
        self.assertTrue(failed["terminal"])
        with self.assertRaisesRegex(ValueError, "invalid_background_job_units"):
            update_job(self.database, "oos-family:def", completed_units=4)
        with self.assertRaisesRegex(ValueError, "invalid_background_job_limit"):
            list_jobs(self.database, limit=0)


if __name__ == "__main__":
    unittest.main()
