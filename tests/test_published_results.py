import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from workspace_analytics.execution import equity_delivery_cost_calculator
from workspace_analytics.jobs import finish_job, list_jobs, start_job, update_job
from workspace_analytics.ranking import cross_strategy_fdr_from_payloads
from workspace_analytics.regimes import market_regime_classification
from workspace_analytics.trend_strategies import moving_average_crossover_series


FIXTURE = Path(__file__).parent / "fixtures" / "published_analytics_v1.json"


class PublishedResultRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = json.loads(FIXTURE.read_text())
        if cls.fixture.get("fixture_contract") != "published-analytics-regression-fixture-v1":
            raise AssertionError("unexpected_published_fixture_contract")

    def test_equity_delivery_cost_result_is_frozen(self) -> None:
        inputs = self.fixture["execution"]
        result = equity_delivery_cost_calculator(
            inputs["buy_turnover_inr"], inputs["sell_turnover_inr"]
        )
        keys = self.fixture["expected"]["execution"].keys()
        self.assertEqual({key: result[key] for key in keys}, self.fixture["expected"]["execution"])

    def test_trend_signal_result_is_frozen(self) -> None:
        inputs = self.fixture["trend"]
        start = date.fromisoformat(inputs["start_date"])
        rows = [
            {
                "session_date": (start + timedelta(days=index)).isoformat(),
                "open": close,
                "close": close,
            }
            for index, close in enumerate(inputs["closes"])
        ]

        def metrics(_rows, signals, _start, events):
            return {
                "status": "ready",
                "current_signal": signals[-1],
                "last_event": events[-1] if events else None,
                "events": events,
            }

        result = moving_average_crossover_series(
            rows, inputs["fast_window"], inputs["slow_window"], metrics=metrics
        )
        keys = self.fixture["expected"]["trend"].keys()
        self.assertEqual({key: result[key] for key in keys}, self.fixture["expected"]["trend"])

    def test_market_regime_result_is_frozen(self) -> None:
        inputs = self.fixture["regime"]
        result = market_regime_classification(
            [dict(inputs["row"]) for _ in range(inputs["repeat"])]
        )
        keys = self.fixture["expected"]["regime"].keys()
        self.assertEqual({key: result[key] for key in keys}, self.fixture["expected"]["regime"])

    def test_cross_strategy_fdr_result_is_frozen(self) -> None:
        result = cross_strategy_fdr_from_payloads(self.fixture["cross_strategy_payloads"])
        self.assertEqual(result, self.fixture["expected"]["cross_strategy_fdr"])

    def test_persistent_job_contract_survives_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "jobs.sqlite3"
            start_job(database, "fixture-job", "fixture-build", "fixture-v1", 4, 1)
            update_job(database, "fixture-job", 3, current_stage="ranking")
            finish_job(database, "fixture-job", "complete")
            result = list_jobs(database)
        self.assertEqual(result["contract_version"], "background-research-jobs-v1")
        self.assertEqual(result["count"], 1)
        job = result["jobs"][0]
        self.assertEqual(job["contract_version"], "background-research-job-v1")
        self.assertEqual(job["status"], "complete")
        self.assertEqual(job["progress_pct"], 100.0)
        self.assertTrue(job["terminal"])
        self.assertIsNone(job["current_stage"])


if __name__ == "__main__":
    unittest.main()
