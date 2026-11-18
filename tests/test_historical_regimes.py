import json
import re
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from workspace_analytics.historical_regimes import historical_regimes_overview
from workspace_analytics.regimes import MARKET_REGIME_CONTRACT
from workspace_analytics.sqlite_store import connect_database


ROOT = Path(__file__).resolve().parents[1]


class HistoricalRegimeOverviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "regimes.sqlite3"
        connection = connect_database(self.database)
        connection.execute(
            """CREATE TABLE market_regime_ledger(
                   contract_version TEXT, session_date TEXT,
                   classification_status TEXT, ranking_admissible INTEGER,
                   regime_family TEXT, trend_direction TEXT, trend_strength TEXT,
                   range_state TEXT, volatility_state TEXT, breadth_state TEXT,
                   regime_key TEXT, market_return_20d_pct REAL,
                   market_distance_sma200_pct REAL, market_volatility_20d_pct REAL,
                   market_breadth_50dma_pct REAL, market_breadth_200dma_pct REAL,
                   priced_coverage_ratio REAL, breadth_50dma_coverage_ratio REAL,
                   breadth_200dma_coverage_ratio REAL, source_dates_json TEXT,
                   missing_inputs_json TEXT
               )"""
        )
        rows = [
            ("2026-01-02", "bearish_trend", "down", -8.0, 24.0, 30.0, 35.0),
            ("2026-01-05", "bearish_trend", "down", -6.0, 22.0, 34.0, 38.0),
            ("2026-01-06", "transition", "neutral", -1.0, 18.0, 44.0, 42.0),
            ("2026-01-07", "range", "neutral", 1.0, 14.0, 50.0, 48.0),
            ("2026-01-08", "bullish_trend", "up", 6.0, 16.0, 64.0, 58.0),
            ("2026-01-09", "bullish_trend", "up", 8.0, 17.0, 68.0, 61.0),
        ]
        for session, family, direction, return20, volatility, breadth50, breadth200 in rows:
            connection.execute(
                """INSERT INTO market_regime_ledger VALUES(
                       ?,?,'descriptive_only',0,?,?, 'moderate','transition','normal',?,
                       ?,?,0,?,?,?,0.98,0.98,0.98,?,?)""",
                (
                    MARKET_REGIME_CONTRACT, session, family, direction,
                    "positive" if direction == "up" else "negative",
                    f"{family}/moderate/transition/normal",
                    return20, volatility, breadth50, breadth200,
                    json.dumps({"market": session}), json.dumps([]),
                ),
            )
        connection.commit()
        connection.close()

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_overview_publishes_distribution_segments_and_current_state(self) -> None:
        result = historical_regimes_overview(self.database, limit=100)
        self.assertEqual(result["contract_version"], "historical-regimes-overview-v3")
        self.assertEqual(result["status"], "descriptive")
        self.assertEqual(result["coverage"]["sessions"], 6)
        self.assertEqual(result["coverage"]["segments"], 4)
        self.assertEqual(result["coverage"]["transitions"], 3)
        self.assertEqual(result["current"]["regime_family"], "bullish_trend")
        distribution = {row["regime_family"]: row for row in result["distribution"]}
        self.assertEqual(distribution["bearish_trend"]["sessions"], 2)
        self.assertEqual(distribution["bullish_trend"]["share_pct"], 33.33)
        self.assertEqual(result["segments"][0]["sessions"], 2)
        self.assertEqual(result["segments"][-1]["sessions"], 2)
        self.assertEqual(result["policy_cycle"]["status"], "gated")
        self.assertEqual(result["analogues"]["status"], "gated")
        self.assertIn("completed_session_prices", result["analogues"]["gates"])
        self.assertFalse(result["orders_enabled"])

    def test_trailing_only_analogue_engine_passes_with_sufficient_independent_episodes(self) -> None:
        connection = connect_database(self.database)
        connection.execute("DELETE FROM market_regime_ledger")
        connection.execute(
            """CREATE TABLE candles(
                   instrument_id TEXT, session_date TEXT, close REAL,
                   validation_status TEXT
               )"""
        )
        session = date(2024, 1, 2)
        sessions = []
        while len(sessions) < 500:
            if session.weekday() < 5:
                sessions.append(session.isoformat())
            session += timedelta(days=1)
        families = ("bearish_trend", "transition", "range", "bullish_trend")
        for index, session_date in enumerate(sessions):
            family = families[(index // 10) % len(families)]
            direction = "up" if family == "bullish_trend" else ("down" if family == "bearish_trend" else "neutral")
            connection.execute(
                """INSERT INTO market_regime_ledger VALUES(
                       ?,?,'descriptive_only',0,?,?, 'moderate','transition','normal',?,
                       ?,?,0,?,?,?,0.98,0.98,0.98,?,?)""",
                (
                    MARKET_REGIME_CONTRACT, session_date, family, direction,
                    "positive" if direction == "up" else ("negative" if direction == "down" else "mixed"),
                    f"{family}/moderate/transition/normal",
                    float((index % 40) - 20), 15.0, 55.0, 50.0,
                    json.dumps({"market": session_date}), json.dumps([]),
                ),
            )
            connection.execute(
                "INSERT INTO candles VALUES('index:Nifty 50',?,?, 'validated')",
                (session_date, 10000.0 + index * 5.0 + (index % 7) * 2.0),
            )
        connection.commit()
        connection.close()

        result = historical_regimes_overview(self.database, limit=500)
        analogues = result["analogues"]
        self.assertEqual(analogues["status"], "ready")
        self.assertEqual(analogues["release_decision"], "descriptive_ready")
        self.assertEqual(analogues["release_audit"]["passed"], analogues["release_audit"]["total"])
        self.assertGreaterEqual(analogues["current"]["windows"]["60"]["sample_size"], 5)
        self.assertTrue(analogues["rules"]["trailing_only"])
        self.assertFalse(analogues["rules"]["overlapping_anchors"])

    def test_validated_rbi_events_publish_policy_period_context(self) -> None:
        connection = connect_database(self.database)
        connection.execute(
            """CREATE TABLE historical_series_observations(
                   series_key TEXT, observation_date TEXT, period_label TEXT,
                   frequency TEXT, value REAL, unit TEXT, base_period TEXT,
                   source_title TEXT, source_url TEXT, source_authority TEXT,
                   vintage_date TEXT, retrieved_at TEXT, validation_status TEXT,
                   metadata_json TEXT, schema_version INTEGER
               )"""
        )
        for session, rate, change, decision in (
            ("2025-12-05", 5.25, -25, "decrease"),
            ("2026-01-06", 5.25, 0, "hold"),
            ("2026-01-08", 5.50, 25, "increase"),
        ):
            connection.execute(
                "INSERT INTO historical_series_observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "india_policy_repo_rate_pct", session, session, "event", rate,
                    "Per cent per annum", "Not applicable", f"RBI MPC {session}",
                    f"https://rbi.example/{session}", "Reserve Bank of India", session,
                    f"{session}T12:00:00+00:00", "validated",
                    json.dumps({"change_bps": change, "decision": decision}), 1,
                ),
            )
        connection.commit()
        connection.close()
        result = historical_regimes_overview(self.database, limit=100)
        policy = result["policy_cycle"]
        self.assertEqual(policy["status"], "ready")
        self.assertEqual(policy["coverage"]["validated_events"], 3)
        self.assertEqual(policy["current"]["repo_rate_pct"], 5.5)
        self.assertEqual(policy["current"]["cycle_phase"], "tightening")
        self.assertEqual(policy["periods"][0]["classified_sessions"], 2)
        self.assertEqual(policy["periods"][0]["dominant_regime"], "bullish_trend")

    def test_missing_ledger_returns_honest_gate(self) -> None:
        missing = Path(self.directory.name) / "missing.sqlite3"
        result = historical_regimes_overview(missing)
        self.assertEqual(result["status"], "gated")
        self.assertIn("market_regime_ledger_missing", result["gates"])
        self.assertEqual(result["segments"], [])

    def test_limit_is_bounded(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid_historical_regime_limit"):
            historical_regimes_overview(self.database, limit=5)

    def test_historical_regime_page_contract_and_endpoint_are_present(self) -> None:
        html = (ROOT / "dist" / "index.html").read_text()
        script = (ROOT / "dist" / "app.js").read_text()
        server = (ROOT / "server.py").read_text()
        ids = re.findall(r'\bid="([^"]+)"', html)
        self.assertEqual(len(ids), len(set(ids)), "duplicate HTML ids")
        required = {
            "historicalRegimesView", "regimeTimeline", "regimeDistribution",
            "regimePolicyDecisionList", "regimeCrossAssetList",
            "regimeAnalogueStatus", "regimeAnalogueWindows",
            "regimeAnalogueAudit", "regimeAnalogueBody", "regimeEvidenceBody",
        }
        self.assertTrue(required.issubset(set(ids)))
        self.assertIn("/api/historical-regimes/overview", script + server)
        self.assertIn("aria-label=\"Historical regime summary\"", html)
        self.assertIn("aria-label=\"${escapeHtml(title)}\"", script)
        self.assertIn("Forward outcomes never alter the classifier", script)

    def test_historical_regime_responsive_rules_are_frozen(self) -> None:
        styles = (ROOT / "dist" / "styles.css").read_text()
        for breakpoint in (
            "@media (max-width: 1100px)",
            "@media (max-width: 760px)",
            "@media (max-width: 520px)",
        ):
            self.assertIn(breakpoint, styles)
        self.assertIn(".regime-analogue-layout { grid-template-columns: 1fr; }", styles)
        self.assertIn(".regime-analogue-windows { grid-template-columns: 1fr; }", styles)


if __name__ == "__main__":
    unittest.main()
