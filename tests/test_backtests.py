import unittest
import sqlite3
import tempfile
from datetime import date, timedelta
from pathlib import Path

from workspace_engine import (
    _bollinger_mean_reversion_series,
    _chronological_holdout_validation,
    _donchian_breakout_series,
    equity_delivery_cost_calculator,
    _ema_crossover_series,
    _moving_average_crossover_series,
    _pullback_uptrend_series,
    _rsi_mean_reversion_series,
    _rolling_walk_forward_validation,
    _selloff_recovery_series,
    _time_series_momentum_series,
    _trade_edge_p_value,
    _triple_sma_alignment_series,
    _walk_forward_stability_gate,
    _zscore_mean_reversion_series,
    _quality_checked_backtest_series,
    _historical_size_map,
    point_in_time_universe_contract,
    removed_constituent_history_audit,
)


class HistoricalSizeClassificationTests(unittest.TestCase):
    def test_reverses_official_semiannual_bucket_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot_db = Path(directory) / "snapshots.sqlite3"
            connection = sqlite3.connect(snapshot_db)
            connection.execute(
                """CREATE TABLE equity_size_classifications(
                       snapshot_date TEXT,symbol TEXT,size_bucket TEXT,
                       source_index TEXT,source_url TEXT,retrieved_at TEXT,
                       validation_status TEXT)"""
            )
            connection.executemany(
                "INSERT INTO equity_size_classifications VALUES ('2026-11-12',?,?,?,?,?,?)",
                [
                    ("CUMMINSIND", "Large", "Nifty 100", "official", "2026-11-12", "validated"),
                    ("BAJAJHFL", "Mid", "Nifty Midcap 150", "official", "2026-11-12", "validated"),
                    ("BSE", "Large", "Nifty 100", "official", "2026-11-12", "validated"),
                    ("INDHOTEL", "Mid", "Nifty Midcap 150", "official", "2026-11-12", "validated"),
                    ("AETHER", "Small", "Nifty Smallcap 250", "official", "2026-11-12", "validated"),
                ],
            )
            connection.commit()
            connection.close()

            mapping, provenance = _historical_size_map(snapshot_db, date(2026, 1, 15))
            self.assertEqual(mapping["CUMMINSIND"], "Mid")
            self.assertEqual(mapping["BAJAJHFL"], "Large")
            self.assertEqual(mapping["BSE"], "Mid")
            self.assertEqual(mapping["INDHOTEL"], "Large")
            self.assertNotIn("AETHER", mapping)
            reversed_dates = {row["effective_date"] for row in provenance["notices_reversed"]}
            self.assertIn("2026-03-30", reversed_dates)
            self.assertIn("2026-09-30", reversed_dates)


def rows(closes):
    return [
        {"session_date": f"2026-01-{index + 1:02d}", "open": close, "close": close}
        for index, close in enumerate(closes)
    ]


class MovingAverageCrossoverTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _moving_average_crossover_series(rows([100, 101, 102]), 2, 4)
        self.assertEqual(result["status"], "insufficient_history")
        self.assertEqual(result["required_observations"], 5)


    def test_bullish_cross_enters_long_state(self):
        result = _moving_average_crossover_series(
            rows([10, 10, 10, 10, 10, 11, 12, 13, 14, 15]),
            2,
            4,
        )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["current_signal"], "invested")
        self.assertGreater(result["current_spread_pct"], 0)
        self.assertTrue(any(event["type"] == "bullish_cross" for event in result["crossover_events"]))

    def test_shifted_signal_does_not_capture_pre_signal_gain(self):
        result = _moving_average_crossover_series(
            rows([10, 10, 10, 10, 20, 20]),
            2,
            4,
        )
        self.assertEqual(result["strategy_return_pct"], 0.0)
        self.assertEqual(result["buy_and_hold_return_pct"], 0.0)

    def test_signal_executes_at_next_session_open(self):
        sample = rows([10, 10, 10, 10, 20, 30])
        sample[-1]["open"] = 30
        result = _moving_average_crossover_series(sample, 2, 4)
        self.assertEqual(result["execution_convention"], "completed_close_signal_next_session_open")
        self.assertEqual(result["strategy_return_pct"], 0.0)
        executed = [event for event in result["crossover_events"] if event["execution_status"] == "executed"]
        self.assertEqual(executed[-1]["execution_price"], 30.0)

    def test_missing_open_rejects_signal_transition(self):
        sample = rows([10, 10, 10, 10, 20, 20])
        sample[-1]["open"] = None
        result = _moving_average_crossover_series(sample, 2, 4)
        self.assertEqual(result["rejected_execution_events"], 1)
        self.assertEqual(result["current_executed_position"], "cash")

    def test_reports_profit_factor_and_trade_expectancy(self):
        result = _moving_average_crossover_series(
            rows([10, 10, 10, 10, 11, 12, 13, 12, 11, 10, 11, 12, 13, 12, 11, 10]),
            2,
            4,
        )
        self.assertIn("profit_factor", result)
        self.assertIn("trade_expectancy_pct", result)
        self.assertIn("trade_edge_p_value", result)
        self.assertEqual(
            result["completed_trades"],
            result["winning_trades"] + result["losing_trades"] + result["breakeven_trades"],
        )


class ChronologicalHoldoutTests(unittest.TestCase):
    def test_freezes_parameters_across_ordered_train_validation_and_test_periods(self):
        start = date(2020, 1, 1)
        sample = [
            {
                "session_date": (start + timedelta(days=index)).isoformat(),
                "open": 100 + index * 0.1,
                "close": 100 + index * 0.1,
            }
            for index in range(220)
        ]
        result = _chronological_holdout_validation(
            sample, _moving_average_crossover_series, (5, 20), 21
        )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["primary_published_period"], "untouched_test")
        self.assertEqual(
            [period["period"] for period in result["periods"]],
            ["training", "validation", "untouched_test"],
        )
        self.assertTrue(all(period["parameters_frozen"] for period in result["periods"]))
        self.assertLess(result["periods"][0]["end_date"], result["periods"][1]["start_date"])
        self.assertLess(result["periods"][1]["end_date"], result["periods"][2]["start_date"])

    def test_rolling_folds_advance_without_reading_future_test_periods(self):
        start = date(2018, 1, 1)
        sample = [
            {
                "session_date": (start + timedelta(days=index)).isoformat(),
                "open": 100 + index * 0.02,
                "close": 100 + index * 0.02,
            }
            for index in range(1_050)
        ]
        result = _rolling_walk_forward_validation(
            sample, _moving_average_crossover_series, (5, 20), 21
        )
        self.assertEqual(result["status"], "ready")
        self.assertGreaterEqual(result["fold_count"], 3)
        self.assertEqual(result["primary_published_period"], "test")
        first, second = result["folds"][:2]
        self.assertLess(first["validation"]["end_date"], first["test"]["start_date"])
        self.assertLess(first["test"]["end_date"], second["test"]["start_date"])
        self.assertTrue(all(fold["test"]["parameters_frozen"] for fold in result["folds"]))

    def test_stability_gate_requires_multiple_admitted_profitable_folds(self):
        validation = {
            "folds": [
                {
                    "point_in_time_universe": {"ranking_admissible": True},
                    "test": {
                        "status": "ready", "completed_trades": 6,
                        "net_strategy_return_pct": 4 + index,
                        "net_profit_factor": 1.2 + index * 0.05,
                        "net_trade_expectancy_pct": 0.4 + index * 0.1,
                        "net_maximum_drawdown_pct": -10 - index,
                    },
                }
                for index in range(3)
            ]
        }
        result = _walk_forward_stability_gate(validation)
        self.assertTrue(result["ranking_admissible"])
        self.assertEqual(result["status"], "admitted")
        self.assertEqual(result["metrics"]["completed_trades"], 18)

    def test_stability_gate_rejects_too_few_point_in_time_folds(self):
        validation = {
            "folds": [{
                "point_in_time_universe": {"ranking_admissible": True},
                "test": {
                    "status": "ready", "completed_trades": 20,
                    "net_strategy_return_pct": 10, "net_profit_factor": 2,
                    "net_trade_expectancy_pct": 1, "net_maximum_drawdown_pct": -5,
                },
            }]
        }
        result = _walk_forward_stability_gate(validation)
        self.assertFalse(result["ranking_admissible"])
        self.assertIn("admitted_test_folds", result["failed_checks"])


class DonchianBreakoutTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _donchian_breakout_series(rows([10, 11, 12, 13]), 4, 2)
        self.assertEqual(result["status"], "insufficient_history")
        self.assertEqual(result["required_observations"], 6)

    def test_upside_breakout_enters_long_state(self):
        result = _donchian_breakout_series(
            rows([10, 10, 10, 10, 10, 11, 12, 13]),
            4,
            2,
        )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["current_signal"], "invested")
        self.assertTrue(any(event["type"] == "upside_breakout" for event in result["events"]))

    def test_shorter_channel_exits_position(self):
        result = _donchian_breakout_series(
            rows([10, 10, 10, 10, 11, 12, 13, 12, 11, 10]),
            4,
            2,
        )
        self.assertEqual(result["current_signal"], "cash")
        self.assertTrue(any(event["type"] == "downside_exit" for event in result["events"]))

    def test_signal_is_shifted_before_return_attribution(self):
        result = _donchian_breakout_series(
            rows([10, 10, 10, 20, 20, 20]),
            3,
            2,
        )
        self.assertEqual(result["strategy_return_pct"], 0.0)
        self.assertIn("profit_factor", result)
        self.assertIn("trade_expectancy_pct", result)


class EmaCrossoverTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _ema_crossover_series(rows([10, 11, 12]), 2, 4)
        self.assertEqual(result["status"], "insufficient_history")
        self.assertEqual(result["required_observations"], 5)

    def test_bullish_ema_cross_enters_long_state(self):
        result = _ema_crossover_series(rows([10, 10, 10, 10, 11, 12, 13]), 2, 4)
        self.assertEqual(result["current_signal"], "invested")
        self.assertGreater(result["current_spread_pct"], 0)
        self.assertTrue(any(event["type"] == "bullish_cross" for event in result["events"]))


class TimeSeriesMomentumTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _time_series_momentum_series(rows([10, 11, 12, 13]), 3)
        self.assertEqual(result["status"], "insufficient_history")
        self.assertEqual(result["required_observations"], 5)

    def test_positive_momentum_enters_only_after_signal(self):
        result = _time_series_momentum_series(rows([10, 10, 10, 20, 20]), 3)
        self.assertEqual(result["current_signal"], "invested")
        self.assertEqual(result["strategy_return_pct"], 0.0)
        self.assertGreater(result["current_lookback_return_pct"], 0)


class TripleSmaAlignmentTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _triple_sma_alignment_series(rows([10, 11, 12, 13]), 2, 3, 4)
        self.assertEqual(result["status"], "insufficient_history")
        self.assertEqual(result["required_observations"], 5)

    def test_rising_alignment_enters_long_state(self):
        result = _triple_sma_alignment_series(rows([10, 11, 12, 13, 14, 15]), 2, 3, 4)
        self.assertEqual(result["current_signal"], "invested")
        self.assertGreater(result["current_fast_sma"], result["current_medium_sma"])
        self.assertGreater(result["current_medium_sma"], result["current_slow_sma"])


class RsiMeanReversionTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _rsi_mean_reversion_series(rows([10, 9, 8]), 2, 10, 70)
        self.assertEqual(result["status"], "insufficient_history")

    def test_oversold_rsi_enters_long_state(self):
        result = _rsi_mean_reversion_series(rows([10, 10, 9, 8]), 2, 10, 70)
        self.assertEqual(result["current_signal"], "invested")
        self.assertEqual(result["current_rsi"], 0.0)
        self.assertTrue(any(event["type"] == "oversold_entry" for event in result["events"]))


class BollingerMeanReversionTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _bollinger_mean_reversion_series(rows([10, 10, 10]), 3, 1)
        self.assertEqual(result["status"], "insufficient_history")

    def test_lower_band_entry_exits_at_mean(self):
        result = _bollinger_mean_reversion_series(rows([10, 10, 10, 5, 10]), 3, 1)
        self.assertEqual(result["current_signal"], "cash")
        self.assertTrue(any(event["type"] == "lower_band_entry" for event in result["events"]))
        self.assertTrue(any(event["type"] == "middle_band_exit" for event in result["events"]))


class ZscoreMeanReversionTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _zscore_mean_reversion_series(rows([10, 10, 10]), 3, -1, 0)
        self.assertEqual(result["status"], "insufficient_history")

    def test_negative_zscore_recovers_to_cash(self):
        result = _zscore_mean_reversion_series(rows([10, 10, 10, 5, 6, 10]), 3, -1, 0)
        self.assertEqual(result["current_signal"], "cash")
        self.assertTrue(any(event["type"] == "negative_zscore_entry" for event in result["events"]))
        self.assertTrue(any(event["type"] == "mean_recovery_exit" for event in result["events"]))


class PullbackUptrendTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _pullback_uptrend_series(rows([10, 12, 14, 18]), 2, 4)
        self.assertEqual(result["status"], "insufficient_history")

    def test_pullback_enters_and_recovery_exits(self):
        result = _pullback_uptrend_series(rows([10, 12, 14, 18, 15, 18]), 2, 4)
        self.assertEqual(result["current_signal"], "cash")
        self.assertTrue(any(event["type"] == "uptrend_pullback_entry" for event in result["events"]))
        self.assertTrue(any(event["type"] == "pullback_recovery_exit" for event in result["events"]))


class SelloffRecoveryTests(unittest.TestCase):
    def test_reports_insufficient_history(self):
        result = _selloff_recovery_series(rows([10, 9, 8, 7]), 3, 3)
        self.assertEqual(result["status"], "insufficient_history")

    def test_three_day_selloff_enters_and_recovers(self):
        result = _selloff_recovery_series(rows([10, 9, 8, 7, 9]), 3, 3)
        self.assertEqual(result["current_signal"], "cash")
        self.assertTrue(any(event["type"] == "selloff_entry" for event in result["events"]))
        self.assertTrue(any(event["type"] == "recovery_exit" for event in result["events"]))


class TradeEdgeSignificanceTests(unittest.TestCase):
    def test_requires_at_least_two_completed_trades(self):
        self.assertIsNone(_trade_edge_p_value([2.5]))

    def test_zero_mean_has_unit_p_value(self):
        self.assertEqual(_trade_edge_p_value([-1.0, 1.0]), 1.0)

    def test_positive_sample_reports_two_sided_significance(self):
        probability = _trade_edge_p_value([1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertIsNotNone(probability)
        self.assertAlmostEqual(probability, 0.013236, places=5)

    def test_bootstrap_requires_adequate_completed_trade_sample(self):
        from workspace_engine import _bootstrap_trade_evidence

        evidence = _bootstrap_trade_evidence([1.0] * 14, resamples=200)
        self.assertEqual(evidence["status"], "insufficient_sample")
        self.assertEqual(evidence["sample"]["minimum_required"], 15)

    def test_bootstrap_is_deterministic_and_reports_both_intervals(self):
        from workspace_engine import _bootstrap_trade_evidence

        returns = [3.0, -1.0, 2.0, -0.5, 1.5] * 4
        first = _bootstrap_trade_evidence(returns, resamples=250)
        second = _bootstrap_trade_evidence(returns, resamples=250)
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "ready")
        self.assertEqual(first["ordinary_bootstrap"]["resamples"], 250)
        self.assertEqual(first["moving_block_bootstrap"]["block_length_trades"], 4)
        self.assertGreater(first["ordinary_bootstrap"]["net_expectancy_ci_95_pct"][0], 0)

    def test_benjamini_hochberg_q_values_are_monotone_and_attached(self):
        from workspace_engine import _attach_benjamini_hochberg_fdr

        rows = [
            {"status": "ready", "net_trade_edge_p_value": 0.01},
            {"status": "ready", "net_trade_edge_p_value": 0.04},
            {"status": "ready", "net_trade_edge_p_value": 0.03},
            {"status": "data_quality_gated", "net_trade_edge_p_value": None},
        ]
        contract = _attach_benjamini_hochberg_fdr(rows, alpha=0.05)
        self.assertEqual(contract["tested_hypotheses"], 3)
        self.assertEqual(rows[0]["net_trade_edge_q_value"], 0.03)
        self.assertEqual(rows[1]["net_trade_edge_q_value"], 0.04)
        self.assertEqual(rows[2]["net_trade_edge_q_value"], 0.04)
        self.assertIsNone(rows[3]["net_trade_edge_q_value"])

    def test_cross_strategy_family_adjusts_all_strategy_stock_pairs_together(self):
        from workspace_engine import _cross_strategy_fdr_from_payloads

        payload = _cross_strategy_fdr_from_payloads([
            {"engine": "alpha", "results": [
                {"status": "ready", "symbol": "AAA", "net_trade_edge_p_value": 0.01},
                {"status": "ready", "symbol": "BBB", "net_trade_edge_p_value": 0.20},
            ]},
            {"engine": "beta", "results": [
                {"status": "ready", "symbol": "AAA", "net_trade_edge_p_value": 0.02},
                {"status": "data_quality_gated", "symbol": "CCC", "net_trade_edge_p_value": None},
            ]},
        ])
        self.assertEqual(payload["tested_hypotheses"], 3)
        self.assertEqual(payload["by_engine"]["alpha"]["AAA"]["cross_strategy_q_value"], 0.03)
        self.assertEqual(payload["by_engine"]["beta"]["AAA"]["cross_strategy_q_value"], 0.03)


class EquityDeliveryCostTests(unittest.TestCase):
    def test_official_charge_components_are_reported(self):
        result = equity_delivery_cost_calculator(
            100000,
            100000,
            {"position_capital_inr": 100000, "slippage_bps_each_side": 0, "market_impact_bps_each_side": 0},
        )
        self.assertEqual(result["contract_version"], "equity-delivery-cost-v1")
        self.assertEqual(result["buy_charges"]["brokerage"], 0.0)
        self.assertEqual(result["buy_charges"]["stt"], 100.0)
        self.assertEqual(result["buy_charges"]["stamp_duty"], 15.0)
        self.assertEqual(result["sell_charges"]["dp_charge"], 15.34)
        self.assertGreater(result["total_cost_inr"], 230)

    def test_net_backtest_return_includes_default_costs(self):
        result = _moving_average_crossover_series(
            rows([10, 10, 10, 10, 11, 12, 13, 12, 11, 10]),
            2,
            4,
        )
        self.assertLessEqual(result["net_strategy_return_pct"], result["gross_strategy_return_pct"])
        self.assertEqual(result["cost_model"]["schedule_version"], "zerodha-nse-equity-delivery-2026-03-v1")
        self.assertEqual(result["cost_model"]["assumptions"]["position_capital_inr"], 50000.0)


class PointInTimeUniverseTests(unittest.TestCase):
    def _stores(self, directory):
        market_db = Path(directory) / "market.sqlite3"
        snapshot_db = Path(directory) / "snapshots.sqlite3"
        market = sqlite3.connect(market_db)
        market.execute(
            """CREATE TABLE instruments(
                   instrument_id TEXT PRIMARY KEY, kind TEXT,
                   adjustment_status TEXT, adjustment_source_url TEXT, source TEXT)"""
        )
        market.execute(
            """CREATE TABLE candles(
                   instrument_id TEXT, session_date TEXT, open REAL, high REAL,
                   low REAL, close REAL, volume REAL, source TEXT, retrieved_at TEXT)"""
        )
        market.commit()
        market.close()
        snapshots = sqlite3.connect(snapshot_db)
        snapshots.execute(
            """CREATE TABLE index_membership_snapshots(
                   index_name TEXT, snapshot_date TEXT, symbol TEXT, company_name TEXT,
                   industry TEXT, source_url TEXT, retrieved_at TEXT)"""
        )
        snapshots.executemany(
            "INSERT INTO index_membership_snapshots VALUES ('NIFTY 500','2026-11-03',?,?,?,?,?)",
            [
                ("JSWCEMENT", "JSW Cement", "Cement", "https://example.test/current", "2026-11-03"),
                ("ACUTAAS", "Acutaas", "Chemicals", "https://example.test/current", "2026-11-03"),
                ("AETHER", "Aether", "Chemicals", "https://example.test/current", "2026-11-03"),
                ("HEGAM", "HEG Advanced Materials", "Capital Goods", "https://example.test/current", "2026-11-03"),
            ],
        )
        snapshots.commit()
        snapshots.close()
        return market_db, snapshot_db

    def test_reverses_reviewed_official_notice_without_claiming_complete_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            market_db, snapshot_db = self._stores(directory)
            result = point_in_time_universe_contract(market_db, snapshot_db, "2025-12-30")
            symbols = {row["symbol"] for row in result["members"]}
            self.assertIn("SKFINDIA", symbols)
            self.assertNotIn("JSWCEMENT", symbols)
            self.assertEqual(result["membership_status"], "partial_official_reconstruction")
            self.assertFalse(result["ranking_admissible"])
            self.assertFalse(result["survivorship_bias_gate_complete"])
            self.assertEqual(result["coverage"]["reviewed_notices"], 10)
            self.assertEqual(result["coverage"]["reviewed_events"], 163)
            self.assertTrue(result["coverage"]["ad_hoc_notice_review_complete"])
            self.assertFalse(result["coverage"]["historical_sector_and_size_complete"])

    def test_preserves_dummy_to_listed_lineage_and_investability(self):
        with tempfile.TemporaryDirectory() as directory:
            market_db, snapshot_db = self._stores(directory)
            dummy_result = point_in_time_universe_contract(market_db, snapshot_db, "2026-05-15")
            dummy = {row["symbol"]: row for row in dummy_result["members"]}
            self.assertIn("DUMMYVEDL1", dummy)
            self.assertFalse(dummy["DUMMYVEDL1"]["is_investable"])
            self.assertEqual(dummy["DUMMYVEDL1"]["membership_role"], "zero_price_corporate_action_dummy")
            self.assertEqual(len(dummy_result["active_lineages"]), 4)

            listed_result = point_in_time_universe_contract(market_db, snapshot_db, "2026-06-16")
            listed = {row["symbol"]: row for row in listed_result["members"]}
            self.assertNotIn("DUMMYVEDL1", listed)
            self.assertIn("VAML", listed)
            self.assertTrue(listed["VAML"]["is_investable"])
            self.assertEqual(listed["VAML"]["membership_role"], "listed_resulting_company")

            exited_result = point_in_time_universe_contract(market_db, snapshot_db, "2026-06-24")
            exited = {row["symbol"] for row in exited_result["members"]}
            self.assertNotIn("VAML", exited)
            self.assertNotIn("VEDPOWER", exited)
            self.assertNotIn("VOGL", exited)
            self.assertNotIn("VISL", exited)

    def test_heg_symbol_change_and_dummy_do_not_leak_into_earlier_universe(self):
        with tempfile.TemporaryDirectory() as directory:
            market_db, snapshot_db = self._stores(directory)
            before = point_in_time_universe_contract(market_db, snapshot_db, "2026-09-06")
            before_symbols = {row["symbol"] for row in before["members"]}
            self.assertIn("HEG", before_symbols)
            self.assertNotIn("HEGAM", before_symbols)
            self.assertNotIn("DUMMYHEG", before_symbols)

            after = point_in_time_universe_contract(market_db, snapshot_db, "2026-09-07")
            after_rows = {row["symbol"]: row for row in after["members"]}
            self.assertIn("HEGAM", after_rows)
            self.assertIn("DUMMYHEG", after_rows)
            self.assertFalse(after_rows["DUMMYHEG"]["is_investable"])

    def test_removed_history_audit_excludes_dummy_phase_and_checks_listed_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            market_db = Path(directory) / "market.sqlite3"
            connection = sqlite3.connect(market_db)
            connection.executescript(
                """CREATE TABLE instruments(
                       instrument_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
                       display_name TEXT NOT NULL, provider_token TEXT NOT NULL,
                       source TEXT NOT NULL, updated_at TEXT NOT NULL,
                       schema_version INTEGER NOT NULL DEFAULT 1,
                       adjustment_status TEXT NOT NULL DEFAULT 'provider_unspecified',
                       adjustment_source_url TEXT);
                   CREATE TABLE candles(
                       instrument_id TEXT NOT NULL, session_date TEXT NOT NULL,
                       open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
                       close REAL NOT NULL, source TEXT NOT NULL, retrieved_at TEXT NOT NULL,
                       validation_status TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1,
                       volume INTEGER, metadata_retrieved_at TEXT,
                       PRIMARY KEY(instrument_id,session_date));"""
            )
            connection.executemany(
                "INSERT INTO instruments VALUES (?,?,?,?,?,?,?,?,?)",
                [
                    ("index:Nifty 50", "index", "Nifty 50", "1", "test", "2026-11-05", 1, "provider_reported_adjusted", "https://example.test"),
                    ("stock:VAML", "stock", "VAML", "2", "test", "2026-11-05", 1, "provider_reported_adjusted", "https://example.test"),
                ],
            )
            dates = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18"]
            connection.executemany(
                "INSERT INTO candles VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (instrument, session, 100, 101, 99, 100, "test", "2026-11-05", "valid", 1, 1000, "2026-11-05")
                    for instrument in ("index:Nifty 50", "stock:VAML") for session in dates
                ],
            )
            connection.commit()
            connection.close()

            audit = removed_constituent_history_audit(market_db)
            vaml = next(row for row in audit["exits"] if row["symbol"] == "VAML")
            self.assertEqual(vaml["required_sessions"], 4)
            self.assertEqual(vaml["stored_sessions"], 4)
            self.assertEqual(vaml["status"], "complete")
            self.assertNotIn("DUMMYVEDL1", {row["symbol"] for row in audit["exits"]})
            self.assertIn("VEDPOWER", audit["missing_symbols"])
            self.assertFalse(audit["coverage"]["complete"])

            connection = sqlite3.connect(market_db)
            connection.execute(
                "DELETE FROM candles WHERE instrument_id='stock:VAML' AND session_date='2026-06-15'"
            )
            connection.execute(
                """INSERT INTO cash_archive_symbol_checks(
                       session_date,symbol,present,source_url,retrieved_at,schema_version)
                   VALUES (?,?,?,?,?,?)""",
                ("2026-06-15", "VAML", 0, "https://example.test/archive.csv", "2026-11-05", 1),
            )
            connection.commit()
            connection.close()

            audited_absence = removed_constituent_history_audit(market_db)
            vaml = next(row for row in audited_absence["exits"] if row["symbol"] == "VAML")
            self.assertEqual(vaml["reference_window_sessions"], 4)
            self.assertEqual(vaml["archive_confirmed_absent_sessions"], 1)
            self.assertEqual(vaml["required_sessions"], 3)
            self.assertEqual(vaml["stored_sessions"], 3)
            self.assertEqual(vaml["status"], "complete")


class BacktestComparabilityGateTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript("""
            CREATE TABLE instruments (
                instrument_id TEXT PRIMARY KEY, adjustment_status TEXT,
                adjustment_source_url TEXT, source TEXT, updated_at TEXT
            );
            CREATE TABLE candles (
                instrument_id TEXT, session_date TEXT, open REAL, high REAL,
                low REAL, close REAL, volume REAL, source TEXT, retrieved_at TEXT
            );
        """)
        self.connection.execute(
            "INSERT INTO instruments VALUES (?,?,?,?,?)",
            ("stock:TEST", "provider_reported_adjusted", "https://example.test/policy", "test", "2026-11-05T00:00:00Z"),
        )
        self.connection.executescript("""
            CREATE TABLE corporate_actions (
                instrument_id TEXT, action_type TEXT, purpose TEXT, ex_date TEXT,
                record_date TEXT, adjustment_requirement TEXT, review_status TEXT,
                source_url TEXT, retrieved_at TEXT
            );
            CREATE TABLE symbol_history (
                instrument_id TEXT, old_symbol TEXT, new_symbol TEXT, effective_date TEXT,
                review_status TEXT, source_url TEXT, retrieved_at TEXT
            );
            CREATE TABLE workspace_metadata (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT);
        """)
        self.connection.execute(
            "INSERT INTO workspace_metadata VALUES (?,?,?)",
            ("corporate_action_coverage", '{"from":"2025-01-01","through":"2026-12-31"}', "2026-11-05T00:00:00Z"),
        )
        self.dates = [(date(2025, 1, 1) + timedelta(days=index)).isoformat() for index in range(254)]
        self.rows = [
            ("stock:TEST", session, 100 + index / 10, 101 + index / 10,
             99 + index / 10, 100 + index / 10, 1000, "test candles", "2026-11-05T00:00:00Z")
            for index, session in enumerate(self.dates)
        ]
        self.connection.executemany("INSERT INTO candles VALUES (?,?,?,?,?,?,?,?,?)", self.rows)
        self.context = {"common_as_of_date": self.dates[-1], "reference_dates": self.dates}

    def tearDown(self):
        self.connection.close()

    def test_complete_common_window_is_admitted(self):
        selected, audit = _quality_checked_backtest_series(self.connection, "stock:TEST", 3, self.context)
        self.assertEqual(audit["status"], "ready")
        self.assertEqual(len(selected), 254)
        self.assertEqual(audit["comparison_sessions"], 252)

    def test_full_history_mode_uses_all_continuous_sessions(self):
        selected, audit = _quality_checked_backtest_series(
            self.connection, "stock:TEST", 3, self.context, history_mode="full"
        )
        self.assertEqual(audit["status"], "ready")
        self.assertEqual(audit["history_mode"], "full")
        self.assertIsNone(audit["comparison_sessions"])
        self.assertEqual(audit["history_start_date"], self.dates[0])
        self.assertEqual(len(selected), len(self.dates))

    def test_full_history_reports_partial_corporate_ledger_as_warning(self):
        self.connection.execute(
            "UPDATE workspace_metadata SET value=? WHERE key='corporate_action_coverage'",
            ('{"from":"2025-02-01","through":"2026-12-31"}',),
        )
        selected, audit = _quality_checked_backtest_series(
            self.connection, "stock:TEST", 3, self.context, history_mode="full"
        )
        self.assertEqual(audit["status"], "ready")
        self.assertTrue(selected)
        self.assertIn("corporate_action_ledger_partial_full_history", audit["warnings"])

    def test_missing_reference_session_is_gated(self):
        self.connection.execute(
            "DELETE FROM candles WHERE instrument_id=? AND session_date=?", ("stock:TEST", self.dates[100])
        )
        selected, audit = _quality_checked_backtest_series(self.connection, "stock:TEST", 3, self.context)
        self.assertEqual(selected, [])
        self.assertIn("missing_reference_sessions", audit["blocking_gates"])
        self.assertEqual(audit["missing_session_count"], 1)

    def test_unverified_adjustment_provenance_is_gated(self):
        self.connection.execute(
            "UPDATE instruments SET adjustment_status='provider_unspecified' WHERE instrument_id='stock:TEST'"
        )
        selected, audit = _quality_checked_backtest_series(self.connection, "stock:TEST", 3, self.context)
        self.assertEqual(selected, [])
        self.assertIn("adjustment_provenance_unverified", audit["blocking_gates"])

    def test_manual_corporate_action_review_is_gated(self):
        self.connection.execute(
            "INSERT INTO corporate_actions VALUES (?,?,?,?,?,?,?,?,?)",
            ("stock:TEST", "delisting", "Voluntary delisting", self.dates[200], None,
             "terminal_value_and_membership_review_required", "manual_review",
             "https://example.test/action", "2026-11-05T00:00:00Z"),
        )
        selected, audit = _quality_checked_backtest_series(self.connection, "stock:TEST", 3, self.context)
        self.assertEqual(selected, [])
        self.assertIn("corporate_action_manual_review", audit["blocking_gates"])
        self.assertEqual(audit["unresolved_corporate_action_count"], 1)


if __name__ == "__main__":
    unittest.main()
