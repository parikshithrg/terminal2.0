import unittest
import sqlite3
import tempfile
from datetime import date, timedelta
from pathlib import Path

from workspace_engine import (
    _bollinger_mean_reversion_series,
    _build_opportunity_feature_rows,
    _chronological_holdout_validation,
    _completed_session_price_features,
    _donchian_breakout_series,
    _same_contract_fno_feature,
    _market_regime_sample_summary,
    equity_delivery_cost_calculator,
    ensure_workspace_schema,
    forward_move_label_contract,
    market_regime_classification,
    sector_trend_classification,
    stock_leadership_classification,
    crossover_state_conditioning_contract,
    crossover_ablation_contract,
    top_down_oos_admission_contract,
    opportunity_board_release_contract,
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
    _attach_comparison_evidence_rankings,
    _attach_oos_evidence_rankings,
    point_in_time_universe_contract,
    removed_constituent_history_audit,
)


class EvidenceRankingTests(unittest.TestCase):
    def test_composite_ranking_never_assigns_rank_to_failed_evidence(self):
        rows = [{
            "status": "ready",
            "symbol": "TEST",
            "completed_trades": 40,
            "net_profit_factor": 2.2,
            "net_trade_expectancy_pct": 1.4,
            "net_trade_edge_q_value": 0.01,
            "maximum_drawdown_pct": -8.0,
            "excess_return_pct": 12.0,
            "exposure_pct": 55.0,
            "turnover_pct": 18.0,
            "estimated_cost_drag_pct": 0.3,
        }]
        contract = _attach_comparison_evidence_rankings(rows)
        ranking = rows[0]["evidence_ranking"]
        self.assertEqual(contract["ranking_admitted"], 0)
        self.assertFalse(ranking["ranking_admissible"])
        self.assertIsNone(ranking["score"])
        self.assertGreater(ranking["diagnostic_score"], 0)
        self.assertIn("out_of_sample_stability", ranking["failed_checks"])

    def test_full_history_oos_score_is_published_only_after_every_gate_passes(self):
        rows = [{"status": "ready", "symbol": "TEST"}]
        evidence = {"TEST": {
            "status": "ready",
            "net_trade_edge_p_value": 0.01,
            "metrics": {
                "admitted_test_folds": 4,
                "completed_trades": 24,
                "positive_fold_ratio": 0.75,
                "median_net_profit_factor": 1.6,
                "median_net_trade_expectancy_pct": 1.2,
                "worst_net_drawdown_pct": -12.0,
                "median_excess_return_pct": 4.0,
                "median_exposure_pct": 55.0,
                "median_cost_drag_pct": 0.4,
                "completed_sells": 12,
                "evaluated_test_sessions": 504,
            },
        }}
        contract = _attach_oos_evidence_rankings(rows, evidence)
        ranking = rows[0]["evidence_ranking"]
        self.assertEqual(contract["ranking_admitted"], 1)
        self.assertTrue(ranking["ranking_admissible"])
        self.assertIsNotNone(ranking["score"])
        self.assertEqual(ranking["failed_checks"], [])

        evidence["TEST"]["metrics"]["positive_fold_ratio"] = 0.25
        _attach_oos_evidence_rankings(rows, evidence)
        self.assertFalse(rows[0]["evidence_ranking"]["ranking_admissible"])
        self.assertIsNone(rows[0]["evidence_ranking"]["score"])
        self.assertIn("positive_fold_ratio", rows[0]["evidence_ranking"]["failed_checks"])


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


class ForwardMoveLabelTests(unittest.TestCase):
    @staticmethod
    def price_rows(count=90, start_price=100.0):
        start = date(2026, 1, 1)
        output = []
        for index in range(count):
            close = start_price * (1 + 0.001 * index + (0.002 if index % 2 else -0.001))
            output.append({
                "session_date": (start + timedelta(days=index)).isoformat(),
                "open": close,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
            })
        return output

    @staticmethod
    def label_for(result, session):
        return next(row for row in result["labels"] if row["feature_session"] == session)

    def test_outcome_window_starts_after_feature_candle(self):
        sample = self.price_rows(30)
        sample[20]["high"] = sample[20]["close"] * 10
        sample[20]["low"] = sample[20]["close"] * 0.1
        result = forward_move_label_contract(
            sample, horizons=(2,), volatility_lookback=5
        )
        feature = self.label_for(result, sample[20]["session_date"])
        outcome = feature["outcomes"]["2"]
        self.assertEqual(outcome["label_start_session"], sample[21]["session_date"])
        self.assertEqual(outcome["label_end_session"], sample[22]["session_date"])
        expected_mfe = 100 * (max(sample[21]["high"], sample[22]["high"]) / sample[20]["close"] - 1)
        expected_mae = 100 * (min(sample[21]["low"], sample[22]["low"]) / sample[20]["close"] - 1)
        self.assertAlmostEqual(outcome["maximum_favourable_excursion_pct"], expected_mfe, places=6)
        self.assertAlmostEqual(outcome["maximum_adverse_excursion_pct"], expected_mae, places=6)
        self.assertLess(outcome["maximum_favourable_excursion_pct"], 10)

    def test_future_prices_do_not_change_ex_ante_volatility(self):
        original = self.price_rows(35)
        changed = [dict(row) for row in original]
        changed[22]["close"] *= 1.25
        changed[22]["high"] = changed[22]["close"] * 1.01
        changed[22]["low"] = changed[22]["close"] * 0.99
        feature_session = original[20]["session_date"]
        first = self.label_for(
            forward_move_label_contract(original, horizons=(2,), volatility_lookback=10),
            feature_session,
        )["outcomes"]["2"]
        second = self.label_for(
            forward_move_label_contract(changed, horizons=(2,), volatility_lookback=10),
            feature_session,
        )["outcomes"]["2"]
        self.assertEqual(first["ex_ante_daily_volatility_pct"], second["ex_ante_daily_volatility_pct"])
        self.assertNotEqual(first["forward_return_pct"], second["forward_return_pct"])

    def test_relative_returns_use_exact_aligned_reference_sessions(self):
        stock = self.price_rows(12, 100)
        sector = self.price_rows(12, 200)
        benchmark = self.price_rows(12, 300)
        stock[5]["close"] = 100
        stock[7]["close"] = 110
        stock[7]["high"] = 111
        stock[7]["low"] = 109
        sector[5]["close"] = 200
        sector[7]["close"] = 208
        sector[7]["high"] = 209
        sector[7]["low"] = 207
        benchmark[5]["close"] = 300
        benchmark[7]["close"] = 315
        benchmark[7]["high"] = 316
        benchmark[7]["low"] = 314
        result = forward_move_label_contract(
            stock, sector, benchmark, horizons=(2,), volatility_lookback=3
        )
        outcome = self.label_for(result, stock[5]["session_date"])["outcomes"]["2"]
        self.assertAlmostEqual(outcome["forward_return_pct"], 10.0)
        self.assertAlmostEqual(outcome["sector_forward_return_pct"], 4.0)
        self.assertAlmostEqual(outcome["relative_to_sector_pct"], 6.0)
        self.assertAlmostEqual(outcome["nifty50_forward_return_pct"], 5.0)
        self.assertAlmostEqual(outcome["relative_to_nifty50_pct"], 5.0)

    def test_incomplete_future_horizons_are_censored(self):
        sample = self.price_rows(10)
        result = forward_move_label_contract(sample, horizons=(5,), volatility_lookback=3)
        self.assertEqual(result["coverage"]["labels_by_horizon"]["5"], 5)
        self.assertEqual(result["coverage"]["tail_censored_by_horizon"]["5"], 5)
        self.assertEqual(len(result["labels"]), 5)

    def test_large_move_rule_retains_continuous_outcome(self):
        sample = self.price_rows(30)
        sample[22]["close"] = sample[20]["close"] * 1.20
        sample[22]["high"] = sample[22]["close"] * 1.01
        sample[22]["low"] = sample[22]["close"] * 0.99
        result = forward_move_label_contract(
            sample, horizons=(2,), volatility_lookback=10, large_move_sigma=1.5
        )
        outcome = self.label_for(result, sample[20]["session_date"])["outcomes"]["2"]
        self.assertTrue(outcome["large_move"])
        self.assertEqual(outcome["large_move_direction"], "up")
        self.assertGreater(outcome["volatility_normalised_move"], 1.5)
        self.assertEqual(result["contract_version"], "forward-move-labels-v1")

    def test_missing_reference_sessions_are_disclosed_not_inferred(self):
        stock = self.price_rows(12)
        benchmark = self.price_rows(12)
        benchmark.pop(7)
        result = forward_move_label_contract(
            stock, benchmark_rows=benchmark, horizons=(2,), volatility_lookback=3
        )
        outcome = self.label_for(result, stock[5]["session_date"])["outcomes"]["2"]
        self.assertIsNone(outcome["nifty50_forward_return_pct"])
        self.assertEqual(result["coverage"]["missing_nifty50_reference_by_horizon"]["2"], 2)


class OpportunityFeatureLedgerTests(unittest.TestCase):
    @staticmethod
    def price_rows(count=220, start_price=100.0):
        start = date(2025, 1, 1)
        return [
            {
                "session_date": (start + timedelta(days=index)).isoformat(),
                "open": start_price + index,
                "high": start_price + index + 1,
                "low": start_price + index - 1,
                "close": start_price + index,
                "volume": 1000 + index,
            }
            for index in range(count)
        ]

    @staticmethod
    def feature_connection():
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        connection.executescript("""
            CREATE TABLE instruments (
                instrument_id TEXT PRIMARY KEY, kind TEXT, display_name TEXT,
                provider_token TEXT, source TEXT, updated_at TEXT, schema_version INTEGER,
                adjustment_status TEXT, adjustment_source_url TEXT
            );
            CREATE TABLE candles (
                instrument_id TEXT, session_date TEXT, open REAL, high REAL, low REAL,
                close REAL, source TEXT, retrieved_at TEXT, validation_status TEXT,
                schema_version INTEGER, volume INTEGER,
                PRIMARY KEY(instrument_id, session_date)
            );
            CREATE TABLE futures_contracts (
                contract_key TEXT PRIMARY KEY, underlying TEXT, tradingsymbol TEXT,
                exchange TEXT, provider_token TEXT, expiry_date TEXT, lot_size INTEGER,
                source TEXT, first_seen_at TEXT, last_seen_at TEXT, schema_version INTEGER
            );
            CREATE TABLE futures_eod_snapshots (
                contract_key TEXT, session_date TEXT, open REAL, high REAL, low REAL,
                close REAL, volume INTEGER, open_interest INTEGER, source TEXT,
                retrieved_at TEXT, schema_version INTEGER,
                PRIMARY KEY(contract_key, session_date)
            );
        """)
        return connection

    @staticmethod
    def insert_series(connection, instrument_id, rows):
        connection.executemany(
            "INSERT INTO candles VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    instrument_id, row["session_date"], row["open"], row["high"], row["low"],
                    row["close"], "test", "2026-11-16T00:00:00Z", "validated", 1,
                    row["volume"],
                )
                for row in rows
            ],
        )

    def test_future_candles_cannot_change_completed_session_features(self):
        sample = self.price_rows(40)
        as_of = sample[30]["session_date"]
        first = _completed_session_price_features(sample, as_of)
        changed = [dict(row) for row in sample]
        changed[35].update({"close": 10000, "high": 10001, "low": 9999})
        second = _completed_session_price_features(changed, as_of)
        self.assertEqual(first, second)
        self.assertEqual(first["source_session"], as_of)

    def test_future_dated_classification_is_not_admitted(self):
        connection = self.feature_connection()
        try:
            sample = self.price_rows()
            as_of = sample[-1]["session_date"]
            for instrument_id, name in (
                ("index:Nifty 50", "Nifty 50"), ("stock:KNOWN", "KNOWN"), ("stock:FUTURE", "FUTURE"),
            ):
                kind = "index" if instrument_id.startswith("index:") else "stock"
                connection.execute(
                    "INSERT INTO instruments VALUES (?,?,?,?,?,?,?,?,?)",
                    (instrument_id, kind, name, "1", "test", as_of, 1, "verified_adjusted", "test"),
                )
                self.insert_series(connection, instrument_id, sample)
            future_source = (date.fromisoformat(as_of) + timedelta(days=1)).isoformat()
            universe = {
                "ok": True,
                "requested_as_of_date": as_of,
                "anchor_snapshot_date": as_of,
                "membership_status": "official_notice_reconstruction",
                "ranking_admissible": True,
                "coverage": {"classification_audit": {"size_provenance": {"source_date": as_of}}},
                "members": [
                    {"symbol": "KNOWN", "industry": "Energy", "sector_source_date": as_of,
                     "size_bucket": "Large", "membership_role": "ordinary_constituent", "is_investable": True},
                    {"symbol": "FUTURE", "industry": "Technology", "sector_source_date": future_source,
                     "size_bucket": "Large", "membership_role": "ordinary_constituent", "is_investable": True},
                ],
            }
            result = _build_opportunity_feature_rows(connection, as_of, universe)
            future_row = next(row for row in result["rows"] if row["symbol"] == "FUTURE")
            self.assertIsNone(future_row["sector"])
            self.assertIn("sector_classification_not_known_at_feature_time", future_row["missing_features"])
            known_row = next(row for row in result["rows"] if row["symbol"] == "KNOWN")
            self.assertEqual(known_row["sector"], "Energy")
        finally:
            connection.close()

    def test_fno_feature_uses_previous_observation_of_same_contract(self):
        connection = self.feature_connection()
        try:
            connection.execute(
                "INSERT INTO futures_contracts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("NFO:TEST", "TEST", "TEST26NOVFUT", "NFO", "1", "2026-11-26", 1,
                 "test", "2026-01-01", "2026-11-16", 1),
            )
            connection.executemany(
                "INSERT INTO futures_eod_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [
                    ("NFO:TEST", "2026-11-12", 100, 101, 99, 100, 1000, 10000, "test", "now", 1),
                    ("NFO:TEST", "2026-11-13", 102, 103, 101, 102, 1100, 11000, "test", "now", 1),
                ],
            )
            result = _same_contract_fno_feature(connection, "TEST", "2026-11-13")
            self.assertEqual(result["state"], "long_build_up")
            self.assertEqual(result["contract_key"], "NFO:TEST")
            self.assertEqual(result["previous_session"], "2026-11-12")
        finally:
            connection.close()

    def test_persisted_schema_cannot_store_forward_outcomes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "market.sqlite3"
            connection = sqlite3.connect(path)
            connection.executescript("""
                CREATE TABLE instruments (
                    instrument_id TEXT PRIMARY KEY, kind TEXT, display_name TEXT,
                    provider_token TEXT, source TEXT, updated_at TEXT, schema_version INTEGER,
                    adjustment_status TEXT DEFAULT 'provider_unspecified', adjustment_source_url TEXT
                );
                CREATE TABLE candles (
                    instrument_id TEXT, session_date TEXT, open REAL, high REAL, low REAL,
                    close REAL, source TEXT, retrieved_at TEXT, validation_status TEXT,
                    schema_version INTEGER, volume INTEGER,
                    PRIMARY KEY(instrument_id, session_date)
                );
            """)
            connection.close()
            ensure_workspace_schema(path)
            connection = sqlite3.connect(path)
            columns = [row[1] for row in connection.execute("PRAGMA table_info(opportunity_feature_ledger)")]
            regime_columns = [row[1] for row in connection.execute("PRAGMA table_info(market_regime_ledger)")]
            sector_columns = [row[1] for row in connection.execute("PRAGMA table_info(sector_trend_ledger)")]
            stock_columns = [row[1] for row in connection.execute("PRAGMA table_info(stock_leadership_ledger)")]
            conditioning_columns = [row[1] for row in connection.execute("PRAGMA table_info(crossover_conditioning_ledger)")]
            ablation_columns = [row[1] for row in connection.execute("PRAGMA table_info(crossover_ablation_ledger)")]
            admission_columns = [row[1] for row in connection.execute("PRAGMA table_info(top_down_oos_admission_ledger)")]
            release_columns = [row[1] for row in connection.execute("PRAGMA table_info(opportunity_board_release_ledger)")]
            connection.close()
            self.assertIn("return_20d_pct", columns)
            self.assertFalse(any(
                token in column for column in columns for token in ("forward", "large_move", "mfe", "mae")
            ))
            self.assertIn("regime_family", regime_columns)
            self.assertFalse(any(
                token in column for column in regime_columns for token in ("strategy", "forward", "trade", "profit")
            ))
            self.assertIn("relative_strength_120d_pct", sector_columns)
            self.assertFalse(any(
                token in column for column in sector_columns for token in ("strategy", "forward", "trade", "profit")
            ))
            self.assertIn("breakout_proximity_20d_pct", stock_columns)
            self.assertFalse(any(
                token in column for column in stock_columns for token in ("strategy", "forward", "trade", "profit")
            ))
            self.assertIn("net_trade_expectancy_pct", conditioning_columns)
            self.assertIn("state_observations", conditioning_columns)
            self.assertIn("delta_net_expectancy_pct", ablation_columns)
            self.assertIn("unavailable_state_trades", ablation_columns)
            self.assertIn("admission_decision", admission_columns)
            self.assertIn("failure_reasons_json", admission_columns)
            self.assertIn("board_authorised", release_columns)
            self.assertIn("ui_mode", release_columns)


class SectorTrendClassificationTests(unittest.TestCase):
    @staticmethod
    def feature_rows(coverage=1.0, ranking_admissible=True):
        rows = []
        definitions = {
            "Leaders": (8.0, 15.0, 25.0, 2.0),
            "Middle": (3.0, 7.0, 10.0, 0.4),
            "Laggards": (-3.0, -6.0, -12.0, -0.5),
        }
        for sector, returns in definitions.items():
            for index in range(10):
                available = index < int(10 * coverage)
                rows.append({
                    "session_date": "2026-11-13",
                    "symbol": f"{sector[:2]}{index}",
                    "sector": sector,
                    "sector_source_date": "2026-01-01",
                    "close": 100.0 if available else None,
                    "return_20d_pct": returns[0] if available else None,
                    "return_60d_pct": returns[1] if available else None,
                    "return_120d_pct": returns[2] if available else None,
                    "market_return_20d_pct": 4.0,
                    "market_return_60d_pct": 8.0,
                    "market_return_120d_pct": 12.0,
                    "distance_sma20_pct": returns[3] if available else None,
                    "distance_sma50_pct": returns[3] if available else None,
                    "distance_sma200_pct": returns[3] if available else None,
                    "sma20_slope_5d_pct": returns[3] / 10 if available else None,
                    "realised_volatility_20d_pct": 20.0 if available else None,
                    "ranking_admissible": ranking_admissible,
                })
        return rows

    @staticmethod
    def history():
        rows = []
        for offset in range(10):
            session = (date(2026, 10, 30) + timedelta(days=offset)).isoformat()
            for rank, sector in enumerate(("Leaders", "Middle", "Laggards"), start=1):
                rows.append({
                    "session_date": session,
                    "sector": sector,
                    "relative_strength_20d_pct": 3.0 - rank,
                    "rank_20d": rank,
                    "realised_volatility_20d_pct": 18.0 + offset / 10,
                })
        return rows

    def test_classifies_leading_and_lagging_sectors_with_relative_ranks(self):
        result = sector_trend_classification(self.feature_rows(), self.history())
        by_sector = {row["sector"]: row for row in result["rows"]}
        self.assertEqual(by_sector["Leaders"]["sector_state"], "leading")
        self.assertEqual(by_sector["Leaders"]["rank_20d"], 1)
        self.assertEqual(by_sector["Leaders"]["relative_strength_20d_pct"], 4.0)
        self.assertEqual(by_sector["Laggards"]["sector_state"], "lagging")
        self.assertTrue(by_sector["Leaders"]["ranking_admissible"])

    def test_constituent_coverage_gate_withholds_sector_state(self):
        result = sector_trend_classification(self.feature_rows(coverage=0.7), self.history())
        leader = next(row for row in result["rows"] if row["sector"] == "Leaders")
        self.assertEqual(leader["classification_status"], "gated")
        self.assertIsNone(leader["sector_state"])
        self.assertIn("constituent_coverage_below_80_pct", leader["missing_inputs"])

    def test_history_gate_discloses_missing_persistence_and_volatility_baseline(self):
        result = sector_trend_classification(self.feature_rows(), self.history()[:9])
        leader = next(row for row in result["rows"] if row["sector"] == "Leaders")
        self.assertEqual(leader["classification_status"], "gated")
        self.assertIn("five_session_sector_history", leader["missing_inputs"])
        self.assertIn("ten_session_volatility_history", leader["missing_inputs"])

    def test_forward_and_strategy_fields_cannot_change_sector_state(self):
        rows = self.feature_rows()
        first = sector_trend_classification(rows, self.history())
        for row in rows:
            row.update({"forward_return_20d_pct": -99.0, "strategy_profit_factor": 9.0})
        second = sector_trend_classification(rows, self.history())
        self.assertEqual(
            [(row["sector"], row["sector_state"]) for row in first["rows"]],
            [(row["sector"], row["sector_state"]) for row in second["rows"]],
        )

    def test_unreviewed_universe_remains_descriptive_only(self):
        result = sector_trend_classification(
            self.feature_rows(ranking_admissible=False), self.history()
        )
        leader = next(row for row in result["rows"] if row["sector"] == "Leaders")
        self.assertEqual(leader["classification_status"], "descriptive_only")
        self.assertFalse(leader["ranking_admissible"])


class StockLeadershipClassificationTests(unittest.TestCase):
    @staticmethod
    def feature_rows():
        rows = []
        for index, (symbol, return20, return60, return120) in enumerate((
            ("LEADER", 18.0, 30.0, 45.0),
            ("FOLLOWER", 10.0, 20.0, 30.0),
            ("LAGGARD", -5.0, -8.0, -12.0),
        )):
            bullish = symbol != "LAGGARD"
            rows.append({
                "session_date": "2026-11-13",
                "instrument_id": f"stock:{symbol}",
                "symbol": symbol,
                "sector": "Technology",
                "sector_source_date": "2026-01-01",
                "ranking_admissible": True,
                "return_20d_pct": return20,
                "return_60d_pct": return60,
                "return_120d_pct": return120,
                "market_return_20d_pct": 8.0,
                "market_return_60d_pct": 12.0,
                "market_return_120d_pct": 20.0,
                "sector_return_20d_pct": 9.0,
                "sector_return_60d_pct": 15.0,
                "sector_return_120d_pct": 25.0,
                "distance_sma20_pct": 3.0 if bullish else -3.0,
                "distance_sma50_pct": 5.0 if bullish else -5.0,
                "distance_sma200_pct": 12.0 if bullish else -12.0,
                "sma20_slope_5d_pct": 1.2 if bullish else -1.0,
                "sma20_50_spread_pct": 2.0 if bullish else -2.0,
                "sma20_50_crossover_age_sessions": 8,
                "range_compression_ratio_20v60": 0.70,
                "breakout_proximity_20d_pct": -1.0,
                "volatility_expansion_ratio_20v60": 0.90,
                "volume_ratio_20d": 1.2,
                "fno_eligible": index < 2,
                "fno_state": "long_build_up" if index == 0 else None,
            })
        return rows

    @staticmethod
    def sector_rows(state="leading"):
        return [{
            "session_date": "2026-11-13", "sector": "Technology",
            "sector_state": state, "rank_20d": 1, "ranking_admissible": True,
        }]

    @staticmethod
    def history():
        rows = []
        for offset in range(5):
            session = (date(2026, 11, 2) + timedelta(days=offset)).isoformat()
            for symbol, stock_return in (("LEADER", 12.0 + offset), ("FOLLOWER", 9.0), ("LAGGARD", -3.0)):
                rows.append({
                    "session_date": session, "symbol": symbol,
                    "return_20d_pct": stock_return, "sector_return_20d_pct": 8.0,
                })
        return rows

    def test_leader_approaching_breakout_is_distinct_from_extended_stock(self):
        rows = self.feature_rows()
        rows[1]["distance_sma20_pct"] = 10.0
        result = stock_leadership_classification(rows, self.sector_rows(), self.history())
        by_symbol = {row["symbol"]: row for row in result["rows"]}
        self.assertEqual(by_symbol["LEADER"]["leadership_state"], "leading")
        self.assertEqual(by_symbol["LEADER"]["setup_state"], "approaching_trigger")
        self.assertEqual(by_symbol["FOLLOWER"]["setup_state"], "extended")
        self.assertEqual(by_symbol["LEADER"]["stock_sector_rank_20d"], 1)

    def test_missing_sector_state_gates_stock_classification(self):
        result = stock_leadership_classification(
            self.feature_rows(), self.sector_rows(state=None), self.history()
        )
        leader = next(row for row in result["rows"] if row["symbol"] == "LEADER")
        self.assertEqual(leader["classification_status"], "gated")
        self.assertIsNone(leader["leadership_state"])
        self.assertIn("published_sector_state", leader["missing_inputs"])

    def test_insufficient_stock_history_is_disclosed_not_inferred(self):
        result = stock_leadership_classification(
            self.feature_rows(), self.sector_rows(), self.history()[:6]
        )
        leader = next(row for row in result["rows"] if row["symbol"] == "LEADER")
        self.assertEqual(leader["classification_status"], "gated")
        self.assertIn("five_session_stock_history", leader["missing_inputs"])

    def test_forward_and_strategy_fields_do_not_change_stock_state(self):
        rows = self.feature_rows()
        first = stock_leadership_classification(rows, self.sector_rows(), self.history())
        for row in rows:
            row.update({"forward_return_pct": 99.0, "strategy_profit_factor": 10.0})
        second = stock_leadership_classification(rows, self.sector_rows(), self.history())
        self.assertEqual(
            [(row["symbol"], row["leadership_state"], row["setup_state"]) for row in first["rows"]],
            [(row["symbol"], row["leadership_state"], row["setup_state"]) for row in second["rows"]],
        )


class CrossoverStateConditioningTests(unittest.TestCase):
    @staticmethod
    def trades(count=16):
        trades = []
        for index in range(count):
            trades.append({
                "strategy": "sma_20_50" if index % 2 == 0 else "ema_20_50",
                "market_regime": "bullish_trend",
                "sector_state": "leading",
                "stock_leadership": "leading",
                "setup_state": "triggered",
                "fno_state": "long_build_up",
                "gross_return_pct": 4.0 if index % 3 else -1.0,
                "net_return_pct": 3.5 if index % 3 else -1.5,
                "move_capture_ratio": 0.6,
            })
        return trades

    @staticmethod
    def observations():
        return {
            ("market_regime", "bullish_trend"): 20,
            ("sector_state", "leading"): 120,
            ("stock_leadership", "leading"): 400,
            ("setup_state", "triggered"): 50,
            ("fno_state", "long_build_up"): 35,
        }

    def test_frozen_conditioning_reports_state_sample_and_net_metrics(self):
        result = crossover_state_conditioning_contract(self.trades(40), self.observations())
        sma_market = next(row for row in result["rows"] if row["strategy"] == "sma_20_50" and row["dimension"] == "market_regime")
        self.assertEqual(sma_market["state_observations"], 20)
        self.assertEqual(sma_market["completed_trades"], 20)
        self.assertEqual(sma_market["evidence_status"], "descriptive_sample_sufficient")
        self.assertIsNotNone(sma_market["net_trade_expectancy_pct"])
        self.assertIsNotNone(sma_market["net_maximum_drawdown_pct"])
        self.assertEqual(result["parameters"]["sma_windows"], [20, 50])
        self.assertEqual(result["parameters"]["ema_windows"], [20, 50])
        self.assertFalse(result["parameters"]["parameters_optimised"])
        sma_fno = next(row for row in result["rows"] if row["strategy"] == "sma_20_50" and row["dimension"] == "fno_state")
        self.assertEqual(sma_fno["state"], "long_build_up")
        self.assertEqual(sma_fno["state_observations"], 35)
        self.assertEqual(sma_fno["completed_trades"], 20)

    def test_small_conditioned_sample_is_visible_but_not_admitted(self):
        result = crossover_state_conditioning_contract(self.trades(8), self.observations())
        self.assertTrue(all(row["evidence_status"] == "insufficient_evidence" for row in result["rows"]))
        self.assertTrue(all("completed_trades_below_15" in row["missing_evidence"] for row in result["rows"]))


class CrossoverAblationTests(unittest.TestCase):
    @staticmethod
    def samples():
        baseline = []
        joined = []
        for strategy in ("sma_20_50", "ema_20_50"):
            for index in range(30):
                base = {
                    "strategy": strategy, "symbol": f"S{index:02d}",
                    "gross_return_pct": 2.0, "net_return_pct": 1.5,
                    "move_capture_ratio": 0.5,
                    "exit_execution_date": f"2026-10-{(index % 28) + 1:02d}",
                }
                baseline.append(base)
                if index < 20:
                    joined.append({
                        **base,
                        "market_regime": "transition",
                        "sector_state": "leading" if index < 18 else "neutral",
                        "stock_leadership": "leading" if index < 16 else "neutral",
                        "setup_state": "triggered" if index < 16 else "unprimed",
                        "fno_state": "long_build_up" if index < 15 else None,
                    })
        return baseline, joined

    def test_frozen_ladder_is_nested_and_discloses_availability_loss(self):
        baseline, joined = self.samples()
        result = crossover_ablation_contract(baseline, joined)
        sma = [row for row in result["rows"] if row["strategy"] == "sma_20_50"]
        self.assertEqual([row["completed_trades"] for row in sma], [30, 20, 18, 16, 15])
        self.assertEqual(sma[1]["unavailable_state_trades"], 10)
        self.assertEqual(sma[2]["filter_excluded_trades"], 2)
        self.assertEqual(sma[-1]["sample_status"], "descriptive_sample_sufficient")
        self.assertEqual(
            [stage["key"] for stage in result["stage_definitions"]],
            ["crossover_only", "market_regime", "sector_trend", "stock_priming", "fno_alignment"],
        )

    def test_final_layer_is_withheld_when_fno_sample_is_too_small(self):
        baseline, joined = self.samples()
        for row in joined:
            if int(row["symbol"][1:]) >= 5:
                row["fno_state"] = None
        result = crossover_ablation_contract(baseline, joined)
        final_rows = [row for row in result["rows"] if row["stage_key"] == "fno_alignment"]
        self.assertTrue(all(row["completed_trades"] == 5 for row in final_rows))
        self.assertTrue(all(row["sample_status"] == "insufficient_evidence" for row in final_rows))


class TopDownOosAdmissionTests(unittest.TestCase):
    @staticmethod
    def rejected_ablation_rows():
        rows = []
        for strategy in ("sma_20_50", "ema_20_50"):
            for order, key, trades in (
                (1, "crossover_only", 100),
                (2, "market_regime", 30),
                (3, "sector_trend", 20),
                (4, "stock_priming", 7),
                (5, "fno_alignment", 3),
            ):
                rows.append({
                    "strategy": strategy, "stage_order": order, "stage_key": key,
                    "completed_trades": trades, "state_eligible_trades": 30 if order == 2 else trades,
                    "net_trade_expectancy_pct": -1.0, "net_profit_factor": 0.7,
                    "incremental_status": "baseline" if order == 1 else "no_incremental_improvement",
                })
        return rows

    def test_failed_preadmission_gates_stop_oos_and_publish_no_score(self):
        audit = {
            "contract_version": "cross-strategy-oos-benjamini-hochberg-fdr-v2",
            "coverage_complete": False, "tested_hypotheses": 0, "fdr_q_below_alpha": 0,
        }
        result = top_down_oos_admission_contract(self.rejected_ablation_rows(), audit)
        self.assertEqual(result["admitted_strategies"], 0)
        self.assertEqual(result["rejected_strategies"], 2)
        for row in result["rows"]:
            self.assertEqual(row["admission_decision"], "rejected")
            self.assertEqual(row["walk_forward_status"], "not_run_pre_admission_failed")
            self.assertIsNone(row["oos_score"])
            self.assertIn("ablation_gate_failed", row["failure_reasons"])
            self.assertIn("final_stage_completed_trades_below_15", row["failure_reasons"])
            self.assertIn("cross_strategy_oos_family_incomplete", row["failure_reasons"])

    def test_missing_oos_evidence_cannot_be_interpreted_as_pass(self):
        rows = self.rejected_ablation_rows()
        for row in rows:
            row["incremental_status"] = (
                "baseline" if row["stage_order"] == 1 else "incremental_improvement_observed"
            )
            row["completed_trades"] = 20
            row["net_trade_expectancy_pct"] = 1.0
            row["net_profit_factor"] = 1.3
            if row["stage_order"] == 2:
                row["state_eligible_trades"] = 100
        result = top_down_oos_admission_contract(rows, {
            "coverage_complete": False, "tested_hypotheses": 0, "fdr_q_below_alpha": 0,
        })
        self.assertTrue(all(row["admission_decision"] == "pending_oos" for row in result["rows"]))
        self.assertTrue(all(row["oos_score"] is None for row in result["rows"]))
        self.assertTrue(all("cross_strategy_oos_family_incomplete" in row["failure_reasons"] for row in result["rows"]))


class OpportunityBoardReleaseTests(unittest.TestCase):
    def test_rejected_strategies_withhold_board_and_prohibit_trade_outputs(self):
        result = opportunity_board_release_contract([
            {"strategy": "sma_20_50", "admission_decision": "rejected", "failure_reasons": ["ablation_gate_failed"]},
            {"strategy": "ema_20_50", "admission_decision": "rejected", "failure_reasons": ["minimum_trade_gate_failed"]},
        ])
        self.assertFalse(result["board_authorised"])
        self.assertEqual(result["release_decision"], "withheld")
        self.assertEqual(result["ui_mode"], "research_status_only")
        self.assertIn("opportunity_rank", result["prohibited_outputs"])
        self.assertIn("order_control", result["prohibited_outputs"])

    def test_only_an_admitted_contract_can_authorise_board(self):
        result = opportunity_board_release_contract([
            {"strategy": "future_model", "admission_decision": "admitted", "failure_reasons": []},
        ])
        self.assertTrue(result["board_authorised"])
        self.assertEqual(result["release_decision"], "authorised")
        self.assertEqual(result["prohibited_outputs"], [])


class MarketRegimeClassificationTests(unittest.TestCase):
    @staticmethod
    def feature_rows(
        count=100, *, market_return=5.0, distance200=4.0, volatility=25.0,
        breadth50=65.0, breadth200=60.0, ranking_admissible=True,
    ):
        return [
            {
                "session_date": "2026-11-13",
                "symbol": f"STOCK{index:03d}",
                "close": 100 + index,
                "distance_sma50_pct": 1.0,
                "distance_sma200_pct": 1.0,
                "market_return_20d_pct": market_return,
                "market_distance_sma200_pct": distance200,
                "market_volatility_20d_pct": volatility,
                "market_breadth_50dma_pct": breadth50,
                "market_breadth_200dma_pct": breadth200,
                "ranking_admissible": ranking_admissible,
                "source_dates": {"feature_session": "2026-11-13"},
            }
            for index in range(count)
        ]

    def test_classifies_frozen_strong_uptrend_dimensions(self):
        result = market_regime_classification(self.feature_rows())
        self.assertEqual(result["regime_family"], "bullish_trend")
        self.assertEqual(result["trend_direction"], "up")
        self.assertEqual(result["trend_strength"], "strong")
        self.assertEqual(result["range_state"], "trend_expansion")
        self.assertEqual(result["volatility_state"], "elevated")
        self.assertEqual(result["breadth_state"], "broad_positive")
        self.assertTrue(result["ranking_admissible"])

    def test_classifies_range_without_using_strategy_results(self):
        rows = self.feature_rows(
            market_return=1.0, distance200=1.0, volatility=15.0,
            breadth50=50.0, breadth200=48.0,
        )
        first = market_regime_classification(rows)
        for row in rows:
            row.update({"strategy_return_pct": 500.0, "forward_return_pct": -90.0, "trade_won": True})
        second = market_regime_classification(rows)
        self.assertEqual(first["regime_key"], second["regime_key"])
        self.assertEqual(second["regime_family"], "range")
        self.assertEqual(second["range_state"], "range_bound")
        self.assertFalse(second["definitions"]["strategy_results_used"])
        self.assertFalse(second["definitions"]["forward_labels_used"])

    def test_gates_when_cross_sectional_coverage_is_below_95_percent(self):
        rows = self.feature_rows()
        for row in rows[:6]:
            row["distance_sma200_pct"] = None
        result = market_regime_classification(rows)
        self.assertEqual(result["classification_status"], "gated")
        self.assertIsNone(result["regime_family"])
        self.assertIn("breadth_200dma_coverage_below_95_pct", result["missing_inputs"])
        self.assertFalse(result["ranking_admissible"])

    def test_partial_universe_remains_descriptive(self):
        result = market_regime_classification(self.feature_rows(ranking_admissible=False))
        self.assertEqual(result["classification_status"], "descriptive_only")
        self.assertEqual(result["regime_family"], "bullish_trend")
        self.assertFalse(result["ranking_admissible"])

    def test_sample_counts_keep_trade_join_separate(self):
        rows = [
            {"session_date": "2026-11-12", "regime_family": "range",
             "trend_direction": "neutral", "trend_strength": "conflicted",
             "range_state": "range_bound", "volatility_state": "normal", "breadth_state": "mixed"},
            {"session_date": "2026-11-13", "regime_family": "bullish_trend",
             "trend_direction": "up", "trend_strength": "strong",
             "range_state": "trend_expansion", "volatility_state": "elevated", "breadth_state": "broad_positive"},
        ]
        without_trades = _market_regime_sample_summary(rows)
        self.assertIsNone(without_trades["by_regime_family"]["range"]["trades"])
        with_trades = _market_regime_sample_summary(rows, ["2026-11-13", "2026-11-13"])
        self.assertEqual(with_trades["by_regime_family"]["bullish_trend"]["trades"], 2)
        self.assertEqual(with_trades["sessions"], 2)


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
                {"status": "ready", "symbol": "AAA", "evidence_ranking": {"oos_net_trade_edge_p_value": 0.01}},
                {"status": "ready", "symbol": "BBB", "evidence_ranking": {"oos_net_trade_edge_p_value": 0.20}},
            ]},
            {"engine": "beta", "results": [
                {"status": "ready", "symbol": "AAA", "evidence_ranking": {"oos_net_trade_edge_p_value": 0.02}},
                {"status": "data_quality_gated", "symbol": "CCC", "evidence_ranking": {}},
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
