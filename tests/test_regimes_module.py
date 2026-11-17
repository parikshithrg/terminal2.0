import copy
import unittest

import workspace_engine
from workspace_analytics.regimes import (
    MARKET_REGIME_CONTRACT,
    SECTOR_TREND_CONTRACT,
    STOCK_LEADERSHIP_CONTRACT,
    market_regime_classification,
    market_regime_sample_summary,
    sector_trend_classification,
    stock_leadership_classification,
)


class MarketRegimeModuleTests(unittest.TestCase):
    def feature_rows(self):
        row = {
            "session_date": "2026-11-13",
            "close": 100.0,
            "distance_sma50_pct": 4.0,
            "distance_sma200_pct": 6.0,
            "market_return_20d_pct": 5.0,
            "market_distance_sma200_pct": 6.0,
            "market_volatility_20d_pct": 16.0,
            "market_breadth_50dma_pct": 65.0,
            "market_breadth_200dma_pct": 60.0,
            "ranking_admissible": True,
            "source_dates": {"feature_session": "2026-11-13"},
        }
        return [copy.deepcopy(row) for _ in range(20)]

    def test_frozen_contract_and_engine_binding(self):
        self.assertIs(workspace_engine.market_regime_classification, market_regime_classification)
        result = market_regime_classification(self.feature_rows())
        self.assertEqual(result["contract_version"], MARKET_REGIME_CONTRACT)
        self.assertEqual(result["regime_family"], "bullish_trend")
        self.assertEqual(result["classification_status"], "ready")
        self.assertTrue(result["ranking_admissible"])

    def test_coverage_failure_remains_explicit(self):
        rows = self.feature_rows()
        rows[0]["close"] = None
        rows[1]["close"] = None
        rows[1]["distance_sma50_pct"] = None
        rows[2]["distance_sma50_pct"] = None
        result = market_regime_classification(rows)
        self.assertEqual(result["classification_status"], "gated")
        self.assertIn("priced_coverage_below_95_pct", result["missing_inputs"])
        self.assertIn("breadth_50dma_coverage_below_95_pct", result["missing_inputs"])

    def test_sample_summary_does_not_invent_trade_counts(self):
        regimes = [{
            "session_date": "2026-11-13",
            "regime_family": "bullish_trend",
            "trend_direction": "up",
            "trend_strength": "strong",
            "range_state": "trend_expansion",
            "volatility_state": "normal",
            "breadth_state": "broad_positive",
        }]
        unjoined = market_regime_sample_summary(regimes)
        self.assertIsNone(unjoined["by_regime_family"]["bullish_trend"]["trades"])
        joined = market_regime_sample_summary(regimes, ["2026-11-13", "2026-11-13"])
        self.assertEqual(joined["by_regime_family"]["bullish_trend"]["trades"], 2)


class SectorTrendModuleTests(unittest.TestCase):
    @staticmethod
    def feature_rows(coverage: float = 1.0):
        definitions = {
            "Leaders": (8.0, 15.0, 24.0, 5.0),
            "Middle": (4.0, 8.0, 12.0, 1.0),
            "Laggards": (-4.0, -8.0, -12.0, -5.0),
        }
        rows = []
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
                    "ranking_admissible": True,
                })
        return rows

    @staticmethod
    def history():
        rows = []
        for offset in range(10):
            session = f"2026-10-{20 + offset:02d}"
            for rank, sector in enumerate(("Leaders", "Middle", "Laggards"), start=1):
                rows.append({
                    "session_date": session,
                    "sector": sector,
                    "relative_strength_20d_pct": 3.0 - rank,
                    "rank_20d": rank,
                    "realised_volatility_20d_pct": 18.0 + offset / 10,
                })
        return rows

    def test_frozen_sector_contract_and_engine_binding(self):
        self.assertIs(workspace_engine.sector_trend_classification, sector_trend_classification)
        result = sector_trend_classification(self.feature_rows(), self.history())
        by_sector = {row["sector"]: row for row in result["rows"]}
        self.assertEqual(result["contract_version"], SECTOR_TREND_CONTRACT)
        self.assertEqual(by_sector["Leaders"]["sector_state"], "leading")
        self.assertEqual(by_sector["Laggards"]["sector_state"], "lagging")

    def test_sector_coverage_gate_is_preserved(self):
        result = sector_trend_classification(self.feature_rows(0.7), self.history())
        leader = next(row for row in result["rows"] if row["sector"] == "Leaders")
        self.assertEqual(leader["classification_status"], "gated")
        self.assertIn("constituent_coverage_below_80_pct", leader["missing_inputs"])


class StockLeadershipModuleTests(unittest.TestCase):
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
            for symbol, stock_return in (("LEADER", 12.0 + offset), ("FOLLOWER", 9.0), ("LAGGARD", -3.0)):
                rows.append({
                    "session_date": f"2026-11-{2 + offset:02d}",
                    "symbol": symbol,
                    "return_20d_pct": stock_return,
                    "sector_return_20d_pct": 8.0,
                })
        return rows

    def test_frozen_stock_contract_and_engine_binding(self):
        self.assertIs(workspace_engine.stock_leadership_classification, stock_leadership_classification)
        rows = self.feature_rows()
        rows[1]["distance_sma20_pct"] = 10.0
        result = stock_leadership_classification(rows, self.sector_rows(), self.history())
        by_symbol = {row["symbol"]: row for row in result["rows"]}
        self.assertEqual(result["contract_version"], STOCK_LEADERSHIP_CONTRACT)
        self.assertEqual(by_symbol["LEADER"]["leadership_state"], "leading")
        self.assertEqual(by_symbol["LEADER"]["setup_state"], "approaching_trigger")
        self.assertEqual(by_symbol["FOLLOWER"]["setup_state"], "extended")

    def test_missing_sector_state_still_gates_stock(self):
        result = stock_leadership_classification(
            self.feature_rows(), self.sector_rows(state=None), self.history()
        )
        leader = next(row for row in result["rows"] if row["symbol"] == "LEADER")
        self.assertEqual(leader["classification_status"], "gated")
        self.assertIn("published_sector_state", leader["missing_inputs"])


if __name__ == "__main__":
    unittest.main()
