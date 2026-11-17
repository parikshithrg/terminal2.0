import unittest
from datetime import date, timedelta

import workspace_engine
from workspace_analytics.mean_reversion_strategies import (
    bollinger_mean_reversion_series,
    pullback_uptrend_series,
    rsi_mean_reversion_series,
    selloff_recovery_series,
    zscore_mean_reversion_series,
)
from workspace_analytics.trend_strategies import (
    donchian_breakout_series,
    ema_crossover_series,
    moving_average_crossover_series,
    time_series_momentum_series,
    triple_sma_alignment_series,
)


class StrategyFamilyModuleTests(unittest.TestCase):
    @staticmethod
    def rows():
        rows = []
        previous = 100.0
        for index in range(260):
            close = 100.0 + index * 0.12 + ((index % 17) - 8) * 0.65
            opening = previous
            rows.append({
                "session_date": (date(2025, 1, 1) + timedelta(days=index)).isoformat(),
                "open": opening,
                "high": max(opening, close) + 1.0,
                "low": min(opening, close) - 1.0,
                "close": close,
                "volume": 100_000 + index,
            })
            previous = close
        return rows

    def assert_component_matches_engine(self, engine_function, component, arguments):
        rows = self.rows()
        expected = component(
            rows, *arguments, metrics=workspace_engine._long_cash_metrics,
        )
        actual = engine_function(rows, *arguments)
        self.assertEqual(actual, expected)

    def test_all_five_trend_components_match_engine_contracts(self):
        cases = (
            (workspace_engine._moving_average_crossover_series, moving_average_crossover_series, (20, 50)),
            (workspace_engine._donchian_breakout_series, donchian_breakout_series, (55, 20)),
            (workspace_engine._ema_crossover_series, ema_crossover_series, (20, 50)),
            (workspace_engine._time_series_momentum_series, time_series_momentum_series, (63,)),
            (workspace_engine._triple_sma_alignment_series, triple_sma_alignment_series, (20, 50, 200)),
        )
        for engine_function, component, arguments in cases:
            with self.subTest(component=component.__name__):
                self.assert_component_matches_engine(engine_function, component, arguments)

    def test_all_five_mean_reversion_components_match_engine_contracts(self):
        cases = (
            (workspace_engine._rsi_mean_reversion_series, rsi_mean_reversion_series, (2, 10.0, 70.0)),
            (workspace_engine._bollinger_mean_reversion_series, bollinger_mean_reversion_series, (20, 2.0)),
            (workspace_engine._zscore_mean_reversion_series, zscore_mean_reversion_series, (10, -1.5, 0.0)),
            (workspace_engine._pullback_uptrend_series, pullback_uptrend_series, (5, 200)),
            (workspace_engine._selloff_recovery_series, selloff_recovery_series, (3, 5)),
        )
        for engine_function, component, arguments in cases:
            with self.subTest(component=component.__name__):
                self.assert_component_matches_engine(engine_function, component, arguments)


if __name__ == "__main__":
    unittest.main()
