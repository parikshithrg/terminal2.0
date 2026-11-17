import unittest

from workspace_analytics.execution import (
    EQUITY_DELIVERY_COST_SCHEDULE,
    equity_delivery_cost_calculator,
    normalise_cost_config,
)
from workspace_analytics.metrics import maximum_drawdown_pct


class ExecutionModuleContractTests(unittest.TestCase):
    def test_default_capital_and_schedule_are_frozen(self):
        assumptions = normalise_cost_config()
        self.assertEqual(assumptions["position_capital_inr"], 50000.0)
        self.assertEqual(
            EQUITY_DELIVERY_COST_SCHEDULE["schedule_version"],
            "zerodha-nse-equity-delivery-2026-03-v1",
        )

    def test_public_cost_contract_remains_versioned(self):
        contract = equity_delivery_cost_calculator(50000, 55000)
        self.assertEqual(contract["contract_version"], "equity-delivery-cost-v1")
        self.assertGreater(contract["total_cost_inr"], 0)


class MetricsModuleContractTests(unittest.TestCase):
    def test_maximum_drawdown_uses_peak_to_trough_path(self):
        self.assertEqual(maximum_drawdown_pct([1.0, 1.2, 0.9, 1.1]), -25.0)
        self.assertIsNone(maximum_drawdown_pct([]))


if __name__ == "__main__":
    unittest.main()

