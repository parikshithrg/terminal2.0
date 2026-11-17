import unittest

from workspace_analytics.statistics import (
    attach_benjamini_hochberg_fdr,
    bootstrap_trade_evidence,
    trade_edge_p_value,
)


class StatisticsModuleContractTests(unittest.TestCase):
    def test_trade_edge_contract_matches_frozen_reference(self):
        self.assertEqual(trade_edge_p_value([-1.0, 1.0]), 1.0)
        self.assertAlmostEqual(
            trade_edge_p_value([1.0, 2.0, 3.0, 4.0, 5.0]),
            0.013236,
            places=5,
        )

    def test_bootstrap_contract_is_deterministic(self):
        returns = [3.0, -1.0, 2.0, -0.5, 1.5] * 4
        first = bootstrap_trade_evidence(returns, resamples=250)
        second = bootstrap_trade_evidence(returns, resamples=250)
        self.assertEqual(first, second)
        self.assertEqual(first["contract_version"], "trade-bootstrap-evidence-v1")

    def test_fdr_contract_preserves_monotone_q_values(self):
        rows = [
            {"status": "ready", "net_trade_edge_p_value": 0.01},
            {"status": "ready", "net_trade_edge_p_value": 0.04},
            {"status": "ready", "net_trade_edge_p_value": 0.03},
        ]
        contract = attach_benjamini_hochberg_fdr(rows)
        self.assertEqual(contract["tested_hypotheses"], 3)
        self.assertEqual([row["net_trade_edge_q_value"] for row in rows], [0.03, 0.04, 0.04])


if __name__ == "__main__":
    unittest.main()

