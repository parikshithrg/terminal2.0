import unittest

from workspace_analytics.ranking import (
    attach_comparison_evidence_rankings,
    attach_oos_evidence_rankings,
    cross_strategy_fdr_from_payloads,
)


class RankingModuleContractTests(unittest.TestCase):
    def test_comparison_gate_never_ranks_failed_evidence(self):
        rows = [{"status": "data_quality_gated"}]
        contract = attach_comparison_evidence_rankings(rows)
        self.assertEqual(contract["ranking_admitted"], 0)
        self.assertIsNone(rows[0]["evidence_ranking"]["score"])

    def test_oos_gate_requires_complete_evidence(self):
        rows = [{"status": "ready", "symbol": "TEST"}]
        contract = attach_oos_evidence_rankings(rows, {})
        self.assertEqual(contract["ranking_admitted"], 0)
        self.assertIn("full_history_walk_forward_unavailable", rows[0]["evidence_ranking"]["failed_checks"])

    def test_cross_strategy_family_remains_combined(self):
        payload = cross_strategy_fdr_from_payloads([{
            "engine": "alpha",
            "results": [{
                "status": "ready",
                "symbol": "AAA",
                "evidence_ranking": {
                    "ranking_admissible": True,
                    "diagnostic_score": 80,
                    "oos_net_trade_edge_p_value": 0.01,
                },
            }],
        }])
        self.assertEqual(payload["contract_version"], "cross-strategy-oos-benjamini-hochberg-fdr-v2")
        self.assertEqual(payload["tested_hypotheses"], 1)


if __name__ == "__main__":
    unittest.main()

