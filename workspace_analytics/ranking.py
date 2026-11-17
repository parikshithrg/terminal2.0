"""Evidence-ranking gates for comparison, OOS, and cross-strategy families."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from .statistics import attach_benjamini_hochberg_fdr


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(value, digits)


def _clamp_unit(value: float) -> float:
    return min(1.0, max(0.0, value))


def attach_comparison_evidence_rankings(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Attach diagnostics while refusing a rank when any frozen gate fails."""
    weights = {
        "net_profit_factor": 20.0,
        "net_trade_edge": 15.0,
        "completed_trade_sample": 10.0,
        "within_strategy_confidence": 15.0,
        "drawdown_control": 10.0,
        "excess_return": 10.0,
        "exposure_quality": 5.0,
        "turnover_efficiency": 5.0,
        "cost_efficiency": 5.0,
        "period_stability": 5.0,
    }
    admitted = 0
    for row in results:
        if row.get("status") != "ready":
            row["evidence_ranking"] = {
                "status": "gated",
                "ranking_admissible": False,
                "score": None,
                "diagnostic_score": None,
                "failed_checks": ["data_quality"],
            }
            continue
        profit_factor = float(row.get("net_profit_factor") or 0.0)
        edge = float(row.get("net_trade_expectancy_pct") or 0.0)
        trades = int(row.get("completed_trades") or 0)
        q_value = row.get("net_trade_edge_q_value")
        drawdown = float(row.get("net_maximum_drawdown_pct") or -100.0)
        excess = float(row.get("excess_return_pct") or 0.0)
        exposure = float(row.get("exposure_pct") or 0.0)
        completed_turns = int(row.get("executed_sells") or 0)
        cost_drag = max(0.0, float(row.get("estimated_cost_drag_pct") or 0.0))
        components = {
            "net_profit_factor": weights["net_profit_factor"] * _clamp_unit((profit_factor - 1.0) / 1.5),
            "net_trade_edge": weights["net_trade_edge"] * _clamp_unit(edge / 5.0),
            "completed_trade_sample": weights["completed_trade_sample"] * _clamp_unit(trades / 25.0),
            "within_strategy_confidence": weights["within_strategy_confidence"] * _clamp_unit((0.10 - float(q_value)) / 0.10) if q_value is not None else 0.0,
            "drawdown_control": weights["drawdown_control"] * _clamp_unit((drawdown + 30.0) / 30.0),
            "excess_return": weights["excess_return"] * _clamp_unit((excess + 10.0) / 30.0),
            "exposure_quality": weights["exposure_quality"] * _clamp_unit(1.0 - abs(exposure - 50.0) / 50.0),
            "turnover_efficiency": weights["turnover_efficiency"] * _clamp_unit(1.0 - completed_turns / 20.0),
            "cost_efficiency": weights["cost_efficiency"] * _clamp_unit(1.0 - cost_drag / 10.0),
            "period_stability": 0.0,
        }
        checks = {
            "minimum_completed_trades": trades >= 15,
            "net_profit_factor": profit_factor >= 1.10,
            "positive_net_trade_edge": edge > 0,
            "within_strategy_fdr": q_value is not None and float(q_value) <= 0.05,
            "drawdown_limit": drawdown >= -25.0,
            "positive_excess_return": excess > 0,
            "out_of_sample_stability": False,
        }
        failed = [name for name, passed in checks.items() if not passed]
        diagnostic = sum(components.values())
        ranking_admissible = not failed
        admitted += int(ranking_admissible)
        row["evidence_ranking"] = {
            "contract_version": "comparison-evidence-ranking-v1",
            "status": "admitted" if ranking_admissible else "gated",
            "ranking_admissible": ranking_admissible,
            "score": _round(diagnostic, 2) if ranking_admissible else None,
            "diagnostic_score": _round(diagnostic, 2),
            "components": {name: _round(value, 2) for name, value in components.items()},
            "weights": weights,
            "checks": checks,
            "failed_checks": failed,
            "provisional": True,
            "decision_boundary": "A diagnostic score is visible for audit, but failed evidence is never assigned a rank.",
        }
    return {
        "contract_version": "comparison-evidence-ranking-v1",
        "status": "provisional_pending_oos_substitution",
        "admitted": admitted,
        "ranking_admitted": admitted,
        "gated": len(results) - admitted,
        "weights": weights,
        "hard_gates": [
            "minimum_completed_trades", "net_profit_factor", "positive_net_trade_edge",
            "within_strategy_fdr", "drawdown_limit", "positive_excess_return",
            "out_of_sample_stability",
        ],
    }


def attach_oos_evidence_rankings(
    results: list[dict[str, Any]],
    oos_evidence: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Attach full-history OOS scores only after every admission gate passes."""
    hypotheses = []
    for row in results:
        evidence = oos_evidence.get(str(row.get("symbol") or ""), {})
        probability = evidence.get("net_trade_edge_p_value")
        if evidence.get("status") == "ready" and probability is not None:
            hypotheses.append({
                "status": "ready",
                "symbol": row.get("symbol"),
                "net_trade_edge_p_value": probability,
            })
    oos_fdr = attach_benjamini_hochberg_fdr(hypotheses)
    q_values = {row["symbol"]: row.get("net_trade_edge_q_value") for row in hypotheses}
    weights = {
        "oos_net_profit_factor": 20.0,
        "oos_net_trade_edge": 15.0,
        "oos_trade_sample": 10.0,
        "oos_adjusted_confidence": 15.0,
        "oos_drawdown_control": 10.0,
        "oos_excess_return": 10.0,
        "oos_exposure_quality": 5.0,
        "oos_turnover_efficiency": 5.0,
        "oos_cost_efficiency": 5.0,
        "period_stability": 5.0,
    }
    admitted = 0
    for row in results:
        symbol = str(row.get("symbol") or "")
        evidence = oos_evidence.get(symbol, {})
        row["oos_evidence"] = evidence
        metrics = evidence.get("metrics") or {}
        q_value = q_values.get(symbol)
        if row.get("status") != "ready" or evidence.get("status") != "ready":
            failed = list(evidence.get("failed_checks") or ["full_history_walk_forward_unavailable"])
            row["evidence_ranking"] = {
                "contract_version": "full-history-oos-evidence-ranking-v2",
                "status": "gated",
                "ranking_admissible": False,
                "score": None,
                "diagnostic_score": None,
                "failed_checks": failed,
                "oos_evidence_status": evidence.get("status") or "unavailable",
            }
            continue
        profit_factor = float(metrics.get("median_net_profit_factor") or 0.0)
        edge = float(metrics.get("median_net_trade_expectancy_pct") or 0.0)
        trades = int(metrics.get("completed_trades") or 0)
        drawdown = float(metrics.get("worst_net_drawdown_pct") or -100.0)
        excess = float(metrics.get("median_excess_return_pct") or 0.0)
        exposure = float(metrics.get("median_exposure_pct") or 0.0)
        positive_ratio = float(metrics.get("positive_fold_ratio") or 0.0)
        completed_sells = int(metrics.get("completed_sells") or 0)
        evaluated_sessions = int(metrics.get("evaluated_test_sessions") or 0)
        turnover_rate = completed_sells / max(1.0, evaluated_sessions / 252.0)
        cost_drag = max(0.0, float(metrics.get("median_cost_drag_pct") or 0.0))
        components = {
            "oos_net_profit_factor": weights["oos_net_profit_factor"] * _clamp_unit((profit_factor - 1.0) / 1.5),
            "oos_net_trade_edge": weights["oos_net_trade_edge"] * _clamp_unit(edge / 5.0),
            "oos_trade_sample": weights["oos_trade_sample"] * _clamp_unit(trades / 25.0),
            "oos_adjusted_confidence": weights["oos_adjusted_confidence"] * _clamp_unit((0.10 - float(q_value)) / 0.10) if q_value is not None else 0.0,
            "oos_drawdown_control": weights["oos_drawdown_control"] * _clamp_unit((drawdown + 30.0) / 30.0),
            "oos_excess_return": weights["oos_excess_return"] * _clamp_unit((excess + 10.0) / 30.0),
            "oos_exposure_quality": weights["oos_exposure_quality"] * _clamp_unit(1.0 - abs(exposure - 50.0) / 50.0),
            "oos_turnover_efficiency": weights["oos_turnover_efficiency"] * _clamp_unit(1.0 - turnover_rate / 20.0),
            "oos_cost_efficiency": weights["oos_cost_efficiency"] * _clamp_unit(1.0 - cost_drag / 10.0),
            "period_stability": weights["period_stability"] * _clamp_unit(positive_ratio),
        }
        checks = {
            "minimum_admitted_test_folds": int(metrics.get("admitted_test_folds") or 0) >= 3,
            "minimum_completed_trades": trades >= 15,
            "positive_fold_ratio": positive_ratio >= 0.60,
            "net_profit_factor": profit_factor >= 1.10,
            "positive_net_trade_edge": edge > 0,
            "oos_within_strategy_fdr": q_value is not None and float(q_value) <= 0.05,
            "drawdown_limit": drawdown >= -25.0,
            "positive_excess_return": excess > 0,
        }
        failed = [name for name, passed in checks.items() if not passed]
        diagnostic = sum(components.values())
        ranking_admissible = not failed
        admitted += int(ranking_admissible)
        row["evidence_ranking"] = {
            "contract_version": "full-history-oos-evidence-ranking-v2",
            "status": "admitted" if ranking_admissible else "gated",
            "ranking_admissible": ranking_admissible,
            "score": _round(diagnostic, 2) if ranking_admissible else None,
            "diagnostic_score": _round(diagnostic, 2),
            "components": {name: _round(value, 2) for name, value in components.items()},
            "weights": weights,
            "checks": checks,
            "failed_checks": failed,
            "oos_net_trade_edge_p_value": evidence.get("net_trade_edge_p_value"),
            "oos_net_trade_edge_q_value": q_value,
            "oos_metrics": metrics,
            "decision_boundary": "Only point-in-time-admitted, full-history walk-forward evidence can receive a ranking score.",
        }
    return {
        "contract_version": "full-history-oos-evidence-ranking-v2",
        "status": "ready",
        "ranking_admitted": admitted,
        "gated": len(results) - admitted,
        "weights": weights,
        "oos_multiple_testing": oos_fdr,
        "point_in_time_membership_enforced_per_fold": True,
    }


def cross_strategy_fdr_from_payloads(
    payloads: list[dict[str, Any]],
    alpha: float = 0.05,
) -> dict[str, Any]:
    """Correct the combined full-history stock-by-strategy hypothesis family."""
    hypotheses: list[dict[str, Any]] = []
    for payload in payloads:
        engine = str(payload.get("engine") or "unknown")
        for row in payload.get("results", []):
            base_ranking = row.get("evidence_ranking") or {}
            probability = base_ranking.get("oos_net_trade_edge_p_value")
            if row.get("status") != "ready" or probability is None:
                continue
            hypotheses.append({
                "status": "ready",
                "engine": engine,
                "symbol": row.get("symbol"),
                "net_trade_edge_p_value": probability,
                "base_evidence_ranking": base_ranking,
            })
    contract = attach_benjamini_hochberg_fdr(hypotheses, alpha)
    by_engine: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in hypotheses:
        base_ranking = row.get("base_evidence_ranking") or {}
        global_fdr_passed = bool(
            row["net_trade_edge_q_value"] is not None
            and row["net_trade_edge_q_value"] <= alpha
        )
        ranking_admissible = bool(base_ranking.get("ranking_admissible") and global_fdr_passed)
        failed_checks = list(base_ranking.get("failed_checks") or [])
        if not global_fdr_passed:
            failed_checks.append("all_strategy_fdr")
        by_engine[row["engine"]][str(row["symbol"])] = {
            "raw_p_value": row["net_trade_edge_p_value"],
            "cross_strategy_q_value": row["net_trade_edge_q_value"],
            "status": row["multiple_testing_status"],
            "evidence_ranking": {
                **base_ranking,
                "contract_version": "cross-strategy-evidence-ranking-v1",
                "ranking_admissible": ranking_admissible,
                "status": "admitted" if ranking_admissible else "gated",
                "score": base_ranking.get("diagnostic_score") if ranking_admissible else None,
                "failed_checks": failed_checks,
                "global_fdr_passed": global_fdr_passed,
            },
        }
    strategy_counts = {
        engine: {
            "tested_hypotheses": len(rows),
            "fdr_q_below_alpha": sum(
                item["cross_strategy_q_value"] <= alpha for item in rows.values()
            ),
        }
        for engine, rows in by_engine.items()
    }
    return {
        "ok": True,
        "contract_version": "cross-strategy-oos-benjamini-hochberg-fdr-v2",
        "family": "all_default_strategy_stock_pairs_full_history_oos",
        "alpha": alpha,
        "tested_hypotheses": contract["tested_hypotheses"],
        "raw_p_below_alpha": contract["raw_p_below_alpha"],
        "fdr_q_below_alpha": contract["fdr_q_below_alpha"],
        "ranking_admitted": sum(
            item["evidence_ranking"]["ranking_admissible"]
            for rows in by_engine.values() for item in rows.values()
        ),
        "strategy_counts": strategy_counts,
        "by_engine": dict(by_engine),
        "interpretation": "Controls the expected false-discovery proportion across all full-history, point-in-time-admitted default stock-by-strategy OOS tests in one combined family.",
    }

