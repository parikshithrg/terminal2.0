"""Frozen completed-session market-regime classification contracts.

This module is deliberately independent of persistence and strategy results.  It
accepts the point-in-time feature rows produced by the engine and returns the
published classification payload; database orchestration remains in
``workspace_engine``.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import date, datetime, timezone
from statistics import median
from typing import Any


MARKET_REGIME_CONTRACT = "market-regime-classification-v1"
MARKET_REGIME_MINIMUM_COVERAGE_RATIO = 0.95
SECTOR_TREND_CONTRACT = "sector-trend-classification-v1"
SECTOR_TREND_MINIMUM_COVERAGE_RATIO = 0.80
SECTOR_TREND_PERSISTENCE_SESSIONS = 5
SECTOR_TREND_VOLATILITY_BASELINE_SESSIONS = 10
STOCK_LEADERSHIP_CONTRACT = "stock-leadership-priming-v1"
STOCK_LEADERSHIP_HISTORY_SESSIONS = 5


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(float(value), digits)


def _assign_descending_rank(
    rows: list[dict[str, Any]], value_key: str, rank_key: str,
) -> None:
    ranked = sorted(
        (row for row in rows if row.get(value_key) is not None),
        key=lambda row: (-float(row[value_key]), str(row.get("symbol") or row.get("sector") or "")),
    )
    for position, row in enumerate(ranked, start=1):
        row[rank_key] = position


def _mean_present(rows: list[dict[str, Any]], key: str) -> tuple[float | None, int]:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return (sum(values) / len(values), len(values)) if values else (None, 0)


def market_regime_classification(
    feature_rows: list[Any], session_date: str | None = None,
) -> dict[str, Any]:
    """Classify one completed feature-ledger session without forward labels."""
    rows = [dict(row) for row in feature_rows]
    sessions = sorted({str(row.get("session_date") or "") for row in rows if row.get("session_date")})
    if session_date:
        try:
            date.fromisoformat(session_date)
        except ValueError as error:
            raise ValueError("invalid_market_regime_session_date") from error
    if len(sessions) > 1 or (session_date and sessions and sessions[0] != session_date):
        raise ValueError("market_regime_requires_single_feature_session")
    resolved_session = session_date or (sessions[0] if sessions else None)
    if not rows or not resolved_session:
        return {"ok": False, "reason": "feature_ledger_session_unavailable"}

    input_keys = (
        "market_return_20d_pct", "market_distance_sma200_pct",
        "market_volatility_20d_pct", "market_breadth_50dma_pct",
        "market_breadth_200dma_pct",
    )
    inputs: dict[str, float | None] = {}
    missing_inputs: list[str] = []
    inconsistent_inputs: list[str] = []
    for key in input_keys:
        values = sorted({round(float(row[key]), 8) for row in rows if row.get(key) is not None})
        if not values:
            inputs[key] = None
            missing_inputs.append(key)
        elif len(values) > 1:
            inputs[key] = None
            inconsistent_inputs.append(key)
        else:
            inputs[key] = values[0]

    universe_rows = len(rows)
    priced_rows = sum(row.get("close") is not None for row in rows)
    breadth50_denominator = sum(row.get("distance_sma50_pct") is not None for row in rows)
    breadth200_denominator = sum(row.get("distance_sma200_pct") is not None for row in rows)
    priced_coverage = priced_rows / universe_rows if universe_rows else 0.0
    breadth50_coverage = breadth50_denominator / universe_rows if universe_rows else 0.0
    breadth200_coverage = breadth200_denominator / universe_rows if universe_rows else 0.0
    coverage_failures = []
    if priced_coverage < MARKET_REGIME_MINIMUM_COVERAGE_RATIO:
        coverage_failures.append("priced_coverage_below_95_pct")
    if breadth50_coverage < MARKET_REGIME_MINIMUM_COVERAGE_RATIO:
        coverage_failures.append("breadth_50dma_coverage_below_95_pct")
    if breadth200_coverage < MARKET_REGIME_MINIMUM_COVERAGE_RATIO:
        coverage_failures.append("breadth_200dma_coverage_below_95_pct")
    missing_inputs.extend(f"inconsistent_{key}" for key in inconsistent_inputs)
    missing_inputs.extend(coverage_failures)
    universe_admissible = all(bool(row.get("ranking_admissible")) for row in rows)

    trend_direction = None
    trend_strength = None
    range_state = None
    volatility_state = None
    breadth_state = None
    regime_family = None
    regime_key = None
    if not missing_inputs:
        return20 = float(inputs["market_return_20d_pct"])
        distance200 = float(inputs["market_distance_sma200_pct"])
        volatility20 = float(inputs["market_volatility_20d_pct"])
        breadth50 = float(inputs["market_breadth_50dma_pct"])
        breadth200 = float(inputs["market_breadth_200dma_pct"])

        if distance200 >= 3 and return20 >= 2 and breadth50 >= 60 and breadth200 >= 55:
            trend_direction, trend_strength = "up", "strong"
        elif distance200 > 0 and return20 > 0:
            trend_direction = "up"
            trend_strength = "moderate" if breadth50 >= 50 else "weak"
        elif distance200 <= -3 and return20 <= -2 and breadth50 <= 40 and breadth200 <= 45:
            trend_direction, trend_strength = "down", "strong"
        elif distance200 < 0 and return20 < 0:
            trend_direction = "down"
            trend_strength = "moderate" if breadth50 < 50 else "weak"
        else:
            trend_direction, trend_strength = "neutral", "conflicted"

        if abs(distance200) <= 2 and abs(return20) <= 3 and 40 <= breadth50 <= 60:
            range_state = "range_bound"
        elif trend_strength == "strong":
            range_state = "trend_expansion"
        else:
            range_state = "transition"

        if volatility20 < 12:
            volatility_state = "low"
        elif volatility20 < 20:
            volatility_state = "normal"
        elif volatility20 < 30:
            volatility_state = "elevated"
        else:
            volatility_state = "high"

        if breadth50 >= 60 and breadth200 >= 55:
            breadth_state = "broad_positive"
        elif breadth50 <= 40 and breadth200 <= 45:
            breadth_state = "broad_negative"
        elif breadth50 >= 50 and breadth200 >= 45:
            breadth_state = "positive"
        elif breadth50 < 50 and breadth200 < 55:
            breadth_state = "negative"
        else:
            breadth_state = "mixed"

        if range_state == "range_bound":
            regime_family = "range"
        elif trend_direction == "up":
            regime_family = "bullish_trend"
        elif trend_direction == "down":
            regime_family = "bearish_trend"
        else:
            regime_family = "transition"
        regime_key = "/".join((
            regime_family, trend_strength, range_state, volatility_state, breadth_state,
        ))

    source_dates = {}
    for row in rows:
        raw = row.get("source_dates")
        if raw is None and row.get("source_dates_json"):
            try:
                raw = json.loads(row["source_dates_json"])
            except (TypeError, json.JSONDecodeError):
                raw = None
        if raw:
            source_dates = dict(raw)
            break
    classification_status = (
        "gated" if missing_inputs else "ready" if universe_admissible else "descriptive_only"
    )
    generated_at = datetime.now(timezone.utc).isoformat()
    return {
        "ok": True,
        "contract_version": MARKET_REGIME_CONTRACT,
        "session_date": resolved_session,
        "classification_status": classification_status,
        "ranking_admissible": bool(not missing_inputs and universe_admissible),
        "regime_family": regime_family,
        "trend_direction": trend_direction,
        "trend_strength": trend_strength,
        "range_state": range_state,
        "volatility_state": volatility_state,
        "breadth_state": breadth_state,
        "regime_key": regime_key,
        **inputs,
        "universe_rows": universe_rows,
        "priced_rows": priced_rows,
        "breadth_50dma_denominator": breadth50_denominator,
        "breadth_200dma_denominator": breadth200_denominator,
        "priced_coverage_ratio": _round(priced_coverage, 6),
        "breadth_50dma_coverage_ratio": _round(breadth50_coverage, 6),
        "breadth_200dma_coverage_ratio": _round(breadth200_coverage, 6),
        "source_dates": source_dates,
        "missing_inputs": sorted(set(missing_inputs)),
        "generated_at": generated_at,
        "definitions": {
            "trend": {
                "strong_up": "distance_vs_200dma >= 3%, 20-session return >= 2%, breadth50 >= 60%, breadth200 >= 55%",
                "moderate_up": "distance_vs_200dma > 0, 20-session return > 0, breadth50 >= 50%",
                "strong_down": "distance_vs_200dma <= -3%, 20-session return <= -2%, breadth50 <= 40%, breadth200 <= 45%",
                "moderate_down": "distance_vs_200dma < 0, 20-session return < 0, breadth50 < 50%",
            },
            "range_bound": "absolute 200dma distance <= 2%, absolute 20-session return <= 3%, breadth50 between 40% and 60%",
            "volatility": "annualised 20-session realised volatility: low <12%, normal <20%, elevated <30%, high >=30%",
            "breadth": "frozen 50dma/200dma participation thresholds",
            "minimum_cross_sectional_coverage": MARKET_REGIME_MINIMUM_COVERAGE_RATIO,
            "strategy_results_used": False,
            "forward_labels_used": False,
        },
    }


def market_regime_sample_summary(
    regimes: list[dict[str, Any]], trade_sessions: list[str] | None = None,
) -> dict[str, Any]:
    """Summarise frozen states; trades are absent until explicitly joined."""
    trade_counts = defaultdict(int)
    if trade_sessions is not None:
        for session in trade_sessions:
            trade_counts[str(session)] += 1
    family_counts: dict[str, dict[str, Any]] = {}
    dimension_counts: dict[str, dict[str, int]] = {
        "trend_direction": defaultdict(int),
        "trend_strength": defaultdict(int),
        "range_state": defaultdict(int),
        "volatility_state": defaultdict(int),
        "breadth_state": defaultdict(int),
    }
    for row in regimes:
        family = str(row.get("regime_family") or "gated")
        aggregate = family_counts.setdefault(family, {"sessions": 0, "trades": 0})
        aggregate["sessions"] += 1
        aggregate["trades"] += trade_counts.get(str(row.get("session_date")), 0)
        for dimension in dimension_counts:
            dimension_counts[dimension][str(row.get(dimension) or "gated")] += 1
    if trade_sessions is None:
        for aggregate in family_counts.values():
            aggregate["trades"] = None
    return {
        "contract_version": "market-regime-sample-summary-v1",
        "sessions": len(regimes),
        "trade_sample_status": "available" if trade_sessions is not None else "not_joined_until_strategy_conditioning",
        "by_regime_family": family_counts,
        "by_dimension": {key: dict(value) for key, value in dimension_counts.items()},
    }


def sector_trend_classification(
    feature_rows: list[Any], historical_sector_rows: list[Any] | None = None,
    session_date: str | None = None,
) -> dict[str, Any]:
    """Rank and classify sectors from completed-session feature rows only."""
    rows = [dict(row) for row in feature_rows]
    sessions = sorted({str(row.get("session_date") or "") for row in rows if row.get("session_date")})
    if session_date:
        date.fromisoformat(session_date)
    if len(sessions) > 1 or (session_date and sessions and sessions[0] != session_date):
        raise ValueError("sector_trend_requires_single_feature_session")
    resolved_session = session_date or (sessions[0] if sessions else None)
    if not rows or not resolved_session:
        return {"ok": False, "reason": "feature_ledger_session_unavailable"}

    history_by_sector: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for historical in historical_sector_rows or []:
        item = dict(historical)
        if item.get("sector") and str(item.get("session_date") or "") < resolved_session:
            history_by_sector[str(item["sector"])].append(item)
    for sector_history in history_by_sector.values():
        sector_history.sort(key=lambda item: str(item.get("session_date") or ""))

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("sector"):
            grouped[str(row["sector"])].append(row)
    generated_at = datetime.now(timezone.utc).isoformat()
    summaries: list[dict[str, Any]] = []
    for sector, members in sorted(grouped.items()):
        member_count = len(members)
        priced_count = sum(row.get("close") is not None for row in members)
        summary: dict[str, Any] = {
            "contract_version": SECTOR_TREND_CONTRACT,
            "session_date": resolved_session,
            "sector": sector,
            "member_count": member_count,
            "priced_count": priced_count,
            "constituent_coverage_ratio": priced_count / member_count if member_count else 0.0,
            "ranking_admissible": False,
            "sector_state": None,
            "generated_at": generated_at,
        }
        missing: list[str] = []
        for lookback in (20, 60, 120):
            value, count = _mean_present(members, f"return_{lookback}d_pct")
            coverage = count / member_count if member_count else 0.0
            summary[f"sector_return_{lookback}d_pct"] = _round(value, 6)
            summary[f"return_{lookback}d_coverage_ratio"] = _round(coverage, 6)
            market_values = {
                round(float(row[f"market_return_{lookback}d_pct"]), 8)
                for row in members if row.get(f"market_return_{lookback}d_pct") is not None
            }
            market_value = next(iter(market_values)) if len(market_values) == 1 else None
            summary[f"market_return_{lookback}d_pct"] = market_value
            summary[f"relative_strength_{lookback}d_pct"] = (
                _round(value - market_value, 6) if value is not None and market_value is not None else None
            )
            if coverage < SECTOR_TREND_MINIMUM_COVERAGE_RATIO:
                missing.append(f"return_{lookback}d_coverage_below_80_pct")
            if market_value is None:
                missing.append(f"market_return_{lookback}d_pct")

        breadth_rows = [row for row in members if row.get("distance_sma50_pct") is not None]
        alignment_rows = [
            row for row in members
            if all(row.get(key) is not None for key in (
                "distance_sma20_pct", "distance_sma50_pct", "distance_sma200_pct",
            ))
        ]
        slope, slope_count = _mean_present(members, "sma20_slope_5d_pct")
        volatility, volatility_count = _mean_present(members, "realised_volatility_20d_pct")
        summary["breadth_50dma_pct"] = _round(
            100 * sum(float(row["distance_sma50_pct"]) > 0 for row in breadth_rows) / len(breadth_rows), 6
        ) if breadth_rows else None
        summary["ma_alignment_pct"] = _round(
            100 * sum(
                float(row["distance_sma20_pct"]) > 0
                and float(row["distance_sma50_pct"]) > 0
                and float(row["distance_sma200_pct"]) > 0
                for row in alignment_rows
            ) / len(alignment_rows), 6
        ) if alignment_rows else None
        summary["average_sma20_slope_5d_pct"] = _round(slope, 6)
        summary["realised_volatility_20d_pct"] = _round(volatility, 6)
        for name, count in (
            ("breadth_50dma", len(breadth_rows)), ("ma_alignment", len(alignment_rows)),
            ("sma20_slope", slope_count), ("realised_volatility_20d", volatility_count),
        ):
            if not member_count or count / member_count < SECTOR_TREND_MINIMUM_COVERAGE_RATIO:
                missing.append(f"{name}_coverage_below_80_pct")
        if summary["constituent_coverage_ratio"] < SECTOR_TREND_MINIMUM_COVERAGE_RATIO:
            missing.append("constituent_coverage_below_80_pct")
        summary["missing_inputs"] = missing
        summary["source_dates"] = {
            "feature_session": resolved_session,
            "sector_source_dates": sorted({
                str(row["sector_source_date"]) for row in members if row.get("sector_source_date")
            }),
        }
        summaries.append(summary)

    for lookback in (20, 60, 120):
        _assign_descending_rank(
            summaries, f"relative_strength_{lookback}d_pct", f"rank_{lookback}d"
        )
    sector_count = len(summaries)
    top_rank_cutoff = max(1, math.ceil(sector_count * 0.30))
    for summary in summaries:
        history = history_by_sector.get(summary["sector"], [])
        previous_five = history[-SECTOR_TREND_PERSISTENCE_SESSIONS:]
        previous_ten = history[-SECTOR_TREND_VOLATILITY_BASELINE_SESSIONS:]
        if len(previous_five) < SECTOR_TREND_PERSISTENCE_SESSIONS:
            summary["missing_inputs"].append("five_session_sector_history")
            summary["persistence_ratio"] = None
            summary["acceleration_5_sessions_pct"] = None
        else:
            persistence_window = previous_five[-4:] + [summary]
            persistent = sum(
                float(row.get("relative_strength_20d_pct") or 0) > 0
                and int(row.get("rank_20d") or sector_count + 1) <= math.ceil(sector_count / 2)
                for row in persistence_window
            )
            summary["persistence_ratio"] = _round(persistent / len(persistence_window), 6)
            baseline = previous_five[0].get("relative_strength_20d_pct")
            current = summary.get("relative_strength_20d_pct")
            summary["acceleration_5_sessions_pct"] = (
                _round(float(current) - float(baseline), 6)
                if current is not None and baseline is not None else None
            )
            if summary["acceleration_5_sessions_pct"] is None:
                summary["missing_inputs"].append("acceleration_5_sessions_pct")
        prior_volatility = [
            float(row["realised_volatility_20d_pct"])
            for row in previous_ten if row.get("realised_volatility_20d_pct") is not None
        ]
        if len(previous_ten) < SECTOR_TREND_VOLATILITY_BASELINE_SESSIONS or len(prior_volatility) < 8:
            summary["volatility_expansion_ratio"] = None
            summary["missing_inputs"].append("ten_session_volatility_history")
        else:
            baseline_volatility = median(prior_volatility)
            current_volatility = summary.get("realised_volatility_20d_pct")
            summary["volatility_expansion_ratio"] = (
                _round(float(current_volatility) / baseline_volatility, 6)
                if current_volatility is not None and baseline_volatility > 0 else None
            )
        summary["missing_inputs"] = sorted(set(summary["missing_inputs"]))
        universe_admissible = all(bool(row.get("ranking_admissible")) for row in grouped[summary["sector"]])
        if summary["missing_inputs"]:
            summary["classification_status"] = "gated"
        else:
            summary["classification_status"] = "ready" if universe_admissible else "descriptive_only"
            rel20 = float(summary["relative_strength_20d_pct"])
            rel60 = float(summary["relative_strength_60d_pct"])
            rel120 = float(summary["relative_strength_120d_pct"])
            breadth = float(summary["breadth_50dma_pct"])
            alignment = float(summary["ma_alignment_pct"])
            acceleration = float(summary["acceleration_5_sessions_pct"])
            persistence = float(summary["persistence_ratio"])
            if all(value > 0 for value in (rel20, rel60, rel120)) and int(summary["rank_20d"]) <= top_rank_cutoff and breadth >= 60 and alignment >= 50 and persistence >= 0.60:
                summary["sector_state"] = "leading"
            elif rel20 > 0 and acceleration > 0 and breadth >= 50:
                summary["sector_state"] = "improving"
            elif all(value < 0 for value in (rel20, rel60, rel120)) and breadth <= 40:
                summary["sector_state"] = "lagging"
            elif (rel20 < 0 or acceleration < 0) and breadth < 50:
                summary["sector_state"] = "weakening"
            else:
                summary["sector_state"] = "neutral"
        summary["ranking_admissible"] = bool(
            summary["classification_status"] == "ready" and universe_admissible
        )
        for ratio_key in (
            "constituent_coverage_ratio", "return_20d_coverage_ratio",
            "return_60d_coverage_ratio", "return_120d_coverage_ratio",
        ):
            summary[ratio_key] = _round(summary[ratio_key], 6)

    return {
        "ok": True,
        "contract_version": SECTOR_TREND_CONTRACT,
        "session_date": resolved_session,
        "rows": summaries,
        "coverage": {
            "feature_rows": len(rows),
            "classified_sector_rows": sum(row["sector_state"] is not None for row in summaries),
            "sectors": len(summaries),
        },
        "definitions": {
            "minimum_constituent_coverage": SECTOR_TREND_MINIMUM_COVERAGE_RATIO,
            "persistence_sessions": SECTOR_TREND_PERSISTENCE_SESSIONS,
            "volatility_baseline_sessions": SECTOR_TREND_VOLATILITY_BASELINE_SESSIONS,
            "forward_labels_used": False,
            "strategy_results_used": False,
        },
    }


def stock_leadership_classification(
    feature_rows: list[Any], sector_trend_rows: list[Any],
    historical_feature_rows: list[Any] | None = None, session_date: str | None = None,
) -> dict[str, Any]:
    """Classify stock leadership and setup state from dated evidence only."""
    rows = [dict(row) for row in feature_rows]
    sessions = sorted({str(row.get("session_date") or "") for row in rows if row.get("session_date")})
    if session_date:
        date.fromisoformat(session_date)
    if len(sessions) > 1 or (session_date and sessions and sessions[0] != session_date):
        raise ValueError("stock_leadership_requires_single_feature_session")
    resolved_session = session_date or (sessions[0] if sessions else None)
    if not rows or not resolved_session:
        return {"ok": False, "reason": "feature_ledger_session_unavailable"}

    sector_by_name = {str(dict(row).get("sector")): dict(row) for row in sector_trend_rows}
    history_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for historical in historical_feature_rows or []:
        item = dict(historical)
        if item.get("symbol") and str(item.get("session_date") or "") < resolved_session:
            history_by_symbol[str(item["symbol"])].append(item)
    for history in history_by_symbol.values():
        history.sort(key=lambda item: str(item.get("session_date") or ""))

    generated_at = datetime.now(timezone.utc).isoformat()
    output: list[dict[str, Any]] = []
    for feature in rows:
        sector = str(feature.get("sector") or "")
        sector_row = sector_by_name.get(sector, {})
        missing: list[str] = []
        gating: list[str] = []

        def difference(left: str, right: str, name: str, required: bool = True) -> float | None:
            left_value = feature.get(left)
            right_value = feature.get(right)
            if left_value is None or right_value is None:
                missing.append(name)
                if required:
                    gating.append(name)
                return None
            return _round(float(left_value) - float(right_value), 6)

        relative_nifty = {
            lookback: difference(
                f"return_{lookback}d_pct", f"market_return_{lookback}d_pct",
                f"relative_nifty_{lookback}d_pct",
            ) for lookback in (20, 60, 120)
        }
        relative_sector = {
            lookback: difference(
                f"return_{lookback}d_pct", f"sector_return_{lookback}d_pct",
                f"relative_sector_{lookback}d_pct",
            ) for lookback in (20, 60, 120)
        }
        if not sector:
            missing.append("sector_classification")
            gating.append("sector_classification")
        sector_state = sector_row.get("sector_state")
        if not sector_state:
            missing.append("published_sector_state")
            gating.append("published_sector_state")

        history = history_by_symbol.get(str(feature.get("symbol") or ""), [])
        previous = history[-STOCK_LEADERSHIP_HISTORY_SESSIONS:]
        persistence = None
        acceleration = None
        if len(previous) < STOCK_LEADERSHIP_HISTORY_SESSIONS:
            missing.append("five_session_stock_history")
            gating.append("five_session_stock_history")
        else:
            historical_relative = []
            for item in previous[-4:]:
                if item.get("return_20d_pct") is not None and item.get("sector_return_20d_pct") is not None:
                    historical_relative.append(
                        float(item["return_20d_pct"]) - float(item["sector_return_20d_pct"])
                    )
            if relative_sector[20] is not None and len(historical_relative) == 4:
                persistence = _round(
                    sum(value > 0 for value in historical_relative + [float(relative_sector[20])]) / 5, 6
                )
            else:
                missing.append("relative_strength_persistence_ratio")
                gating.append("relative_strength_persistence_ratio")
            baseline = previous[0]
            if (
                relative_sector[20] is not None
                and baseline.get("return_20d_pct") is not None
                and baseline.get("sector_return_20d_pct") is not None
            ):
                baseline_relative = float(baseline["return_20d_pct"]) - float(baseline["sector_return_20d_pct"])
                acceleration = _round(float(relative_sector[20]) - baseline_relative, 6)
            else:
                missing.append("relative_strength_acceleration_5_sessions_pct")
                gating.append("relative_strength_acceleration_5_sessions_pct")

        distance20 = feature.get("distance_sma20_pct")
        distance50 = feature.get("distance_sma50_pct")
        distance200 = feature.get("distance_sma200_pct")
        if all(value is not None for value in (distance20, distance50, distance200)):
            if all(float(value) > 0 for value in (distance20, distance50, distance200)):
                ma_alignment = "bullish"
            elif all(float(value) < 0 for value in (distance20, distance50, distance200)):
                ma_alignment = "bearish"
            else:
                ma_alignment = "mixed"
        else:
            ma_alignment = None
            missing.append("ma_alignment")
            gating.append("ma_alignment")

        optional_fields = (
            "sma20_50_crossover_age_sessions", "volume_ratio_20d", "fno_state",
        )
        for key in optional_fields:
            if feature.get(key) is None:
                missing.append(key)
        required_fields = (
            "sma20_slope_5d_pct", "sma20_50_spread_pct", "range_compression_ratio_20v60",
            "breakout_proximity_20d_pct", "volatility_expansion_ratio_20v60",
        )
        for key in required_fields:
            if feature.get(key) is None:
                missing.append(key)
                gating.append(key)

        row = {
            "contract_version": STOCK_LEADERSHIP_CONTRACT,
            "session_date": resolved_session,
            "instrument_id": feature.get("instrument_id"),
            "symbol": feature.get("symbol"),
            "sector": feature.get("sector"),
            "classification_status": None,
            "ranking_admissible": False,
            "leadership_state": None,
            "setup_state": None,
            "sector_state": sector_state,
            "return_20d_pct": feature.get("return_20d_pct"),
            "return_60d_pct": feature.get("return_60d_pct"),
            "return_120d_pct": feature.get("return_120d_pct"),
            "relative_nifty_20d_pct": relative_nifty[20],
            "relative_nifty_60d_pct": relative_nifty[60],
            "relative_nifty_120d_pct": relative_nifty[120],
            "relative_sector_20d_pct": relative_sector[20],
            "relative_sector_60d_pct": relative_sector[60],
            "relative_sector_120d_pct": relative_sector[120],
            "sector_rank_20d": sector_row.get("rank_20d"),
            "stock_sector_rank_20d": None,
            "stock_sector_rank_60d": None,
            "stock_sector_rank_120d": None,
            "stock_universe_rank_20d": None,
            "stock_universe_rank_60d": None,
            "stock_universe_rank_120d": None,
            "relative_strength_persistence_ratio": persistence,
            "relative_strength_acceleration_5_sessions_pct": acceleration,
            "ma_alignment": ma_alignment,
            "sma20_slope_5d_pct": feature.get("sma20_slope_5d_pct"),
            "sma20_50_spread_pct": feature.get("sma20_50_spread_pct"),
            "sma20_50_crossover_age_sessions": feature.get("sma20_50_crossover_age_sessions"),
            "range_compression_ratio_20v60": feature.get("range_compression_ratio_20v60"),
            "breakout_proximity_20d_pct": feature.get("breakout_proximity_20d_pct"),
            "volatility_expansion_ratio_20v60": feature.get("volatility_expansion_ratio_20v60"),
            "volume_ratio_20d": feature.get("volume_ratio_20d"),
            "participation_status": "volume_available" if feature.get("volume_ratio_20d") is not None else "delivery_and_volume_unavailable",
            "fno_eligible": bool(feature.get("fno_eligible")),
            "fno_state": feature.get("fno_state"),
            "source_dates": {
                "feature_session": resolved_session,
                "sector_source_date": feature.get("sector_source_date"),
                "sector_trend_session": sector_row.get("session_date"),
                "fno_current_session": (
                    json.loads(feature["source_dates_json"]).get("fno_current_session")
                    if feature.get("source_dates_json") else None
                ),
            },
            "missing_inputs": sorted(set(missing)),
            "_gating_inputs": sorted(set(gating)),
            "_feature_ranking_admissible": bool(feature.get("ranking_admissible")),
            "_sector_ranking_admissible": bool(sector_row.get("ranking_admissible")),
            "generated_at": generated_at,
        }
        output.append(row)

    sector_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in output:
        if row.get("sector"):
            sector_groups[str(row["sector"])].append(row)
    for sector_rows in sector_groups.values():
        for lookback in (20, 60, 120):
            _assign_descending_rank(
                sector_rows, f"relative_sector_{lookback}d_pct", f"stock_sector_rank_{lookback}d"
            )
    for lookback in (20, 60, 120):
        _assign_descending_rank(output, f"relative_nifty_{lookback}d_pct", f"stock_universe_rank_{lookback}d")

    feature_by_instrument = {feature.get("instrument_id"): feature for feature in rows}
    for row in output:
        gating = row.pop("_gating_inputs")
        feature_admissible = row.pop("_feature_ranking_admissible")
        sector_admissible = row.pop("_sector_ranking_admissible")
        if gating:
            row["classification_status"] = "gated"
            continue
        row["classification_status"] = "ready" if feature_admissible and sector_admissible else "descriptive_only"
        sector_size = len(sector_groups[str(row["sector"])])
        top_cutoff = max(1, math.ceil(sector_size * 0.30))
        rel20 = float(row["relative_sector_20d_pct"])
        rel60 = float(row["relative_sector_60d_pct"])
        persistence = float(row["relative_strength_persistence_ratio"])
        acceleration = float(row["relative_strength_acceleration_5_sessions_pct"])
        slope = float(row["sma20_slope_5d_pct"])
        if rel20 > 0 and rel60 > 0 and int(row["stock_sector_rank_20d"]) <= top_cutoff and persistence >= 0.60 and row["ma_alignment"] == "bullish":
            row["leadership_state"] = "leading"
        elif rel20 > 0 and acceleration > 0 and slope > 0:
            row["leadership_state"] = "improving"
        elif rel20 < 0 and rel60 < 0 and row["ma_alignment"] == "bearish":
            row["leadership_state"] = "lagging"
        elif rel20 < 0 or acceleration < 0:
            row["leadership_state"] = "weakening"
        else:
            row["leadership_state"] = "neutral"

        breakout = float(row["breakout_proximity_20d_pct"])
        compression = float(row["range_compression_ratio_20v60"])
        distance20 = float(feature_by_instrument[row["instrument_id"]]["distance_sma20_pct"])
        if distance20 >= 8 or breakout >= 3:
            row["setup_state"] = "extended"
        elif breakout >= 0 and row["ma_alignment"] == "bullish":
            row["setup_state"] = "triggered"
        elif -3 <= breakout < 0 and compression <= 0.80 and row["ma_alignment"] == "bullish" and row["leadership_state"] in ("leading", "improving") and row["sector_state"] in ("leading", "improving"):
            row["setup_state"] = "approaching_trigger"
        elif breakout >= -6 and slope > 0 and row["ma_alignment"] == "bullish" and row["leadership_state"] in ("leading", "improving") and row["sector_state"] in ("leading", "improving"):
            row["setup_state"] = "primed"
        elif row["sector_state"] in ("weakening", "lagging") or row["leadership_state"] in ("weakening", "lagging"):
            row["setup_state"] = "conflicted"
        else:
            row["setup_state"] = "unprimed"
        row["ranking_admissible"] = bool(
            row["classification_status"] == "ready" and feature_admissible and sector_admissible
        )

    return {
        "ok": True,
        "contract_version": STOCK_LEADERSHIP_CONTRACT,
        "session_date": resolved_session,
        "rows": output,
        "coverage": {
            "feature_rows": len(rows),
            "classified_rows": sum(row["leadership_state"] is not None for row in output),
            "gated_rows": sum(row["classification_status"] == "gated" for row in output),
            "sectors": len(sector_groups),
        },
        "definitions": {
            "trigger": "completed close at or above the prior 20-session high with bullish MA alignment",
            "approaching_trigger": "within 3% below the prior 20-session high, compressed 20/60 range, bullish alignment, and leading/improving stock plus sector",
            "extended": "at least 8% above the 20DMA or at least 3% above the prior 20-session high",
            "delivery_participation": "not stored; volume participation is used only when available",
            "forward_labels_used": False,
            "strategy_results_used": False,
        },
    }
