"""Statistical evidence contracts for completed-trade research.

This module is deliberately independent of the database and strategy runners so
the published statistical contracts can be regression-tested in isolation.
"""

from __future__ import annotations

import hashlib
import math
import random
from statistics import mean
from typing import Any


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None or not math.isfinite(value) else round(value, digits)


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    """Numerically evaluate the continued fraction used by incomplete beta."""
    maximum_iterations, epsilon, floor = 200, 3e-14, 1e-300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < floor:
        d = floor
    d = 1.0 / d
    value = d
    for iteration in range(1, maximum_iterations + 1):
        doubled = 2 * iteration
        coefficient = iteration * (b - iteration) * x / ((qam + doubled) * (a + doubled))
        d = 1.0 + coefficient * d
        c = 1.0 + coefficient / c
        if abs(d) < floor:
            d = floor
        if abs(c) < floor:
            c = floor
        d = 1.0 / d
        value *= d * c
        coefficient = -(a + iteration) * (qab + iteration) * x / ((a + doubled) * (qap + doubled))
        d = 1.0 + coefficient * d
        c = 1.0 + coefficient / c
        if abs(d) < floor:
            d = floor
        if abs(c) < floor:
            c = floor
        d = 1.0 / d
        delta = d * c
        value *= delta
        if abs(delta - 1.0) < epsilon:
            break
    return value


def _regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    factor = math.exp(
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
        + a * math.log(x) + b * math.log1p(-x)
    )
    if x < (a + 1.0) / (a + b + 2.0):
        return factor * _beta_continued_fraction(a, b, x) / a
    return 1.0 - factor * _beta_continued_fraction(b, a, 1.0 - x) / b


def trade_edge_p_value(completed_trade_returns: list[float]) -> float | None:
    """Two-sided one-sample Student t-test of mean trade return versus zero."""
    count = len(completed_trade_returns)
    if count < 2:
        return None
    average = mean(completed_trade_returns)
    sample_variance = sum((value - average) ** 2 for value in completed_trade_returns) / (count - 1)
    if sample_variance <= 0:
        return 1.0 if average == 0 else 0.0
    statistic = abs(average) / math.sqrt(sample_variance / count)
    degrees = count - 1
    probability = _regularized_incomplete_beta(
        degrees / 2.0,
        0.5,
        degrees / (degrees + statistic * statistic),
    )
    return _round(min(1.0, max(0.0, probability)), 6)


def _percentile(values: list[float], probability: float) -> float | None:
    finite = sorted(value for value in values if math.isfinite(value))
    if not finite:
        return None
    position = (len(finite) - 1) * min(1.0, max(0.0, probability))
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return finite[lower]
    weight = position - lower
    return finite[lower] * (1.0 - weight) + finite[upper] * weight


def _sample_profit_factor(sample: list[float]) -> float | None:
    gross_profit = sum(value for value in sample if value > 0)
    gross_loss = abs(sum(value for value in sample if value < 0))
    return gross_profit / gross_loss if gross_loss > 0 else None


def bootstrap_trade_evidence(
    completed_trade_returns: list[float],
    resamples: int = 1000,
    minimum_trades: int = 15,
) -> dict[str, Any]:
    """Return deterministic IID and circular moving-block bootstrap evidence."""
    sample = [float(value) for value in completed_trade_returns if math.isfinite(float(value))]
    count = len(sample)
    block_length = min(count, max(2, int(round(math.sqrt(count))))) if count else None
    base = {
        "contract_version": "trade-bootstrap-evidence-v1",
        "sample": {
            "completed_net_trades": count,
            "minimum_required": minimum_trades,
            "sampling_unit": "ordered_cost_adjusted_completed_trade_return",
        },
        "confidence_level": 0.95,
        "ordinary_bootstrap": None,
        "moving_block_bootstrap": None,
    }
    if count < minimum_trades:
        return {
            **base,
            "status": "insufficient_sample",
            "interpretation": (
                f"Requires at least {minimum_trades} completed cost-adjusted trades; "
                f"{count} are available."
            ),
        }

    iterations = max(200, min(int(resamples), 5000))
    seed_material = "|".join(f"{value:.10f}" for value in sample)
    seed = int(hashlib.sha256(seed_material.encode("utf-8")).hexdigest()[:16], 16)
    independent_rng = random.Random(seed)
    block_rng = random.Random(seed ^ 0x9E3779B97F4A7C15)

    def summarise(draws: list[list[float]], method: str) -> dict[str, Any]:
        expectancies = [mean(draw) for draw in draws]
        profit_factors = [
            value for value in (_sample_profit_factor(draw) for draw in draws)
            if value is not None
        ]
        expectancy_interval = [_percentile(expectancies, 0.025), _percentile(expectancies, 0.975)]
        profit_factor_interval = [_percentile(profit_factors, 0.025), _percentile(profit_factors, 0.975)]
        return {
            "method": method,
            "resamples": iterations,
            "valid_profit_factor_resamples": len(profit_factors),
            "net_expectancy_ci_95_pct": [_round(value) for value in expectancy_interval],
            "net_profit_factor_ci_95": [_round(value) for value in profit_factor_interval],
            "probability_net_expectancy_positive": _round(
                sum(value > 0 for value in expectancies) / iterations,
                4,
            ),
        }

    independent_draws = [
        [sample[independent_rng.randrange(count)] for _ in range(count)]
        for _ in range(iterations)
    ]
    block_draws: list[list[float]] = []
    for _ in range(iterations):
        draw: list[float] = []
        while len(draw) < count:
            start = block_rng.randrange(count)
            draw.extend(sample[(start + offset) % count] for offset in range(block_length or 1))
        block_draws.append(draw[:count])

    return {
        **base,
        "status": "ready",
        "ordinary_bootstrap": summarise(independent_draws, "iid_trade_bootstrap"),
        "moving_block_bootstrap": {
            **summarise(block_draws, "circular_moving_block_trade_bootstrap"),
            "block_length_trades": block_length,
        },
        "interpretation": (
            "Intervals quantify sampling uncertainty in net completed-trade evidence; "
            "they are not forecasts or trade instructions."
        ),
    }


def attach_benjamini_hochberg_fdr(
    results: list[dict[str, Any]],
    alpha: float = 0.05,
) -> dict[str, Any]:
    """Attach monotone Benjamini-Hochberg q-values to one strategy universe."""
    tests = [
        (index, float(row["net_trade_edge_p_value"]))
        for index, row in enumerate(results)
        if row.get("status") == "ready" and row.get("net_trade_edge_p_value") is not None
    ]
    ordered = sorted(tests, key=lambda item: item[1])
    adjusted: dict[int, float] = {}
    running = 1.0
    total = len(ordered)
    for rank in range(total, 0, -1):
        index, probability = ordered[rank - 1]
        running = min(running, probability * total / rank)
        adjusted[index] = min(1.0, max(0.0, running))
    for index, row in enumerate(results):
        q_value = adjusted.get(index)
        row["net_trade_edge_q_value"] = _round(q_value, 6)
        row["multiple_testing_status"] = (
            "fdr_significant" if q_value is not None and q_value <= alpha else
            "tested_not_significant" if q_value is not None else "not_tested"
        )
    return {
        "contract_version": "benjamini-hochberg-fdr-v1",
        "family": "one_strategy_across_comparable_stocks",
        "alpha": alpha,
        "tested_hypotheses": total,
        "raw_p_below_alpha": sum(probability <= alpha for _, probability in tests),
        "fdr_q_below_alpha": sum(value <= alpha for value in adjusted.values()),
        "interpretation": (
            "Controls the expected false-discovery proportion within this strategy's "
            "comparable stock family; it does not correct across strategy families."
        ),
    }

