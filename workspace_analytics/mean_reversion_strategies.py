"""Frozen completed-close signal rules for the five mean-reversion strategies."""

from __future__ import annotations

import math
from statistics import mean
from typing import Any, Callable


MetricsFunction = Callable[[list[Any], list[int], int, list[dict[str, Any]]], dict[str, Any]]


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(float(value), digits)


def _pct(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return 100 * (float(current) / float(previous) - 1.0)


def _state_machine_signals(
    dates: list[str], closes: list[float], start: int,
    entry_flags: list[bool], exit_flags: list[bool],
    entry_type: str, exit_type: str, contexts: list[dict[str, Any]],
) -> tuple[list[int], list[dict[str, Any]]]:
    signals = [0] * len(closes)
    events: list[dict[str, Any]] = []
    for index in range(start, len(closes)):
        prior_signal = signals[index - 1] if index > start else 0
        current_signal = prior_signal
        event_type = None
        if not prior_signal and entry_flags[index]:
            current_signal, event_type = 1, entry_type
        elif prior_signal and exit_flags[index]:
            current_signal, event_type = 0, exit_type
        signals[index] = current_signal
        if event_type:
            events.append({
                "signal_date": dates[index],
                "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                "type": event_type,
                "new_signal": current_signal,
                "close": _round(closes[index]),
                **contexts[index],
            })
    return signals, events


def rsi_mean_reversion_series(
    rows: list[Any], period: int = 2, entry_rsi: float = 10.0,
    exit_rsi: float = 70.0, *, metrics: MetricsFunction,
) -> dict[str, Any]:
    required = period + 2
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    rsi_values: list[float | None] = [None] * len(closes)
    for index in range(period, len(closes)):
        changes = [closes[pos] - closes[pos - 1] for pos in range(index - period + 1, index + 1)]
        gains = sum(max(change, 0.0) for change in changes) / period
        losses = sum(max(-change, 0.0) for change in changes) / period
        rsi_values[index] = 100.0 if losses == 0 else 100.0 - 100.0 / (1.0 + gains / losses)
    entry_flags = [False] * len(closes)
    exit_flags = [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    for index in range(period, len(closes)):
        rsi = float(rsi_values[index] or 0)
        entry_flags[index], exit_flags[index] = rsi <= entry_rsi, rsi >= exit_rsi
        contexts[index] = {"rsi": _round(rsi)}
    signals, events = _state_machine_signals(
        dates, closes, period, entry_flags, exit_flags,
        "oversold_entry", "rsi_recovery_exit", contexts,
    )
    result = metrics(rows, signals, period, events)
    result.update({"current_rsi": _round(rsi_values[-1]), "signal_changes": len(events)})
    return result


def bollinger_mean_reversion_series(
    rows: list[Any], window: int = 20, deviations: float = 2.0,
    *, metrics: MetricsFunction,
) -> dict[str, Any]:
    required = window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    means: list[float | None] = [None] * len(closes)
    lower_bands: list[float | None] = [None] * len(closes)
    entry_flags, exit_flags = [False] * len(closes), [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    start = window - 1
    for index in range(start, len(closes)):
        sample = closes[index - window + 1:index + 1]
        average = mean(sample)
        deviation = math.sqrt(mean((value - average) ** 2 for value in sample))
        lower = average - deviations * deviation
        means[index], lower_bands[index] = average, lower
        entry_flags[index], exit_flags[index] = closes[index] < lower, closes[index] >= average
        contexts[index] = {"middle_band": _round(average), "lower_band": _round(lower)}
    signals, events = _state_machine_signals(
        dates, closes, start, entry_flags, exit_flags,
        "lower_band_entry", "middle_band_exit", contexts,
    )
    result = metrics(rows, signals, start, events)
    result.update({"current_middle_band": _round(means[-1]),
                   "current_lower_band": _round(lower_bands[-1]),
                   "distance_to_middle_pct": _round(_pct(closes[-1], means[-1])),
                   "signal_changes": len(events)})
    return result


def zscore_mean_reversion_series(
    rows: list[Any], window: int = 10, entry_z: float = -1.5,
    exit_z: float = 0.0, *, metrics: MetricsFunction,
) -> dict[str, Any]:
    required = window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    zscores: list[float | None] = [None] * len(closes)
    entry_flags, exit_flags = [False] * len(closes), [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    start = window - 1
    for index in range(start, len(closes)):
        sample = closes[index - window + 1:index + 1]
        average = mean(sample)
        deviation = math.sqrt(mean((value - average) ** 2 for value in sample))
        zscore = (closes[index] - average) / deviation if deviation > 0 else 0.0
        zscores[index] = zscore
        entry_flags[index], exit_flags[index] = zscore <= entry_z, zscore >= exit_z
        contexts[index] = {"zscore": _round(zscore), "rolling_mean": _round(average)}
    signals, events = _state_machine_signals(
        dates, closes, start, entry_flags, exit_flags,
        "negative_zscore_entry", "mean_recovery_exit", contexts,
    )
    result = metrics(rows, signals, start, events)
    result.update({"current_zscore": _round(zscores[-1]), "signal_changes": len(events)})
    return result


def pullback_uptrend_series(
    rows: list[Any], pullback_window: int = 5, trend_window: int = 200,
    *, metrics: MetricsFunction,
) -> dict[str, Any]:
    required = trend_window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    fast_values: list[float | None] = [None] * len(closes)
    trend_values: list[float | None] = [None] * len(closes)
    entry_flags, exit_flags = [False] * len(closes), [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    start = trend_window - 1
    for index in range(start, len(closes)):
        fast = mean(closes[index - pullback_window + 1:index + 1])
        trend = mean(closes[index - trend_window + 1:index + 1])
        fast_values[index], trend_values[index] = fast, trend
        entry_flags[index] = closes[index] < fast and closes[index] > trend
        exit_flags[index] = closes[index] >= fast or closes[index] <= trend
        contexts[index] = {"pullback_sma": _round(fast), "trend_sma": _round(trend)}
    signals, events = _state_machine_signals(
        dates, closes, start, entry_flags, exit_flags,
        "uptrend_pullback_entry", "pullback_recovery_exit", contexts,
    )
    result = metrics(rows, signals, start, events)
    result.update({"current_pullback_sma": _round(fast_values[-1]),
                   "current_trend_sma": _round(trend_values[-1]),
                   "distance_to_pullback_sma_pct": _round(_pct(closes[-1], fast_values[-1])),
                   "signal_changes": len(events)})
    return result


def selloff_recovery_series(
    rows: list[Any], down_sessions: int = 3, exit_window: int = 5,
    *, metrics: MetricsFunction,
) -> dict[str, Any]:
    start = max(down_sessions, exit_window - 1)
    required = start + 2
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    exit_averages: list[float | None] = [None] * len(closes)
    entry_flags, exit_flags = [False] * len(closes), [False] * len(closes)
    contexts: list[dict[str, Any]] = [{} for _ in closes]
    for index in range(start, len(closes)):
        exit_average = mean(closes[index - exit_window + 1:index + 1])
        selloff = all(closes[pos] < closes[pos - 1] for pos in range(index - down_sessions + 1, index + 1))
        exit_averages[index] = exit_average
        entry_flags[index], exit_flags[index] = selloff and closes[index] < exit_average, closes[index] >= exit_average
        contexts[index] = {"exit_sma": _round(exit_average), "down_sessions": down_sessions}
    signals, events = _state_machine_signals(
        dates, closes, start, entry_flags, exit_flags,
        "selloff_entry", "recovery_exit", contexts,
    )
    result = metrics(rows, signals, start, events)
    result.update({"current_exit_sma": _round(exit_averages[-1]),
                   "distance_to_exit_sma_pct": _round(_pct(closes[-1], exit_averages[-1])),
                   "signal_changes": len(events)})
    return result
