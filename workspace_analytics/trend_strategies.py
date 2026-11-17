"""Frozen completed-close signal rules for the five trend strategies."""

from __future__ import annotations

from typing import Any, Callable


MetricsFunction = Callable[[list[Any], list[int], int, list[dict[str, Any]]], dict[str, Any]]


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(float(value), digits)


def _pct(current: float | None, previous: float | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return 100 * (float(current) / float(previous) - 1.0)


def moving_average_crossover_series(
    rows: list[Any], fast_window: int = 20, slow_window: int = 50,
    *, metrics: MetricsFunction,
) -> dict[str, Any]:
    required = slow_window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    fast_values: list[float | None] = [None] * len(closes)
    slow_values: list[float | None] = [None] * len(closes)
    fast_sum = slow_sum = 0.0
    for index, close in enumerate(closes):
        fast_sum += close
        slow_sum += close
        if index >= fast_window:
            fast_sum -= closes[index - fast_window]
        if index >= slow_window:
            slow_sum -= closes[index - slow_window]
        if index >= fast_window - 1:
            fast_values[index] = fast_sum / fast_window
        if index >= slow_window - 1:
            slow_values[index] = slow_sum / slow_window
    signals: list[int | None] = [None] * len(closes)
    for index in range(slow_window - 1, len(closes)):
        signals[index] = int((fast_values[index] or 0) > (slow_values[index] or 0))
    events = []
    for index in range(slow_window, len(closes)):
        prior_signal = int(signals[index - 1] or 0)
        current_signal = int(signals[index] or 0)
        if current_signal == prior_signal:
            continue
        events.append({
            "signal_date": dates[index],
            "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
            "type": "bullish_cross" if current_signal else "bearish_cross",
            "new_signal": current_signal,
            "close": _round(closes[index]),
            "fast_sma": _round(fast_values[index]),
            "slow_sma": _round(slow_values[index]),
        })
    result = metrics(rows, [int(signal or 0) for signal in signals], slow_window - 1, events)
    result.update({
        "current_fast_sma": _round(fast_values[-1]),
        "current_slow_sma": _round(slow_values[-1]),
        "current_spread_pct": _round(_pct(fast_values[-1], slow_values[-1])),
        "crossovers": len(events),
        "last_crossover": result.get("last_event"),
        "crossover_events": result.pop("events"),
    })
    return result


def donchian_breakout_series(
    rows: list[Any], entry_window: int = 55, exit_window: int = 20,
    *, metrics: MetricsFunction,
) -> dict[str, Any]:
    required = max(entry_window, exit_window) + 2
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    start = max(entry_window, exit_window)
    signals = [0] * len(closes)
    upper_channels: list[float | None] = [None] * len(closes)
    lower_channels: list[float | None] = [None] * len(closes)
    events = []
    for index in range(start, len(closes)):
        upper = max(closes[index - entry_window:index])
        lower = min(closes[index - exit_window:index])
        upper_channels[index] = upper
        lower_channels[index] = lower
        prior_signal = signals[index - 1]
        current_signal = prior_signal
        event_type = None
        if not prior_signal and closes[index] > upper:
            current_signal, event_type = 1, "upside_breakout"
        elif prior_signal and closes[index] < lower:
            current_signal, event_type = 0, "downside_exit"
        signals[index] = current_signal
        if event_type:
            events.append({
                "signal_date": dates[index],
                "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                "type": event_type,
                "new_signal": current_signal,
                "close": _round(closes[index]),
                "entry_channel": _round(upper),
                "exit_channel": _round(lower),
            })
    result = metrics(rows, signals, start, events)
    result.update({
        "current_entry_channel": _round(upper_channels[-1]),
        "current_exit_channel": _round(lower_channels[-1]),
        "distance_to_entry_pct": _round(_pct(closes[-1], upper_channels[-1])),
        "distance_to_exit_pct": _round(_pct(closes[-1], lower_channels[-1])),
        "breakouts": sum(event["type"] == "upside_breakout" for event in events),
    })
    return result


def ema_crossover_series(
    rows: list[Any], fast_window: int = 20, slow_window: int = 50,
    *, metrics: MetricsFunction,
) -> dict[str, Any]:
    required = slow_window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    fast_alpha, slow_alpha = 2 / (fast_window + 1), 2 / (slow_window + 1)
    fast_values, slow_values = [closes[0]], [closes[0]]
    for close in closes[1:]:
        fast_values.append(fast_alpha * close + (1 - fast_alpha) * fast_values[-1])
        slow_values.append(slow_alpha * close + (1 - slow_alpha) * slow_values[-1])
    start = slow_window - 1
    signals = [0] * len(closes)
    events = []
    for index in range(start, len(closes)):
        signals[index] = int(fast_values[index] > slow_values[index])
        prior_signal = signals[index - 1] if index > start else 0
        if signals[index] != prior_signal:
            events.append({"signal_date": dates[index],
                           "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                           "type": "bullish_cross" if signals[index] else "bearish_cross",
                           "new_signal": signals[index], "close": _round(closes[index]),
                           "fast_ema": _round(fast_values[index]), "slow_ema": _round(slow_values[index])})
    result = metrics(rows, signals, start, events)
    result.update({"current_fast_ema": _round(fast_values[-1]),
                   "current_slow_ema": _round(slow_values[-1]),
                   "current_spread_pct": _round(_pct(fast_values[-1], slow_values[-1])),
                   "crossovers": len(events)})
    return result


def time_series_momentum_series(
    rows: list[Any], lookback: int = 63, *, metrics: MetricsFunction,
) -> dict[str, Any]:
    required = lookback + 2
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]
    signals = [0] * len(closes)
    events = []
    for index in range(lookback, len(closes)):
        momentum = 100 * (closes[index] / closes[index - lookback] - 1.0)
        signals[index] = int(momentum > 0)
        prior_signal = signals[index - 1] if index > lookback else 0
        if signals[index] != prior_signal:
            events.append({"signal_date": dates[index],
                           "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                           "type": "momentum_positive" if signals[index] else "momentum_non_positive",
                           "new_signal": signals[index], "close": _round(closes[index]),
                           "lookback_return_pct": _round(momentum)})
    result = metrics(rows, signals, lookback, events)
    result.update({"current_lookback_return_pct": _round(100 * (closes[-1] / closes[-1 - lookback] - 1.0)),
                   "signal_changes": len(events)})
    return result


def triple_sma_alignment_series(
    rows: list[Any], fast_window: int = 20, medium_window: int = 50,
    slow_window: int = 200, *, metrics: MetricsFunction,
) -> dict[str, Any]:
    required = slow_window + 1
    if len(rows) < required:
        return {"status": "insufficient_history", "observations": len(rows),
                "required_observations": required,
                "reason": f"At least {required} completed sessions are required."}
    dates = [str(row["session_date"]) for row in rows]
    closes = [float(row["close"]) for row in rows]

    def rolling_average(window: int) -> list[float | None]:
        values: list[float | None] = [None] * len(closes)
        running = 0.0
        for index, close in enumerate(closes):
            running += close
            if index >= window:
                running -= closes[index - window]
            if index >= window - 1:
                values[index] = running / window
        return values

    fast_values = rolling_average(fast_window)
    medium_values = rolling_average(medium_window)
    slow_values = rolling_average(slow_window)
    start = slow_window - 1
    signals = [0] * len(closes)
    events = []
    for index in range(start, len(closes)):
        signals[index] = int(float(fast_values[index] or 0) > float(medium_values[index] or 0) > float(slow_values[index] or 0))
        prior_signal = signals[index - 1] if index > start else 0
        if signals[index] != prior_signal:
            events.append({"signal_date": dates[index],
                           "effective_from": dates[index + 1] if index + 1 < len(dates) else None,
                           "type": "alignment_on" if signals[index] else "alignment_off",
                           "new_signal": signals[index], "close": _round(closes[index]),
                           "fast_sma": _round(fast_values[index]),
                           "medium_sma": _round(medium_values[index]),
                           "slow_sma": _round(slow_values[index])})
    result = metrics(rows, signals, start, events)
    result.update({"current_fast_sma": _round(fast_values[-1]),
                   "current_medium_sma": _round(medium_values[-1]),
                   "current_slow_sma": _round(slow_values[-1]),
                   "alignment_changes": len(events)})
    return result
