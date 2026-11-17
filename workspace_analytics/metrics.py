"""Reusable strategy performance metrics."""

from __future__ import annotations

import math


def maximum_drawdown_pct(equity_curve: list[float]) -> float | None:
    """Return the worst peak-to-trough drawdown as a negative percentage."""
    if not equity_curve:
        return None
    peak = equity_curve[0]
    maximum_drawdown = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        if peak > 0:
            maximum_drawdown = min(maximum_drawdown, equity / peak - 1.0)
    value = 100 * maximum_drawdown
    return None if not math.isfinite(value) else round(value, 2)

