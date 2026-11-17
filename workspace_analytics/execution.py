"""Execution-cost assumptions and contracts for long/cash research."""

from __future__ import annotations

import math
from typing import Any


EQUITY_DELIVERY_COST_SCHEDULE = {
    "schedule_version": "zerodha-nse-equity-delivery-2026-03-v1",
    "effective_from": "2026-03-01",
    "brokerage_rate": 0.0,
    "stt_rate_each_side": 0.001,
    "nse_transaction_charge_per_crore": 306.99,
    "nse_ipft_per_crore": 0.01,
    "sebi_charge_per_crore": 10.0,
    "gst_rate": 0.18,
    "stamp_duty_buy_rate": 0.00015,
    "dp_charge_sell_inr": 15.34,
    "sources": [
        "https://zerodha.com/charges/",
        "https://zerodha.com/brokerage-calculator/",
        "https://nsearchives.nseindia.com/content/circulars/FA73061.pdf",
    ],
}

DEFAULT_BACKTEST_COST_CONFIG = {
    "position_capital_inr": 50000.0,
    "slippage_bps_each_side": 5.0,
    "market_impact_bps_each_side": 0.0,
    "include_dp_charge": True,
}


def _round_finite(value: float | None, digits: int = 2) -> float | None:
    return None if value is None or not math.isfinite(value) else round(value, digits)


def normalise_cost_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    values = dict(DEFAULT_BACKTEST_COST_CONFIG)
    if config:
        values.update(config)
    capital = float(values["position_capital_inr"])
    slippage = float(values["slippage_bps_each_side"])
    impact = float(values["market_impact_bps_each_side"])
    if not math.isfinite(capital) or capital <= 0:
        raise ValueError("invalid_position_capital")
    if not math.isfinite(slippage) or slippage < 0 or slippage > 500:
        raise ValueError("invalid_slippage_bps")
    if not math.isfinite(impact) or impact < 0 or impact > 500:
        raise ValueError("invalid_market_impact_bps")
    return {
        "position_capital_inr": capital,
        "slippage_bps_each_side": slippage,
        "market_impact_bps_each_side": impact,
        "include_dp_charge": bool(values.get("include_dp_charge", True)),
    }


def equity_delivery_side_charges(
    turnover_inr: float,
    side: str,
    config: dict[str, Any],
) -> dict[str, float]:
    turnover = max(0.0, float(turnover_inr))
    if side not in {"buy", "sell"}:
        raise ValueError("invalid_trade_side")
    schedule = EQUITY_DELIVERY_COST_SCHEDULE
    brokerage = turnover * float(schedule["brokerage_rate"])
    stt = turnover * float(schedule["stt_rate_each_side"])
    exchange = turnover * float(schedule["nse_transaction_charge_per_crore"]) / 10_000_000
    ipft = turnover * float(schedule["nse_ipft_per_crore"]) / 10_000_000
    sebi = turnover * float(schedule["sebi_charge_per_crore"]) / 10_000_000
    gst = float(schedule["gst_rate"]) * (brokerage + exchange + ipft + sebi)
    stamp = turnover * float(schedule["stamp_duty_buy_rate"]) if side == "buy" else 0.0
    dp = (
        float(schedule["dp_charge_sell_inr"])
        if side == "sell" and config["include_dp_charge"] and turnover > 0 else 0.0
    )
    slippage = turnover * float(config["slippage_bps_each_side"]) / 10_000
    market_impact = turnover * float(config["market_impact_bps_each_side"]) / 10_000
    components = {
        "brokerage": brokerage,
        "stt": stt,
        "exchange_transaction": exchange,
        "nse_ipft": ipft,
        "sebi": sebi,
        "gst": gst,
        "stamp_duty": stamp,
        "dp_charge": dp,
        "slippage": slippage,
        "market_impact": market_impact,
    }
    components["total"] = sum(components.values())
    return components


def equity_delivery_cost_calculator(
    buy_turnover_inr: float,
    sell_turnover_inr: float,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Estimate Zerodha/NSE equity-delivery costs for one buy and one sell."""
    assumptions = normalise_cost_config(config)
    buy = equity_delivery_side_charges(buy_turnover_inr, "buy", assumptions)
    sell = equity_delivery_side_charges(sell_turnover_inr, "sell", assumptions)
    total = buy["total"] + sell["total"]
    gross_turnover = max(0.0, float(buy_turnover_inr)) + max(0.0, float(sell_turnover_inr))
    return {
        "ok": True,
        "contract_version": "equity-delivery-cost-v1",
        "segment": "NSE equity delivery",
        "schedule": EQUITY_DELIVERY_COST_SCHEDULE,
        "assumptions": assumptions,
        "buy_turnover_inr": _round_finite(float(buy_turnover_inr)),
        "sell_turnover_inr": _round_finite(float(sell_turnover_inr)),
        "buy_charges": {key: _round_finite(value, 4) for key, value in buy.items()},
        "sell_charges": {key: _round_finite(value, 4) for key, value in sell.items()},
        "total_cost_inr": _round_finite(total, 4),
        "total_cost_pct_of_gross_turnover": (
            _round_finite(100 * total / gross_turnover, 6) if gross_turnover else None
        ),
        "rate_basis": (
            "Current published schedule applied uniformly; historical rate changes "
            "are not reconstructed."
        ),
    }

