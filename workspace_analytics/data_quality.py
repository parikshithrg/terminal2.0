"""Point-in-time price-history quality gates for strategy research."""

from __future__ import annotations

import json
import math
import sqlite3
from typing import Any, Callable


def _round_finite(value: float | None, digits: int = 2) -> float | None:
    return None if value is None or not math.isfinite(value) else round(value, digits)


def quality_checked_backtest_series(
    connection: sqlite3.Connection,
    instrument_id: str,
    required_observations: int,
    reference_context: dict[str, Any],
    history_mode: str = "comparison",
    *,
    instrument_series: Callable[[sqlite3.Connection, str], list[sqlite3.Row]],
    comparison_sessions: int,
    abnormal_jump_pct: float,
) -> tuple[list[sqlite3.Row], dict[str, Any]]:
    """Select an admissible price series and return its complete gate audit."""
    if history_mode not in {"comparison", "full"}:
        raise ValueError("invalid_backtest_history_mode")
    common_as_of = reference_context.get("common_as_of_date")
    reference_dates = list(reference_context.get("reference_dates") or [])
    comparison_required_total = required_observations + comparison_sessions - 1
    all_rows = instrument_series(connection, instrument_id)
    row_by_date = {
        str(row["session_date"]): row
        for row in all_rows
        if not common_as_of or str(row["session_date"]) <= common_as_of
    }
    if history_mode == "full" and row_by_date:
        first_observation = min(row_by_date)
        target_dates = [session for session in reference_dates if session >= first_observation]
        required_total = required_observations
    else:
        target_dates = (
            reference_dates[-comparison_required_total:]
            if len(reference_dates) >= comparison_required_total else []
        )
        required_total = comparison_required_total
    selected_rows = [row_by_date[session] for session in target_dates if session in row_by_date]
    missing_dates = [session for session in target_dates if session not in row_by_date]
    invalid_ohlc: list[str] = []
    abnormal_jumps: list[dict[str, Any]] = []
    previous_close: float | None = None
    for row in selected_rows:
        session = str(row["session_date"])
        try:
            opening, high, low, close = (float(row[key]) for key in ("open", "high", "low", "close"))
        except (TypeError, ValueError):
            invalid_ohlc.append(session)
            continue
        valid_prices = all(
            math.isfinite(value) and value > 0
            for value in (opening, high, low, close)
        )
        if (
            not valid_prices
            or high < max(opening, close)
            or low > min(opening, close)
            or high < low
        ):
            invalid_ohlc.append(session)
        if previous_close and previous_close > 0:
            jump = 100 * (close / previous_close - 1.0)
            if abs(jump) > abnormal_jump_pct:
                abnormal_jumps.append({
                    "session_date": session,
                    "return_pct": _round_finite(jump),
                })
        previous_close = close

    metadata = connection.execute(
        "SELECT adjustment_status,adjustment_source_url,source,updated_at FROM instruments WHERE instrument_id=?",
        (instrument_id,),
    ).fetchone()
    source_rows = connection.execute(
        """SELECT source, COUNT(*) AS rows, MAX(retrieved_at) AS retrieved_at
           FROM candles WHERE instrument_id=? AND (? IS NULL OR session_date<=?)
           GROUP BY source ORDER BY rows DESC""",
        (instrument_id, common_as_of, common_as_of),
    ).fetchall()
    duplicate_rows = connection.execute(
        """SELECT session_date, COUNT(*) AS rows FROM candles
           WHERE instrument_id=? AND (? IS NULL OR session_date<=?)
           GROUP BY session_date HAVING COUNT(*) > 1 ORDER BY session_date""",
        (instrument_id, common_as_of, common_as_of),
    ).fetchall()
    action_rows = connection.execute(
        """SELECT action_type,purpose,ex_date,record_date,adjustment_requirement,
                  review_status,source_url,retrieved_at
           FROM corporate_actions
           WHERE instrument_id=? AND ex_date BETWEEN ? AND ? ORDER BY ex_date""",
        (
            instrument_id,
            target_dates[0] if target_dates else "0000-01-01",
            common_as_of or "9999-12-31",
        ),
    ).fetchall()
    lineage_rows = connection.execute(
        """SELECT old_symbol,new_symbol,effective_date,review_status,source_url,retrieved_at
           FROM symbol_history WHERE instrument_id=? ORDER BY effective_date""",
        (instrument_id,),
    ).fetchall()
    coverage_row = connection.execute(
        "SELECT value,updated_at FROM workspace_metadata WHERE key='corporate_action_coverage'"
    ).fetchone()
    corporate_coverage: dict[str, Any] | None = None
    if coverage_row:
        try:
            corporate_coverage = json.loads(coverage_row["value"])
        except (TypeError, json.JSONDecodeError):
            corporate_coverage = None
    latest_observation = max(row_by_date, default=None)
    gates: list[str] = []
    warnings: list[str] = []
    if not common_as_of or not target_dates:
        gates.append("reference_window_unavailable")
    if latest_observation != common_as_of:
        gates.append("stale_latest_observation")
    insufficient_history = (
        len(selected_rows) < required_observations
        if history_mode == "full" else len(all_rows) < required_total
    )
    if insufficient_history:
        gates.append("insufficient_common_history")
    if missing_dates:
        gates.append("missing_reference_sessions")
    if duplicate_rows:
        gates.append("duplicate_sessions")
    if invalid_ohlc:
        gates.append("invalid_ohlc")
    if abnormal_jumps:
        gates.append("abnormal_price_jump")
    adjustment_status = metadata["adjustment_status"] if metadata else "unknown"
    if adjustment_status not in {"provider_reported_adjusted", "verified_adjusted"}:
        gates.append("adjustment_provenance_unverified")
    coverage_incomplete = (
        not corporate_coverage
        or not target_dates
        or corporate_coverage.get("from", "9999-12-31") > target_dates[0]
        or corporate_coverage.get("through", "0000-01-01") < (common_as_of or "9999-12-31")
    )
    if coverage_incomplete:
        if history_mode == "full":
            warnings.append("corporate_action_ledger_partial_full_history")
        else:
            gates.append("corporate_action_ledger_incomplete")
    unresolved_actions = [
        row for row in action_rows
        if row["review_status"] in {"manual_review", "manual_lineage_review"}
    ]
    if unresolved_actions:
        gates.append("corporate_action_manual_review")
    audit = {
        "status": "ready" if not gates else "gated",
        "history_mode": history_mode,
        "common_as_of_date": common_as_of,
        "comparison_sessions": comparison_sessions if history_mode == "comparison" else None,
        "required_warmup_observations": required_observations - 1,
        "required_total_observations": required_total,
        "stored_observations": len(all_rows),
        "selected_observations": len(selected_rows),
        "evaluation_start_date": (
            target_dates[required_observations - 1]
            if len(target_dates) >= required_observations else None
        ),
        "history_start_date": target_dates[0] if target_dates else None,
        "latest_observation": latest_observation,
        "missing_session_count": len(missing_dates),
        "missing_sessions": missing_dates[:20],
        "duplicate_session_count": len(duplicate_rows),
        "duplicate_sessions": [dict(row) for row in duplicate_rows[:20]],
        "invalid_ohlc_count": len(invalid_ohlc),
        "invalid_ohlc_sessions": invalid_ohlc[:20],
        "abnormal_jump_count": len(abnormal_jumps),
        "abnormal_jumps": abnormal_jumps[:20],
        "blocking_gates": gates,
        "warnings": warnings,
        "adjustment_status": adjustment_status,
        "adjustment_source_url": metadata["adjustment_source_url"] if metadata else None,
        "adjustment_gate_enforced": True,
        "corporate_action_count": len(action_rows),
        "corporate_actions": [dict(row) for row in action_rows],
        "unresolved_corporate_action_count": len(unresolved_actions),
        "corporate_action_coverage": corporate_coverage,
        "corporate_action_coverage_updated_at": coverage_row["updated_at"] if coverage_row else None,
        "symbol_lineage": [dict(row) for row in lineage_rows],
        "return_contract": "price_return_excluding_ordinary_cash_dividends",
        "instrument_source": metadata["source"] if metadata else None,
        "instrument_updated_at": metadata["updated_at"] if metadata else None,
        "candle_sources": [dict(row) for row in source_rows],
    }
    return (selected_rows if not gates else []), audit

