"""Completed-session historical regime timeline and evidence summaries."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any

from workspace_analytics.regimes import MARKET_REGIME_CONTRACT
from workspace_analytics.sqlite_store import connect_database


HISTORICAL_REGIMES_CONTRACT = "historical-regimes-overview-v3"

ANALOGUE_FORWARD_WINDOWS = (5, 20, 60)
ANALOGUE_MIN_EPISODES = 5
ANALOGUE_MIN_LEDGER_SESSIONS = 500


def _mean(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return round(sum(values) / len(values), 4) if values else None


def _segment_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    segments: list[dict[str, Any]] = []
    for row in rows:
        family = str(row.get("regime_family") or "gated")
        if not segments or segments[-1]["regime_family"] != family:
            segments.append({
                "regime_family": family,
                "from_date": row["session_date"],
                "through_date": row["session_date"],
                "sessions": 1,
                "rows": [row],
            })
        else:
            segment = segments[-1]
            segment["through_date"] = row["session_date"]
            segment["sessions"] += 1
            segment["rows"].append(row)
    result = []
    for segment in segments:
        members = segment.pop("rows")
        result.append({
            **segment,
            "average_return_20d_pct": _mean(members, "market_return_20d_pct"),
            "average_volatility_20d_pct": _mean(members, "market_volatility_20d_pct"),
            "average_breadth_50dma_pct": _mean(members, "market_breadth_50dma_pct"),
            "average_breadth_200dma_pct": _mean(members, "market_breadth_200dma_pct"),
            "classification_status": members[-1].get("classification_status"),
        })
    return result


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,),
    ).fetchone() is not None


def _policy_phase(change_bps: float | None, previous: str | None) -> str:
    if change_bps is not None and change_bps > 0:
        return "tightening"
    if change_bps is not None and change_bps < 0:
        return "easing"
    if previous == "tightening":
        return "tightening_hold"
    if previous == "easing":
        return "easing_hold"
    return previous or "hold"


def _load_policy_events(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(connection, "historical_series_observations"):
        return []
    stored = connection.execute(
        """SELECT observation_date,period_label,value,unit,source_title,source_url,
                  source_authority,vintage_date,retrieved_at,validation_status,
                  metadata_json
           FROM historical_series_observations
           WHERE series_key='india_policy_repo_rate_pct'
             AND validation_status='validated'
           ORDER BY observation_date"""
    ).fetchall()
    phase: str | None = None
    events = []
    for source in stored:
        event = dict(source)
        try:
            metadata = json.loads(event.pop("metadata_json") or "{}")
        except json.JSONDecodeError:
            metadata = {}
        repo_rate = event.pop("value")
        change = metadata.get("change_bps")
        phase = _policy_phase(float(change) if change is not None else None, phase)
        events.append({
            **event,
            "repo_rate_pct": repo_rate,
            "change_bps": change,
            "decision": metadata.get("decision") or "unavailable",
            "cycle_phase": phase,
            "release_id": metadata.get("release_id"),
            "observation_basis": metadata.get("observation_basis"),
        })
    return events


def _policy_cycle(events: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {
            "status": "gated",
            "reason": "validated_RBI_policy_rate_history_not_stored",
            "required_source": "validated_RBI_policy_repo_event_series",
        }
    from_date = rows[0]["session_date"]
    through_date = rows[-1]["session_date"]
    relevant = [event for event in events if event["observation_date"] <= through_date]
    preceding = [event for event in relevant if event["observation_date"] <= from_date]
    in_window = [event for event in relevant if from_date < event["observation_date"] <= through_date]
    window_events = ([preceding[-1]] if preceding else []) + in_window
    periods = []
    for index, event in enumerate(window_events):
        next_date = window_events[index + 1]["observation_date"] if index + 1 < len(window_events) else None
        members = [
            row for row in rows
            if row["session_date"] >= max(from_date, event["observation_date"])
            and (next_date is None or row["session_date"] < next_date)
        ]
        counts: dict[str, int] = defaultdict(int)
        for member in members:
            counts[str(member.get("regime_family") or "gated")] += 1
        dominant = max(counts, key=counts.get) if counts else None
        periods.append({
            **event,
            "applies_from": max(from_date, event["observation_date"]),
            "applies_through": members[-1]["session_date"] if members else None,
            "classified_sessions": len(members),
            "dominant_regime": dominant,
            "regime_distribution": dict(sorted(counts.items())),
            "average_return_20d_pct": _mean(members, "market_return_20d_pct"),
            "average_breadth_50dma_pct": _mean(members, "market_breadth_50dma_pct"),
        })
    return {
        "status": "ready",
        "source_authority": "Reserve Bank of India",
        "series_contract": "rbi-release-vintage-import-v1",
        "coverage": {
            "from_date": events[0]["observation_date"],
            "through_date": events[-1]["observation_date"],
            "validated_events": len(events),
            "events_in_regime_window": len(window_events),
        },
        "current": relevant[-1] if relevant else None,
        "periods": list(reversed(periods)),
        "methodology": (
            "Cycle phase is a deterministic label from the latest RBI repo-rate "
            "change: increase=tightening, decrease=easing, unchanged=hold after "
            "the preceding change. It is context only and does not alter regime classification."
        ),
    }


def _latest_global_risk(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not _table_exists(connection, "global_risk_observations"):
        return {}
    latest = connection.execute(
        "SELECT MAX(observation_date) FROM global_risk_observations"
    ).fetchone()[0]
    if not latest:
        return {}
    return {
        row["metric_key"]: dict(row)
        for row in connection.execute(
            """SELECT observation_date,metric_key,value,unit,source_series,source,
                      retrieved_at
               FROM global_risk_observations WHERE observation_date=?""",
            (latest,),
        ).fetchall()
    }


def _latest_yield(path: Path, market: str, through_date: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    connection = connect_database(path)
    try:
        if not _table_exists(connection, "sovereign_yield_observations"):
            return None
        row = connection.execute(
            """SELECT market,observation_date,tenor_years,yield_pct,curve_type,
                      source_url,retrieved_at,validation_status
               FROM sovereign_yield_observations
               WHERE market=? AND tenor_years=10 AND validation_status='validated'
                 AND observation_date<=?
               ORDER BY observation_date DESC LIMIT 1""",
            (market, through_date),
        ).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def _cross_asset_context(
    market_path: Path,
    current: dict[str, Any],
    global_risk: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    through_date = current["session_date"]
    snapshot_path = market_path.with_name("workspace_snapshots.sqlite3")
    india = _latest_yield(snapshot_path, "India", through_date)
    united_states = _latest_yield(snapshot_path, "United States", through_date)
    observations = []
    if india:
        observations.append({
            "key": "india_10y", "label": "India 10Y", "value": india["yield_pct"],
            "unit": "percent", "change_20d_pct": None,
            "observation_date": india["observation_date"], "source_url": india["source_url"],
        })
    if united_states:
        observations.append({
            "key": "us_10y", "label": "U.S. 10Y", "value": united_states["yield_pct"],
            "unit": "percent", "change_20d_pct": None,
            "observation_date": united_states["observation_date"],
            "source_url": united_states["source_url"],
        })
    if india and united_states:
        spread = round(100.0 * (float(india["yield_pct"]) - float(united_states["yield_pct"])), 1)
        observations.append({
            "key": "india_us_10y_spread", "label": "India–U.S. 10Y spread",
            "value": spread, "unit": "basis_points", "change_20d_pct": None,
            "observation_date": min(india["observation_date"], united_states["observation_date"]),
            "source_url": None,
        })
    for key, change_key, label, unit in (
        ("usd_inr", "usd_inr_20d_change_pct", "USD/INR", "INR per USD"),
        ("brent_crude", "brent_crude_20d_change_pct", "Brent crude", "USD/bbl"),
        ("india_vix", "india_vix_20d_change_pct", "India VIX", "index"),
    ):
        value = global_risk.get(key)
        if value:
            observations.append({
                "key": key, "label": label, "value": value["value"], "unit": unit,
                "change_20d_pct": (global_risk.get(change_key) or {}).get("value"),
                "observation_date": value["observation_date"], "source_url": value["source"],
            })
    observations.append({
        "key": "nifty_50_return_20d", "label": "Nifty 50 20-session",
        "value": current.get("market_return_20d_pct"), "unit": "percent_change",
        "change_20d_pct": None, "observation_date": through_date,
        "source_url": None,
    })
    return {
        "status": "ready" if observations else "gated",
        "through_date": max(
            (row["observation_date"] for row in observations if row.get("observation_date")),
            default=None,
        ),
        "observations": observations,
        "methodology": "Latest validated observation on or before the regime session; descriptive context only.",
    }


def _load_benchmark_closes(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(connection, "candles"):
        return []
    return [dict(row) for row in connection.execute(
        """SELECT session_date,close FROM candles
           WHERE instrument_id='index:Nifty 50' AND validation_status='validated'
           ORDER BY session_date"""
    ).fetchall()]


def _forward_return(
    closes: list[dict[str, Any]], index_by_date: dict[str, int],
    session_date: str, sessions: int, through_date: str,
) -> float | None:
    start_index = index_by_date.get(session_date)
    if start_index is None or start_index + sessions >= len(closes):
        return None
    start = closes[start_index]
    finish = closes[start_index + sessions]
    if finish["session_date"] > through_date or not start["close"]:
        return None
    return round(100.0 * (float(finish["close"]) / float(start["close"]) - 1.0), 4)


def _window_summary(values: list[float], minimum: int) -> dict[str, Any]:
    count = len(values)
    return {
        "status": "ready" if count >= minimum else "gated",
        "sample_size": count,
        "minimum_sample": minimum,
        "median_return_pct": round(median(values), 4) if values else None,
        "average_return_pct": round(sum(values) / count, 4) if values else None,
        "positive_rate_pct": round(100.0 * sum(value > 0 for value in values) / count, 2) if values else None,
        "worst_return_pct": round(min(values), 4) if values else None,
        "best_return_pct": round(max(values), 4) if values else None,
    }


def _regime_analogues(
    rows: list[dict[str, Any]], segments: list[dict[str, Any]],
    closes: list[dict[str, Any]],
) -> dict[str, Any]:
    through_date = rows[-1]["session_date"]
    current_family = str(rows[-1].get("regime_family") or "gated")
    index_by_date = {row["session_date"]: index for index, row in enumerate(closes)}
    episode_anchors = [
        {
            "regime_family": segment["regime_family"],
            "session_date": segment["from_date"],
        }
        for segment in segments
        if segment.get("regime_family") not in (None, "gated")
    ]
    catalogue = []
    for family in ("bullish_trend", "range", "transition", "bearish_trend"):
        anchors = [row for row in episode_anchors if row["regime_family"] == family]
        windows = {}
        for horizon in ANALOGUE_FORWARD_WINDOWS:
            values = [
                outcome for anchor in anchors
                if (outcome := _forward_return(
                    closes, index_by_date, anchor["session_date"], horizon, through_date,
                )) is not None
            ]
            windows[str(horizon)] = _window_summary(values, ANALOGUE_MIN_EPISODES)
        catalogue.append({
            "regime_family": family,
            "episode_anchors": len(anchors),
            "windows": windows,
        })
    current = next(
        (row for row in catalogue if row["regime_family"] == current_family),
        {"regime_family": current_family, "episode_anchors": 0, "windows": {}},
    )
    benchmark_ready = bool(closes)
    window_ready = all(
        current["windows"].get(str(horizon), {}).get("status") == "ready"
        for horizon in ANALOGUE_FORWARD_WINDOWS
    )
    coverage_ready = len(rows) >= ANALOGUE_MIN_LEDGER_SESSIONS
    checks = [
        {"key": "frozen_classifier", "label": "Frozen classifier contract", "passed": True,
         "detail": MARKET_REGIME_CONTRACT},
        {"key": "completed_session_prices", "label": "Completed-session benchmark closes", "passed": benchmark_ready,
         "detail": f"{len(closes):,} validated Nifty 50 closes" if closes else "Nifty 50 close history unavailable"},
        {"key": "independent_episodes", "label": "One anchor per contiguous regime episode", "passed": True,
         "detail": f"{len(episode_anchors):,} independent anchors"},
        {"key": "minimum_history", "label": "Minimum classified-history coverage", "passed": coverage_ready,
         "detail": f"{len(rows):,} of {ANALOGUE_MIN_LEDGER_SESSIONS:,} required sessions"},
        {"key": "current_regime_samples", "label": "Current-regime sample gate", "passed": window_ready,
         "detail": f"Minimum {ANALOGUE_MIN_EPISODES} mature episodes in each forward window"},
        {"key": "outcome_isolation", "label": "Forward outcomes excluded from classification", "passed": True,
         "detail": "Outcomes are computed only after the stored regime label is fixed"},
    ]
    ready = all(check["passed"] for check in checks)
    gates = [check["key"] for check in checks if not check["passed"]]
    return {
        "status": "ready" if ready else "gated",
        "release_decision": "descriptive_ready" if ready else "withheld",
        "gates": gates,
        "current_regime": current_family,
        "current": current,
        "catalogue": catalogue,
        "rules": {
            "anchor": "first completed session of each contiguous regime segment",
            "match": "exact regime-family match",
            "forward_windows_sessions": list(ANALOGUE_FORWARD_WINDOWS),
            "minimum_independent_episodes": ANALOGUE_MIN_EPISODES,
            "minimum_ledger_sessions": ANALOGUE_MIN_LEDGER_SESSIONS,
            "overlapping_anchors": False,
            "trailing_only": True,
        },
        "release_audit": {
            "status": "passed" if ready else "withheld",
            "checks": checks,
            "passed": sum(check["passed"] for check in checks),
            "total": len(checks),
        },
        "detail": (
            "Current-regime analogue statistics are descriptive and trailing-only."
            if ready else
            "Analogue publication is withheld until the classified ledger and each current-regime forward window pass their minimum sample gates."
        ),
    }


def historical_regimes_overview(
    path: Path, limit: int = 500,
) -> dict[str, Any]:
    if limit < 20 or limit > 2000:
        raise ValueError("invalid_historical_regime_limit")
    connection = connect_database(path)
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_regime_ledger'"
        ).fetchone()
        if not exists:
            return _gated("market_regime_ledger_missing")
        stored = connection.execute(
            """SELECT session_date,classification_status,ranking_admissible,
                      regime_family,trend_direction,trend_strength,range_state,
                      volatility_state,breadth_state,regime_key,
                      market_return_20d_pct,market_distance_sma200_pct,
                      market_volatility_20d_pct,market_breadth_50dma_pct,
                      market_breadth_200dma_pct,priced_coverage_ratio,
                      breadth_50dma_coverage_ratio,breadth_200dma_coverage_ratio,
                      source_dates_json,missing_inputs_json
               FROM market_regime_ledger
               WHERE contract_version=? ORDER BY session_date DESC LIMIT ?""",
            (MARKET_REGIME_CONTRACT, limit),
        ).fetchall()
        policy_events = _load_policy_events(connection)
        global_risk = _latest_global_risk(connection)
        benchmark_closes = _load_benchmark_closes(connection)
    except sqlite3.Error as error:
        return _gated("market_regime_ledger_unavailable", str(error))
    finally:
        connection.close()
    if not stored:
        return _gated("market_regime_ledger_empty")

    rows = []
    for source in reversed(stored):
        row = dict(source)
        row["ranking_admissible"] = bool(row["ranking_admissible"])
        try:
            row["source_dates"] = json.loads(row.pop("source_dates_json") or "{}")
        except json.JSONDecodeError:
            row["source_dates"] = {}
        try:
            row["missing_inputs"] = json.loads(row.pop("missing_inputs_json") or "[]")
        except json.JSONDecodeError:
            row["missing_inputs"] = ["invalid_missing_inputs_json"]
        rows.append(row)

    family_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        family_rows[str(row.get("regime_family") or "gated")].append(row)
    distribution = []
    for family in ("bullish_trend", "range", "transition", "bearish_trend", "gated"):
        members = family_rows.get(family, [])
        if not members:
            continue
        distribution.append({
            "regime_family": family,
            "sessions": len(members),
            "share_pct": round(100.0 * len(members) / len(rows), 2),
            "average_return_20d_pct": _mean(members, "market_return_20d_pct"),
            "average_volatility_20d_pct": _mean(members, "market_volatility_20d_pct"),
            "average_breadth_50dma_pct": _mean(members, "market_breadth_50dma_pct"),
            "average_breadth_200dma_pct": _mean(members, "market_breadth_200dma_pct"),
        })
    segments = _segment_rows(rows)
    current = rows[-1]
    return {
        "ok": True,
        "contract_version": HISTORICAL_REGIMES_CONTRACT,
        "status": "descriptive",
        "gates": [],
        "coverage": {
            "from_date": rows[0]["session_date"],
            "through_date": current["session_date"],
            "sessions": len(rows),
            "segments": len(segments),
            "transitions": max(len(segments) - 1, 0),
            "ranking_admissible_sessions": sum(bool(row["ranking_admissible"]) for row in rows),
            "classification_statuses": dict(sorted(
                (status, sum(row["classification_status"] == status for row in rows))
                for status in {row["classification_status"] for row in rows}
            )),
        },
        "current": current,
        "distribution": distribution,
        "segments": segments,
        "recent_evidence": list(reversed(rows[-20:])),
        "policy_cycle": _policy_cycle(policy_events, rows),
        "cross_asset_context": _cross_asset_context(path, current, global_risk),
        "analogues": _regime_analogues(rows, segments, benchmark_closes),
        "methodology": {
            "classification_contract": MARKET_REGIME_CONTRACT,
            "timeline": "contiguous_stored_completed_sessions_collapsed_by_regime_family",
            "strategy_results_used": False,
            "forward_outcomes_used": True,
            "forward_outcomes_change_classifier": False,
            "policy_context_changes_classifier": False,
            "cross_asset_context_changes_classifier": False,
            "decision_boundary": "descriptive regime history; no forecast or trade instruction",
        },
        "local_only": True,
        "orders_enabled": False,
    }


def _gated(reason: str, detail: str | None = None) -> dict[str, Any]:
    return {
        "ok": True,
        "contract_version": HISTORICAL_REGIMES_CONTRACT,
        "status": "gated",
        "gates": [reason],
        "detail": detail,
        "coverage": {},
        "current": {},
        "distribution": [],
        "segments": [],
        "recent_evidence": [],
        "policy_cycle": {"status": "gated", "reason": "policy_rate_history_not_stored"},
        "cross_asset_context": {
            "status": "gated",
            "reason": "cross_asset_history_unavailable",
            "observations": [],
        },
        "analogues": {
            "status": "gated",
            "release_decision": "withheld",
            "reason": "market_regime_ledger_unavailable",
            "catalogue": [],
            "release_audit": {"status": "withheld", "checks": [], "passed": 0, "total": 0},
        },
        "local_only": True,
        "orders_enabled": False,
    }
