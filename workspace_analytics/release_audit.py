"""Workspace-wide static, data-store and operational release audit."""

from __future__ import annotations

import re
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any


WORKSPACE_RELEASE_CONTRACT = "workspace-release-audit-v1"
REQUIRED_VIEWS = (
    "dashboardView",
    "marketStateView",
    "fnoPositioningView",
    "stockResearchView",
    "portfolioView",
    "historicalRegimesView",
)
REQUIRED_NAVIGATION = (
    "dashboardNav",
    "marketStateNav",
    "fnoNav",
    "stockResearchNav",
    "portfolioNav",
    "historicalRegimesNav",
)
REQUIRED_ROUTES = (
    "/api/dashboard/overview",
    "/api/market-sentiment/domestic-core",
    "/api/kite/futures/eod-summary",
    "/api/stocks/ytd",
    "/api/portfolio/overview",
    "/api/historical-regimes/overview",
)


def _check(key: str, label: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"key": key, "label": label, "passed": bool(passed), "detail": detail}


def _database_integrity(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, f"Missing {path.name}"
    try:
        connection = sqlite3.connect(path)
        result = connection.execute("PRAGMA quick_check").fetchone()[0]
        connection.close()
    except sqlite3.Error as error:
        return False, f"{path.name}: {error}"
    return result == "ok", f"{path.name}: {result}"


def _latest_date(path: Path, table: str, column: str, where: str = "") -> str | None:
    if not path.exists():
        return None
    connection = sqlite3.connect(path)
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,),
        ).fetchone()
        if not exists:
            return None
        query = f"SELECT MAX({column}) FROM {table}"
        if where:
            query += f" WHERE {where}"
        return connection.execute(query).fetchone()[0]
    finally:
        connection.close()


def workspace_release_audit(
    root: Path, market_db: Path, snapshot_db: Path, today: date | None = None,
) -> dict[str, Any]:
    """Audit the local release surface without mutating data or contacting a source."""
    root = root.resolve()
    today = today or date.today()
    html = (root / "dist" / "index.html").read_text(encoding="utf-8")
    script = (root / "dist" / "app.js").read_text(encoding="utf-8")
    styles = (root / "dist" / "styles.css").read_text(encoding="utf-8")
    server = (root / "server.py").read_text(encoding="utf-8")
    readme = (root / "README.md").read_text(encoding="utf-8")

    ids = re.findall(r'\bid="([^"]+)"', html)
    id_set = set(ids)
    views_ready = all(view in id_set for view in REQUIRED_VIEWS)
    nav_ready = all(nav in id_set and f"$('{{nav}}')".replace("{nav}", nav) in script for nav in REQUIRED_NAVIGATION)
    routes_missing = [route for route in REQUIRED_ROUTES if route not in server]
    responsive = all(
        breakpoint in styles for breakpoint in (
            "@media (max-width: 1100px)",
            "@media (max-width: 760px)",
            "@media (max-width: 520px)",
        )
    )
    accessible = all(token in html for token in (
        'href="#mainContent"',
        'aria-label="Primary navigation"',
        'aria-label="Historical regime summary"',
        'aria-labelledby="factorDialogTitle"',
        'aria-labelledby="researchDetailTitle"',
    ))
    expense_removed = "expense analyser" not in (html + script).lower()
    operational = all(heading in readme for heading in (
        "## Run locally", "## Refresh local snapshots", "## Backup and recovery",
        "## Release verification",
    ))

    market_ok, market_detail = _database_integrity(market_db)
    snapshot_ok, snapshot_detail = _database_integrity(snapshot_db)
    latest_market = _latest_date(
        market_db, "candles", "session_date",
        "instrument_id='index:Nifty 50' AND validation_status='validated'",
    )
    latest_snapshot = _latest_date(
        snapshot_db, "sovereign_yield_observations", "observation_date",
        "validation_status='validated'",
    )
    market_age = (today - date.fromisoformat(latest_market)).days if latest_market else None
    freshness_ready = market_age is not None and 0 <= market_age <= 7

    checks = [
        _check("page_views", "Six completed page views", views_ready,
               f"{sum(view in id_set for view in REQUIRED_VIEWS)} of {len(REQUIRED_VIEWS)} present"),
        _check("unique_dom_ids", "Unique DOM identifiers", len(ids) == len(id_set),
               f"{len(ids)} IDs · {len(ids) - len(id_set)} duplicates"),
        _check("navigation", "Navigation wiring", nav_ready,
               f"{sum(nav in id_set for nav in REQUIRED_NAVIGATION)} navigation controls"),
        _check("api_routes", "Required page API routes", not routes_missing,
               "All routes present" if not routes_missing else f"Missing: {', '.join(routes_missing)}"),
        _check("responsive", "Desktop, tablet and mobile breakpoints", responsive,
               "1100px · 760px · 520px"),
        _check("accessibility", "Core accessibility landmarks", accessible,
               "Skip link, navigation label, summaries and dialog labels"),
        _check("expense_removed", "Expense Analyser excluded", expense_removed,
               "No UI or browser-script reference" if expense_removed else "Forbidden reference remains"),
        _check("market_database", "Market database integrity", market_ok, market_detail),
        _check("snapshot_database", "Snapshot database integrity", snapshot_ok, snapshot_detail),
        _check("completed_session_freshness", "Completed-session freshness", freshness_ready,
               f"Nifty 50 through {latest_market or 'unavailable'} · {market_age if market_age is not None else '—'} calendar days old"),
        _check("operational_guidance", "Run, refresh, backup and verification guidance", operational,
               "README operational sections present" if operational else "README operational section missing"),
    ]
    passed = sum(check["passed"] for check in checks)
    ready = passed == len(checks)
    return {
        "ok": True,
        "contract_version": WORKSPACE_RELEASE_CONTRACT,
        "status": "ready" if ready else "withheld",
        "release_decision": "local_release_ready" if ready else "withheld",
        "passed": passed,
        "total": len(checks),
        "checks": checks,
        "coverage": {
            "pages": len(REQUIRED_VIEWS),
            "latest_market_session": latest_market,
            "latest_snapshot_observation": latest_snapshot,
            "market_database": str(market_db),
            "snapshot_database": str(snapshot_db),
        },
        "boundaries": {
            "local_only": True,
            "orders_enabled": False,
            "runtime_proxy_dependency": False,
            "release_scope": "local completed-session research workspace",
        },
    }
