"""Persistent, source-versioned result caching for completed-session contracts."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from workspace_analytics.sqlite_store import connect_database


CACHE_SCHEMA_VERSION = 1


def ensure_result_cache_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """CREATE TABLE IF NOT EXISTS analytics_result_cache (
               cache_key TEXT NOT NULL,
               contract_version TEXT NOT NULL,
               source_version TEXT NOT NULL,
               payload_json TEXT NOT NULL,
               created_at TEXT NOT NULL,
               last_accessed_at TEXT NOT NULL,
               hit_count INTEGER NOT NULL DEFAULT 0,
               schema_version INTEGER NOT NULL DEFAULT 1,
               PRIMARY KEY(cache_key, contract_version, source_version)
           )"""
    )
    connection.execute(
        """CREATE INDEX IF NOT EXISTS analytics_result_cache_lookup_idx
               ON analytics_result_cache(cache_key, contract_version, created_at)"""
    )


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def semantic_source_version(
    databases: Iterable[
        tuple[Path, Iterable[tuple[str, str | None, str | None]]]
    ],
    context: dict[str, Any] | None = None,
) -> str:
    """Hash only declared source tables, never cache-table or file metadata."""
    markers: list[dict[str, Any]] = []
    for path, specifications in databases:
        connection = connect_database(path, read_only=path.exists(), row_factory=False)
        try:
            for table, date_column, update_column in specifications:
                marker: dict[str, Any] = {"database": path.name, "table": table}
                if not _table_exists(connection, table):
                    marker["status"] = "missing"
                else:
                    selections = ["COUNT(*)"]
                    if date_column:
                        selections.append(f"MAX({date_column})")
                    if update_column:
                        selections.append(f"MAX({update_column})")
                    row = connection.execute(
                        f"SELECT {','.join(selections)} FROM {table}"
                    ).fetchone()
                    marker["rows"] = int(row[0])
                    offset = 1
                    if date_column:
                        marker["latest_date"] = row[offset]
                        offset += 1
                    if update_column:
                        marker["latest_update"] = row[offset]
                markers.append(marker)
        finally:
            connection.close()
    payload = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "sources": sorted(markers, key=lambda item: (item["database"], item["table"])),
        "context": context or {},
    }
    serialised = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()


def load_versioned_result(
    path: Path, cache_key: str, contract_version: str, source_version: str,
) -> dict[str, Any] | None:
    connection = connect_database(path, row_factory=False)
    try:
        ensure_result_cache_schema(connection)
        row = connection.execute(
            """SELECT payload_json FROM analytics_result_cache
               WHERE cache_key=? AND contract_version=? AND source_version=?""",
            (cache_key, contract_version, source_version),
        ).fetchone()
        if not row:
            connection.commit()
            return None
        now = datetime.now(timezone.utc).isoformat()
        connection.execute(
            """UPDATE analytics_result_cache
               SET last_accessed_at=?,hit_count=hit_count+1
               WHERE cache_key=? AND contract_version=? AND source_version=?""",
            (now, cache_key, contract_version, source_version),
        )
        connection.commit()
        return json.loads(row[0])
    finally:
        connection.close()


def load_versioned_results_by_prefix(
    path: Path, cache_key_prefix: str, contract_version: str, source_version: str,
) -> dict[str, dict[str, Any]]:
    """Load a restart-safe family of results for one semantic source version."""
    connection = connect_database(path, row_factory=False)
    try:
        ensure_result_cache_schema(connection)
        rows = connection.execute(
            """SELECT cache_key,payload_json FROM analytics_result_cache
               WHERE cache_key LIKE ? AND contract_version=? AND source_version=?
               ORDER BY cache_key""",
            (f"{cache_key_prefix}%", contract_version, source_version),
        ).fetchall()
        if rows:
            now = datetime.now(timezone.utc).isoformat()
            connection.execute(
                """UPDATE analytics_result_cache
                   SET last_accessed_at=?,hit_count=hit_count+1
                   WHERE cache_key LIKE ? AND contract_version=? AND source_version=?""",
                (now, f"{cache_key_prefix}%", contract_version, source_version),
            )
        connection.commit()
        return {str(row[0]): json.loads(row[1]) for row in rows}
    finally:
        connection.close()


def store_versioned_result(
    path: Path, cache_key: str, contract_version: str, source_version: str,
    payload: dict[str, Any],
) -> None:
    connection = connect_database(path, row_factory=False)
    try:
        ensure_result_cache_schema(connection)
        now = datetime.now(timezone.utc).isoformat()
        connection.execute(
            """INSERT OR REPLACE INTO analytics_result_cache(
                   cache_key,contract_version,source_version,payload_json,
                   created_at,last_accessed_at,hit_count,schema_version)
               VALUES (?,?,?,?,?,?,0,?)""",
            (
                cache_key, contract_version, source_version,
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                now, now, CACHE_SCHEMA_VERSION,
            ),
        )
        # A contract key retains only its current source version. Other contracts
        # remain intact, which is the incremental invalidation boundary.
        connection.execute(
            """DELETE FROM analytics_result_cache
               WHERE cache_key=? AND contract_version=? AND source_version<>?""",
            (cache_key, contract_version, source_version),
        )
        connection.commit()
    finally:
        connection.close()


def result_cache_status(path: Path) -> dict[str, Any]:
    connection = connect_database(path)
    try:
        ensure_result_cache_schema(connection)
        rows = connection.execute(
            """SELECT cache_key,contract_version,source_version,created_at,
                      last_accessed_at,hit_count
               FROM analytics_result_cache ORDER BY cache_key,contract_version"""
        ).fetchall()
        connection.commit()
        return {
            "contract_version": "analytics-result-cache-status-v1",
            "entries": [dict(row) for row in rows],
            "entry_count": len(rows),
        }
    finally:
        connection.close()
