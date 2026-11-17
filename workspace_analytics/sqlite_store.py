"""Shared SQLite connection policy for Workspace-owned analytical stores."""

from __future__ import annotations

import sqlite3
from pathlib import Path


SQLITE_BUSY_TIMEOUT_MS = 30_000
SQLITE_JOURNAL_MODE = "wal"
SQLITE_SYNCHRONOUS = "normal"


def connect_database(
    path: Path,
    *,
    read_only: bool = False,
    row_factory: bool = True,
) -> sqlite3.Connection:
    """Open a Workspace store with one concurrency and integrity policy.

    Write-capable connections use WAL so refresh writes do not block dashboard
    readers. Read-only connections retain the same busy timeout and are made
    explicitly query-only.
    """
    if read_only:
        connection = sqlite3.connect(
            f"file:{path}?mode=ro", uri=True,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
        )
    else:
        connection = sqlite3.connect(
            path, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
        )
    try:
        connection.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA foreign_keys=ON")
        if read_only:
            connection.execute("PRAGMA query_only=ON")
        else:
            connection.execute(f"PRAGMA journal_mode={SQLITE_JOURNAL_MODE}")
            connection.execute(f"PRAGMA synchronous={SQLITE_SYNCHRONOUS}")
        if row_factory:
            connection.row_factory = sqlite3.Row
        return connection
    except Exception:
        connection.close()
        raise


def connection_policy(connection: sqlite3.Connection) -> dict[str, object]:
    """Return the effective settings for diagnostics and regression tests."""
    return {
        "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
        "busy_timeout_ms": int(connection.execute("PRAGMA busy_timeout").fetchone()[0]),
        "foreign_keys": bool(connection.execute("PRAGMA foreign_keys").fetchone()[0]),
        "synchronous": int(connection.execute("PRAGMA synchronous").fetchone()[0]),
        "query_only": bool(connection.execute("PRAGMA query_only").fetchone()[0]),
    }
