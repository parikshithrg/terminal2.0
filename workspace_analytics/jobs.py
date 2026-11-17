"""Persistent progress records for bounded background research jobs."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workspace_analytics.sqlite_store import connect_database


JOB_CONTRACT = "background-research-job-v1"


def _connect(path: Path) -> sqlite3.Connection:
    return connect_database(path)


def ensure_job_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """CREATE TABLE IF NOT EXISTS background_research_jobs (
               job_id TEXT PRIMARY KEY,
               contract_version TEXT NOT NULL,
               job_type TEXT NOT NULL,
               source_version TEXT NOT NULL,
               status TEXT NOT NULL,
               completed_units INTEGER NOT NULL,
               total_units INTEGER NOT NULL,
               current_stage TEXT,
               started_at TEXT NOT NULL,
               updated_at TEXT NOT NULL,
               completed_at TEXT,
               errors_json TEXT NOT NULL,
               detail_json TEXT NOT NULL,
               schema_version INTEGER NOT NULL DEFAULT 1
           )"""
    )
    connection.execute(
        """CREATE INDEX IF NOT EXISTS background_research_jobs_updated_idx
               ON background_research_jobs(updated_at DESC)"""
    )


def _serialise(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    payload["errors"] = json.loads(payload.pop("errors_json"))
    payload["detail"] = json.loads(payload.pop("detail_json"))
    total = int(payload["total_units"])
    completed = int(payload["completed_units"])
    payload["progress_pct"] = round(100 * completed / total, 1) if total else 0.0
    payload["terminal"] = payload["status"] in {
        "complete", "complete_with_errors", "failed", "cancelled", "interrupted",
    }
    return payload


def start_job(
    path: Path, job_id: str, job_type: str, source_version: str,
    total_units: int, completed_units: int = 0,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if total_units < 1 or completed_units < 0 or completed_units > total_units:
        raise ValueError("invalid_background_job_units")
    now = datetime.now(timezone.utc).isoformat()
    connection = _connect(path)
    try:
        ensure_job_schema(connection)
        connection.execute(
            """INSERT OR REPLACE INTO background_research_jobs(
                   job_id,contract_version,job_type,source_version,status,
                   completed_units,total_units,current_stage,started_at,updated_at,
                   completed_at,errors_json,detail_json,schema_version)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
            (
                job_id, JOB_CONTRACT, job_type, source_version, "running",
                completed_units, total_units, None, now, now, None, "{}",
                json.dumps(detail or {}, sort_keys=True),
            ),
        )
        connection.commit()
        row = connection.execute(
            "SELECT * FROM background_research_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        return _serialise(row) or {}
    finally:
        connection.close()


def update_job(
    path: Path, job_id: str, completed_units: int,
    current_stage: str | None = None, errors: dict[str, str] | None = None,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    connection = _connect(path)
    try:
        ensure_job_schema(connection)
        current = connection.execute(
            "SELECT * FROM background_research_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if current is None:
            raise ValueError("background_job_not_found")
        total = int(current["total_units"])
        if completed_units < 0 or completed_units > total:
            raise ValueError("invalid_background_job_units")
        merged_errors = json.loads(current["errors_json"])
        merged_errors.update(errors or {})
        merged_detail = json.loads(current["detail_json"])
        merged_detail.update(detail or {})
        now = datetime.now(timezone.utc).isoformat()
        connection.execute(
            """UPDATE background_research_jobs
               SET completed_units=?,current_stage=?,updated_at=?,errors_json=?,detail_json=?
               WHERE job_id=?""",
            (
                completed_units, current_stage, now,
                json.dumps(merged_errors, sort_keys=True),
                json.dumps(merged_detail, sort_keys=True), job_id,
            ),
        )
        connection.commit()
        row = connection.execute(
            "SELECT * FROM background_research_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        return _serialise(row) or {}
    finally:
        connection.close()


def finish_job(
    path: Path, job_id: str, status: str = "complete",
    errors: dict[str, str] | None = None,
) -> dict[str, Any]:
    if status not in {"complete", "complete_with_errors", "failed", "cancelled", "interrupted"}:
        raise ValueError("invalid_background_job_terminal_status")
    connection = _connect(path)
    try:
        ensure_job_schema(connection)
        current = connection.execute(
            "SELECT * FROM background_research_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if current is None:
            raise ValueError("background_job_not_found")
        merged_errors = json.loads(current["errors_json"])
        merged_errors.update(errors or {})
        now = datetime.now(timezone.utc).isoformat()
        completed = int(current["completed_units"])
        if status in {"complete", "complete_with_errors"}:
            completed = int(current["total_units"])
        connection.execute(
            """UPDATE background_research_jobs
               SET status=?,completed_units=?,current_stage=NULL,updated_at=?,
                   completed_at=?,errors_json=? WHERE job_id=?""",
            (status, completed, now, now, json.dumps(merged_errors, sort_keys=True), job_id),
        )
        connection.commit()
        row = connection.execute(
            "SELECT * FROM background_research_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        return _serialise(row) or {}
    finally:
        connection.close()


def job_status(path: Path, job_id: str) -> dict[str, Any] | None:
    connection = _connect(path)
    try:
        ensure_job_schema(connection)
        row = connection.execute(
            "SELECT * FROM background_research_jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        connection.commit()
        return _serialise(row)
    finally:
        connection.close()


def list_jobs(path: Path, limit: int = 20) -> dict[str, Any]:
    if limit < 1 or limit > 100:
        raise ValueError("invalid_background_job_limit")
    connection = _connect(path)
    try:
        ensure_job_schema(connection)
        rows = connection.execute(
            "SELECT * FROM background_research_jobs ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        connection.commit()
        return {
            "ok": True,
            "contract_version": "background-research-jobs-v1",
            "jobs": [_serialise(row) for row in rows],
            "count": len(rows),
        }
    finally:
        connection.close()
