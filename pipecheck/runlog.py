"""Run log: record and retrieve per-pipeline run events with duration tracking."""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class RunEvent:
    pipeline: str
    status: str          # "success" | "failure" | "skipped"
    started_at: float    # unix timestamp
    duration_ms: int
    message: str = ""


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_runlog_db(db_path: str) -> None:
    """Create the run_log table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS run_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline    TEXT    NOT NULL,
                status      TEXT    NOT NULL,
                started_at  REAL    NOT NULL,
                duration_ms INTEGER NOT NULL,
                message     TEXT    DEFAULT ''
            )
            """
        )
        conn.commit()


def record_run(db_path: str, event: RunEvent) -> None:
    """Persist a RunEvent to the database."""
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO run_log (pipeline, status, started_at, duration_ms, message) "
            "VALUES (?, ?, ?, ?, ?)",
            (event.pipeline, event.status, event.started_at, event.duration_ms, event.message),
        )
        conn.commit()


def load_run_log(
    db_path: str,
    pipeline: Optional[str] = None,
    limit: int = 50,
) -> List[RunEvent]:
    """Return recent RunEvents, optionally filtered by pipeline name."""
    with _connect(db_path) as conn:
        if pipeline:
            rows = conn.execute(
                "SELECT pipeline, status, started_at, duration_ms, message "
                "FROM run_log WHERE pipeline = ? ORDER BY started_at DESC LIMIT ?",
                (pipeline, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT pipeline, status, started_at, duration_ms, message "
                "FROM run_log ORDER BY started_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [
        RunEvent(
            pipeline=r["pipeline"],
            status=r["status"],
            started_at=r["started_at"],
            duration_ms=r["duration_ms"],
            message=r["message"],
        )
        for r in rows
    ]
