"""Persist and retrieve check run history using a local SQLite database."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List

from pipecheck.checks import CheckResult

DEFAULT_DB_PATH = Path.home() / ".pipecheck" / "history.db"


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    """Create the runs table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        TEXT    NOT NULL,
                pipeline  TEXT    NOT NULL,
                ok        INTEGER NOT NULL,
                status    INTEGER,
                latency   REAL,
                message   TEXT
            )
            """
        )


def save_results(results: List[CheckResult], db_path: Path = DEFAULT_DB_PATH) -> None:
    """Persist a list of CheckResult objects with the current timestamp."""
    init_db(db_path)
    ts = datetime.utcnow().isoformat()
    rows = [
        (ts, r.pipeline, int(r.ok), r.status, r.latency, r.message)
        for r in results
    ]
    with _connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO runs (ts, pipeline, ok, status, latency, message) VALUES (?,?,?,?,?,?)",
            rows,
        )


def load_history(pipeline: str, limit: int = 20, db_path: Path = DEFAULT_DB_PATH) -> List[dict]:
    """Return the most recent *limit* run records for a given pipeline."""
    init_db(db_path)
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT ts, ok, status, latency, message FROM runs WHERE pipeline=? ORDER BY id DESC LIMIT ?",
            (pipeline, limit),
        ).fetchall()
    return [dict(r) for r in rows]
