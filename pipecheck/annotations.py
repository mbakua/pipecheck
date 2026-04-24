"""Pipeline annotations: attach freeform notes to pipelines, stored in SQLite."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

DEFAULT_DB = Path("pipecheck_history.db")


@dataclass
class Annotation:
    pipeline: str
    note: str
    author: str
    created_at: str  # ISO-8601


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_annotations_db(db_path: Path = DEFAULT_DB) -> None:
    """Create the annotations table if it does not exist."""
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS annotations (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline  TEXT    NOT NULL,
                note      TEXT    NOT NULL,
                author    TEXT    NOT NULL DEFAULT '',
                created_at TEXT   NOT NULL
            )
            """
        )


def add_annotation(
    pipeline: str,
    note: str,
    author: str = "",
    db_path: Path = DEFAULT_DB,
) -> Annotation:
    """Persist a new annotation and return it."""
    init_annotations_db(db_path)
    created_at = datetime.now(timezone.utc).isoformat()
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO annotations (pipeline, note, author, created_at) VALUES (?, ?, ?, ?)",
            (pipeline, note, author, created_at),
        )
    return Annotation(pipeline=pipeline, note=note, author=author, created_at=created_at)


def get_annotations(
    pipeline: str,
    limit: int = 20,
    db_path: Path = DEFAULT_DB,
) -> List[Annotation]:
    """Return the most recent annotations for a pipeline, newest first."""
    init_annotations_db(db_path)
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT pipeline, note, author, created_at
            FROM annotations
            WHERE pipeline = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (pipeline, limit),
        ).fetchall()
    return [Annotation(**dict(r)) for r in rows]


def delete_annotations(pipeline: str, db_path: Path = DEFAULT_DB) -> int:
    """Remove all annotations for a pipeline. Returns number of rows deleted."""
    init_annotations_db(db_path)
    with _connect(db_path) as conn:
        cur = conn.execute(
            "DELETE FROM annotations WHERE pipeline = ?", (pipeline,)
        )
        return cur.rowcount
