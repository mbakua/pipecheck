"""Periodic rollup: aggregate per-pipeline check history into hourly/daily summaries."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipecheck.history import _connect


@dataclass
class RollupEntry:
    pipeline: str
    period: str          # 'hourly' or 'daily'
    bucket: str          # ISO-formatted truncated timestamp
    total: int
    success: int
    failure: int
    success_rate: float  # 0.0 – 1.0


def as_dict(entry: RollupEntry) -> dict:
    return {
        "pipeline": entry.pipeline,
        "period": entry.period,
        "bucket": entry.bucket,
        "total": entry.total,
        "success": entry.success,
        "failure": entry.failure,
        "success_rate": round(entry.success_rate, 4),
    }


def _bucket_ts(ts: float, period: str) -> str:
    """Truncate a Unix timestamp to the start of its hour or day (UTC)."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    if period == "hourly":
        return dt.strftime("%Y-%m-%dT%H:00:00Z")
    return dt.strftime("%Y-%m-%dT00:00:00Z")


def compute_rollup(
    db_path: str,
    period: str = "hourly",
    pipeline: Optional[str] = None,
) -> List[RollupEntry]:
    """Compute rollup summaries from the history table.

    Args:
        db_path: Path to the SQLite history database.
        period:  'hourly' or 'daily'.
        pipeline: If given, restrict to a single pipeline name.

    Returns:
        List of RollupEntry sorted by pipeline then bucket.
    """
    if period not in ("hourly", "daily"):
        raise ValueError(f"Unknown period {period!r}; choose 'hourly' or 'daily'")

    conn = _connect(db_path)
    try:
        query = "SELECT pipeline, checked_at, status FROM checks"
        params: tuple = ()
        if pipeline:
            query += " WHERE pipeline = ?"
            params = (pipeline,)
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    # Aggregate in Python so we stay compatible with any SQLite version
    buckets: dict[tuple[str, str], dict] = {}
    for row_pipeline, checked_at, status in rows:
        bucket = _bucket_ts(checked_at, period)
        key = (row_pipeline, bucket)
        if key not in buckets:
            buckets[key] = {"total": 0, "success": 0, "failure": 0}
        buckets[key]["total"] += 1
        if status == "ok":
            buckets[key]["success"] += 1
        else:
            buckets[key]["failure"] += 1

    entries: List[RollupEntry] = []
    for (pipe, bucket), counts in sorted(buckets.items()):
        total = counts["total"]
        success = counts["success"]
        entries.append(
            RollupEntry(
                pipeline=pipe,
                period=period,
                bucket=bucket,
                total=total,
                success=success,
                failure=counts["failure"],
                success_rate=success / total if total else 0.0,
            )
        )
    return entries
