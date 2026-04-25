"""SLA tracking: define expected check intervals and detect overdue pipelines."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from pipecheck.history import _connect


@dataclass
class SLAPolicy:
    pipeline: str
    max_interval_minutes: int  # alert if no successful run within this window


@dataclass
class SLAResult:
    pipeline: str
    max_interval_minutes: int
    last_success: Optional[datetime]
    minutes_since_success: Optional[float]
    breached: bool

    def as_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "max_interval_minutes": self.max_interval_minutes,
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "minutes_since_success": round(self.minutes_since_success, 1)
            if self.minutes_since_success is not None
            else None,
            "breached": self.breached,
        }


def _last_success_time(db_path: str, pipeline: str) -> Optional[datetime]:
    """Return the timestamp of the most recent successful check for *pipeline*."""
    con = _connect(db_path)
    try:
        row = con.execute(
            """
            SELECT checked_at FROM results
            WHERE pipeline = ? AND ok = 1
            ORDER BY checked_at DESC
            LIMIT 1
            """,
            (pipeline,),
        ).fetchone()
    finally:
        con.close()
    if row is None:
        return None
    return datetime.fromisoformat(row[0])


def check_sla(policy: SLAPolicy, db_path: str, now: Optional[datetime] = None) -> SLAResult:
    """Evaluate a single SLA policy against stored history."""
    if now is None:
        now = datetime.utcnow()
    last = _last_success_time(db_path, policy.pipeline)
    if last is None:
        return SLAResult(
            pipeline=policy.pipeline,
            max_interval_minutes=policy.max_interval_minutes,
            last_success=None,
            minutes_since_success=None,
            breached=True,
        )
    elapsed = (now - last).total_seconds() / 60.0
    return SLAResult(
        pipeline=policy.pipeline,
        max_interval_minutes=policy.max_interval_minutes,
        last_success=last,
        minutes_since_success=elapsed,
        breached=elapsed > policy.max_interval_minutes,
    )


def check_all_slas(
    policies: list[SLAPolicy], db_path: str, now: Optional[datetime] = None
) -> list[SLAResult]:
    """Evaluate every SLA policy and return results."""
    return [check_sla(p, db_path, now) for p in policies]
