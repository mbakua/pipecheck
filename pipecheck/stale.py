"""Stale pipeline detection — flags pipelines that haven't succeeded within a threshold."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from pipecheck.history import load_history


@dataclass
class StaleResult:
    pipeline: str
    is_stale: bool
    last_success: Optional[datetime]
    hours_since_success: Optional[float]
    threshold_hours: float


def as_dict(r: StaleResult) -> dict:
    return {
        "pipeline": r.pipeline,
        "is_stale": r.is_stale,
        "last_success": r.last_success.isoformat() if r.last_success else None,
        "hours_since_success": round(r.hours_since_success, 2) if r.hours_since_success is not None else None,
        "threshold_hours": r.threshold_hours,
    }


def _last_success_dt(db_path: str, pipeline: str) -> Optional[datetime]:
    rows = load_history(db_path, pipeline=pipeline, limit=200)
    for row in rows:
        if row.get("status") == "ok":
            ts = row.get("timestamp")
            if ts:
                return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
    return None


def check_stale(
    db_path: str,
    pipeline: str,
    threshold_hours: float,
    now: Optional[datetime] = None,
) -> StaleResult:
    """Return a StaleResult indicating whether the pipeline is stale."""
    if now is None:
        now = datetime.now(timezone.utc)

    last_success = _last_success_dt(db_path, pipeline)

    if last_success is None:
        return StaleResult(
            pipeline=pipeline,
            is_stale=True,
            last_success=None,
            hours_since_success=None,
            threshold_hours=threshold_hours,
        )

    delta_hours = (now - last_success).total_seconds() / 3600.0
    return StaleResult(
        pipeline=pipeline,
        is_stale=delta_hours > threshold_hours,
        last_success=last_success,
        hours_since_success=delta_hours,
        threshold_hours=threshold_hours,
    )


def check_all_stale(
    db_path: str,
    pipelines: list[dict],
    default_threshold_hours: float = 24.0,
    now: Optional[datetime] = None,
) -> list[StaleResult]:
    """Run stale check for every pipeline in the list."""
    results = []
    for p in pipelines:
        name = p["name"]
        threshold = p.get("stale_threshold_hours", default_threshold_hours)
        results.append(check_stale(db_path, name, threshold, now=now))
    return results
