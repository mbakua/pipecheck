"""Compute trends from historical check results."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List, Dict, Any

from pipecheck.history import load_history


@dataclass
class TrendSummary:
    pipeline: str
    total_runs: int
    success_count: int
    failure_count: int
    success_rate: float  # 0.0 – 1.0
    last_status: str
    flapping: bool  # True when last 3 statuses alternate


def as_dict(ts: TrendSummary) -> Dict[str, Any]:
    return asdict(ts)


def _is_flapping(statuses: List[str]) -> bool:
    """Return True if the last (up to) 3 statuses are not all the same."""
    tail = statuses[-3:]
    return len(tail) >= 2 and len(set(tail)) > 1


def compute_trend(pipeline_name: str, db_path: str, limit: int = 20) -> TrendSummary | None:
    """Return a TrendSummary for *pipeline_name* using the last *limit* runs.

    Returns None when no history exists for that pipeline.
    """
    rows = load_history(db_path, pipeline=pipeline_name, limit=limit)
    if not rows:
        return None

    statuses = [r["status"] for r in rows]
    success_count = statuses.count("ok")
    failure_count = len(statuses) - success_count

    return TrendSummary(
        pipeline=pipeline_name,
        total_runs=len(statuses),
        success_count=success_count,
        failure_count=failure_count,
        success_rate=round(success_count / len(statuses), 4),
        last_status=statuses[0],  # load_history returns newest-first
        flapping=_is_flapping(statuses),
    )
