"""Watchdog: detect pipelines that have not been checked recently."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from pipecheck.history import _connect


@dataclass
class WatchdogResult:
    pipeline: str
    last_checked: Optional[datetime]  # None => never checked
    silence_seconds: Optional[float]  # None => never checked
    threshold_seconds: float
    stale: bool


def as_dict(r: WatchdogResult) -> dict:
    return {
        "pipeline": r.pipeline,
        "last_checked": r.last_checked.isoformat() if r.last_checked else None,
        "silence_seconds": round(r.silence_seconds, 1) if r.silence_seconds is not None else None,
        "threshold_seconds": r.threshold_seconds,
        "stale": r.stale,
    }


def _last_check_time(db_path: str, pipeline: str) -> Optional[datetime]:
    """Return the timestamp of the most recent row for *pipeline* in the DB."""
    con = _connect(db_path)
    try:
        row = con.execute(
            "SELECT checked_at FROM results WHERE pipeline = ? ORDER BY checked_at DESC LIMIT 1",
            (pipeline,),
        ).fetchone()
    finally:
        con.close()
    if row is None:
        return None
    return datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)


def check_watchdog(
    pipelines: List[str],
    threshold_seconds: float,
    db_path: str,
    *,
    now: Optional[datetime] = None,
) -> List[WatchdogResult]:
    """Return a WatchdogResult for every pipeline in *pipelines*."""
    if now is None:
        now = datetime.now(tz=timezone.utc)

    results: List[WatchdogResult] = []
    for name in pipelines:
        last = _last_check_time(db_path, name)
        if last is None:
            silence = None
            stale = True
        else:
            silence = (now - last).total_seconds()
            stale = silence > threshold_seconds
        results.append(
            WatchdogResult(
                pipeline=name,
                last_checked=last,
                silence_seconds=silence,
                threshold_seconds=threshold_seconds,
                stale=stale,
            )
        )
    return results
