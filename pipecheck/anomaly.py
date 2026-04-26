"""Anomaly detection for pipeline check durations and failure rates.

Uses a simple rolling z-score approach: if a pipeline's latest response
time or failure rate deviates more than `threshold` standard deviations
from its recent history, it is flagged as anomalous.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from typing import List, Optional

from pipecheck.history import _connect


@dataclass
class AnomalyResult:
    pipeline: str
    metric: str          # "duration_ms" or "failure_rate"
    current_value: float
    mean: float
    stddev: float
    z_score: float
    is_anomaly: bool


def as_dict(result: AnomalyResult) -> dict:
    return {
        "pipeline": result.pipeline,
        "metric": result.metric,
        "current_value": round(result.current_value, 3),
        "mean": round(result.mean, 3),
        "stddev": round(result.stddev, 3),
        "z_score": round(result.z_score, 3),
        "is_anomaly": result.is_anomaly,
    }


def _mean_stddev(values: List[float]):
    """Return (mean, population stddev) for a list of floats."""
    if not values:
        return 0.0, 0.0
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return mean, math.sqrt(variance)


def _fetch_rows(db_path: str, pipeline: str, limit: int):
    """Fetch recent (ok, duration_ms) rows for a pipeline from history."""
    conn = _connect(db_path)
    try:
        cur = conn.execute(
            """
            SELECT ok, duration_ms
            FROM results
            WHERE pipeline = ?
            ORDER BY ts DESC
            LIMIT ?
            """,
            (pipeline, limit),
        )
        return cur.fetchall()
    finally:
        conn.close()


def detect_anomalies(
    db_path: str,
    pipeline: str,
    window: int = 30,
    threshold: float = 2.5,
) -> List[AnomalyResult]:
    """Detect anomalies in duration and failure rate for *pipeline*.

    Args:
        db_path:   Path to the SQLite history database.
        pipeline:  Pipeline name to analyse.
        window:    Number of historical runs to consider (including the latest).
        threshold: Z-score threshold above which a value is flagged.

    Returns:
        A list of :class:`AnomalyResult` (one per metric checked).
        Returns an empty list when there is insufficient history.
    """
    rows = _fetch_rows(db_path, pipeline, window)
    if len(rows) < 3:
        # Not enough data to compute meaningful statistics.
        return []

    # Most-recent run is first (ORDER BY ts DESC).
    latest_ok, latest_dur = rows[0]
    history = rows[1:]  # exclude the current run from baseline stats

    results: List[AnomalyResult] = []

    # --- Duration anomaly ---
    durations = [r[1] for r in history if r[1] is not None]
    if durations and latest_dur is not None:
        mean, stddev = _mean_stddev(durations)
        z = (latest_dur - mean) / stddev if stddev > 0 else 0.0
        results.append(
            AnomalyResult(
                pipeline=pipeline,
                metric="duration_ms",
                current_value=float(latest_dur),
                mean=mean,
                stddev=stddev,
                z_score=z,
                is_anomaly=abs(z) > threshold,
            )
        )

    # --- Failure-rate anomaly ---
    # Treat each historical run as 0 (ok) or 1 (failed).
    failure_flags = [0.0 if r[0] else 1.0 for r in history]
    if failure_flags:
        mean, stddev = _mean_stddev(failure_flags)
        current_failure = 0.0 if latest_ok else 1.0
        z = (current_failure - mean) / stddev if stddev > 0 else 0.0
        results.append(
            AnomalyResult(
                pipeline=pipeline,
                metric="failure_rate",
                current_value=current_failure,
                mean=mean,
                stddev=stddev,
                z_score=z,
                is_anomaly=abs(z) > threshold,
            )
        )

    return results


def detect_all_anomalies(
    db_path: str,
    pipelines: List[str],
    window: int = 30,
    threshold: float = 2.5,
) -> List[AnomalyResult]:
    """Run anomaly detection across all *pipelines* and return flagged results."""
    flagged: List[AnomalyResult] = []
    for name in pipelines:
        for result in detect_anomalies(db_path, name, window, threshold):
            if result.is_anomaly:
                flagged.append(result)
    return flagged
