"""Correlation analysis for pipeline failures.

Detects pipelines that tend to fail together, which can indicate
shared infrastructure dependencies or upstream/downstream relationships
not captured in the explicit dependency graph.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from pipecheck.history import load_history


@dataclass
class CorrelationPair:
    """Represents a correlated failure pair between two pipelines."""

    pipeline_a: str
    pipeline_b: str
    # Number of runs where both failed at the same time window
    co_failures: int
    # Total runs considered for the pair
    total_windows: int
    # co_failures / total_windows
    score: float

    def as_dict(self) -> dict:
        return {
            "pipeline_a": self.pipeline_a,
            "pipeline_b": self.pipeline_b,
            "co_failures": self.co_failures,
            "total_windows": self.total_windows,
            "score": round(self.score, 4),
        }


def _bucket(ts: str, window_minutes: int = 10) -> str:
    """Truncate an ISO timestamp to a time bucket for co-occurrence analysis."""
    # ts format: '2024-01-15 12:34:56' or ISO with T
    ts_clean = ts.replace("T", " ").split(".")[0]
    date_part, time_part = ts_clean.split(" ")
    h, m, s = time_part.split(":")
    bucket_m = (int(m) // window_minutes) * window_minutes
    return f"{date_part} {int(h):02d}:{bucket_m:02d}"


def compute_correlations(
    db_path: str,
    pipeline_names: List[str],
    limit: int = 200,
    window_minutes: int = 10,
    min_score: float = 0.5,
    min_co_failures: int = 2,
) -> List[CorrelationPair]:
    """Compute pairwise failure correlations across pipelines.

    Args:
        db_path: Path to the SQLite history database.
        pipeline_names: List of pipeline names to analyse.
        limit: Number of recent history rows to load per pipeline.
        window_minutes: Time bucket width for co-occurrence grouping.
        min_score: Minimum correlation score (0-1) to include in results.
        min_co_failures: Minimum co-failure count to include in results.

    Returns:
        List of CorrelationPair sorted by score descending.
    """
    # Build a mapping: pipeline -> set of failure buckets
    failure_buckets: Dict[str, set] = {}
    all_buckets: Dict[str, set] = {}

    for name in pipeline_names:
        rows = load_history(db_path, name, limit=limit)
        f_set: set = set()
        a_set: set = set()
        for row in rows:
            ts = row.get("checked_at", "")
            if not ts:
                continue
            b = _bucket(ts, window_minutes)
            a_set.add(b)
            if row.get("status") != "ok":
                f_set.add(b)
        failure_buckets[name] = f_set
        all_buckets[name] = a_set

    pairs: List[CorrelationPair] = []

    names = list(pipeline_names)
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a, b = names[i], names[j]
            shared_windows = all_buckets[a] & all_buckets[b]
            if not shared_windows:
                continue
            co_fail = len(failure_buckets[a] & failure_buckets[b])
            if co_fail < min_co_failures:
                continue
            score = co_fail / len(shared_windows)
            if score < min_score:
                continue
            pairs.append(
                CorrelationPair(
                    pipeline_a=a,
                    pipeline_b=b,
                    co_failures=co_fail,
                    total_windows=len(shared_windows),
                    score=score,
                )
            )

    pairs.sort(key=lambda p: p.score, reverse=True)
    return pairs


def format_correlations(pairs: List[CorrelationPair]) -> str:
    """Render correlation pairs as a human-readable table."""
    if not pairs:
        return "No significant correlations found."

    lines = ["Pipeline Failure Correlations", "=" * 50]
    for p in pairs:
        bar_len = int(p.score * 20)
        bar = "#" * bar_len + "-" * (20 - bar_len)
        lines.append(
            f"  {p.pipeline_a} <-> {p.pipeline_b}"
        )
        lines.append(
            f"    [{bar}] {p.score:.0%}  "
            f"({p.co_failures} co-failures / {p.total_windows} windows)"
        )
    return "\n".join(lines)
