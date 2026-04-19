"""Daily/periodic digest report generation for pipeline health."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional

from pipecheck.history import load_history
from pipecheck.trend import compute_trend


@dataclass
class DigestEntry:
    pipeline: str
    total_runs: int
    success_runs: int
    failure_runs: int
    success_rate: float
    trend: Optional[str]
    flapping: bool


def build_digest(db_path: str, pipeline_names: List[str], hours: int = 24) -> List[DigestEntry]:
    """Build a digest of pipeline health over the past *hours* hours."""
    since = datetime.utcnow() - timedelta(hours=hours)
    entries: List[DigestEntry] = []

    for name in pipeline_names:
        rows = load_history(db_path, pipeline=name, limit=1000)
        recent = [r for r in rows if r.get("checked_at", "") >= since.isoformat()]

        total = len(recent)
        successes = sum(1 for r in recent if r.get("ok"))
        failures = total - successes
        rate = (successes / total * 100) if total else 0.0

        trend_summary = compute_trend(db_path, name)
        trend_label = trend_summary.trend if trend_summary else None
        flapping = trend_summary.flapping if trend_summary else False

        entries.append(
            DigestEntry(
                pipeline=name,
                total_runs=total,
                success_runs=successes,
                failure_runs=failures,
                success_rate=round(rate, 1),
                trend=trend_label,
                flapping=flapping,
            )
        )

    return entries


def format_digest(entries: List[DigestEntry], hours: int = 24) -> str:
    """Return a human-readable digest string."""
    lines = [f"=== PipeCheck Digest (last {hours}h) ==="]
    for e in entries:
        flag = " [FLAPPING]" if e.flapping else ""
        trend = f" trend={e.trend}" if e.trend else ""
        lines.append(
            f"  {e.pipeline}: {e.success_rate}% ok "
            f"({e.success_runs}/{e.total_runs}){trend}{flag}"
        )
    if not entries:
        lines.append("  No pipelines tracked.")
    return "\n".join(lines)
