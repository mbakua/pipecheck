"""Compute simple trend/health metrics from historical run data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class TrendSummary:
    pipeline: str
    total: int
    success_rate: float  # 0.0 – 1.0
    avg_latency: float | None
    last_status: str  # "ok" | "fail" | "unknown"

    def as_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "total": self.total,
            "success_rate": round(self.success_rate, 4),
            "avg_latency": round(self.avg_latency, 3) if self.avg_latency is not None else None,
            "last_status": self.last_status,
        }


def compute_trend(pipeline: str, history: List[dict]) -> TrendSummary:
    """Derive a TrendSummary from raw history rows (newest-first order)."""
    if not history:
        return TrendSummary(pipeline=pipeline, total=0, success_rate=0.0,
                            avg_latency=None, last_status="unknown")

    total = len(history)
    successes = sum(1 for r in history if r["ok"])
    success_rate = successes / total

    latencies = [r["latency"] for r in history if r["latency"] is not None]
    avg_latency = sum(latencies) / len(latencies) if latencies else None

    last_status = "ok" if history[0]["ok"] else "fail"

    return TrendSummary(
        pipeline=pipeline,
        total=total,
        success_rate=success_rate,
        avg_latency=avg_latency,
        last_status=last_status,
    )
