"""Simple failure-rate forecast based on recent history."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipecheck.history import load_history


@dataclass
class ForecastResult:
    pipeline: str
    window_size: int          # number of runs examined
    failure_rate: float       # 0.0 – 1.0
    predicted_failures: int   # in next *horizon* runs
    horizon: int
    confidence: str           # "low" | "medium" | "high"


def as_dict(r: ForecastResult) -> dict:
    return {
        "pipeline": r.pipeline,
        "window_size": r.window_size,
        "failure_rate": round(r.failure_rate, 4),
        "predicted_failures": r.predicted_failures,
        "horizon": r.horizon,
        "confidence": r.confidence,
    }


def _confidence(window_size: int) -> str:
    if window_size >= 20:
        return "high"
    if window_size >= 10:
        return "medium"
    return "low"


def compute_forecast(
    db_path: str,
    pipeline_name: str,
    window: int = 30,
    horizon: int = 10,
) -> Optional[ForecastResult]:
    """Return a ForecastResult for *pipeline_name* or None if no history."""
    rows = load_history(db_path, pipeline_name, limit=window)
    if not rows:
        return None

    failures = sum(1 for r in rows if not r["ok"])
    rate = failures / len(rows)
    predicted = round(rate * horizon)

    return ForecastResult(
        pipeline=pipeline_name,
        window_size=len(rows),
        failure_rate=rate,
        predicted_failures=predicted,
        horizon=horizon,
        confidence=_confidence(len(rows)),
    )


def format_forecast(results: list[ForecastResult], fmt: str = "text") -> str:
    if fmt == "json":
        import json
        return json.dumps([as_dict(r) for r in results], indent=2)

    if not results:
        return "No forecast data available."

    lines = ["Failure Forecast", "=" * 40]
    for r in results:
        lines.append(
            f"  {r.pipeline}: {r.failure_rate*100:.1f}% failure rate "
            f"over last {r.window_size} runs → "
            f"{r.predicted_failures}/{r.horizon} predicted failures "
            f"[{r.confidence} confidence]"
        )
    return "\n".join(lines)
