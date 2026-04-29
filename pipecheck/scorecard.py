"""Pipeline scorecard: aggregate health score per pipeline over a time window."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional
import sqlite3

from pipecheck.history import _connect


@dataclass
class ScorecardEntry:
    pipeline: str
    total: int
    successes: int
    failures: int
    score: float          # 0.0 – 100.0
    grade: str            # A / B / C / D / F


def as_dict(entry: ScorecardEntry) -> dict:
    return {
        "pipeline": entry.pipeline,
        "total": entry.total,
        "successes": entry.successes,
        "failures": entry.failures,
        "score": round(entry.score, 2),
        "grade": entry.grade,
    }


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def compute_scorecard(
    db_path: str,
    hours: int = 24,
    pipelines: Optional[List[str]] = None,
) -> List[ScorecardEntry]:
    """Return a scorecard for each pipeline using the last *hours* of history."""
    conn = _connect(db_path)
    cutoff = f"datetime('now', '-{hours} hours')"
    query = (
        "SELECT pipeline, status FROM results "
        f"WHERE checked_at >= {cutoff}"
    )
    rows = conn.execute(query).fetchall()
    conn.close()

    counts: dict[str, dict] = {}
    for pipeline, status in rows:
        if pipelines and pipeline not in pipelines:
            continue
        entry = counts.setdefault(pipeline, {"total": 0, "ok": 0})
        entry["total"] += 1
        if status == "ok":
            entry["ok"] += 1

    results: List[ScorecardEntry] = []
    for name, c in sorted(counts.items()):
        total = c["total"]
        ok = c["ok"]
        score = (ok / total * 100.0) if total else 0.0
        results.append(
            ScorecardEntry(
                pipeline=name,
                total=total,
                successes=ok,
                failures=total - ok,
                score=score,
                grade=_grade(score),
            )
        )
    return results


def format_scorecard(entries: List[ScorecardEntry], fmt: str = "text") -> str:
    if fmt == "json":
        import json
        return json.dumps([as_dict(e) for e in entries], indent=2)
    lines = [f"{'Pipeline':<30} {'Score':>7} {'Grade':>5} {'OK':>5} {'Fail':>5} {'Total':>6}"]
    lines.append("-" * 62)
    for e in entries:
        lines.append(
            f"{e.pipeline:<30} {e.score:>6.1f}% {e.grade:>5} {e.successes:>5} {e.failures:>5} {e.total:>6}"
        )
    return "\n".join(lines)
