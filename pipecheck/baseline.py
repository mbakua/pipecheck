"""Baseline management: snapshot expected pipeline states for drift detection."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

from pipecheck.checks import CheckResult

DEFAULT_BASELINE_PATH = Path(".pipecheck_baseline.json")


@dataclass
class BaselineEntry:
    pipeline: str
    expected_status: int
    ok: bool


def save_baseline(results: List[CheckResult], path: Path = DEFAULT_BASELINE_PATH) -> None:
    """Persist current results as the baseline."""
    entries = [
        {"pipeline": r.pipeline, "expected_status": r.status_code, "ok": r.ok}
        for r in results
    ]
    path.write_text(json.dumps(entries, indent=2))


def load_baseline(path: Path = DEFAULT_BASELINE_PATH) -> Optional[Dict[str, BaselineEntry]]:
    """Load baseline from disk. Returns None if file doesn't exist."""
    if not path.exists():
        return None
    raw = json.loads(path.read_text())
    return {
        e["pipeline"]: BaselineEntry(
            pipeline=e["pipeline"],
            expected_status=e["expected_status"],
            ok=e["ok"],
        )
        for e in raw
    }


@dataclass
class DriftResult:
    pipeline: str
    drifted: bool
    reason: str


def compare_baseline(
    results: List[CheckResult], path: Path = DEFAULT_BASELINE_PATH
) -> List[DriftResult]:
    """Compare current results against baseline; return drift findings."""
    baseline = load_baseline(path)
    if baseline is None:
        return []

    drifts: List[DriftResult] = []
    for r in results:
        entry = baseline.get(r.pipeline)
        if entry is None:
            drifts.append(DriftResult(r.pipeline, True, "new pipeline not in baseline"))
        elif r.ok != entry.ok:
            drifts.append(
                DriftResult(
                    r.pipeline,
                    True,
                    f"ok changed: baseline={entry.ok} current={r.ok}",
                )
            )
        else:
            drifts.append(DriftResult(r.pipeline, False, "ok"))
    return drifts
