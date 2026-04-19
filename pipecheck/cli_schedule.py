"""CLI helpers that honour schedule windows before running checks."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pipecheck.checks import run_check, CheckResult
from pipecheck.config import PipelineConfig
from pipecheck.schedule import should_run


def run_with_schedule(
    pipelines: list[PipelineConfig],
    now: Optional[datetime] = None,
    verbose: bool = False,
) -> tuple[list[CheckResult], list[str]]:
    """Run checks only for pipelines whose schedule window is active.

    Returns:
        results  – CheckResult list for pipelines that ran.
        skipped  – names of pipelines that were outside their window.
    """
    results: list[CheckResult] = []
    skipped: list[str] = []

    for cfg in pipelines:
        if not should_run(cfg, now):
            skipped.append(cfg.name)
            if verbose:
                print(f"[schedule] skipping '{cfg.name}' – outside run window")
            continue
        results.append(run_check(cfg))

    return results, skipped
