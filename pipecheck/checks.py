"""Pipeline health check runners."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

import requests

from pipecheck.config import PipelineConfig


@dataclass
class CheckResult:
    pipeline_name: str
    success: bool
    latency_ms: Optional[float] = None
    status_code: Optional[int] = None
    error: Optional[str] = None
    checked_at: datetime = field(default_factory=datetime.utcnow)

    def summary(self) -> str:
        status = "OK" if self.success else "FAIL"
        latency = f"{self.latency_ms:.1f}ms" if self.latency_ms is not None else "n/a"
        return f"[{status}] {self.pipeline_name} — latency={latency} error={self.error}"


def run_check(pipeline: PipelineConfig, timeout: int = 10) -> CheckResult:
    """Run a single HTTP health check against a pipeline endpoint."""
    if not pipeline.endpoint:
        return CheckResult(
            pipeline_name=pipeline.name,
            success=False,
            error="No endpoint configured",
        )

    start = time.monotonic()
    try:
        response = requests.get(pipeline.endpoint, timeout=timeout)
        latency_ms = (time.monotonic() - start) * 1000
        success = response.status_code == pipeline.expected_status_code
        return CheckResult(
            pipeline_name=pipeline.name,
            success=success,
            latency_ms=latency_ms,
            status_code=response.status_code,
            error=None if success else f"Expected {pipeline.expected_status_code}, got {response.status_code}",
        )
    except requests.RequestException as exc:
        latency_ms = (time.monotonic() - start) * 1000
        return CheckResult(
            pipeline_name=pipeline.name,
            success=False,
            latency_ms=latency_ms,
            error=str(exc),
        )


def run_all_checks(pipelines: List[PipelineConfig], timeout: int = 10) -> List[CheckResult]:
    """Run health checks for all configured pipelines."""
    return [run_check(p, timeout=timeout) for p in pipelines]
