"""Retry policy for pipeline checks."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from pipecheck.checks import CheckResult, run_check
from pipecheck.config import PipelineConfig


@dataclass
class RetryPolicy:
    attempts: int = 3
    delay: float = 2.0
    backoff: float = 2.0


def run_with_retry(
    pipeline: PipelineConfig,
    policy: Optional[RetryPolicy] = None,
    _sleep: Callable[[float], None] = time.sleep,
) -> CheckResult:
    """Run a pipeline check with retry logic.

    Retries on non-OK results up to *policy.attempts* times,
    applying exponential back-off between attempts.
    Returns the last result regardless of outcome.
    """
    if policy is None:
        policy = RetryPolicy()

    delay = policy.delay
    result: CheckResult = run_check(pipeline)

    for attempt in range(1, policy.attempts):
        if result.ok:
            break
        _sleep(delay)
        delay *= policy.backoff
        result = run_check(pipeline)
        result = CheckResult(
            pipeline=result.pipeline,
            ok=result.ok,
            status_code=result.status_code,
            message=result.message,
            latency_ms=result.latency_ms,
            extra={
                **(result.extra or {}),
                "retry_attempt": attempt,
            },
        )

    return result
