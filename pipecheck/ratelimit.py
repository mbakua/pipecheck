"""Per-pipeline check rate limiting to prevent hammering endpoints."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DEFAULT_RATELIMIT_FILE = ".pipecheck_ratelimit.json"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def load_ratelimit(path: str = DEFAULT_RATELIMIT_FILE) -> dict:
    """Load rate limit state from disk. Returns empty dict if missing."""
    if not Path(path).exists():
        return {}
    with open(path) as f:
        return json.load(f)


def save_ratelimit(state: dict, path: str = DEFAULT_RATELIMIT_FILE) -> None:
    """Persist rate limit state to disk."""
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def is_rate_limited(
    pipeline_name: str,
    min_interval_seconds: int,
    path: str = DEFAULT_RATELIMIT_FILE,
) -> bool:
    """Return True if the pipeline was checked too recently."""
    if min_interval_seconds <= 0:
        return False
    state = load_ratelimit(path)
    last_str = state.get(pipeline_name)
    if last_str is None:
        return False
    last = datetime.fromisoformat(last_str)
    elapsed = (_now() - last).total_seconds()
    return elapsed < min_interval_seconds


def record_check(
    pipeline_name: str,
    path: str = DEFAULT_RATELIMIT_FILE,
) -> None:
    """Record that a check was just executed for a pipeline."""
    state = load_ratelimit(path)
    state[pipeline_name] = _now().isoformat()
    save_ratelimit(state, path)


def clear_ratelimit(
    pipeline_name: Optional[str] = None,
    path: str = DEFAULT_RATELIMIT_FILE,
) -> None:
    """Clear rate limit entry for one pipeline or all pipelines."""
    if pipeline_name is None:
        if Path(path).exists():
            os.remove(path)
        return
    state = load_ratelimit(path)
    state.pop(pipeline_name, None)
    save_ratelimit(state, path)
