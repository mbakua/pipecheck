"""Pipeline check quota enforcement — cap how many checks run per time window."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

DEFAULT_QUOTA_FILE = ".pipecheck_quota.json"


def _now() -> float:
    return time.time()


@dataclass
class QuotaEntry:
    pipeline: str
    window_seconds: int
    max_checks: int
    timestamps: List[float] = field(default_factory=list)


def load_quota(path: str = DEFAULT_QUOTA_FILE) -> Dict[str, QuotaEntry]:
    p = Path(path)
    if not p.exists():
        return {}
    raw = json.loads(p.read_text())
    result: Dict[str, QuotaEntry] = {}
    for name, data in raw.items():
        result[name] = QuotaEntry(
            pipeline=name,
            window_seconds=data["window_seconds"],
            max_checks=data["max_checks"],
            timestamps=data.get("timestamps", []),
        )
    return result


def save_quota(entries: Dict[str, QuotaEntry], path: str = DEFAULT_QUOTA_FILE) -> None:
    data = {
        name: {
            "window_seconds": e.window_seconds,
            "max_checks": e.max_checks,
            "timestamps": e.timestamps,
        }
        for name, e in entries.items()
    }
    Path(path).write_text(json.dumps(data, indent=2))


def is_quota_exceeded(pipeline: str, entries: Dict[str, QuotaEntry]) -> bool:
    """Return True if the pipeline has exhausted its quota for the current window."""
    entry = entries.get(pipeline)
    if entry is None:
        return False
    cutoff = _now() - entry.window_seconds
    recent = [t for t in entry.timestamps if t >= cutoff]
    return len(recent) >= entry.max_checks


def record_check(pipeline: str, entries: Dict[str, QuotaEntry]) -> None:
    """Record a check timestamp for the pipeline, pruning old entries."""
    entry = entries.get(pipeline)
    if entry is None:
        return
    now = _now()
    cutoff = now - entry.window_seconds
    entry.timestamps = [t for t in entry.timestamps if t >= cutoff]
    entry.timestamps.append(now)


def set_quota(
    pipeline: str,
    window_seconds: int,
    max_checks: int,
    entries: Dict[str, QuotaEntry],
) -> None:
    """Create or update a quota rule for a pipeline."""
    existing = entries.get(pipeline)
    ts = existing.timestamps if existing else []
    entries[pipeline] = QuotaEntry(
        pipeline=pipeline,
        window_seconds=window_seconds,
        max_checks=max_checks,
        timestamps=ts,
    )
