"""Alert deduplication: suppress repeated alerts for the same pipeline
within a configurable cooldown window so operators aren't spammed."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

DEFAULT_COOLDOWN = 3600  # seconds


@dataclass
class DedupEntry:
    pipeline: str
    last_alerted: float  # unix timestamp
    alert_count: int


def _now() -> float:
    return time.time()


def load_dedup(path: Path) -> Dict[str, DedupEntry]:
    """Load dedup state from a JSON file. Returns empty dict if missing."""
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    return {
        k: DedupEntry(
            pipeline=k,
            last_alerted=v["last_alerted"],
            alert_count=v["alert_count"],
        )
        for k, v in raw.items()
    }


def save_dedup(path: Path, state: Dict[str, DedupEntry]) -> None:
    """Persist dedup state to a JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        k: {"last_alerted": v.last_alerted, "alert_count": v.alert_count}
        for k, v in state.items()
    }
    path.write_text(json.dumps(data, indent=2))


def is_duplicate(path: Path, pipeline: str, cooldown: int = DEFAULT_COOLDOWN) -> bool:
    """Return True if an alert for *pipeline* was already sent within *cooldown* seconds."""
    state = load_dedup(path)
    entry = state.get(pipeline)
    if entry is None:
        return False
    return (_now() - entry.last_alerted) < cooldown


def record_alert(path: Path, pipeline: str) -> DedupEntry:
    """Record that an alert was just sent for *pipeline*. Returns updated entry."""
    state = load_dedup(path)
    existing = state.get(pipeline)
    count = (existing.alert_count + 1) if existing else 1
    entry = DedupEntry(pipeline=pipeline, last_alerted=_now(), alert_count=count)
    state[pipeline] = entry
    save_dedup(path, state)
    return entry


def reset_pipeline(path: Path, pipeline: str) -> bool:
    """Remove dedup record for *pipeline* (e.g. after it recovers). Returns True if removed."""
    state = load_dedup(path)
    if pipeline not in state:
        return False
    del state[pipeline]
    save_dedup(path, state)
    return True


def get_entry(path: Path, pipeline: str) -> Optional[DedupEntry]:
    """Return the dedup entry for *pipeline*, or None."""
    return load_dedup(path).get(pipeline)
