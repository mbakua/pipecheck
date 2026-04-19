"""Alert throttling: suppress repeated alerts for the same pipeline."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, Optional

DEFAULT_THROTTLE_FILE = Path(".pipecheck_throttle.json")


def _now() -> float:
    return time.time()


def load_throttle(path: Path = DEFAULT_THROTTLE_FILE) -> Dict[str, float]:
    """Load throttle state from disk. Returns mapping of pipeline -> last_alert_ts."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_throttle(state: Dict[str, float], path: Path = DEFAULT_THROTTLE_FILE) -> None:
    """Persist throttle state to disk."""
    path.write_text(json.dumps(state, indent=2))


def is_throttled(
    pipeline_name: str,
    cooldown_seconds: int,
    path: Path = DEFAULT_THROTTLE_FILE,
) -> bool:
    """Return True if an alert for this pipeline was sent within cooldown_seconds."""
    state = load_throttle(path)
    last = state.get(pipeline_name)
    if last is None:
        return False
    return (_now() - last) < cooldown_seconds


def record_alert(
    pipeline_name: str,
    path: Path = DEFAULT_THROTTLE_FILE,
) -> None:
    """Record that an alert was just sent for this pipeline."""
    state = load_throttle(path)
    state[pipeline_name] = _now()
    save_throttle(state, path)


def clear_throttle(
    pipeline_name: str,
    path: Path = DEFAULT_THROTTLE_FILE,
) -> bool:
    """Remove throttle entry for a pipeline. Returns True if it existed."""
    state = load_throttle(path)
    if pipeline_name not in state:
        return False
    del state[pipeline_name]
    save_throttle(state, path)
    return True
