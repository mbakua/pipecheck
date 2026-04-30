"""Maintenance window management for pipecheck.

Allows pipelines to be placed in a maintenance window so that
check failures are suppressed and alerts are not dispatched.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_DEFAULT_FILE = Path(".pipecheck_maintenance.json")


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class MaintenanceWindow:
    pipeline: str
    start: str   # ISO-8601
    end: str     # ISO-8601
    reason: str = ""

    def is_active(self, at: Optional[datetime] = None) -> bool:
        t = at or _now()
        start_dt = datetime.fromisoformat(self.start)
        end_dt = datetime.fromisoformat(self.end)
        return start_dt <= t <= end_dt


def _load_raw(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open() as fh:
        return json.load(fh)


def load_windows(path: Path = _DEFAULT_FILE) -> list[MaintenanceWindow]:
    return [MaintenanceWindow(**r) for r in _load_raw(path)]


def save_windows(windows: list[MaintenanceWindow], path: Path = _DEFAULT_FILE) -> None:
    with path.open("w") as fh:
        json.dump([asdict(w) for w in windows], fh, indent=2)


def add_window(
    pipeline: str,
    start: datetime,
    end: datetime,
    reason: str = "",
    path: Path = _DEFAULT_FILE,
) -> MaintenanceWindow:
    windows = load_windows(path)
    entry = MaintenanceWindow(
        pipeline=pipeline,
        start=start.isoformat(),
        end=end.isoformat(),
        reason=reason,
    )
    windows.append(entry)
    save_windows(windows, path)
    return entry


def remove_window(pipeline: str, path: Path = _DEFAULT_FILE) -> bool:
    windows = load_windows(path)
    new = [w for w in windows if w.pipeline != pipeline]
    if len(new) == len(windows):
        return False
    save_windows(new, path)
    return True


def is_in_maintenance(
    pipeline: str,
    at: Optional[datetime] = None,
    path: Path = _DEFAULT_FILE,
) -> bool:
    for w in load_windows(path):
        if w.pipeline == pipeline and w.is_active(at):
            return True
    return False
