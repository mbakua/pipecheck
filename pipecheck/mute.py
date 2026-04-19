"""Mute/suppress alerts for specific pipelines during maintenance windows."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DEFAULT_MUTE_FILE = Path(".pipecheck_mutes.json")


@dataclass
class MuteEntry:
    pipeline: str
    reason: str
    muted_at: str
    expires_at: Optional[str] = None  # ISO format or None = indefinite


def _now() -> datetime:
    return datetime.now(timezone.utc)


def load_mutes(path: Path = DEFAULT_MUTE_FILE) -> list[MuteEntry]:
    if not path.exists():
        return []
    with path.open() as f:
        raw = json.load(f)
    return [MuteEntry(**e) for e in raw]


def save_mutes(mutes: list[MuteEntry], path: Path = DEFAULT_MUTE_FILE) -> None:
    with path.open("w") as f:
        json.dump([asdict(e) for e in mutes], f, indent=2)


def mute_pipeline(
    pipeline: str,
    reason: str,
    expires_at: Optional[datetime] = None,
    path: Path = DEFAULT_MUTE_FILE,
) -> MuteEntry:
    mutes = load_mutes(path)
    mutes = [m for m in mutes if m.pipeline != pipeline]  # replace if exists
    entry = MuteEntry(
        pipeline=pipeline,
        reason=reason,
        muted_at=_now().isoformat(),
        expires_at=expires_at.isoformat() if expires_at else None,
    )
    mutes.append(entry)
    save_mutes(mutes, path)
    return entry


def unmute_pipeline(pipeline: str, path: Path = DEFAULT_MUTE_FILE) -> bool:
    mutes = load_mutes(path)
    new_mutes = [m for m in mutes if m.pipeline != pipeline]
    if len(new_mutes) == len(mutes):
        return False
    save_mutes(new_mutes, path)
    return True


def is_muted(pipeline: str, path: Path = DEFAULT_MUTE_FILE) -> bool:
    now = _now()
    for entry in load_mutes(path):
        if entry.pipeline != pipeline:
            continue
        if entry.expires_at is None:
            return True
        if datetime.fromisoformat(entry.expires_at) > now:
            return True
    return False


def active_mutes(path: Path = DEFAULT_MUTE_FILE) -> list[MuteEntry]:
    now = _now()
    result = []
    for entry in load_mutes(path):
        if entry.expires_at is None or datetime.fromisoformat(entry.expires_at) > now:
            result.append(entry)
    return result
