"""Pipeline run snapshot diffing — capture and compare check results over time."""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipecheck.checks import CheckResult

_DEFAULT_SNAPSHOT_DIR = Path(".pipecheck_snapshots")


@dataclass
class Snapshot:
    captured_at: str
    results: list[dict]


def _snapshot_path(label: str, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    safe_label = label.replace("/", "_").replace(" ", "_")
    return directory / f"{safe_label}.json"


def save_snapshot(
    label: str,
    results: list[CheckResult],
    directory: Path = _DEFAULT_SNAPSHOT_DIR,
) -> Path:
    """Persist a list of CheckResults as a named snapshot."""
    path = _snapshot_path(label, directory)
    snapshot = Snapshot(
        captured_at=datetime.now(timezone.utc).isoformat(),
        results=[asdict(r) for r in results],
    )
    path.write_text(json.dumps(asdict(snapshot), indent=2))
    return path


def load_snapshot(
    label: str,
    directory: Path = _DEFAULT_SNAPSHOT_DIR,
) -> Optional[Snapshot]:
    """Load a previously saved snapshot by label. Returns None if not found."""
    path = _snapshot_path(label, directory)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Snapshot file for '{label}' contains invalid JSON: {exc}") from exc
    if "captured_at" not in data or "results" not in data:
        raise ValueError(
            f"Snapshot file for '{label}' is missing required fields "
            "('captured_at', 'results')."
        )
    return Snapshot(**data)


@dataclass
class SnapshotDiff:
    added: list[str]       # pipelines present in new but not old
    removed: list[str]     # pipelines present in old but not new
    changed: list[str]     # pipelines whose status changed
    unchanged: list[str]   # pipelines with identical status


def diff_snapshots(old: Snapshot, new: Snapshot) -> SnapshotDiff:
    """Compare two snapshots and return a structured diff."""
    old_map = {r["pipeline"]: r["ok"] for r in old.results}
    new_map = {r["pipeline"]: r["ok"] for r in new.results}

    old_keys = set(old_map)
    new_keys = set(new_map)

    added = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)
    changed = sorted(k for k in old_keys & new_keys if old_map[k] != new_map[k])
    unchanged = sorted(k for k in old_keys & new_keys if old_map[k] == new_map[k])

    return SnapshotDiff(added=added, removed=removed, changed=changed, unchanged=unchanged)
