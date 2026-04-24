"""Tests for pipecheck.snapshots."""

from __future__ import annotations

import pytest
from pathlib import Path

from pipecheck.checks import CheckResult
from pipecheck.snapshots import (
    save_snapshot,
    load_snapshot,
    diff_snapshots,
    SnapshotDiff,
)


@pytest.fixture()
def snap_dir(tmp_path: Path) -> Path:
    return tmp_path / "snaps"


def _result(pipeline: str, ok: bool) -> CheckResult:
    return CheckResult(
        pipeline=pipeline,
        ok=ok,
        status_code=200 if ok else 500,
        message="ok" if ok else "error",
        latency_ms=10.0,
    )


def test_save_creates_file(snap_dir: Path) -> None:
    results = [_result("pipe-a", True)]
    path = save_snapshot("test-label", results, directory=snap_dir)
    assert path.exists()
    assert "test-label" in path.name


def test_load_returns_none_for_missing(snap_dir: Path) -> None:
    assert load_snapshot("nonexistent", snap_dir) is None


def test_roundtrip_preserves_results(snap_dir: Path) -> None:
    results = [_result("pipe-a", True), _result("pipe-b", False)]
    save_snapshot("roundtrip", results, directory=snap_dir)
    snap = load_snapshot("roundtrip", snap_dir)
    assert snap is not None
    assert len(snap.results) == 2
    names = {r["pipeline"] for r in snap.results}
    assert names == {"pipe-a", "pipe-b"}


def test_diff_no_changes(snap_dir: Path) -> None:
    results = [_result("pipe-a", True), _result("pipe-b", True)]
    save_snapshot("v1", results, directory=snap_dir)
    save_snapshot("v2", results, directory=snap_dir)
    old = load_snapshot("v1", snap_dir)
    new = load_snapshot("v2", snap_dir)
    diff = diff_snapshots(old, new)  # type: ignore[arg-type]
    assert diff.changed == []
    assert diff.added == []
    assert diff.removed == []
    assert sorted(diff.unchanged) == ["pipe-a", "pipe-b"]


def test_diff_detects_status_change(snap_dir: Path) -> None:
    old_results = [_result("pipe-a", True)]
    new_results = [_result("pipe-a", False)]
    save_snapshot("old", old_results, directory=snap_dir)
    save_snapshot("new", new_results, directory=snap_dir)
    old = load_snapshot("old", snap_dir)
    new = load_snapshot("new", snap_dir)
    diff = diff_snapshots(old, new)  # type: ignore[arg-type]
    assert "pipe-a" in diff.changed


def test_diff_detects_added_and_removed(snap_dir: Path) -> None:
    old_results = [_result("pipe-old", True)]
    new_results = [_result("pipe-new", True)]
    save_snapshot("s-old", old_results, directory=snap_dir)
    save_snapshot("s-new", new_results, directory=snap_dir)
    old = load_snapshot("s-old", snap_dir)
    new = load_snapshot("s-new", snap_dir)
    diff = diff_snapshots(old, new)  # type: ignore[arg-type]
    assert "pipe-new" in diff.added
    assert "pipe-old" in diff.removed
