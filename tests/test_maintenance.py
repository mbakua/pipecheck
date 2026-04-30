"""Tests for pipecheck.maintenance."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipecheck.maintenance import (
    MaintenanceWindow,
    add_window,
    remove_window,
    load_windows,
    is_in_maintenance,
)


@pytest.fixture()
def mfile(tmp_path: Path) -> Path:
    return tmp_path / "maintenance.json"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_load_missing_file_returns_empty(mfile: Path) -> None:
    assert load_windows(mfile) == []


def test_add_and_load_roundtrip(mfile: Path) -> None:
    start = _now() - timedelta(hours=1)
    end = _now() + timedelta(hours=1)
    add_window("pipe_a", start, end, reason="deploy", path=mfile)
    windows = load_windows(mfile)
    assert len(windows) == 1
    assert windows[0].pipeline == "pipe_a"
    assert windows[0].reason == "deploy"


def test_window_is_active_within_range() -> None:
    now = _now()
    w = MaintenanceWindow(
        pipeline="p",
        start=(now - timedelta(hours=1)).isoformat(),
        end=(now + timedelta(hours=1)).isoformat(),
    )
    assert w.is_active(now) is True


def test_window_not_active_before_start() -> None:
    now = _now()
    w = MaintenanceWindow(
        pipeline="p",
        start=(now + timedelta(hours=1)).isoformat(),
        end=(now + timedelta(hours=2)).isoformat(),
    )
    assert w.is_active(now) is False


def test_window_not_active_after_end() -> None:
    now = _now()
    w = MaintenanceWindow(
        pipeline="p",
        start=(now - timedelta(hours=2)).isoformat(),
        end=(now - timedelta(hours=1)).isoformat(),
    )
    assert w.is_active(now) is False


def test_is_in_maintenance_true(mfile: Path) -> None:
    now = _now()
    add_window("pipe_b", now - timedelta(minutes=30), now + timedelta(minutes=30), path=mfile)
    assert is_in_maintenance("pipe_b", path=mfile) is True


def test_is_in_maintenance_false_for_other_pipeline(mfile: Path) -> None:
    now = _now()
    add_window("pipe_b", now - timedelta(minutes=30), now + timedelta(minutes=30), path=mfile)
    assert is_in_maintenance("pipe_c", path=mfile) is False


def test_remove_existing_window(mfile: Path) -> None:
    now = _now()
    add_window("pipe_d", now - timedelta(hours=1), now + timedelta(hours=1), path=mfile)
    removed = remove_window("pipe_d", path=mfile)
    assert removed is True
    assert load_windows(mfile) == []


def test_remove_nonexistent_returns_false(mfile: Path) -> None:
    assert remove_window("ghost", path=mfile) is False


def test_multiple_windows(mfile: Path) -> None:
    now = _now()
    add_window("p1", now - timedelta(hours=1), now + timedelta(hours=1), path=mfile)
    add_window("p2", now - timedelta(hours=2), now - timedelta(hours=1), path=mfile)
    windows = load_windows(mfile)
    assert len(windows) == 2
    assert is_in_maintenance("p1", path=mfile) is True
    assert is_in_maintenance("p2", path=mfile) is False
