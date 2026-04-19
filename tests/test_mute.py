"""Tests for pipecheck.mute."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipecheck.mute import (
    mute_pipeline,
    unmute_pipeline,
    is_muted,
    active_mutes,
    load_mutes,
)


@pytest.fixture()
def mute_file(tmp_path: Path) -> Path:
    return tmp_path / "mutes.json"


def test_mute_and_is_muted(mute_file):
    mute_pipeline("pipe_a", "maintenance", path=mute_file)
    assert is_muted("pipe_a", path=mute_file)


def test_unmuted_pipeline_not_muted(mute_file):
    assert not is_muted("pipe_b", path=mute_file)


def test_unmute_removes_entry(mute_file):
    mute_pipeline("pipe_a", "testing", path=mute_file)
    removed = unmute_pipeline("pipe_a", path=mute_file)
    assert removed
    assert not is_muted("pipe_a", path=mute_file)


def test_unmute_nonexistent_returns_false(mute_file):
    assert not unmute_pipeline("ghost", path=mute_file)


def test_expired_mute_not_active(mute_file):
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    mute_pipeline("pipe_c", "old", expires_at=past, path=mute_file)
    assert not is_muted("pipe_c", path=mute_file)


def test_future_mute_is_active(mute_file):
    future = datetime.now(timezone.utc) + timedelta(hours=2)
    mute_pipeline("pipe_d", "planned", expires_at=future, path=mute_file)
    assert is_muted("pipe_d", path=mute_file)


def test_active_mutes_excludes_expired(mute_file):
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    mute_pipeline("old_pipe", "expired", expires_at=past, path=mute_file)
    mute_pipeline("new_pipe", "active", expires_at=future, path=mute_file)
    mutes = active_mutes(path=mute_file)
    names = [m.pipeline for m in mutes]
    assert "new_pipe" in names
    assert "old_pipe" not in names


def test_mute_replaces_existing(mute_file):
    mute_pipeline("pipe_a", "first", path=mute_file)
    mute_pipeline("pipe_a", "second", path=mute_file)
    entries = load_mutes(mute_file)
    assert sum(1 for e in entries if e.pipeline == "pipe_a") == 1
    assert entries[-1].reason == "second"
