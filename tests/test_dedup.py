"""Tests for pipecheck.dedup."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from pipecheck.dedup import (
    get_entry,
    is_duplicate,
    load_dedup,
    record_alert,
    reset_pipeline,
)


@pytest.fixture()
def dedup_file(tmp_path: Path) -> Path:
    return tmp_path / "dedup.json"


def test_load_dedup_missing_file_returns_empty(dedup_file: Path) -> None:
    assert load_dedup(dedup_file) == {}


def test_not_duplicate_when_no_record(dedup_file: Path) -> None:
    assert is_duplicate(dedup_file, "pipe_a") is False


def test_record_alert_creates_entry(dedup_file: Path) -> None:
    entry = record_alert(dedup_file, "pipe_a")
    assert entry.pipeline == "pipe_a"
    assert entry.alert_count == 1
    assert entry.last_alerted <= time.time()


def test_record_alert_increments_count(dedup_file: Path) -> None:
    record_alert(dedup_file, "pipe_a")
    entry = record_alert(dedup_file, "pipe_a")
    assert entry.alert_count == 2


def test_is_duplicate_immediately_after_record(dedup_file: Path) -> None:
    record_alert(dedup_file, "pipe_a")
    assert is_duplicate(dedup_file, "pipe_a", cooldown=3600) is True


def test_not_duplicate_after_cooldown_expires(dedup_file: Path, monkeypatch) -> None:
    import pipecheck.dedup as dedup_mod

    record_alert(dedup_file, "pipe_a")
    # Advance time beyond cooldown
    monkeypatch.setattr(dedup_mod, "_now", lambda: time.time() + 7200)
    assert is_duplicate(dedup_file, "pipe_a", cooldown=3600) is False


def test_different_pipelines_tracked_independently(dedup_file: Path) -> None:
    record_alert(dedup_file, "pipe_a")
    assert is_duplicate(dedup_file, "pipe_b", cooldown=3600) is False


def test_reset_pipeline_removes_entry(dedup_file: Path) -> None:
    record_alert(dedup_file, "pipe_a")
    removed = reset_pipeline(dedup_file, "pipe_a")
    assert removed is True
    assert get_entry(dedup_file, "pipe_a") is None


def test_reset_nonexistent_pipeline_returns_false(dedup_file: Path) -> None:
    assert reset_pipeline(dedup_file, "ghost") is False


def test_get_entry_returns_none_for_unknown(dedup_file: Path) -> None:
    assert get_entry(dedup_file, "unknown") is None


def test_get_entry_returns_correct_data(dedup_file: Path) -> None:
    record_alert(dedup_file, "pipe_x")
    entry = get_entry(dedup_file, "pipe_x")
    assert entry is not None
    assert entry.pipeline == "pipe_x"
    assert entry.alert_count == 1
