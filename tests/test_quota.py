"""Tests for pipecheck.quota."""
from __future__ import annotations

import time
from typing import Dict

import pytest

from pipecheck.quota import (
    QuotaEntry,
    is_quota_exceeded,
    load_quota,
    record_check,
    save_quota,
    set_quota,
)


@pytest.fixture()
def quota_file(tmp_path):
    return str(tmp_path / "quota.json")


def _empty() -> Dict[str, QuotaEntry]:
    return {}


def test_load_quota_missing_file_returns_empty(quota_file):
    entries = load_quota(quota_file)
    assert entries == {}


def test_save_and_load_roundtrip(quota_file):
    entries: Dict[str, QuotaEntry] = {}
    set_quota("pipe_a", 600, 5, entries)
    save_quota(entries, quota_file)
    loaded = load_quota(quota_file)
    assert "pipe_a" in loaded
    assert loaded["pipe_a"].max_checks == 5
    assert loaded["pipe_a"].window_seconds == 600


def test_not_exceeded_when_no_record():
    entries: Dict[str, QuotaEntry] = {}
    set_quota("pipe_a", 60, 3, entries)
    assert not is_quota_exceeded("pipe_a", entries)


def test_not_exceeded_below_limit():
    entries: Dict[str, QuotaEntry] = {}
    set_quota("pipe_a", 60, 3, entries)
    record_check("pipe_a", entries)
    record_check("pipe_a", entries)
    assert not is_quota_exceeded("pipe_a", entries)


def test_exceeded_at_limit():
    entries: Dict[str, QuotaEntry] = {}
    set_quota("pipe_a", 60, 3, entries)
    record_check("pipe_a", entries)
    record_check("pipe_a", entries)
    record_check("pipe_a", entries)
    assert is_quota_exceeded("pipe_a", entries)


def test_old_timestamps_pruned_on_record():
    entries: Dict[str, QuotaEntry] = {}
    set_quota("pipe_a", 1, 2, entries)
    # inject two old timestamps
    entries["pipe_a"].timestamps = [time.time() - 10, time.time() - 10]
    # after recording a fresh one, old ones are pruned
    record_check("pipe_a", entries)
    assert len(entries["pipe_a"].timestamps) == 1


def test_unknown_pipeline_not_exceeded():
    entries: Dict[str, QuotaEntry] = {}
    assert not is_quota_exceeded("ghost", entries)


def test_record_check_no_entry_is_noop():
    entries: Dict[str, QuotaEntry] = {}
    record_check("ghost", entries)  # should not raise


def test_set_quota_preserves_existing_timestamps():
    entries: Dict[str, QuotaEntry] = {}
    set_quota("pipe_a", 60, 5, entries)
    entries["pipe_a"].timestamps = [1000.0, 2000.0]
    set_quota("pipe_a", 120, 10, entries)
    assert entries["pipe_a"].timestamps == [1000.0, 2000.0]
    assert entries["pipe_a"].max_checks == 10
