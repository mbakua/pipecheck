"""Tests for pipecheck.throttle."""

import time
from pathlib import Path

import pytest

from pipecheck.throttle import (
    clear_throttle,
    is_throttled,
    load_throttle,
    record_alert,
    save_throttle,
)


@pytest.fixture()
def throttle_file(tmp_path: Path) -> Path:
    return tmp_path / "throttle.json"


def test_load_throttle_missing_file(throttle_file: Path) -> None:
    assert load_throttle(throttle_file) == {}


def test_save_and_load_roundtrip(throttle_file: Path) -> None:
    state = {"pipe_a": 1234567890.0, "pipe_b": 9999999999.0}
    save_throttle(state, throttle_file)
    loaded = load_throttle(throttle_file)
    assert loaded == state


def test_not_throttled_when_no_record(throttle_file: Path) -> None:
    assert not is_throttled("pipe_a", cooldown_seconds=60, path=throttle_file)


def test_throttled_immediately_after_record(throttle_file: Path) -> None:
    record_alert("pipe_a", path=throttle_file)
    assert is_throttled("pipe_a", cooldown_seconds=60, path=throttle_file)


def test_not_throttled_after_cooldown_expires(throttle_file: Path) -> None:
    # Manually write a timestamp far in the past
    past = time.time() - 120
    save_throttle({"pipe_a": past}, throttle_file)
    assert not is_throttled("pipe_a", cooldown_seconds=60, path=throttle_file)


def test_record_alert_updates_timestamp(throttle_file: Path) -> None:
    old_ts = time.time() - 200
    save_throttle({"pipe_a": old_ts}, throttle_file)
    record_alert("pipe_a", path=throttle_file)
    state = load_throttle(throttle_file)
    assert state["pipe_a"] > old_ts


def test_clear_throttle_removes_entry(throttle_file: Path) -> None:
    record_alert("pipe_a", path=throttle_file)
    result = clear_throttle("pipe_a", path=throttle_file)
    assert result is True
    assert not is_throttled("pipe_a", cooldown_seconds=60, path=throttle_file)


def test_clear_throttle_nonexistent_returns_false(throttle_file: Path) -> None:
    result = clear_throttle("no_such_pipe", path=throttle_file)
    assert result is False


def test_multiple_pipelines_independent(throttle_file: Path) -> None:
    record_alert("pipe_a", path=throttle_file)
    assert is_throttled("pipe_a", cooldown_seconds=60, path=throttle_file)
    assert not is_throttled("pipe_b", cooldown_seconds=60, path=throttle_file)
