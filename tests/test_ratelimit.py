"""Tests for pipecheck.ratelimit."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipecheck.ratelimit import (
    clear_ratelimit,
    is_rate_limited,
    load_ratelimit,
    record_check,
    save_ratelimit,
)


@pytest.fixture()
def rl_file(tmp_path: Path) -> str:
    return str(tmp_path / "ratelimit.json")


def test_load_ratelimit_missing_file(rl_file: str) -> None:
    assert load_ratelimit(rl_file) == {}


def test_save_and_load_roundtrip(rl_file: str) -> None:
    data = {"pipe_a": "2024-01-01T00:00:00+00:00"}
    save_ratelimit(data, rl_file)
    assert load_ratelimit(rl_file) == data


def test_not_rate_limited_when_no_record(rl_file: str) -> None:
    assert is_rate_limited("pipe_a", 60, rl_file) is False


def test_rate_limited_immediately_after_record(rl_file: str) -> None:
    record_check("pipe_a", rl_file)
    assert is_rate_limited("pipe_a", 60, rl_file) is True


def test_not_rate_limited_after_interval_passes(rl_file: str) -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=120)
    save_ratelimit({"pipe_a": past.isoformat()}, rl_file)
    assert is_rate_limited("pipe_a", 60, rl_file) is False


def test_zero_interval_never_rate_limited(rl_file: str) -> None:
    record_check("pipe_a", rl_file)
    assert is_rate_limited("pipe_a", 0, rl_file) is False


def test_record_check_writes_timestamp(rl_file: str) -> None:
    record_check("pipe_b", rl_file)
    state = load_ratelimit(rl_file)
    assert "pipe_b" in state
    ts = datetime.fromisoformat(state["pipe_b"])
    assert (datetime.now(timezone.utc) - ts).total_seconds() < 5


def test_clear_single_pipeline(rl_file: str) -> None:
    record_check("pipe_a", rl_file)
    record_check("pipe_b", rl_file)
    clear_ratelimit("pipe_a", rl_file)
    state = load_ratelimit(rl_file)
    assert "pipe_a" not in state
    assert "pipe_b" in state


def test_clear_all_removes_file(rl_file: str) -> None:
    record_check("pipe_a", rl_file)
    clear_ratelimit(None, rl_file)
    assert not Path(rl_file).exists()


def test_clear_all_no_file_is_noop(rl_file: str) -> None:
    # Should not raise even if file is absent
    clear_ratelimit(None, rl_file)
