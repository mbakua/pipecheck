"""Tests for pipecheck.stale."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipecheck.stale import check_stale, check_all_stale, as_dict


@pytest.fixture()
def tmp_db(tmp_path: Path) -> str:
    db = str(tmp_path / "history.db")
    con = sqlite3.connect(db)
    con.execute(
        "CREATE TABLE runs (pipeline TEXT, status TEXT, timestamp TEXT, detail TEXT)"
    )
    con.commit()
    con.close()
    return db


def _insert(db: str, pipeline: str, status: str, ts: str) -> None:
    con = sqlite3.connect(db)
    con.execute(
        "INSERT INTO runs (pipeline, status, timestamp, detail) VALUES (?, ?, ?, '')",
        (pipeline, status, ts),
    )
    con.commit()
    con.close()


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_no_history_is_stale(tmp_db: str) -> None:
    result = check_stale(tmp_db, "pipe_a", threshold_hours=24.0, now=NOW)
    assert result.is_stale is True
    assert result.last_success is None
    assert result.hours_since_success is None


def test_recent_success_not_stale(tmp_db: str) -> None:
    recent = (NOW - timedelta(hours=2)).isoformat()
    _insert(tmp_db, "pipe_a", "ok", recent)
    result = check_stale(tmp_db, "pipe_a", threshold_hours=24.0, now=NOW)
    assert result.is_stale is False
    assert result.hours_since_success is not None
    assert result.hours_since_success < 24.0


def test_old_success_is_stale(tmp_db: str) -> None:
    old = (NOW - timedelta(hours=30)).isoformat()
    _insert(tmp_db, "pipe_a", "ok", old)
    result = check_stale(tmp_db, "pipe_a", threshold_hours=24.0, now=NOW)
    assert result.is_stale is True
    assert result.hours_since_success is not None
    assert result.hours_since_success > 24.0


def test_only_failures_is_stale(tmp_db: str) -> None:
    ts = (NOW - timedelta(hours=1)).isoformat()
    _insert(tmp_db, "pipe_a", "error", ts)
    result = check_stale(tmp_db, "pipe_a", threshold_hours=24.0, now=NOW)
    assert result.is_stale is True
    assert result.last_success is None


def test_as_dict_fields(tmp_db: str) -> None:
    recent = (NOW - timedelta(hours=3)).isoformat()
    _insert(tmp_db, "pipe_b", "ok", recent)
    result = check_stale(tmp_db, "pipe_b", threshold_hours=24.0, now=NOW)
    d = as_dict(result)
    assert d["pipeline"] == "pipe_b"
    assert d["is_stale"] is False
    assert d["threshold_hours"] == 24.0
    assert isinstance(d["hours_since_success"], float)


def test_check_all_stale_uses_per_pipeline_threshold(tmp_db: str) -> None:
    recent = (NOW - timedelta(hours=5)).isoformat()
    _insert(tmp_db, "pipe_x", "ok", recent)
    _insert(tmp_db, "pipe_y", "ok", recent)

    pipelines = [
        {"name": "pipe_x", "stale_threshold_hours": 3.0},   # stale (5 > 3)
        {"name": "pipe_y", "stale_threshold_hours": 12.0},  # fresh (5 < 12)
    ]
    results = check_all_stale(tmp_db, pipelines, now=NOW)
    by_name = {r.pipeline: r for r in results}
    assert by_name["pipe_x"].is_stale is True
    assert by_name["pipe_y"].is_stale is False


def test_check_all_stale_default_threshold(tmp_db: str) -> None:
    old = (NOW - timedelta(hours=48)).isoformat()
    _insert(tmp_db, "pipe_z", "ok", old)
    results = check_all_stale(tmp_db, [{"name": "pipe_z"}], default_threshold_hours=24.0, now=NOW)
    assert results[0].is_stale is True
