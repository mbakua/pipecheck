"""Tests for pipecheck.rollup."""
from __future__ import annotations

import time
import sqlite3
from datetime import datetime, timezone

import pytest

from pipecheck.history import init_db
from pipecheck.rollup import RollupEntry, _bucket_ts, compute_rollup


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "hist.db")
    init_db(db)
    return db


def _insert(db_path: str, pipeline: str, ts: float, status: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO checks (pipeline, checked_at, status, detail) VALUES (?,?,?,?)",
        (pipeline, ts, status, ""),
    )
    conn.commit()
    conn.close()


# ── _bucket_ts ────────────────────────────────────────────────────────────────

def test_bucket_ts_hourly():
    # 2024-06-01 14:37:00 UTC
    ts = datetime(2024, 6, 1, 14, 37, 0, tzinfo=timezone.utc).timestamp()
    assert _bucket_ts(ts, "hourly") == "2024-06-01T14:00:00Z"


def test_bucket_ts_daily():
    ts = datetime(2024, 6, 1, 14, 37, 0, tzinfo=timezone.utc).timestamp()
    assert _bucket_ts(ts, "daily") == "2024-06-01T00:00:00Z"


# ── compute_rollup ────────────────────────────────────────────────────────────

def test_empty_db_returns_empty(tmp_db):
    result = compute_rollup(tmp_db, period="hourly")
    assert result == []


def test_single_success(tmp_db):
    ts = datetime(2024, 6, 1, 10, 5, 0, tzinfo=timezone.utc).timestamp()
    _insert(tmp_db, "pipe_a", ts, "ok")
    entries = compute_rollup(tmp_db, period="hourly")
    assert len(entries) == 1
    e = entries[0]
    assert e.pipeline == "pipe_a"
    assert e.total == 1
    assert e.success == 1
    assert e.failure == 0
    assert e.success_rate == 1.0


def test_mixed_results_aggregate(tmp_db):
    base = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc).timestamp()
    for i, status in enumerate(["ok", "ok", "fail", "ok"]):
        _insert(tmp_db, "pipe_b", base + i * 60, status)
    entries = compute_rollup(tmp_db, period="hourly", pipeline="pipe_b")
    assert len(entries) == 1
    e = entries[0]
    assert e.total == 4
    assert e.success == 3
    assert e.failure == 1
    assert abs(e.success_rate - 0.75) < 1e-6


def test_daily_groups_across_hours(tmp_db):
    d = datetime(2024, 6, 1, tzinfo=timezone.utc)
    for hour in (8, 12, 20):
        ts = d.replace(hour=hour).timestamp()
        _insert(tmp_db, "pipe_c", ts, "ok")
    entries = compute_rollup(tmp_db, period="daily", pipeline="pipe_c")
    assert len(entries) == 1
    assert entries[0].total == 3


def test_pipeline_filter(tmp_db):
    ts = datetime(2024, 6, 1, 9, 0, 0, tzinfo=timezone.utc).timestamp()
    _insert(tmp_db, "alpha", ts, "ok")
    _insert(tmp_db, "beta", ts, "fail")
    entries = compute_rollup(tmp_db, period="hourly", pipeline="alpha")
    assert all(e.pipeline == "alpha" for e in entries)


def test_invalid_period_raises(tmp_db):
    with pytest.raises(ValueError, match="Unknown period"):
        compute_rollup(tmp_db, period="weekly")


def test_multiple_pipelines_sorted(tmp_db):
    ts = datetime(2024, 6, 1, 9, 0, 0, tzinfo=timezone.utc).timestamp()
    for name in ("z_pipe", "a_pipe", "m_pipe"):
        _insert(tmp_db, name, ts, "ok")
    entries = compute_rollup(tmp_db, period="hourly")
    names = [e.pipeline for e in entries]
    assert names == sorted(names)
