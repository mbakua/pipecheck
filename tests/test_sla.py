"""Tests for pipecheck.sla."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

import pytest

from pipecheck.history import init_db
from pipecheck.sla import SLAPolicy, SLAResult, check_sla, check_all_slas


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "history.db")
    init_db(db)
    return db


def _insert(db_path: str, pipeline: str, ok: bool, minutes_ago: float) -> None:
    ts = (datetime.utcnow() - timedelta(minutes=minutes_ago)).isoformat()
    con = sqlite3.connect(db_path)
    con.execute(
        "INSERT INTO results (pipeline, ok, status_code, checked_at) VALUES (?, ?, ?, ?)",
        (pipeline, int(ok), 200 if ok else 500, ts),
    )
    con.commit()
    con.close()


def test_no_history_is_breached(tmp_db):
    policy = SLAPolicy(pipeline="pipe-a", max_interval_minutes=30)
    result = check_sla(policy, tmp_db)
    assert result.breached is True
    assert result.last_success is None
    assert result.minutes_since_success is None


def test_recent_success_not_breached(tmp_db):
    _insert(tmp_db, "pipe-a", ok=True, minutes_ago=10)
    policy = SLAPolicy(pipeline="pipe-a", max_interval_minutes=30)
    result = check_sla(policy, tmp_db)
    assert result.breached is False
    assert result.minutes_since_success is not None
    assert result.minutes_since_success < 30


def test_old_success_is_breached(tmp_db):
    _insert(tmp_db, "pipe-a", ok=True, minutes_ago=90)
    policy = SLAPolicy(pipeline="pipe-a", max_interval_minutes=60)
    result = check_sla(policy, tmp_db)
    assert result.breached is True
    assert result.minutes_since_success > 60


def test_only_failures_counts_as_breached(tmp_db):
    _insert(tmp_db, "pipe-a", ok=False, minutes_ago=5)
    policy = SLAPolicy(pipeline="pipe-a", max_interval_minutes=30)
    result = check_sla(policy, tmp_db)
    assert result.breached is True
    assert result.last_success is None


def test_check_all_slas_returns_one_per_policy(tmp_db):
    _insert(tmp_db, "pipe-a", ok=True, minutes_ago=5)
    _insert(tmp_db, "pipe-b", ok=True, minutes_ago=120)
    policies = [
        SLAPolicy("pipe-a", 60),
        SLAPolicy("pipe-b", 60),
    ]
    results = check_all_slas(policies, tmp_db)
    assert len(results) == 2
    names = {r.pipeline for r in results}
    assert names == {"pipe-a", "pipe-b"}


def test_as_dict_structure(tmp_db):
    _insert(tmp_db, "pipe-a", ok=True, minutes_ago=5)
    policy = SLAPolicy("pipe-a", 60)
    d = check_sla(policy, tmp_db).as_dict()
    assert set(d.keys()) == {
        "pipeline",
        "max_interval_minutes",
        "last_success",
        "minutes_since_success",
        "breached",
    }
    assert d["pipeline"] == "pipe-a"
    assert d["breached"] is False


def test_custom_now_respected(tmp_db):
    """Passing an explicit 'now' allows deterministic testing."""
    anchor = datetime(2024, 1, 1, 12, 0, 0)
    _insert_at(tmp_db, "pipe-x", ok=True, ts=datetime(2024, 1, 1, 11, 0, 0))
    policy = SLAPolicy("pipe-x", 30)
    result = check_sla(policy, tmp_db, now=anchor)
    assert result.breached is True
    assert abs(result.minutes_since_success - 60.0) < 1.0


def _insert_at(db_path: str, pipeline: str, ok: bool, ts: datetime) -> None:
    con = sqlite3.connect(db_path)
    con.execute(
        "INSERT INTO results (pipeline, ok, status_code, checked_at) VALUES (?, ?, ?, ?)",
        (pipeline, int(ok), 200 if ok else 500, ts.isoformat()),
    )
    con.commit()
    con.close()
