"""Tests for pipecheck.trend."""
import pytest
from pipecheck.history import init_db, save_results
from pipecheck.checks import CheckResult
from pipecheck.trend import compute_trend, as_dict


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "history.db")
    init_db(db)
    return db


def _save(db, pipeline, status, n=1):
    results = [CheckResult(pipeline=pipeline, status=status, detail="", latency_ms=10.0)]
    for _ in range(n):
        save_results(db, results)


def test_no_history_returns_none(tmp_db):
    assert compute_trend("missing", tmp_db) is None


def test_all_success(tmp_db):
    _save(tmp_db, "pipe_a", "ok", n=5)
    t = compute_trend("pipe_a", tmp_db)
    assert t is not None
    assert t.success_count == 5
    assert t.failure_count == 0
    assert t.success_rate == 1.0
    assert t.last_status == "ok"
    assert t.flapping is False


def test_all_failure(tmp_db):
    _save(tmp_db, "pipe_b", "error", n=4)
    t = compute_trend("pipe_b", tmp_db)
    assert t.success_rate == 0.0
    assert t.failure_count == 4


def test_mixed_success_rate(tmp_db):
    _save(tmp_db, "pipe_c", "ok", n=3)
    _save(tmp_db, "pipe_c", "error", n=1)
    t = compute_trend("pipe_c", tmp_db, limit=4)
    assert t.total_runs == 4
    assert 0.0 < t.success_rate < 1.0


def test_flapping_detected(tmp_db):
    # Alternate ok/error so last 3 differ
    for status in ["ok", "error", "ok"]:
        _save(tmp_db, "pipe_d", status, n=1)
    t = compute_trend("pipe_d", tmp_db, limit=10)
    assert t.flapping is True


def test_as_dict_keys(tmp_db):
    _save(tmp_db, "pipe_e", "ok", n=2)
    t = compute_trend("pipe_e", tmp_db)
    d = as_dict(t)
    assert set(d.keys()) == {
        "pipeline", "total_runs", "success_count",
        "failure_count", "success_rate", "last_status", "flapping",
    }
