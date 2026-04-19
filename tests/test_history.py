"""Tests for pipecheck.history and pipecheck.trend."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from pipecheck.checks import CheckResult
from pipecheck.history import init_db, save_results, load_history
from pipecheck.trend import compute_trend


@pytest.fixture()
def tmp_db(tmp_path: Path) -> Path:
    return tmp_path / "test_history.db"


def _result(pipeline: str, ok: bool, status: int = 200, latency: float = 0.1) -> CheckResult:
    return CheckResult(pipeline=pipeline, ok=ok, status=status, latency=latency, message="")


def test_init_db_creates_file(tmp_db):
    init_db(tmp_db)
    assert tmp_db.exists()


def test_save_and_load_results(tmp_db):
    results = [_result("pipe_a", True), _result("pipe_b", False, 500)]
    save_results(results, db_path=tmp_db)

    rows_a = load_history("pipe_a", db_path=tmp_db)
    assert len(rows_a) == 1
    assert rows_a[0]["ok"] == 1

    rows_b = load_history("pipe_b", db_path=tmp_db)
    assert rows_b[0]["status"] == 500


def test_load_history_limit(tmp_db):
    results = [_result("pipe_a", True)] * 10
    save_results(results, db_path=tmp_db)
    rows = load_history("pipe_a", limit=3, db_path=tmp_db)
    assert len(rows) == 3


def test_load_history_empty(tmp_db):
    rows = load_history("nonexistent", db_path=tmp_db)
    assert rows == []


# --- trend tests ---

def test_compute_trend_empty():
    t = compute_trend("p", [])
    assert t.total == 0
    assert t.last_status == "unknown"
    assert t.avg_latency is None


def test_compute_trend_success_rate():
    history = [
        {"ok": 1, "latency": 0.2, "status": 200},
        {"ok": 1, "latency": 0.4, "status": 200},
        {"ok": 0, "latency": None, "status": 500},
    ]
    t = compute_trend("pipe", history)
    assert t.total == 3
    assert abs(t.success_rate - 2 / 3) < 1e-6
    assert abs(t.avg_latency - 0.3) < 1e-6
    assert t.last_status == "ok"  # history[0]["ok"] == 1


def test_trend_as_dict_keys():
    history = [{"ok": 0, "latency": 0.5, "status": 500}]
    d = compute_trend("p", history).as_dict()
    assert set(d.keys()) == {"pipeline", "total", "success_rate", "avg_latency", "last_status"}
