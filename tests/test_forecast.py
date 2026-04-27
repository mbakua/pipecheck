"""Tests for pipecheck.forecast."""
from __future__ import annotations

import sqlite3
import tempfile
import os
import pytest

from pipecheck.history import init_db
from pipecheck.forecast import compute_forecast, format_forecast, as_dict


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "history.db")
    init_db(db)
    return db


def _insert(db_path: str, pipeline: str, ok: bool, ts: int = 1_700_000_000):
    con = sqlite3.connect(db_path)
    con.execute(
        "INSERT INTO runs (pipeline, ok, status_code, message, checked_at) VALUES (?,?,?,?,?)",
        (pipeline, int(ok), 200 if ok else 500, "", ts),
    )
    con.commit()
    con.close()


def test_no_history_returns_none(tmp_db):
    result = compute_forecast(tmp_db, "missing_pipeline")
    assert result is None


def test_all_success_zero_failure_rate(tmp_db):
    for i in range(10):
        _insert(tmp_db, "pipe_a", ok=True, ts=1_700_000_000 + i)
    r = compute_forecast(tmp_db, "pipe_a", window=10, horizon=10)
    assert r is not None
    assert r.failure_rate == 0.0
    assert r.predicted_failures == 0
    assert r.window_size == 10


def test_all_failure_full_rate(tmp_db):
    for i in range(10):
        _insert(tmp_db, "pipe_b", ok=False, ts=1_700_000_000 + i)
    r = compute_forecast(tmp_db, "pipe_b", window=10, horizon=10)
    assert r is not None
    assert r.failure_rate == 1.0
    assert r.predicted_failures == 10


def test_mixed_failure_rate(tmp_db):
    for i in range(8):
        _insert(tmp_db, "pipe_c", ok=True, ts=1_700_000_000 + i)
    for i in range(2):
        _insert(tmp_db, "pipe_c", ok=False, ts=1_700_000_010 + i)
    r = compute_forecast(tmp_db, "pipe_c", window=20, horizon=10)
    assert r is not None
    assert abs(r.failure_rate - 0.2) < 1e-6
    assert r.predicted_failures == 2


def test_confidence_levels(tmp_db):
    for i in range(25):
        _insert(tmp_db, "pipe_d", ok=True, ts=1_700_000_000 + i)
    r = compute_forecast(tmp_db, "pipe_d", window=25, horizon=5)
    assert r.confidence == "high"

    for i in range(15):
        _insert(tmp_db, "pipe_e", ok=True, ts=1_700_000_000 + i)
    r2 = compute_forecast(tmp_db, "pipe_e", window=15, horizon=5)
    assert r2.confidence == "medium"

    for i in range(5):
        _insert(tmp_db, "pipe_f", ok=True, ts=1_700_000_000 + i)
    r3 = compute_forecast(tmp_db, "pipe_f", window=5, horizon=5)
    assert r3.confidence == "low"


def test_as_dict_keys(tmp_db):
    _insert(tmp_db, "pipe_g", ok=True)
    r = compute_forecast(tmp_db, "pipe_g", window=5, horizon=5)
    d = as_dict(r)
    assert set(d.keys()) == {
        "pipeline", "window_size", "failure_rate",
        "predicted_failures", "horizon", "confidence",
    }


def test_format_forecast_text(tmp_db):
    _insert(tmp_db, "pipe_h", ok=False)
    r = compute_forecast(tmp_db, "pipe_h", window=5, horizon=5)
    out = format_forecast([r], fmt="text")
    assert "pipe_h" in out
    assert "Failure Forecast" in out


def test_format_forecast_json(tmp_db):
    import json
    _insert(tmp_db, "pipe_i", ok=True)
    r = compute_forecast(tmp_db, "pipe_i", window=5, horizon=5)
    out = format_forecast([r], fmt="json")
    data = json.loads(out)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_i"


def test_format_forecast_empty():
    out = format_forecast([], fmt="text")
    assert "No forecast" in out
