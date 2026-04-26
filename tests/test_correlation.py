"""Tests for pipecheck.correlation — pairwise failure correlation."""

import sqlite3
import tempfile
import os
from datetime import datetime, timedelta

import pytest

from pipecheck.correlation import (
    CorrelationPair,
    as_dict,
    _bucket,
    compute_correlations,
    format_correlations,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db(tmp_path):
    db_path = str(tmp_path / "history.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE runs (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline  TEXT NOT NULL,
            status    TEXT NOT NULL,
            checked_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()
    return db_path


def _insert(db_path, pipeline, status, ts):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO runs (pipeline, status, checked_at) VALUES (?, ?, ?)",
        (pipeline, status, ts.isoformat()),
    )
    conn.commit()
    conn.close()


NOW = datetime(2024, 6, 1, 12, 0, 0)


# ---------------------------------------------------------------------------
# _bucket
# ---------------------------------------------------------------------------

def test_bucket_rounds_down_to_interval():
    ts = datetime(2024, 6, 1, 12, 7, 45)
    assert _bucket(ts, 5) == datetime(2024, 6, 1, 12, 5, 0)


def test_bucket_on_boundary():
    ts = datetime(2024, 6, 1, 12, 10, 0)
    assert _bucket(ts, 10) == datetime(2024, 6, 1, 12, 10, 0)


# ---------------------------------------------------------------------------
# compute_correlations — empty / insufficient data
# ---------------------------------------------------------------------------

def test_no_data_returns_empty(tmp_db):
    result = compute_correlations(tmp_db, window_hours=24)
    assert result == []


def test_single_pipeline_returns_empty(tmp_db):
    _insert(tmp_db, "pipe_a", "failure", NOW)
    result = compute_correlations(tmp_db, window_hours=24)
    assert result == []


# ---------------------------------------------------------------------------
# compute_correlations — correlated failures
# ---------------------------------------------------------------------------

def test_perfectly_correlated_pair(tmp_db):
    """Two pipelines failing in the same time buckets → high score."""
    for i in range(6):
        ts = NOW - timedelta(hours=i)
        _insert(tmp_db, "pipe_a", "failure", ts)
        _insert(tmp_db, "pipe_b", "failure", ts)

    results = compute_correlations(tmp_db, window_hours=24, bucket_minutes=60)
    assert len(results) == 1
    pair = results[0]
    assert pair.pipeline_a in ("pipe_a", "pipe_b")
    assert pair.pipeline_b in ("pipe_a", "pipe_b")
    assert pair.correlation >= 0.9


def test_uncorrelated_pipelines_low_score(tmp_db):
    """Pipelines that never fail together should have low / zero correlation."""
    # pipe_a fails in even buckets, pipe_b fails in odd buckets
    for i in range(8):
        ts = NOW - timedelta(hours=i)
        status_a = "failure" if i % 2 == 0 else "ok"
        status_b = "failure" if i % 2 != 0 else "ok"
        _insert(tmp_db, "pipe_a", status_a, ts)
        _insert(tmp_db, "pipe_b", status_b, ts)

    results = compute_correlations(tmp_db, window_hours=24, bucket_minutes=60)
    if results:
        assert results[0].correlation < 0.2


def test_results_sorted_by_correlation_descending(tmp_db):
    """Results should be ordered highest correlation first."""
    # pipe_a & pipe_b always fail together; pipe_a & pipe_c rarely overlap
    for i in range(8):
        ts = NOW - timedelta(hours=i)
        _insert(tmp_db, "pipe_a", "failure", ts)
        _insert(tmp_db, "pipe_b", "failure", ts)
        status_c = "failure" if i == 0 else "ok"
        _insert(tmp_db, "pipe_c", status_c, ts)

    results = compute_correlations(tmp_db, window_hours=24, bucket_minutes=60)
    scores = [r.correlation for r in results]
    assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# as_dict
# ---------------------------------------------------------------------------

def test_as_dict_keys():
    pair = CorrelationPair(pipeline_a="a", pipeline_b="b", correlation=0.75, shared_failures=3)
    d = as_dict(pair)
    assert set(d.keys()) == {"pipeline_a", "pipeline_b", "correlation", "shared_failures"}
    assert d["correlation"] == 0.75


# ---------------------------------------------------------------------------
# format_correlations
# ---------------------------------------------------------------------------

def test_format_correlations_empty():
    output = format_correlations([])
    assert "no" in output.lower() or output.strip() == ""


def test_format_correlations_contains_pipeline_names():
    pair = CorrelationPair(pipeline_a="ingest", pipeline_b="transform", correlation=0.88, shared_failures=5)
    output = format_correlations([pair])
    assert "ingest" in output
    assert "transform" in output
    assert "0.88" in output
