"""Tests for pipecheck.scorecard"""
from __future__ import annotations

import sqlite3
import tempfile
import os
from datetime import datetime, timedelta

import pytest

from pipecheck.scorecard import compute_scorecard, format_scorecard, _grade, as_dict


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE results "
        "(id INTEGER PRIMARY KEY, pipeline TEXT, status TEXT, detail TEXT, checked_at TEXT)"
    )
    conn.commit()
    conn.close()
    return db


def _insert(db: str, pipeline: str, status: str, ago_hours: float = 1.0):
    ts = (datetime.utcnow() - timedelta(hours=ago_hours)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO results (pipeline, status, detail, checked_at) VALUES (?,?,?,?)",
        (pipeline, status, "", ts),
    )
    conn.commit()
    conn.close()


def test_empty_db_returns_empty(tmp_db):
    assert compute_scorecard(tmp_db) == []


def test_all_success_score_100(tmp_db):
    for _ in range(5):
        _insert(tmp_db, "pipe_a", "ok")
    entries = compute_scorecard(tmp_db)
    assert len(entries) == 1
    e = entries[0]
    assert e.score == 100.0
    assert e.grade == "A"
    assert e.successes == 5
    assert e.failures == 0


def test_all_failure_score_zero(tmp_db):
    for _ in range(3):
        _insert(tmp_db, "pipe_b", "error")
    entries = compute_scorecard(tmp_db)
    assert entries[0].score == 0.0
    assert entries[0].grade == "F"


def test_mixed_score(tmp_db):
    for _ in range(3):
        _insert(tmp_db, "pipe_c", "ok")
    _insert(tmp_db, "pipe_c", "error")
    entries = compute_scorecard(tmp_db)
    assert abs(entries[0].score - 75.0) < 0.01
    assert entries[0].grade == "B"


def test_excludes_old_records(tmp_db):
    _insert(tmp_db, "pipe_d", "ok", ago_hours=0.5)
    _insert(tmp_db, "pipe_d", "error", ago_hours=48.0)  # outside 24h window
    entries = compute_scorecard(tmp_db, hours=24)
    assert entries[0].total == 1
    assert entries[0].successes == 1


def test_pipeline_filter(tmp_db):
    _insert(tmp_db, "pipe_e", "ok")
    _insert(tmp_db, "pipe_f", "error")
    entries = compute_scorecard(tmp_db, pipelines=["pipe_e"])
    assert len(entries) == 1
    assert entries[0].pipeline == "pipe_e"


def test_grade_boundaries():
    assert _grade(95) == "A"
    assert _grade(90) == "A"
    assert _grade(89) == "B"
    assert _grade(75) == "B"
    assert _grade(74) == "C"
    assert _grade(60) == "C"
    assert _grade(59) == "D"
    assert _grade(40) == "D"
    assert _grade(39) == "F"


def test_format_text_contains_headers(tmp_db):
    _insert(tmp_db, "my_pipeline", "ok")
    entries = compute_scorecard(tmp_db)
    text = format_scorecard(entries, fmt="text")
    assert "Pipeline" in text
    assert "Grade" in text
    assert "my_pipeline" in text


def test_format_json_is_valid(tmp_db):
    import json
    _insert(tmp_db, "json_pipe", "ok")
    entries = compute_scorecard(tmp_db)
    data = json.loads(format_scorecard(entries, fmt="json"))
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "json_pipe"
    assert "score" in data[0]
    assert "grade" in data[0]


def test_as_dict_keys(tmp_db):
    _insert(tmp_db, "dict_pipe", "ok")
    entries = compute_scorecard(tmp_db)
    d = as_dict(entries[0])
    assert set(d.keys()) == {"pipeline", "total", "successes", "failures", "score", "grade"}
