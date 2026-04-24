"""Tests for pipecheck.digest."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

import pytest

from pipecheck.history import init_db
from pipecheck.digest import build_digest, format_digest, DigestEntry


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "hist.db")
    init_db(db)
    return db


def _insert(db_path, pipeline, ok, minutes_ago=5):
    ts = (datetime.utcnow() - timedelta(minutes=minutes_ago)).isoformat()
    con = sqlite3.connect(db_path)
    con.execute(
        "INSERT INTO runs (pipeline, ok, status_code, message, checked_at) VALUES (?,?,?,?,?)",
        (pipeline, int(ok), 200 if ok else 500, "", ts),
    )
    con.commit()
    con.close()


def test_build_digest_empty(tmp_db):
    entries = build_digest(tmp_db, ["pipe-a"], hours=24)
    assert len(entries) == 1
    e = entries[0]
    assert e.pipeline == "pipe-a"
    assert e.total_runs == 0
    assert e.success_rate == 0.0


def test_build_digest_counts(tmp_db):
    for _ in range(3):
        _insert(tmp_db, "pipe-a", ok=True)
    _insert(tmp_db, "pipe-a", ok=False)

    entries = build_digest(tmp_db, ["pipe-a"], hours=24)
    e = entries[0]
    assert e.total_runs == 4
    assert e.success_runs == 3
    assert e.failure_runs == 1
    assert e.success_rate == 75.0


def test_build_digest_excludes_old(tmp_db):
    _insert(tmp_db, "pipe-b", ok=True, minutes_ago=120 * 60)  # 5 days ago
    entries = build_digest(tmp_db, ["pipe-b"], hours=24)
    assert entries[0].total_runs == 0


def test_build_digest_multiple_pipelines(tmp_db):
    """Entries are returned in the same order as the requested pipeline list."""
    _insert(tmp_db, "pipe-x", ok=True)
    _insert(tmp_db, "pipe-y", ok=False)

    entries = build_digest(tmp_db, ["pipe-x", "pipe-y"], hours=24)
    assert len(entries) == 2
    assert entries[0].pipeline == "pipe-x"
    assert entries[1].pipeline == "pipe-y"
    assert entries[0].success_runs == 1
    assert entries[1].failure_runs == 1


def test_format_digest_text():
    entries = [
        DigestEntry("alpha", 10, 9, 1, 90.0, "stable", False),
        DigestEntry("beta", 4, 2, 2, 50.0, "degrading", True),
    ]
    text = format_digest(entries, hours=12)
    assert "12h" in text
    assert "alpha" in text
    assert "90.0%" in text
    assert "[FLAPPING]" in text
    assert "degrading" in text


def test_format_digest_no_entries():
    text = format_digest([], hours=24)
    assert "No pipelines" in text
