"""Tests for pipecheck.heatmap."""
from __future__ import annotations

import sqlite3
import pytest
from datetime import datetime, timedelta

from pipecheck.heatmap import (
    HeatmapCell,
    compute_heatmap,
    format_heatmap,
    DAYS,
)


@pytest.fixture()
def tmp_db(tmp_path):
    db = tmp_path / "history.db"
    con = sqlite3.connect(str(db))
    con.execute(
        """
        CREATE TABLE results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline TEXT NOT NULL,
            status TEXT NOT NULL,
            checked_at TEXT NOT NULL
        )
        """
    )
    con.commit()
    con.close()
    return str(db)


def _insert(db_path: str, pipeline: str, status: str, ts: str) -> None:
    con = sqlite3.connect(db_path)
    con.execute(
        "INSERT INTO results (pipeline, status, checked_at) VALUES (?, ?, ?)",
        (pipeline, status, ts),
    )
    con.commit()
    con.close()


def test_empty_db_returns_empty(tmp_db):
    cells = compute_heatmap(tmp_db)
    assert cells == []


def test_single_success_row(tmp_db):
    # Use a fixed Monday 10:00 UTC timestamp within the last 30 days
    ts = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d") + " 10:00:00"
    _insert(tmp_db, "pipe_a", "ok", ts)
    cells = compute_heatmap(tmp_db)
    assert len(cells) == 1
    assert cells[0].total == 1
    assert cells[0].failures == 0
    assert cells[0].failure_rate() == 0.0


def test_failure_counted(tmp_db):
    ts = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d") + " 14:00:00"
    _insert(tmp_db, "pipe_a", "ok", ts)
    _insert(tmp_db, "pipe_a", "error", ts)
    cells = compute_heatmap(tmp_db)
    assert len(cells) == 1
    cell = cells[0]
    assert cell.total == 2
    assert cell.failures == 1
    assert cell.failure_rate() == pytest.approx(0.5)


def test_pipeline_filter(tmp_db):
    ts = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d") + " 08:00:00"
    _insert(tmp_db, "pipe_a", "ok", ts)
    _insert(tmp_db, "pipe_b", "error", ts)
    cells_a = compute_heatmap(tmp_db, pipeline="pipe_a")
    assert all(c.failures == 0 for c in cells_a)
    cells_b = compute_heatmap(tmp_db, pipeline="pipe_b")
    assert all(c.failures == c.total for c in cells_b)


def test_old_records_excluded(tmp_db):
    old_ts = (datetime.utcnow() - timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")
    _insert(tmp_db, "pipe_a", "error", old_ts)
    cells = compute_heatmap(tmp_db, lookback_days=30)
    assert cells == []


def test_as_dict_keys(tmp_db):
    ts = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d") + " 09:00:00"
    _insert(tmp_db, "pipe_a", "ok", ts)
    cell = compute_heatmap(tmp_db)[0]
    d = cell.as_dict()
    assert set(d.keys()) == {"day", "hour", "total", "failures", "failure_rate"}
    assert d["day"] in DAYS


def test_format_heatmap_empty():
    result = format_heatmap([])
    assert "No heatmap data" in result


def test_format_heatmap_contains_days(tmp_db):
    ts = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d") + " 12:00:00"
    _insert(tmp_db, "pipe_a", "ok", ts)
    cells = compute_heatmap(tmp_db)
    output = format_heatmap(cells)
    for day in DAYS:
        assert day in output
    assert "Legend" in output
