"""Integration tests for pipecheck.cli_rollup."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

import pytest
from click.testing import CliRunner

from pipecheck.cli_rollup import rollup_cmd
from pipecheck.history import init_db


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


def test_show_empty_db(tmp_db):
    runner = CliRunner()
    result = runner.invoke(rollup_cmd, ["show", "--db", tmp_db])
    assert result.exit_code == 0
    assert "No history data found" in result.output


def test_show_text_output(tmp_db):
    ts = datetime(2024, 6, 1, 11, 0, 0, tzinfo=timezone.utc).timestamp()
    _insert(tmp_db, "my_pipe", ts, "ok")
    runner = CliRunner()
    result = runner.invoke(rollup_cmd, ["show", "--db", tmp_db, "--period", "hourly"])
    assert result.exit_code == 0
    assert "my_pipe" in result.output
    assert "100.0%" in result.output


def test_show_json_output(tmp_db):
    ts = datetime(2024, 6, 1, 11, 0, 0, tzinfo=timezone.utc).timestamp()
    _insert(tmp_db, "pipe_j", ts, "fail")
    runner = CliRunner()
    result = runner.invoke(
        rollup_cmd, ["show", "--db", tmp_db, "--format", "json"]
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe_j"
    assert data[0]["failure"] == 1
    assert data[0]["success_rate"] == 0.0


def test_show_pipeline_filter(tmp_db):
    ts = datetime(2024, 6, 1, 11, 0, 0, tzinfo=timezone.utc).timestamp()
    _insert(tmp_db, "alpha", ts, "ok")
    _insert(tmp_db, "beta", ts, "ok")
    runner = CliRunner()
    result = runner.invoke(
        rollup_cmd, ["show", "--db", tmp_db, "--pipeline", "alpha", "--format", "json"]
    )
    data = json.loads(result.output)
    assert all(e["pipeline"] == "alpha" for e in data)


def test_show_daily_period(tmp_db):
    for hour in (6, 14):
        ts = datetime(2024, 6, 1, hour, 0, 0, tzinfo=timezone.utc).timestamp()
        _insert(tmp_db, "daily_pipe", ts, "ok")
    runner = CliRunner()
    result = runner.invoke(
        rollup_cmd,
        ["show", "--db", tmp_db, "--period", "daily", "--format", "json"],
    )
    data = json.loads(result.output)
    assert data[0]["total"] == 2
    assert data[0]["period"] == "daily"
