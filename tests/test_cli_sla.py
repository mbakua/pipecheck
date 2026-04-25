"""Integration tests for the 'pipecheck sla check' CLI command."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pipecheck.cli_sla import sla_cmd
from pipecheck.history import init_db


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "history.db")
    init_db(db)
    return db


def _insert(db_path, pipeline, ok, minutes_ago):
    ts = (datetime.utcnow() - timedelta(minutes=minutes_ago)).isoformat()
    con = sqlite3.connect(db_path)
    con.execute(
        "INSERT INTO results (pipeline, ok, status_code, checked_at) VALUES (?, ?, ?, ?)",
        (pipeline, int(ok), 200, ts),
    )
    con.commit()
    con.close()


def _make_config(pipelines):
    cfg = MagicMock()
    cfg.pipelines = pipelines
    return cfg


def _pipeline(name, sla_minutes=None):
    p = MagicMock()
    p.name = name
    p.sla_minutes = sla_minutes
    return p


def test_no_sla_policies_exits_cleanly(tmp_db, tmp_path):
    cfg = _make_config([_pipeline("pipe-a", sla_minutes=None)])
    runner = CliRunner()
    with patch("pipecheck.cli_sla.load_config", return_value=cfg):
        result = runner.invoke(sla_cmd, ["check", "--db", tmp_db])
    assert result.exit_code == 0
    assert "No SLA" in result.output


def test_passing_sla_exits_zero(tmp_db):
    _insert(tmp_db, "pipe-a", ok=True, minutes_ago=5)
    cfg = _make_config([_pipeline("pipe-a", sla_minutes=30)])
    runner = CliRunner()
    with patch("pipecheck.cli_sla.load_config", return_value=cfg):
        result = runner.invoke(sla_cmd, ["check", "--db", tmp_db])
    assert result.exit_code == 0
    assert "pipe-a" in result.output


def test_breached_sla_exits_nonzero(tmp_db):
    _insert(tmp_db, "pipe-a", ok=True, minutes_ago=120)
    cfg = _make_config([_pipeline("pipe-a", sla_minutes=30)])
    runner = CliRunner()
    with patch("pipecheck.cli_sla.load_config", return_value=cfg):
        result = runner.invoke(sla_cmd, ["check", "--db", tmp_db])
    assert result.exit_code == 1
    assert "breach" in result.output.lower()


def test_json_output_parseable(tmp_db):
    _insert(tmp_db, "pipe-a", ok=True, minutes_ago=5)
    cfg = _make_config([_pipeline("pipe-a", sla_minutes=60)])
    runner = CliRunner()
    with patch("pipecheck.cli_sla.load_config", return_value=cfg):
        result = runner.invoke(sla_cmd, ["check", "--db", tmp_db, "--format", "json"])
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "pipe-a"
