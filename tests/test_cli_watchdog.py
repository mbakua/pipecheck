"""Tests for pipecheck.cli_watchdog."""
from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from pipecheck.cli_watchdog import watchdog_cmd
from pipecheck.history import init_db, save_results
from pipecheck.checks import CheckResult
from pipecheck.config import AppConfig, PipelineConfig, AlertConfig


def _make_config(tmp_path, names):
    """Write a minimal yaml config and return its path."""
    cfg_path = tmp_path / "pipecheck.yaml"
    pipelines = "\n".join(
        f"  - name: {n}\n    endpoint: http://example.com/{n}" for n in names
    )
    cfg_path.write_text(f"pipelines:\n{pipelines}\n")
    return str(cfg_path)


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    init_db(db)
    return db


def _ok(name):
    return CheckResult(pipeline=name, success=True, status_code=200, message="ok", latency=0.05)


def test_all_fresh_exits_zero(tmp_path, tmp_db):
    cfg = _make_config(tmp_path, ["alpha"])
    save_results([_ok("alpha")], tmp_db)
    runner = CliRunner()
    result = runner.invoke(
        watchdog_cmd,
        ["check", "--config", cfg, "--db", tmp_db, "--threshold", "99999"],
    )
    assert result.exit_code == 0
    assert "alpha" in result.output


def test_stale_pipeline_exits_nonzero(tmp_path, tmp_db):
    cfg = _make_config(tmp_path, ["beta"])
    # beta was never saved — always stale
    runner = CliRunner()
    result = runner.invoke(
        watchdog_cmd,
        ["check", "--config", cfg, "--db", tmp_db, "--threshold", "3600"],
    )
    assert result.exit_code == 1


def test_json_output_is_parseable(tmp_path, tmp_db):
    cfg = _make_config(tmp_path, ["gamma"])
    runner = CliRunner()
    result = runner.invoke(
        watchdog_cmd,
        ["check", "--config", cfg, "--db", tmp_db, "--threshold", "3600", "--format", "json"],
    )
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["pipeline"] == "gamma"
    assert data[0]["stale"] is True
