"""Integration tests for the forecast CLI sub-command."""
from __future__ import annotations

import sqlite3
import pytest
from click.testing import CliRunner

from pipecheck.cli_forecast import forecast_cmd
from pipecheck.history import init_db


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


def _make_config(tmp_path, names):
    lines = ["pipelines:"]
    for n in names:
        lines += [f"  - name: {n}", f"    endpoint: http://example.com/{n}"]
    cfg = tmp_path / "pipecheck.yaml"
    cfg.write_text("\n".join(lines))
    return str(cfg)


def test_show_no_history_prints_nothing_useful(tmp_path, tmp_db):
    cfg = _make_config(tmp_path, ["alpha"])
    runner = CliRunner()
    result = runner.invoke(
        forecast_cmd, ["show", "--config", cfg, "--db", tmp_db]
    )
    assert result.exit_code == 0
    assert "No forecast" in result.output


def test_show_with_history(tmp_path, tmp_db):
    cfg = _make_config(tmp_path, ["alpha"])
    for i in range(5):
        _insert(tmp_db, "alpha", ok=(i % 2 == 0), ts=1_700_000_000 + i)
    runner = CliRunner()
    result = runner.invoke(
        forecast_cmd, ["show", "--config", cfg, "--db", tmp_db]
    )
    assert result.exit_code == 0
    assert "alpha" in result.output


def test_show_json_format(tmp_path, tmp_db):
    import json
    cfg = _make_config(tmp_path, ["beta"])
    for i in range(5):
        _insert(tmp_db, "beta", ok=True, ts=1_700_000_000 + i)
    runner = CliRunner()
    result = runner.invoke(
        forecast_cmd, ["show", "--config", cfg, "--db", tmp_db, "--format", "json"]
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert any(d["pipeline"] == "beta" for d in data)


def test_missing_config_exits_1(tmp_path, tmp_db):
    runner = CliRunner()
    result = runner.invoke(
        forecast_cmd,
        ["show", "--config", str(tmp_path / "nope.yaml"), "--db", tmp_db],
    )
    assert result.exit_code == 1


def test_unknown_pipeline_filter_exits_1(tmp_path, tmp_db):
    cfg = _make_config(tmp_path, ["gamma"])
    runner = CliRunner()
    result = runner.invoke(
        forecast_cmd,
        ["show", "--config", cfg, "--db", tmp_db, "--pipeline", "nonexistent"],
    )
    assert result.exit_code == 1
