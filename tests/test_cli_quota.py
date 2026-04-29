"""Integration tests for pipecheck.cli_quota."""
from __future__ import annotations

import pytest
from click.testing import CliRunner

from pipecheck.cli_quota import quota_cmd
from pipecheck.quota import load_quota, record_check, save_quota, set_quota


@pytest.fixture()
def quota_file(tmp_path):
    return str(tmp_path / "quota.json")


def test_set_creates_rule(quota_file):
    runner = CliRunner()
    result = runner.invoke(
        quota_cmd,
        ["set", "my_pipe", "--window", "300", "--max", "5", "--file", quota_file],
    )
    assert result.exit_code == 0
    assert "my_pipe" in result.output
    entries = load_quota(quota_file)
    assert "my_pipe" in entries
    assert entries["my_pipe"].max_checks == 5


def test_list_empty(quota_file):
    runner = CliRunner()
    result = runner.invoke(quota_cmd, ["list", "--file", quota_file])
    assert result.exit_code == 0
    assert "No quota" in result.output


def test_list_shows_entries(quota_file):
    entries = {}
    set_quota("alpha", 60, 3, entries)
    save_quota(entries, quota_file)
    runner = CliRunner()
    result = runner.invoke(quota_cmd, ["list", "--file", quota_file])
    assert "alpha" in result.output


def test_remove_existing(quota_file):
    entries = {}
    set_quota("alpha", 60, 3, entries)
    save_quota(entries, quota_file)
    runner = CliRunner()
    result = runner.invoke(quota_cmd, ["remove", "alpha", "--file", quota_file])
    assert result.exit_code == 0
    assert load_quota(quota_file) == {}


def test_remove_nonexistent_exits_nonzero(quota_file):
    runner = CliRunner()
    result = runner.invoke(quota_cmd, ["remove", "ghost", "--file", quota_file])
    assert result.exit_code != 0


def test_status_ok(quota_file):
    entries = {}
    set_quota("pipe_a", 60, 10, entries)
    save_quota(entries, quota_file)
    runner = CliRunner()
    result = runner.invoke(quota_cmd, ["status", "pipe_a", "--file", quota_file])
    assert result.exit_code == 0
    assert "ok" in result.output


def test_status_exceeded(quota_file):
    entries = {}
    set_quota("pipe_a", 60, 2, entries)
    record_check("pipe_a", entries)
    record_check("pipe_a", entries)
    save_quota(entries, quota_file)
    runner = CliRunner()
    result = runner.invoke(quota_cmd, ["status", "pipe_a", "--file", quota_file])
    assert result.exit_code == 1
    assert "exceeded" in result.output
