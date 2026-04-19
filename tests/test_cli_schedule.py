"""Tests for pipecheck.cli_schedule."""
from datetime import datetime
from unittest.mock import patch, MagicMock

from pipecheck.cli_schedule import run_with_schedule
from pipecheck.checks import CheckResult


class _Cfg:
    def __init__(self, name, schedule=None, endpoint=None):
        self.name = name
        self.schedule = schedule
        self.endpoint = endpoint
        self.expected_status = 200
        self.timeout = 10
        self.tags = []


_NOON = datetime(2024, 1, 1, 12, 0)  # Monday noon UTC


def _ok_result(name):
    return CheckResult(pipeline=name, ok=True, status_code=200, message="ok")


def test_all_run_no_schedule():
    cfgs = [_Cfg("a"), _Cfg("b")]
    with patch("pipecheck.cli_schedule.run_check", side_effect=[_ok_result("a"), _ok_result("b")]):
        results, skipped = run_with_schedule(cfgs, now=_NOON)
    assert len(results) == 2
    assert skipped == []


def test_skips_outside_window():
    cfgs = [
        _Cfg("in-window", schedule={"start": "00:00", "end": "23:59"}),
        _Cfg("out-window", schedule={"start": "01:00", "end": "02:00"}),
    ]
    with patch("pipecheck.cli_schedule.run_check", return_value=_ok_result("in-window")) as mock_run:
        results, skipped = run_with_schedule(cfgs, now=_NOON)

    assert len(results) == 1
    assert results[0].pipeline == "in-window"
    assert skipped == ["out-window"]
    assert mock_run.call_count == 1


def test_verbose_prints_skip(capsys):
    cfgs = [_Cfg("p", schedule={"start": "01:00", "end": "02:00"})]
    with patch("pipecheck.cli_schedule.run_check"):
        run_with_schedule(cfgs, now=_NOON, verbose=True)
    captured = capsys.readouterr()
    assert "skipping" in captured.out
    assert "p" in captured.out
