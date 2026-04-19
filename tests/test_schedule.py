"""Tests for pipecheck.schedule."""
from datetime import datetime, time

import pytest

from pipecheck.schedule import ScheduleWindow, parse_schedule, should_run


# ---------------------------------------------------------------------------
# ScheduleWindow.is_active
# ---------------------------------------------------------------------------

def test_inside_window():
    w = ScheduleWindow(start=time(8, 0), end=time(20, 0))
    assert w.is_active(datetime(2024, 1, 1, 12, 0))  # Monday noon


def test_outside_window():
    w = ScheduleWindow(start=time(8, 0), end=time(20, 0))
    assert not w.is_active(datetime(2024, 1, 1, 21, 0))


def test_overnight_window_inside():
    w = ScheduleWindow(start=time(22, 0), end=time(6, 0))
    assert w.is_active(datetime(2024, 1, 1, 23, 30))
    assert w.is_active(datetime(2024, 1, 1, 5, 0))


def test_overnight_window_outside():
    w = ScheduleWindow(start=time(22, 0), end=time(6, 0))
    assert not w.is_active(datetime(2024, 1, 1, 12, 0))


def test_day_filter_active():
    # weekday() for 2024-01-01 is 0 (Monday)
    w = ScheduleWindow(start=time(0, 0), end=time(23, 59), days=[0])
    assert w.is_active(datetime(2024, 1, 1, 10, 0))


def test_day_filter_inactive():
    w = ScheduleWindow(start=time(0, 0), end=time(23, 59), days=[2, 3])  # Wed/Thu
    assert not w.is_active(datetime(2024, 1, 1, 10, 0))  # Monday


# ---------------------------------------------------------------------------
# parse_schedule
# ---------------------------------------------------------------------------

def test_parse_schedule_basic():
    w = parse_schedule({"start": "09:00", "end": "17:30"})
    assert w.start == time(9, 0)
    assert w.end == time(17, 30)
    assert w.days == list(range(7))


def test_parse_schedule_with_days():
    w = parse_schedule({"start": "00:00", "end": "23:59", "days": [5, 6]})
    assert w.days == [5, 6]


# ---------------------------------------------------------------------------
# should_run
# ---------------------------------------------------------------------------

class _Cfg:
    def __init__(self, schedule=None):
        self.name = "test"
        self.schedule = schedule


def test_should_run_no_schedule():
    assert should_run(_Cfg()) is True


def test_should_run_inside():
    cfg = _Cfg(schedule={"start": "00:00", "end": "23:59"})
    assert should_run(cfg, datetime(2024, 1, 1, 12, 0)) is True


def test_should_run_outside():
    cfg = _Cfg(schedule={"start": "08:00", "end": "09:00"})
    assert should_run(cfg, datetime(2024, 1, 1, 22, 0)) is False
