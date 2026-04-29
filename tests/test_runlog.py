"""Tests for pipecheck.runlog."""
from __future__ import annotations

import time

import pytest

from pipecheck.runlog import RunEvent, init_runlog_db, load_run_log, record_run


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "runlog.db")
    init_runlog_db(db)
    return db


def _event(pipeline="pipe_a", status="success", duration_ms=123, message="") -> RunEvent:
    return RunEvent(
        pipeline=pipeline,
        status=status,
        started_at=time.time(),
        duration_ms=duration_ms,
        message=message,
    )


def test_init_db_creates_file(tmp_path):
    db = str(tmp_path / "new.db")
    init_runlog_db(db)
    import os
    assert os.path.exists(db)


def test_empty_db_returns_empty_list(tmp_db):
    assert load_run_log(tmp_db) == []


def test_record_and_load_single_event(tmp_db):
    ev = _event()
    record_run(tmp_db, ev)
    results = load_run_log(tmp_db)
    assert len(results) == 1
    assert results[0].pipeline == "pipe_a"
    assert results[0].status == "success"
    assert results[0].duration_ms == 123


def test_load_returns_newest_first(tmp_db):
    for i in range(3):
        ev = RunEvent(pipeline="p", status="success", started_at=float(i), duration_ms=i)
        record_run(tmp_db, ev)
    results = load_run_log(tmp_db)
    assert results[0].started_at > results[1].started_at


def test_filter_by_pipeline(tmp_db):
    record_run(tmp_db, _event(pipeline="alpha"))
    record_run(tmp_db, _event(pipeline="beta"))
    record_run(tmp_db, _event(pipeline="alpha"))
    results = load_run_log(tmp_db, pipeline="alpha")
    assert all(r.pipeline == "alpha" for r in results)
    assert len(results) == 2


def test_limit_respected(tmp_db):
    for _ in range(10):
        record_run(tmp_db, _event())
    results = load_run_log(tmp_db, limit=4)
    assert len(results) == 4


def test_message_preserved(tmp_db):
    ev = _event(message="timeout after 30s")
    record_run(tmp_db, ev)
    results = load_run_log(tmp_db)
    assert results[0].message == "timeout after 30s"


def test_failure_status_stored(tmp_db):
    record_run(tmp_db, _event(status="failure"))
    results = load_run_log(tmp_db)
    assert results[0].status == "failure"
