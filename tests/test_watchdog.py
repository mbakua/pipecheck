"""Tests for pipecheck.watchdog."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipecheck.history import init_db, save_results
from pipecheck.checks import CheckResult
from pipecheck.watchdog import check_watchdog, WatchdogResult


@pytest.fixture()
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    init_db(db)
    return db


def _result(name: str, ok: bool) -> CheckResult:
    return CheckResult(
        pipeline=name,
        success=ok,
        status_code=200 if ok else 500,
        message="ok" if ok else "fail",
        latency=0.1,
    )


_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_never_checked_is_stale(tmp_db):
    results = check_watchdog(["pipe_a"], 3600.0, tmp_db, now=_NOW)
    assert len(results) == 1
    r = results[0]
    assert r.pipeline == "pipe_a"
    assert r.last_checked is None
    assert r.silence_seconds is None
    assert r.stale is True


def test_recent_check_not_stale(tmp_db):
    save_results([_result("pipe_a", True)], tmp_db)
    # Pretend 'now' is 10 minutes after the save — well within 1-hour threshold
    from datetime import timedelta
    now = datetime.now(tz=timezone.utc) + timedelta(minutes=10)
    results = check_watchdog(["pipe_a"], 3600.0, tmp_db, now=now)
    assert results[0].stale is False
    assert results[0].silence_seconds is not None
    assert results[0].silence_seconds < 3600


def test_old_check_is_stale(tmp_db):
    save_results([_result("pipe_b", True)], tmp_db)
    from datetime import timedelta
    now = datetime.now(tz=timezone.utc) + timedelta(hours=2)
    results = check_watchdog(["pipe_b"], 3600.0, tmp_db, now=now)
    assert results[0].stale is True
    assert results[0].silence_seconds > 3600


def test_multiple_pipelines_mixed(tmp_db):
    save_results([_result("fresh", True)], tmp_db)
    from datetime import timedelta
    now = datetime.now(tz=timezone.utc) + timedelta(minutes=5)
    results = check_watchdog(["fresh", "never"], 3600.0, tmp_db, now=now)
    by_name = {r.pipeline: r for r in results}
    assert by_name["fresh"].stale is False
    assert by_name["never"].stale is True


def test_as_dict_structure(tmp_db):
    from pipecheck.watchdog import as_dict
    results = check_watchdog(["p"], 60.0, tmp_db, now=_NOW)
    d = as_dict(results[0])
    assert set(d.keys()) == {"pipeline", "last_checked", "silence_seconds", "threshold_seconds", "stale"}
    assert d["pipeline"] == "p"
    assert d["last_checked"] is None
    assert d["stale"] is True
