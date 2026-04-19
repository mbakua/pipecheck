"""Tests for pipecheck.baseline module."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipecheck.baseline import (
    compare_baseline,
    load_baseline,
    save_baseline,
)
from pipecheck.checks import CheckResult


def _result(name: str, ok: bool, code: int = 200) -> CheckResult:
    return CheckResult(pipeline=name, ok=ok, status_code=code, error=None)


@pytest.fixture()
def tmp_baseline(tmp_path: Path) -> Path:
    return tmp_path / "baseline.json"


def test_save_and_load_roundtrip(tmp_baseline: Path) -> None:
    results = [_result("pipe_a", True, 200), _result("pipe_b", False, 500)]
    save_baseline(results, tmp_baseline)
    loaded = load_baseline(tmp_baseline)
    assert loaded is not None
    assert loaded["pipe_a"].ok is True
    assert loaded["pipe_b"].expected_status == 500


def test_load_missing_returns_none(tmp_path: Path) -> None:
    assert load_baseline(tmp_path / "nope.json") is None


def test_compare_no_drift(tmp_baseline: Path) -> None:
    results = [_result("pipe_a", True)]
    save_baseline(results, tmp_baseline)
    drifts = compare_baseline(results, tmp_baseline)
    assert len(drifts) == 1
    assert drifts[0].drifted is False


def test_compare_detects_ok_change(tmp_baseline: Path) -> None:
    save_baseline([_result("pipe_a", True)], tmp_baseline)
    drifts = compare_baseline([_result("pipe_a", False)], tmp_baseline)
    assert drifts[0].drifted is True
    assert "ok changed" in drifts[0].reason


def test_compare_new_pipeline(tmp_baseline: Path) -> None:
    save_baseline([_result("pipe_a", True)], tmp_baseline)
    drifts = compare_baseline(
        [_result("pipe_a", True), _result("pipe_new", True)], tmp_baseline
    )
    new_drift = next(d for d in drifts if d.pipeline == "pipe_new")
    assert new_drift.drifted is True
    assert "not in baseline" in new_drift.reason


def test_compare_no_baseline_returns_empty(tmp_path: Path) -> None:
    drifts = compare_baseline([_result("pipe_a", True)], tmp_path / "missing.json")
    assert drifts == []
