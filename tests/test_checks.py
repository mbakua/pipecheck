"""Tests for pipeline health checks."""
from unittest.mock import MagicMock, patch

import pytest

from pipecheck.checks import CheckResult, run_check, run_all_checks
from pipecheck.config import PipelineConfig


def _make_pipeline(name="test", endpoint="http://example.com/health", expected_status=200):
    return PipelineConfig(name=name, endpoint=endpoint, expected_status_code=expected_status)


def test_check_success():
    pipeline = _make_pipeline()
    mock_resp = MagicMock(status_code=200)
    with patch("pipecheck.checks.requests.get", return_value=mock_resp):
        result = run_check(pipeline)
    assert result.success is True
    assert result.status_code == 200
    assert result.error is None
    assert result.latency_ms is not None


def test_check_wrong_status():
    pipeline = _make_pipeline(expected_status=200)
    mock_resp = MagicMock(status_code=503)
    with patch("pipecheck.checks.requests.get", return_value=mock_resp):
        result = run_check(pipeline)
    assert result.success is False
    assert "503" in result.error


def test_check_request_exception():
    import requests as req
    pipeline = _make_pipeline()
    with patch("pipecheck.checks.requests.get", side_effect=req.ConnectionError("refused")):
        result = run_check(pipeline)
    assert result.success is False
    assert result.error is not None


def test_check_no_endpoint():
    pipeline = PipelineConfig(name="no-ep", endpoint=None)
    result = run_check(pipeline)
    assert result.success is False
    assert "No endpoint" in result.error


def test_run_all_checks():
    pipelines = [_make_pipeline(name=f"p{i}") for i in range(3)]
    mock_resp = MagicMock(status_code=200)
    with patch("pipecheck.checks.requests.get", return_value=mock_resp):
        results = run_all_checks(pipelines)
    assert len(results) == 3
    assert all(r.success for r in results)


def test_check_result_summary_ok():
    r = CheckResult(pipeline_name="mypipe", success=True, latency_ms=42.5)
    assert "OK" in r.summary()
    assert "mypipe" in r.summary()


def test_check_result_summary_fail():
    r = CheckResult(pipeline_name="mypipe", success=False, error="timeout")
    assert "FAIL" in r.summary()
    assert "timeout" in r.summary()
