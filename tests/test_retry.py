"""Tests for pipecheck.retry."""
from unittest.mock import MagicMock, patch

import pytest

from pipecheck.checks import CheckResult
from pipecheck.config import PipelineConfig
from pipecheck.retry import RetryPolicy, run_with_retry


def _pipeline(name: str = "pipe") -> PipelineConfig:
    return PipelineConfig(name=name, endpoint="http://example.com/health")


def _ok_result(pipeline):
    return CheckResult(pipeline=pipeline.name, ok=True, status_code=200, message="OK", latency_ms=10.0)


def _fail_result(pipeline):
    return CheckResult(pipeline=pipeline.name, ok=False, status_code=500, message="Error", latency_ms=5.0)


def test_success_on_first_attempt_no_sleep():
    p = _pipeline()
    sleep_mock = MagicMock()
    with patch("pipecheck.retry.run_check", return_value=_ok_result(p)):
        result = run_with_retry(p, RetryPolicy(attempts=3, delay=1.0), _sleep=sleep_mock)
    assert result.ok
    sleep_mock.assert_not_called()


def test_retries_on_failure_and_succeeds():
    p = _pipeline()
    sleep_mock = MagicMock()
    side_effects = [_fail_result(p), _ok_result(p)]
    with patch("pipecheck.retry.run_check", side_effect=side_effects):
        result = run_with_retry(p, RetryPolicy(attempts=3, delay=1.0, backoff=2.0), _sleep=sleep_mock)
    assert result.ok
    sleep_mock.assert_called_once_with(1.0)


def test_exhausts_all_attempts_returns_last_failure():
    p = _pipeline()
    sleep_mock = MagicMock()
    with patch("pipecheck.retry.run_check", return_value=_fail_result(p)):
        result = run_with_retry(p, RetryPolicy(attempts=3, delay=1.0, backoff=2.0), _sleep=sleep_mock)
    assert not result.ok
    assert sleep_mock.call_count == 2


def test_retry_attempt_recorded_in_extra():
    p = _pipeline()
    sleep_mock = MagicMock()
    side_effects = [_fail_result(p), _ok_result(p)]
    with patch("pipecheck.retry.run_check", side_effect=side_effects):
        result = run_with_retry(p, RetryPolicy(attempts=3, delay=0.0), _sleep=sleep_mock)
    assert result.extra.get("retry_attempt") == 1


def test_default_policy_used_when_none():
    p = _pipeline()
    sleep_mock = MagicMock()
    with patch("pipecheck.retry.run_check", return_value=_ok_result(p)):
        result = run_with_retry(p, _sleep=sleep_mock)
    assert result.ok


def test_backoff_increases_delay():
    p = _pipeline()
    delays = []
    with patch("pipecheck.retry.run_check", return_value=_fail_result(p)):
        run_with_retry(
            p,
            RetryPolicy(attempts=3, delay=1.0, backoff=3.0),
            _sleep=lambda d: delays.append(d),
        )
    assert delays == [1.0, 3.0]
