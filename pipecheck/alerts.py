"""Alerting logic for failed pipeline checks."""
from __future__ import annotations

import logging
from typing import List

import requests

from pipecheck.alerts import CheckResult  # re-exported for convenience
from pipecheck.checks import CheckResult
from pipecheck.config import AlertConfig

logger = logging.getLogger(__name__)


def _send_slack(webhook_url: str, message: str) -> bool:
    try:
        resp = requests.post(webhook_url, json={"text": message}, timeout=5)
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.error("Slack alert failed: %s", exc)
        return False


def _send_webhook(url: str, payload: dict) -> bool:
    try:
        resp = requests.post(url, json=payload, timeout=5)
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.error("Webhook alert failed: %s", exc)
        return False


def dispatch_alerts(results: List[CheckResult], alert_config: AlertConfig) -> None:
    """Send alerts for any failed checks according to alert configuration."""
    failures = [r for r in results if not r.success]
    if not failures:
        logger.debug("All checks passed — no alerts dispatched.")
        return

    for result in failures:
        message = f"pipecheck ALERT: {result.summary()}"
        logger.warning(message)

        if alert_config.slack_webhook:
            _send_slack(alert_config.slack_webhook, message)

        if alert_config.webhook_url:
            payload = {
                "pipeline": result.pipeline_name,
                "success": result.success,
                "error": result.error,
                "latency_ms": result.latency_ms,
                "checked_at": result.checked_at.isoformat(),
            }
            _send_webhook(alert_config.webhook_url, payload)
