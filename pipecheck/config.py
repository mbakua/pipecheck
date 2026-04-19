"""Configuration loading and validation for pipecheck."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class PipelineConfig:
    name: str
    source: str
    schedule: str = "* * * * *"
    timeout_seconds: int = 300
    alert_on_failure: bool = True
    tags: list[str] = field(default_factory=list)


@dataclass
class AlertConfig:
    email: str | None = None
    slack_webhook: str | None = None
    pagerduty_key: str | None = None


@dataclass
class AppConfig:
    pipelines: list[PipelineConfig]
    alerts: AlertConfig = field(default_factory=AlertConfig)
    log_level: str = "INFO"


def load_config(path: str | Path) -> AppConfig:
    """Load and parse a YAML config file into an AppConfig."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open() as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    pipelines = [
        PipelineConfig(**p) for p in raw.get("pipelines", [])
    ]

    alert_data = raw.get("alerts", {})
    alerts = AlertConfig(
        email=alert_data.get("email"),
        slack_webhook=alert_data.get("slack_webhook") or os.getenv("PIPECHECK_SLACK_WEBHOOK"),
        pagerduty_key=alert_data.get("pagerduty_key") or os.getenv("PIPECHECK_PD_KEY"),
    )

    return AppConfig(
        pipelines=pipelines,
        alerts=alerts,
        log_level=raw.get("log_level", "INFO"),
    )
