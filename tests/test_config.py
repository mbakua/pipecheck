"""Tests for config loading and schema validation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipecheck.config import load_config, AppConfig, PipelineConfig
from pipecheck.config_schema import validate_config

EXAMPLE_CONFIG = Path("pipecheck/example_config.yaml")


def test_load_example_config():
    cfg = load_config(EXAMPLE_CONFIG)
    assert isinstance(cfg, AppConfig)
    assert len(cfg.pipelines) == 3
    assert cfg.log_level == "INFO"


def test_pipeline_defaults(tmp_path):
    raw = {"pipelines": [{"name": "test", "source": "s3://bucket/"}]}
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.dump(raw))
    cfg = load_config(config_file)
    p: PipelineConfig = cfg.pipelines[0]
    assert p.schedule == "* * * * *"
    assert p.timeout_seconds == 300
    assert p.alert_on_failure is True
    assert p.tags == []


def test_missing_config_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.yaml")


def test_validate_valid_config():
    with EXAMPLE_CONFIG.open() as f:
        raw = yaml.safe_load(f)
    errors = validate_config(raw)
    assert errors == []


def test_validate_missing_required_field():
    raw = {"pipelines": [{"name": "broken"}]}
    errors = validate_config(raw)
    assert any("source" in e for e in errors)


def test_validate_empty_pipelines():
    raw = {"pipelines": []}
    errors = validate_config(raw)
    assert any("minItems" in e or "1" in e for e in errors)


def test_validate_invalid_log_level():
    raw = {
        "pipelines": [{"name": "p", "source": "s3://x"}],
        "log_level": "VERBOSE",
    }
    errors = validate_config(raw)
    assert any("log_level" in e for e in errors)
