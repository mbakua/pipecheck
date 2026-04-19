"""JSON Schema validation for pipecheck YAML config files."""

from __future__ import annotations

from typing import Any

import jsonschema

CONFIG_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["pipelines"],
    "additionalProperties": False,
    "properties": {
        "log_level": {"type": "string", "enum": ["DEBUG", "INFO", "WARNING", "ERROR"]},
        "alerts": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "email": {"type": "string", "format": "email"},
                "slack_webhook": {"type": "string"},
                "pagerduty_key": {"type": "string"},
            },
        },
        "pipelines": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["name", "source"],
                "additionalProperties": False,
                "properties": {
                    "name": {"type": "string"},
                    "source": {"type": "string"},
                    "schedule": {"type": "string"},
                    "timeout_seconds": {"type": "integer", "minimum": 1},
                    "alert_on_failure": {"type": "boolean"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                },
            },
        },
    },
}


def validate_config(raw: dict[str, Any]) -> list[str]:
    """Validate raw config dict against schema. Returns list of error messages."""
    validator = jsonschema.Draft7Validator(CONFIG_SCHEMA)
    errors = sorted(validator.iter_errors(raw), key=lambda e: list(e.path))
    return [f"{'.'.join(str(p) for p in e.path) or 'root'}: {e.message}" for e in errors]
