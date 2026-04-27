"""Tests for pipecheck.suppression."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipecheck.suppression import (
    SuppressionRule,
    add_rule,
    is_suppressed,
    load_rules,
    remove_rule,
    save_rules,
)


@pytest.fixture()
def rules_file(tmp_path) -> Path:
    return tmp_path / "suppressions.json"


def test_load_rules_missing_file_returns_empty(rules_file):
    assert load_rules(rules_file) == []


def test_add_rule_persists(rules_file):
    rule = add_rule("orders_.*", reason="maintenance", path=rules_file)
    assert rule.pattern == "orders_.*"
    loaded = load_rules(rules_file)
    assert len(loaded) == 1
    assert loaded[0].reason == "maintenance"


def test_add_multiple_rules(rules_file):
    add_rule("pipe_a", reason="r1", path=rules_file)
    add_rule("pipe_b", reason="r2", path=rules_file)
    assert len(load_rules(rules_file)) == 2


def test_remove_existing_rule(rules_file):
    add_rule("pipe_x", reason="test", path=rules_file)
    removed = remove_rule("pipe_x", path=rules_file)
    assert removed is True
    assert load_rules(rules_file) == []


def test_remove_nonexistent_rule_returns_false(rules_file):
    assert remove_rule("no_such", path=rules_file) is False


def test_is_suppressed_matches_pattern(rules_file):
    add_rule("orders_.*", reason="maint", path=rules_file)
    result = is_suppressed("orders_daily", path=rules_file)
    assert result is not None
    assert result.pattern == "orders_.*"


def test_is_suppressed_no_match_returns_none(rules_file):
    add_rule("orders_.*", reason="maint", path=rules_file)
    assert is_suppressed("users_daily", path=rules_file) is None


def test_expired_rule_not_suppressed(rules_file):
    add_rule(
        "pipe_old",
        reason="expired",
        expires_at="2000-01-01T00:00:00+00:00",
        path=rules_file,
    )
    assert is_suppressed("pipe_old", path=rules_file) is None


def test_future_expiry_still_suppresses(rules_file):
    add_rule(
        "pipe_future",
        reason="upcoming",
        expires_at="2099-12-31T23:59:59+00:00",
        path=rules_file,
    )
    assert is_suppressed("pipe_future", path=rules_file) is not None


def test_save_and_load_roundtrip(rules_file):
    rules = [
        SuppressionRule(pattern="a_.*", reason="r1", tags=["infra"]),
        SuppressionRule(pattern="b_.*", reason="r2", created_by="ops"),
    ]
    save_rules(rules, rules_file)
    loaded = load_rules(rules_file)
    assert [r.pattern for r in loaded] == ["a_.*", "b_.*"]
    assert loaded[0].tags == ["infra"]
    assert loaded[1].created_by == "ops"
