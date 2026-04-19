"""Tests for pipecheck.tags module."""
import pytest
from pipecheck.tags import parse_tags, pipeline_has_tags, filter_pipelines
from pipecheck.config import PipelineConfig


def _make_pipeline(name: str, tags=None) -> PipelineConfig:
    return PipelineConfig(name=name, tags=tags or [])


def test_parse_tags_empty():
    assert parse_tags("") == []
    assert parse_tags(None) == []


def test_parse_tags_single():
    assert parse_tags("critical") == ["critical"]


def test_parse_tags_multiple():
    assert parse_tags("critical, nightly, prod") == ["critical", "nightly", "prod"]


def test_pipeline_has_tags_no_filter():
    p = _make_pipeline("p1", ["a"])
    assert pipeline_has_tags(p, []) is True


def test_pipeline_has_tags_match():
    p = _make_pipeline("p1", ["critical", "prod"])
    assert pipeline_has_tags(p, ["critical"]) is True


def test_pipeline_has_tags_no_match():
    p = _make_pipeline("p1", ["dev"])
    assert pipeline_has_tags(p, ["critical"]) is False


def test_filter_pipelines_include():
    pipelines = [
        _make_pipeline("a", ["critical"]),
        _make_pipeline("b", ["nightly"]),
        _make_pipeline("c", ["critical", "nightly"]),
    ]
    result = filter_pipelines(pipelines, include_tags=["critical"])
    assert [p.name for p in result] == ["a", "c"]


def test_filter_pipelines_exclude():
    pipelines = [
        _make_pipeline("a", ["critical"]),
        _make_pipeline("b", ["nightly"]),
    ]
    result = filter_pipelines(pipelines, exclude_tags=["nightly"])
    assert [p.name for p in result] == ["a"]


def test_filter_pipelines_include_and_exclude():
    pipelines = [
        _make_pipeline("a", ["critical", "prod"]),
        _make_pipeline("b", ["critical", "dev"]),
        _make_pipeline("c", ["nightly"]),
    ]
    result = filter_pipelines(pipelines, include_tags=["critical"], exclude_tags=["dev"])
    assert [p.name for p in result] == ["a"]


def test_filter_pipelines_no_filters_returns_all():
    pipelines = [_make_pipeline("x"), _make_pipeline("y")]
    assert filter_pipelines(pipelines) == pipelines
