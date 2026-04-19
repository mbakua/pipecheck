"""Tests for pipecheck.reporter."""
import json
from pipecheck.checks import CheckResult
from pipecheck.reporter import format_text, format_json, print_report


def _results():
    return [
        CheckResult(pipeline="orders", ok=True, status="healthy", detail=None),
        CheckResult(pipeline="inventory", ok=False, status="degraded", detail="latency high"),
        CheckResult(pipeline="shipping", ok=False, status=None, detail="connection refused"),
    ]


def test_format_text_contains_pipeline_names():
    out = format_text(_results())
    assert "orders" in out
    assert "inventory" in out
    assert "shipping" in out


def test_format_text_summary_counts():
    out = format_text(_results())
    assert "1/3 pipelines healthy" in out


def test_format_text_shows_detail():
    out = format_text(_results())
    assert "latency high" in out
    assert "connection refused" in out


def test_format_text_icons():
    out = format_text(_results())
    assert "✅" in out
    assert "❌" in out


def test_format_json_structure():
    out = format_json(_results())
    data = json.loads(out)
    assert data["summary"]["total"] == 3
    assert data["summary"]["passed"] == 1
    assert data["summary"]["failed"] == 2
    assert len(data["pipelines"]) == 3


def test_format_json_pipeline_fields():
    out = format_json(_results())
    data = json.loads(out)
    first = data["pipelines"][0]
    assert first["name"] == "orders"
    assert first["ok"] is True
    assert first["status"] == "healthy"


def test_print_report_text(capsys):
    print_report(_results(), fmt="text")
    captured = capsys.readouterr()
    assert "PipeCheck Report" in captured.out


def test_print_report_json(capsys):
    print_report(_results(), fmt="json")
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "summary" in data
