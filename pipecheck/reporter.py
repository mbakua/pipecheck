"""Reporting module: format and output check results."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List

from pipecheck.checks import CheckResult


def _status_icon(ok: bool) -> str:
    return "✅" if ok else "❌"


def format_text(results: List[CheckResult]) -> str:
    """Return a human-readable summary string."""
    lines: List[str] = [
        f"PipeCheck Report — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC",
        "-" * 52,
    ]
    for r in results:
        icon = _status_icon(r.ok)
        detail = f" ({r.detail})" if r.detail else ""
        lines.append(f"{icon}  {r.pipeline:<28} {r.status or 'N/A'}{detail}")
    lines.append("-" * 52)
    total = len(results)
    passed = sum(1 for r in results if r.ok)
    lines.append(f"Result: {passed}/{total} pipelines healthy")
    return "\n".join(lines)


def format_json(results: List[CheckResult]) -> str:
    """Return a JSON-serialisable report string."""
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total": len(results),
            "passed": sum(1 for r in results if r.ok),
            "failed": sum(1 for r in results if not r.ok),
        },
        "pipelines": [
            {
                "name": r.pipeline,
                "ok": r.ok,
                "status": r.status,
                "detail": r.detail,
            }
            for r in results
        ],
    }
    return json.dumps(payload, indent=2)


def print_report(results: List[CheckResult], fmt: str = "text") -> None:
    """Print a report to stdout in the requested format."""
    if fmt == "json":
        print(format_json(results))
    else:
        print(format_text(results))
