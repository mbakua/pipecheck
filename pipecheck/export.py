"""Export pipeline check results to various formats (CSV, JSON Lines, HTML)."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import List

from pipecheck.checks import CheckResult


def export_csv(results: List[CheckResult]) -> str:
    """Serialise a list of CheckResult objects to a CSV string.

    Columns: pipeline, status, ok, latency_ms, message, checked_at
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["pipeline", "status", "ok", "latency_ms", "message", "checked_at"])
    for r in results:
        writer.writerow([
            r.pipeline,
            r.status,
            r.ok,
            f"{r.latency_ms:.1f}" if r.latency_ms is not None else "",
            r.message or "",
            r.checked_at,
        ])
    return buf.getvalue()


def export_jsonl(results: List[CheckResult]) -> str:
    """Serialise results as JSON Lines (one JSON object per line)."""
    lines = []
    for r in results:
        lines.append(json.dumps({
            "pipeline": r.pipeline,
            "status": r.status,
            "ok": r.ok,
            "latency_ms": r.latency_ms,
            "message": r.message,
            "checked_at": r.checked_at,
        }))
    return "\n".join(lines) + ("\n" if lines else "")


def export_html(results: List[CheckResult], title: str = "PipeCheck Report") -> str:
    """Render results as a minimal self-contained HTML table."""
    rows_html = []
    for r in results:
        status_class = "ok" if r.ok else "fail"
        icon = "✅" if r.ok else "❌"
        latency = f"{r.latency_ms:.0f} ms" if r.latency_ms is not None else "—"
        rows_html.append(
            f"  <tr class='{status_class}'>"
            f"<td>{icon}</td>"
            f"<td>{_esc(r.pipeline)}</td>"
            f"<td>{_esc(str(r.status))}</td>"
            f"<td>{latency}</td>"
            f"<td>{_esc(r.message or '')}</td>"
            f"<td>{_esc(r.checked_at)}</td>"
            f"</tr>"
        )

    generated = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    rows = "\n".join(rows_html)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>{_esc(title)}</title>
  <style>
    body {{ font-family: sans-serif; padding: 1rem; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ccc; padding: 6px 10px; text-align: left; }}
    th {{ background: #f4f4f4; }}
    tr.ok td {{ background: #f0fff0; }}
    tr.fail td {{ background: #fff0f0; }}
    .meta {{ color: #888; font-size: 0.85rem; margin-top: 1rem; }}
  </style>
</head>
<body>
  <h1>{_esc(title)}</h1>
  <table>
    <thead>
      <tr>
        <th></th>
        <th>Pipeline</th>
        <th>Status</th>
        <th>Latency</th>
        <th>Message</th>
        <th>Checked At</th>
      </tr>
    </thead>
    <tbody>
{rows}
    </tbody>
  </table>
  <p class="meta">Generated: {generated}</p>
</body>
</html>
"""


def _esc(text: str) -> str:
    """Minimal HTML escaping for safe embedding of arbitrary strings."""
    return (
        text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
