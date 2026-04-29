"""CLI sub-commands for the run log feature."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import click

from pipecheck.runlog import RunEvent, init_runlog_db, load_run_log, record_run


@click.group("runlog")
def runlog_cmd() -> None:
    """View and manage pipeline run logs."""


@runlog_cmd.command("list")
@click.option("--db", default="pipecheck_history.db", show_default=True, help="Path to DB file.")
@click.option("--pipeline", default=None, help="Filter by pipeline name.")
@click.option("--limit", default=20, show_default=True, help="Maximum rows to show.")
@click.option("--format", "fmt", default="text", type=click.Choice(["text", "json"]), show_default=True)
def list_cmd(db: str, pipeline: str | None, limit: int, fmt: str) -> None:
    """List recent run log entries."""
    init_runlog_db(db)
    events = load_run_log(db, pipeline=pipeline, limit=limit)

    if not events:
        click.echo("No run log entries found.")
        return

    if fmt == "json":
        click.echo(
            json.dumps(
                [
                    {
                        "pipeline": e.pipeline,
                        "status": e.status,
                        "started_at": e.started_at,
                        "duration_ms": e.duration_ms,
                        "message": e.message,
                    }
                    for e in events
                ],
                indent=2,
            )
        )
        return

    # text output
    click.echo(f"{'PIPELINE':<30} {'STATUS':<10} {'STARTED':<22} {'DUR(ms)':>8}  MESSAGE")
    click.echo("-" * 90)
    for e in events:
        ts = datetime.fromtimestamp(e.started_at, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        icon = {"success": "✓", "failure": "✗", "skipped": "–"}.get(e.status, "?")
        click.echo(f"{e.pipeline:<30} {icon} {e.status:<8} {ts:<22} {e.duration_ms:>8}  {e.message}")


@runlog_cmd.command("record")
@click.argument("pipeline")
@click.argument("status", type=click.Choice(["success", "failure", "skipped"]))
@click.option("--db", default="pipecheck_history.db", show_default=True)
@click.option("--duration-ms", default=0, show_default=True, help="Run duration in milliseconds.")
@click.option("--message", default="", help="Optional message.")
def record_cmd(pipeline: str, status: str, db: str, duration_ms: int, message: str) -> None:
    """Manually record a run event."""
    init_runlog_db(db)
    event = RunEvent(
        pipeline=pipeline,
        status=status,
        started_at=time.time(),
        duration_ms=duration_ms,
        message=message,
    )
    record_run(db, event)
    click.echo(f"Recorded {status} run for '{pipeline}'.")
