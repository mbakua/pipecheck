"""CLI sub-commands for maintenance window management."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import click

from pipecheck.maintenance import (
    add_window,
    remove_window,
    load_windows,
    is_in_maintenance,
    _DEFAULT_FILE,
)


@click.group(name="maintenance")
def maintenance_cmd() -> None:
    """Manage pipeline maintenance windows."""


@maintenance_cmd.command(name="add")
@click.argument("pipeline")
@click.option("--start", required=True, help="Window start (ISO-8601, e.g. 2024-01-01T02:00:00+00:00)")
@click.option("--end", required=True, help="Window end (ISO-8601)")
@click.option("--reason", default="", help="Optional reason")
@click.option("--file", "mfile", default=str(_DEFAULT_FILE), show_default=True)
def add_cmd(pipeline: str, start: str, end: str, reason: str, mfile: str) -> None:
    """Add a maintenance window for PIPELINE."""
    path = Path(mfile)
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)
    if end_dt <= start_dt:
        raise click.ClickException("--end must be after --start")
    w = add_window(pipeline, start_dt, end_dt, reason, path)
    click.echo(f"Added maintenance window for '{w.pipeline}': {w.start} → {w.end}")


@maintenance_cmd.command(name="remove")
@click.argument("pipeline")
@click.option("--file", "mfile", default=str(_DEFAULT_FILE), show_default=True)
def remove_cmd(pipeline: str, mfile: str) -> None:
    """Remove the maintenance window for PIPELINE."""
    path = Path(mfile)
    if remove_window(pipeline, path):
        click.echo(f"Removed maintenance window for '{pipeline}'.")
    else:
        click.echo(f"No maintenance window found for '{pipeline}'.")


@maintenance_cmd.command(name="list")
@click.option("--file", "mfile", default=str(_DEFAULT_FILE), show_default=True)
def list_cmd(mfile: str) -> None:
    """List all maintenance windows."""
    path = Path(mfile)
    windows = load_windows(path)
    if not windows:
        click.echo("No maintenance windows defined.")
        return
    now = datetime.now(timezone.utc)
    for w in windows:
        active = "[ACTIVE]" if w.is_active(now) else "[inactive]"
        reason = f" — {w.reason}" if w.reason else ""
        click.echo(f"{active} {w.pipeline}: {w.start} → {w.end}{reason}")


@maintenance_cmd.command(name="status")
@click.argument("pipeline")
@click.option("--file", "mfile", default=str(_DEFAULT_FILE), show_default=True)
def status_cmd(pipeline: str, mfile: str) -> None:
    """Check whether PIPELINE is currently in maintenance."""
    path = Path(mfile)
    if is_in_maintenance(pipeline, path=path):
        click.echo(f"'{pipeline}' is currently IN maintenance.")
    else:
        click.echo(f"'{pipeline}' is NOT in maintenance.")
