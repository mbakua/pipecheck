"""CLI commands for managing pipeline alert mutes."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import click

from pipecheck.mute import (
    mute_pipeline,
    unmute_pipeline,
    active_mutes,
    is_muted,
    DEFAULT_MUTE_FILE,
)


@click.group("mute")
def mute_cmd() -> None:
    """Manage alert mutes for pipelines."""


@mute_cmd.command("add")
@click.argument("pipeline")
@click.option("--reason", default="maintenance", show_default=True, help="Reason for muting.")
@click.option("--hours", default=None, type=float, help="Mute duration in hours (omit for indefinite).")
@click.option("--file", "mute_file", default=str(DEFAULT_MUTE_FILE), show_default=True)
def add_cmd(pipeline: str, reason: str, hours: float | None, mute_file: str) -> None:
    """Mute alerts for PIPELINE."""
    expires = None
    if hours is not None:
        expires = datetime.now(timezone.utc) + timedelta(hours=hours)
    entry = mute_pipeline(pipeline, reason, expires_at=expires, path=Path(mute_file))
    exp_str = entry.expires_at or "indefinite"
    click.echo(f"Muted '{pipeline}' until {exp_str}. Reason: {reason}")


@mute_cmd.command("remove")
@click.argument("pipeline")
@click.option("--file", "mute_file", default=str(DEFAULT_MUTE_FILE), show_default=True)
def remove_cmd(pipeline: str, mute_file: str) -> None:
    """Unmute alerts for PIPELINE."""
    removed = unmute_pipeline(pipeline, path=Path(mute_file))
    if removed:
        click.echo(f"Unmuted '{pipeline}'.")
    else:
        click.echo(f"No active mute found for '{pipeline}'.")


@mute_cmd.command("list")
@click.option("--file", "mute_file", default=str(DEFAULT_MUTE_FILE), show_default=True)
def list_cmd(mute_file: str) -> None:
    """List all active mutes."""
    mutes = active_mutes(path=Path(mute_file))
    if not mutes:
        click.echo("No active mutes.")
        return
    for m in mutes:
        exp = m.expires_at or "indefinite"
        click.echo(f"  {m.pipeline:30s}  expires={exp}  reason={m.reason}")
