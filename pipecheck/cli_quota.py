"""CLI sub-commands for pipeline check quota management."""
from __future__ import annotations

import click

from pipecheck.quota import (
    DEFAULT_QUOTA_FILE,
    is_quota_exceeded,
    load_quota,
    record_check,
    save_quota,
    set_quota,
)


@click.group("quota")
def quota_cmd() -> None:
    """Manage per-pipeline check quotas."""


@quota_cmd.command("set")
@click.argument("pipeline")
@click.option("--window", default=3600, show_default=True, help="Window in seconds.")
@click.option("--max", "max_checks", default=10, show_default=True, help="Max checks allowed.")
@click.option("--file", "quota_file", default=DEFAULT_QUOTA_FILE, show_default=True)
def set_cmd(pipeline: str, window: int, max_checks: int, quota_file: str) -> None:
    """Set a quota rule for PIPELINE."""
    entries = load_quota(quota_file)
    set_quota(pipeline, window, max_checks, entries)
    save_quota(entries, quota_file)
    click.echo(f"Quota set: {pipeline} → {max_checks} checks per {window}s window.")


@quota_cmd.command("remove")
@click.argument("pipeline")
@click.option("--file", "quota_file", default=DEFAULT_QUOTA_FILE, show_default=True)
def remove_cmd(pipeline: str, quota_file: str) -> None:
    """Remove the quota rule for PIPELINE."""
    entries = load_quota(quota_file)
    if pipeline not in entries:
        click.echo(f"No quota rule found for '{pipeline}'.")
        raise SystemExit(1)
    del entries[pipeline]
    save_quota(entries, quota_file)
    click.echo(f"Quota rule removed for '{pipeline}'.")


@quota_cmd.command("list")
@click.option("--file", "quota_file", default=DEFAULT_QUOTA_FILE, show_default=True)
def list_cmd(quota_file: str) -> None:
    """List all quota rules."""
    entries = load_quota(quota_file)
    if not entries:
        click.echo("No quota rules configured.")
        return
    for name, e in sorted(entries.items()):
        exceeded = is_quota_exceeded(name, entries)
        status = "[EXCEEDED]" if exceeded else "[ok]"
        recent = len(e.timestamps)
        click.echo(
            f"  {name}: {e.max_checks} checks / {e.window_seconds}s  "
            f"(recent={recent}) {status}"
        )


@quota_cmd.command("status")
@click.argument("pipeline")
@click.option("--file", "quota_file", default=DEFAULT_QUOTA_FILE, show_default=True)
def status_cmd(pipeline: str, quota_file: str) -> None:
    """Show quota status for PIPELINE."""
    entries = load_quota(quota_file)
    if pipeline not in entries:
        click.echo(f"No quota rule for '{pipeline}'.")
        return
    exceeded = is_quota_exceeded(pipeline, entries)
    click.echo("exceeded" if exceeded else "ok")
    raise SystemExit(1 if exceeded else 0)
