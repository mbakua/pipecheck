"""CLI subcommand for managing per-pipeline check rate limits."""

from __future__ import annotations

import click

from pipecheck.ratelimit import (
    DEFAULT_RATELIMIT_FILE,
    clear_ratelimit,
    is_rate_limited,
    load_ratelimit,
)


@click.group("ratelimit")
def ratelimit_cmd() -> None:
    """Manage per-pipeline check rate limits."""


@ratelimit_cmd.command("list")
@click.option("--file", "path", default=DEFAULT_RATELIMIT_FILE, show_default=True)
def list_cmd(path: str) -> None:
    """List all recorded last-check timestamps."""
    state = load_ratelimit(path)
    if not state:
        click.echo("No rate limit records found.")
        return
    click.echo(f"{'Pipeline':<30} {'Last Check (UTC)':<30}")
    click.echo("-" * 62)
    for name, ts in sorted(state.items()):
        click.echo(f"{name:<30} {ts:<30}")


@ratelimit_cmd.command("status")
@click.argument("pipeline")
@click.option("--interval", default=60, show_default=True, help="Min seconds between checks.")
@click.option("--file", "path", default=DEFAULT_RATELIMIT_FILE, show_default=True)
def status_cmd(pipeline: str, interval: int, path: str) -> None:
    """Check whether a pipeline is currently rate limited."""
    limited = is_rate_limited(pipeline, interval, path)
    if limited:
        click.echo(f"⏳ '{pipeline}' is rate limited (interval={interval}s).")
    else:
        click.echo(f"✅ '{pipeline}' is not rate limited.")


@ratelimit_cmd.command("clear")
@click.argument("pipeline", required=False, default=None)
@click.option("--file", "path", default=DEFAULT_RATELIMIT_FILE, show_default=True)
def clear_cmd(pipeline: str | None, path: str) -> None:
    """Clear rate limit record for a pipeline, or all records if none given."""
    clear_ratelimit(pipeline, path)
    if pipeline:
        click.echo(f"Cleared rate limit record for '{pipeline}'.")
    else:
        click.echo("Cleared all rate limit records.")
