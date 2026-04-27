"""CLI sub-command: pipecheck rollup — show aggregated pipeline health summaries."""
from __future__ import annotations

import json
import sys

import click

from pipecheck.rollup import as_dict, compute_rollup


@click.group(name="rollup")
def rollup_cmd() -> None:
    """Aggregate pipeline check history into periodic rollups."""


@rollup_cmd.command(name="show")
@click.option(
    "--db",
    default="pipecheck_history.db",
    show_default=True,
    help="Path to history SQLite database.",
)
@click.option(
    "--period",
    type=click.Choice(["hourly", "daily"]),
    default="hourly",
    show_default=True,
    help="Aggregation period.",
)
@click.option("--pipeline", default=None, help="Filter to a single pipeline name.")
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
)
def show_cmd(db: str, period: str, pipeline: str | None, fmt: str) -> None:
    """Display rollup summaries."""
    entries = compute_rollup(db, period=period, pipeline=pipeline)

    if not entries:
        click.echo("No history data found.")
        sys.exit(0)

    if fmt == "json":
        click.echo(json.dumps([as_dict(e) for e in entries], indent=2))
        return

    # Text table
    header = f"{'Pipeline':<30} {'Bucket':<22} {'Total':>6} {'OK':>6} {'Fail':>6} {'Rate':>7}"
    click.echo(header)
    click.echo("-" * len(header))
    for e in entries:
        rate_str = f"{e.success_rate * 100:.1f}%"
        click.echo(
            f"{e.pipeline:<30} {e.bucket:<22} {e.total:>6} "
            f"{e.success:>6} {e.failure:>6} {rate_str:>7}"
        )
