"""CLI sub-command: pipecheck scorecard"""
from __future__ import annotations

import sys
import click

from pipecheck.scorecard import compute_scorecard, format_scorecard


@click.group("scorecard")
def scorecard_cmd():
    """Pipeline health scorecard."""


@scorecard_cmd.command("show")
@click.option("--db", default="pipecheck.db", show_default=True, help="History DB path.")
@click.option("--hours", default=24, show_default=True, help="Look-back window in hours.")
@click.option("--pipeline", "pipelines", multiple=True, help="Limit to specific pipelines.")
@click.option("--format", "fmt", default="text", type=click.Choice(["text", "json"]), show_default=True)
@click.option("--fail-below", default=None, type=float, help="Exit non-zero if any score is below this threshold.")
def show_cmd(db: str, hours: int, pipelines: tuple, fmt: str, fail_below):
    """Show health scorecard for all (or selected) pipelines."""
    entries = compute_scorecard(db, hours=hours, pipelines=list(pipelines) or None)
    if not entries:
        click.echo("No data found for the requested window.")
        sys.exit(0)

    click.echo(format_scorecard(entries, fmt=fmt))

    if fail_below is not None:
        worst = min(e.score for e in entries)
        if worst < fail_below:
            click.echo(
                f"\n[FAIL] Lowest score {worst:.1f}% is below threshold {fail_below}%",
                err=True,
            )
            sys.exit(1)
