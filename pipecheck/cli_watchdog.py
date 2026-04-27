"""CLI sub-command: pipecheck watchdog."""
from __future__ import annotations

import json
import sys

import click

from pipecheck.config import load_config
from pipecheck.watchdog import as_dict, check_watchdog


@click.group("watchdog")
def watchdog_cmd() -> None:
    """Detect pipelines that have not been checked recently."""


@watchdog_cmd.command("check")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
@click.option("--db", "db_path", default="pipecheck.db", show_default=True)
@click.option(
    "--threshold",
    default=3600.0,
    show_default=True,
    help="Seconds of silence before a pipeline is considered stale.",
)
@click.option("--format", "fmt", default="text", type=click.Choice(["text", "json"]), show_default=True)
def check_cmd(config_path: str, db_path: str, threshold: float, fmt: str) -> None:
    """Report pipelines that have not run within THRESHOLD seconds."""
    cfg = load_config(config_path)
    names = [p.name for p in cfg.pipelines]
    results = check_watchdog(names, threshold, db_path)

    stale = [r for r in results if r.stale]

    if fmt == "json":
        click.echo(json.dumps([as_dict(r) for r in results], indent=2))
    else:
        for r in results:
            icon = "\u26a0\ufe0f " if r.stale else "\u2705 "
            if r.last_checked is None:
                detail = "never checked"
            else:
                detail = f"{r.silence_seconds:.0f}s ago"
            click.echo(f"{icon}{r.pipeline}: {detail}")

    if stale:
        click.echo(
            f"\n{len(stale)} pipeline(s) stale (threshold {threshold:.0f}s).",
            err=True,
        )
        sys.exit(1)
