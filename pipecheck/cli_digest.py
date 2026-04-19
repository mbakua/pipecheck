"""CLI sub-command: pipecheck digest — print a health digest."""
from __future__ import annotations

import json
import sys

import click

from pipecheck.config import load_config
from pipecheck.digest import build_digest, format_digest


@click.command("digest")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True,
              help="Path to config file.")
@click.option("--hours", default=24, show_default=True,
              help="Look-back window in hours.")
@click.option("--format", "output_format", type=click.Choice(["text", "json"]),
              default="text", show_default=True)
@click.option("--db", "db_path", default=None,
              help="Override history DB path.")
def digest_cmd(config_path: str, hours: int, output_format: str, db_path: str | None) -> None:
    """Print a periodic health digest for all configured pipelines."""
    try:
        app_cfg = load_config(config_path)
    except FileNotFoundError:
        click.echo(f"Config file not found: {config_path}", err=True)
        sys.exit(1)

    resolved_db = db_path or app_cfg.history_db
    pipeline_names = [p.name for p in app_cfg.pipelines]

    entries = build_digest(resolved_db, pipeline_names, hours=hours)

    if output_format == "json":
        data = [
            {
                "pipeline": e.pipeline,
                "total_runs": e.total_runs,
                "success_runs": e.success_runs,
                "failure_runs": e.failure_runs,
                "success_rate": e.success_rate,
                "trend": e.trend,
                "flapping": e.flapping,
            }
            for e in entries
        ]
        click.echo(json.dumps(data, indent=2))
    else:
        click.echo(format_digest(entries, hours=hours))
