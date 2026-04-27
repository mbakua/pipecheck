"""CLI sub-command: pipecheck forecast."""
from __future__ import annotations

import sys
import click

from pipecheck.config import load_config
from pipecheck.forecast import compute_forecast, format_forecast


@click.group("forecast")
def forecast_cmd():
    """Predict future failures based on historical data."""


@forecast_cmd.command("show")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
@click.option("--db", "db_path", default="pipecheck_history.db", show_default=True)
@click.option("--window", default=30, show_default=True, help="Runs to examine.")
@click.option("--horizon", default=10, show_default=True, help="Runs to forecast.")
@click.option("--pipeline", "pipeline_filter", default=None, help="Single pipeline name.")
@click.option("--format", "fmt", type=click.Choice(["text", "json"]), default="text")
def show_cmd(
    config_path: str,
    db_path: str,
    window: int,
    horizon: int,
    pipeline_filter: str | None,
    fmt: str,
):
    """Show failure-rate forecast for all (or one) pipeline(s)."""
    try:
        cfg = load_config(config_path)
    except FileNotFoundError:
        click.echo(f"Config file not found: {config_path}", err=True)
        sys.exit(1)

    pipelines = cfg.pipelines
    if pipeline_filter:
        pipelines = [p for p in pipelines if p.name == pipeline_filter]
        if not pipelines:
            click.echo(f"Pipeline '{pipeline_filter}' not found in config.", err=True)
            sys.exit(1)

    results = []
    for p in pipelines:
        fr = compute_forecast(db_path, p.name, window=window, horizon=horizon)
        if fr is not None:
            results.append(fr)

    click.echo(format_forecast(results, fmt=fmt))
