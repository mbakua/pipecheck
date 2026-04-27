"""Entry-point for the pipecheck CLI."""
from __future__ import annotations

import sys
import click

from pipecheck.config import load_config
from pipecheck.config_schema import validate_config
from pipecheck.checks import run_all_checks
from pipecheck.alerts import dispatch_alerts
from pipecheck.reporter import print_report
from pipecheck.history import init_db, save_results
from pipecheck.cli_digest import digest_cmd
from pipecheck.cli_schedule import run_with_schedule
from pipecheck.cli_baseline import baseline_cmd
from pipecheck.cli_mute import mute_cmd
from pipecheck.cli_tags import tags_cmd
from pipecheck.cli_sla import sla_cmd
from pipecheck.cli_ratelimit import ratelimit_cmd
from pipecheck.cli_snapshots import snapshots_cmd
from pipecheck.cli_suppression import suppression_cmd
from pipecheck.cli_rollup import rollup_cmd
from pipecheck.cli_forecast import forecast_cmd


@click.group()
def cli():
    """pipecheck — ETL pipeline health monitor."""


@cli.command("run")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
@click.option("--db", "db_path", default="pipecheck_history.db", show_default=True)
@click.option("--format", "fmt", type=click.Choice(["text", "json"]), default="text")
@click.option("--tags", "tag_filter", default=None, help="Comma-separated tag filter.")
def run_cmd(config_path: str, db_path: str, fmt: str, tag_filter: str | None):
    """Run all pipeline checks."""
    try:
        cfg = load_config(config_path)
    except FileNotFoundError:
        click.echo(f"Config file not found: {config_path}", err=True)
        sys.exit(1)

    pipelines = cfg.pipelines
    if tag_filter:
        from pipecheck.tags import filter_pipelines, parse_tags
        pipelines = filter_pipelines(pipelines, parse_tags(tag_filter))

    from pipecheck.mute import is_muted
    from pipecheck.ratelimit import is_rate_limited, record_check

    active = []
    for p in pipelines:
        if is_muted(p.name):
            click.echo(f"  [muted] {p.name}", err=True)
            continue
        if is_rate_limited(p.name):
            click.echo(f"  [rate-limited] {p.name}", err=True)
            continue
        active.append(p)
        record_check(p.name)

    def _do_run():
        results = run_all_checks(active)
        init_db(db_path)
        save_results(db_path, results)
        dispatch_alerts(results, cfg.alert)
        print_report(results, fmt=fmt)
        if any(not r.ok for r in results):
            sys.exit(2)

    run_with_schedule(cfg, _do_run)


@cli.command("validate")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
def validate_cmd(config_path: str):
    """Validate the configuration file."""
    try:
        import yaml
        with open(config_path) as fh:
            raw = yaml.safe_load(fh)
        errors = validate_config(raw)
        if errors:
            for e in errors:
                click.echo(f"  ERROR: {e}", err=True)
            sys.exit(1)
        click.echo("Config is valid.")
    except FileNotFoundError:
        click.echo(f"Config file not found: {config_path}", err=True)
        sys.exit(1)


cli.add_command(digest_cmd, "digest")
cli.add_command(baseline_cmd, "baseline")
cli.add_command(mute_cmd, "mute")
cli.add_command(tags_cmd, "tags")
cli.add_command(sla_cmd, "sla")
cli.add_command(ratelimit_cmd, "ratelimit")
cli.add_command(snapshots_cmd, "snapshots")
cli.add_command(suppression_cmd, "suppression")
cli.add_command(rollup_cmd, "rollup")
cli.add_command(forecast_cmd, "forecast")


def main():
    cli()


if __name__ == "__main__":
    main()
