"""Main CLI entry point for pipecheck."""

import sys
import click

from pipecheck.config import load_config
from pipecheck.config_schema import validate_config
from pipecheck.checks import run_all_checks
from pipecheck.reporter import print_report
from pipecheck.alerts import dispatch_alerts
from pipecheck.history import init_db, save_results
from pipecheck.cli_digest import digest_cmd
from pipecheck.cli_schedule import run_with_schedule


@click.group()
@click.version_option(package_name="pipecheck")
def cli():
    """pipecheck — validate and monitor ETL pipeline health."""


@cli.command("run")
@click.option(
    "-c", "--config", "config_path",
    default="pipecheck.yaml",
    show_default=True,
    help="Path to the YAML configuration file.",
)
@click.option(
    "--format", "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format for the report.",
)
@click.option(
    "--db", "db_path",
    default="pipecheck.db",
    show_default=True,
    help="Path to the SQLite history database.",
)
@click.option(
    "--no-alerts", is_flag=True, default=False,
    help="Skip sending alerts even if failures are detected.",
)
@click.option(
    "--schedule", is_flag=True, default=False,
    help="Respect schedule windows defined in each pipeline config.",
)
def run_cmd(config_path, output_format, db_path, no_alerts, schedule):
    """Run health checks against all configured pipelines."""
    try:
        app_cfg = load_config(config_path)
    except FileNotFoundError:
        click.echo(f"Error: config file '{config_path}' not found.", err=True)
        sys.exit(1)

    errors = validate_config(app_cfg.raw)
    if errors:
        click.echo("Configuration errors:", err=True)
        for e in errors:
            click.echo(f"  - {e}", err=True)
        sys.exit(1)

    init_db(db_path)

    if schedule:
        results = run_with_schedule(app_cfg)
    else:
        results = run_all_checks(app_cfg.pipelines)

    save_results(db_path, results)
    print_report(results, fmt=output_format)

    failures = [r for r in results if not r.ok]
    if failures and not no_alerts:
        dispatch_alerts(app_cfg.alert, failures)

    if any(not r.ok for r in results):
        sys.exit(2)


@cli.command("validate")
@click.option(
    "-c", "--config", "config_path",
    default="pipecheck.yaml",
    show_default=True,
    help="Path to the YAML configuration file.",
)
def validate_cmd(config_path):
    """Validate the configuration file without running checks."""
    try:
        app_cfg = load_config(config_path)
    except FileNotFoundError:
        click.echo(f"Error: config file '{config_path}' not found.", err=True)
        sys.exit(1)

    errors = validate_config(app_cfg.raw)
    if errors:
        click.echo("Configuration is invalid:", err=True)
        for e in errors:
            click.echo(f"  - {e}", err=True)
        sys.exit(1)

    click.echo("Configuration is valid.")


cli.add_command(digest_cmd, name="digest")


def main():
    cli()


if __name__ == "__main__":
    main()
