"""CLI sub-command for SLA checking."""
from __future__ import annotations

import json
import sys

import click

from pipecheck.config import load_config
from pipecheck.sla import SLAPolicy, check_all_slas


@click.group("sla")
def sla_cmd() -> None:
    """SLA monitoring commands."""


@sla_cmd.command("check")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
@click.option("--db", "db_path", default="pipecheck_history.db", show_default=True)
@click.option("--format", "fmt", type=click.Choice(["text", "json"]), default="text")
def check_cmd(config_path: str, db_path: str, fmt: str) -> None:
    """Check SLA compliance for all configured pipelines."""
    cfg = load_config(config_path)
    policies = [
        SLAPolicy(
            pipeline=p.name,
            max_interval_minutes=getattr(p, "sla_minutes", None) or 60,
        )
        for p in cfg.pipelines
        if getattr(p, "sla_minutes", None)
    ]
    if not policies:
        click.echo("No SLA policies configured (add 'sla_minutes' to pipelines).")
        return

    results = check_all_slas(policies, db_path)
    breached = [r for r in results if r.breached]

    if fmt == "json":
        click.echo(json.dumps([r.as_dict() for r in results], indent=2))
    else:
        for r in results:
            icon = "\u274c" if r.breached else "\u2705"
            since = f"{r.minutes_since_success:.1f}m ago" if r.minutes_since_success is not None else "never"
            click.echo(
                f"{icon} {r.pipeline}: last success {since} "
                f"(limit {r.max_interval_minutes}m)"
            )
        if breached:
            click.echo(f"\n{len(breached)} SLA breach(es) detected.")

    if breached:
        sys.exit(1)
