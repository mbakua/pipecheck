"""CLI commands for managing alert suppression rules."""
from __future__ import annotations

from pathlib import Path

import click

from pipecheck.suppression import (
    DEFAULT_SUPPRESSION_FILE,
    add_rule,
    is_suppressed,
    load_rules,
    remove_rule,
)


@click.group("suppress")
def suppression_cmd() -> None:
    """Manage alert suppression rules."""


@suppression_cmd.command("add")
@click.argument("pattern")
@click.option("--reason", required=True, help="Why this pipeline is suppressed.")
@click.option("--by", default="user", show_default=True, help="Who is adding the rule.")
@click.option("--expires", default=None, help="ISO datetime when the rule expires.")
@click.option("--tag", multiple=True, help="Optional tags for this rule.")
@click.option("--file", "rules_file", default=str(DEFAULT_SUPPRESSION_FILE), show_default=True)
def add_cmd(pattern, reason, by, expires, tag, rules_file):
    """Add a suppression rule matching PATTERN (regex)."""
    rule = add_rule(
        pattern=pattern,
        reason=reason,
        created_by=by,
        expires_at=expires,
        tags=list(tag),
        path=Path(rules_file),
    )
    click.echo(f"Added suppression rule for pattern '{rule.pattern}': {rule.reason}")


@suppression_cmd.command("remove")
@click.argument("pattern")
@click.option("--file", "rules_file", default=str(DEFAULT_SUPPRESSION_FILE), show_default=True)
def remove_cmd(pattern, rules_file):
    """Remove a suppression rule by PATTERN."""
    removed = remove_rule(pattern, path=Path(rules_file))
    if removed:
        click.echo(f"Removed suppression rule for pattern '{pattern}'.")
    else:
        click.echo(f"No rule found for pattern '{pattern}'.")
        raise SystemExit(1)


@suppression_cmd.command("list")
@click.option("--file", "rules_file", default=str(DEFAULT_SUPPRESSION_FILE), show_default=True)
def list_cmd(rules_file):
    """List all suppression rules."""
    rules = load_rules(Path(rules_file))
    if not rules:
        click.echo("No suppression rules defined.")
        return
    for r in rules:
        expiry = r.expires_at or "never"
        click.echo(f"  [{r.created_by}] {r.pattern!r}  — {r.reason}  (expires: {expiry})")


@suppression_cmd.command("check")
@click.argument("pipeline_name")
@click.option("--file", "rules_file", default=str(DEFAULT_SUPPRESSION_FILE), show_default=True)
def check_cmd(pipeline_name, rules_file):
    """Check whether PIPELINE_NAME is currently suppressed."""
    rule = is_suppressed(pipeline_name, path=Path(rules_file))
    if rule:
        click.echo(f"SUPPRESSED by rule '{rule.pattern}': {rule.reason}")
    else:
        click.echo(f"'{pipeline_name}' is NOT suppressed.")
