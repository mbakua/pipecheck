"""CLI commands for snapshot capture and diffing."""

from __future__ import annotations

from pathlib import Path

import click

from pipecheck.checks import run_all_checks
from pipecheck.config import load_config
from pipecheck.snapshots import save_snapshot, load_snapshot, diff_snapshots

_DEFAULT_DIR = Path(".pipecheck_snapshots")


@click.group("snapshots")
def snapshots_cmd() -> None:
    """Capture and diff pipeline check snapshots."""


@snapshots_cmd.command("capture")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
@click.option("--label", required=True, help="Name for this snapshot (e.g. 'pre-deploy')")
@click.option("--dir", "snap_dir", default=str(_DEFAULT_DIR), show_default=True)
def capture_cmd(config_path: str, label: str, snap_dir: str) -> None:
    """Run all checks and save results as a named snapshot."""
    cfg = load_config(config_path)
    results = run_all_checks(cfg.pipelines)
    path = save_snapshot(label, results, directory=Path(snap_dir))
    click.echo(f"Snapshot '{label}' saved to {path}")


@snapshots_cmd.command("diff")
@click.argument("old_label")
@click.argument("new_label")
@click.option("--dir", "snap_dir", default=str(_DEFAULT_DIR), show_default=True)
def diff_cmd(old_label: str, new_label: str, snap_dir: str) -> None:
    """Show differences between two named snapshots."""
    directory = Path(snap_dir)
    old = load_snapshot(old_label, directory)
    new = load_snapshot(new_label, directory)

    if old is None:
        raise click.ClickException(f"Snapshot '{old_label}' not found in {directory}")
    if new is None:
        raise click.ClickException(f"Snapshot '{new_label}' not found in {directory}")

    diff = diff_snapshots(old, new)

    click.echo(f"Diff: '{old_label}' → '{new_label}'")
    click.echo(f"  Added   ({len(diff.added)}): {', '.join(diff.added) or '-'}")
    click.echo(f"  Removed ({len(diff.removed)}): {', '.join(diff.removed) or '-'}")
    click.echo(f"  Changed ({len(diff.changed)}): {', '.join(diff.changed) or '-'}")
    click.echo(f"  Same    ({len(diff.unchanged)}): {', '.join(diff.unchanged) or '-'}")

    if diff.changed or diff.added or diff.removed:
        raise SystemExit(1)
