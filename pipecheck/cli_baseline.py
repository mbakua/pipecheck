"""CLI commands for baseline snapshot and drift comparison."""

from __future__ import annotations

from pathlib import Path

import click

from pipecheck.baseline import compare_baseline, save_baseline
from pipecheck.checks import run_all_checks
from pipecheck.config import load_config


@click.group("baseline")
def baseline_cmd() -> None:
    """Manage pipeline baselines for drift detection."""


@baseline_cmd.command("snapshot")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
@click.option("--output", "out", default=".pipecheck_baseline.json", show_default=True)
def snapshot_cmd(config_path: str, out: str) -> None:
    """Run checks and save results as the new baseline."""
    cfg = load_config(config_path)
    results = run_all_checks(cfg.pipelines)
    save_baseline(results, Path(out))
    click.echo(f"Baseline saved to {out} ({len(results)} pipelines).")


@baseline_cmd.command("diff")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
@click.option("--baseline", "bl", default=".pipecheck_baseline.json", show_default=True)
@click.option("--fail-on-drift", is_flag=True, default=False)
def diff_cmd(config_path: str, bl: str, fail_on_drift: bool) -> None:
    """Compare current pipeline state against saved baseline."""
    cfg = load_config(config_path)
    results = run_all_checks(cfg.pipelines)
    drifts = compare_baseline(results, Path(bl))

    if not drifts:
        click.echo("No baseline found — run 'pipecheck baseline snapshot' first.")
        return

    any_drift = False
    for d in drifts:
        icon = "⚠️ " if d.drifted else "✅"
        click.echo(f"{icon} {d.pipeline}: {d.reason}")
        if d.drifted:
            any_drift = True

    if fail_on_drift and any_drift:
        raise SystemExit(1)
