"""CLI commands for tag-based pipeline filtering."""
import click
from pipecheck.config import load_config
from pipecheck.tags import parse_tags, filter_pipelines


@click.group("tags")
def tags_cmd():
    """Tag-based pipeline filtering utilities."""


@tags_cmd.command("list")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
def list_cmd(config_path: str):
    """List all unique tags across configured pipelines."""
    cfg = load_config(config_path)
    all_tags: set = set()
    for p in cfg.pipelines:
        for t in (getattr(p, "tags", None) or []):
            all_tags.add(t)
    if not all_tags:
        click.echo("No tags defined.")
    else:
        for tag in sorted(all_tags):
            click.echo(tag)


@tags_cmd.command("filter")
@click.option("--config", "config_path", default="pipecheck.yaml", show_default=True)
@click.option("--include", default="", help="Comma-separated tags to include.")
@click.option("--exclude", default="", help="Comma-separated tags to exclude.")
def filter_cmd(config_path: str, include: str, exclude: str):
    """Print pipeline names matching tag filters."""
    cfg = load_config(config_path)
    inc = parse_tags(include)
    exc = parse_tags(exclude)
    matched = filter_pipelines(cfg.pipelines, include_tags=inc, exclude_tags=exc)
    if not matched:
        click.echo("No pipelines match the given filters.")
    else:
        for p in matched:
            click.echo(p.name)
