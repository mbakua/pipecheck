"""Alert suppression rules: silence alerts for pipelines matching criteria."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

DEFAULT_SUPPRESSION_FILE = Path(".pipecheck_suppressions.json")


@dataclass
class SuppressionRule:
    pattern: str          # regex matched against pipeline name
    reason: str
    created_by: str = "user"
    expires_at: Optional[str] = None   # ISO datetime string or None = permanent
    tags: List[str] = field(default_factory=list)


def _load_raw(path: Path) -> list:
    if not path.exists():
        return []
    with path.open() as fh:
        return json.load(fh)


def load_rules(path: Path = DEFAULT_SUPPRESSION_FILE) -> List[SuppressionRule]:
    return [SuppressionRule(**r) for r in _load_raw(path)]


def save_rules(rules: List[SuppressionRule], path: Path = DEFAULT_SUPPRESSION_FILE) -> None:
    with path.open("w") as fh:
        json.dump([r.__dict__ for r in rules], fh, indent=2)


def add_rule(
    pattern: str,
    reason: str,
    created_by: str = "user",
    expires_at: Optional[str] = None,
    tags: Optional[List[str]] = None,
    path: Path = DEFAULT_SUPPRESSION_FILE,
) -> SuppressionRule:
    rules = load_rules(path)
    rule = SuppressionRule(
        pattern=pattern,
        reason=reason,
        created_by=created_by,
        expires_at=expires_at,
        tags=tags or [],
    )
    rules.append(rule)
    save_rules(rules, path)
    return rule


def remove_rule(pattern: str, path: Path = DEFAULT_SUPPRESSION_FILE) -> bool:
    rules = load_rules(path)
    new_rules = [r for r in rules if r.pattern != pattern]
    if len(new_rules) == len(rules):
        return False
    save_rules(new_rules, path)
    return True


def is_suppressed(
    pipeline_name: str,
    path: Path = DEFAULT_SUPPRESSION_FILE,
    now: Optional[str] = None,
) -> Optional[SuppressionRule]:
    """Return the first matching active rule, or None if not suppressed."""
    from datetime import datetime, timezone

    _now = now or datetime.now(timezone.utc).isoformat()
    for rule in load_rules(path):
        if rule.expires_at and rule.expires_at < _now:
            continue
        if re.search(rule.pattern, pipeline_name):
            return rule
    return None
