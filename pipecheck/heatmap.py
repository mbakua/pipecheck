"""Heatmap: aggregate pipeline failure counts by hour-of-day and day-of-week."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import sqlite3

DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
HOURS = list(range(24))


@dataclass
class HeatmapCell:
    day: str          # e.g. "Mon"
    hour: int         # 0-23
    total: int
    failures: int

    def failure_rate(self) -> float:
        return self.failures / self.total if self.total else 0.0

    def as_dict(self) -> dict:
        return {
            "day": self.day,
            "hour": self.hour,
            "total": self.total,
            "failures": self.failures,
            "failure_rate": round(self.failure_rate(), 4),
        }


def compute_heatmap(
    db_path: str,
    pipeline: Optional[str] = None,
    lookback_days: int = 30,
) -> List[HeatmapCell]:
    """Return a heatmap of failure rates keyed by (day-of-week, hour-of-day)."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        cutoff = f"datetime('now', '-{lookback_days} days')"
        pipeline_filter = "AND pipeline = ?" if pipeline else ""
        params: list = [pipeline] if pipeline else []

        query = f"""
            SELECT
                CAST(strftime('%w', checked_at) AS INTEGER) AS dow,
                CAST(strftime('%H', checked_at) AS INTEGER) AS hour,
                COUNT(*) AS total,
                SUM(CASE WHEN status != 'ok' THEN 1 ELSE 0 END) AS failures
            FROM results
            WHERE checked_at >= {cutoff}
            {pipeline_filter}
            GROUP BY dow, hour
        """
        rows = con.execute(query, params).fetchall()
    finally:
        con.close()

    # SQLite %w: 0=Sunday … 6=Saturday; remap to Mon-first
    _dow_map = {0: 6, 1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5}

    cells: Dict[tuple, HeatmapCell] = {}
    for row in rows:
        day_idx = _dow_map[row["dow"]]
        key = (day_idx, row["hour"])
        cells[key] = HeatmapCell(
            day=DAYS[day_idx],
            hour=row["hour"],
            total=row["total"],
            failures=row["failures"] or 0,
        )
    return sorted(cells.values(), key=lambda c: (DAYS.index(c.day), c.hour))


def format_heatmap(cells: List[HeatmapCell]) -> str:
    """Render a compact ASCII heatmap grid (days × hours)."""
    if not cells:
        return "No heatmap data available."

    # Build lookup
    lookup: Dict[tuple, float] = {
        (c.day, c.hour): c.failure_rate() for c in cells
    }

    def _symbol(rate: float) -> str:
        if rate == 0:
            return "."
        if rate < 0.25:
            return "o"
        if rate < 0.5:
            return "*"
        if rate < 0.75:
            return "#"
        return "X"

    header = "     " + "".join(f"{h:02d} " for h in HOURS)
    lines = [header]
    for day in DAYS:
        row = f"{day:3s}  "
        for h in HOURS:
            rate = lookup.get((day, h), -1.0)
            row += (" ? " if rate < 0 else f" {_symbol(rate)} ")
        lines.append(row)
    lines.append("Legend: . =0%  o <25%  * <50%  # <75%  X >=75%  ? no data")
    return "\n".join(lines)
