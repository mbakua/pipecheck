"""Schedule-based check suppression and run-window enforcement."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Optional


@dataclass
class ScheduleWindow:
    """Defines an allowed run window (UTC) for a pipeline check."""

    start: time  # e.g. time(6, 0)
    end: time    # e.g. time(22, 0)
    days: list[int] = field(default_factory=lambda: list(range(7)))  # 0=Mon

    def is_active(self, now: Optional[datetime] = None) -> bool:
        """Return True if *now* falls inside this window."""
        if now is None:
            now = datetime.utcnow()
        if now.weekday() not in self.days:
            return False
        current = now.time().replace(second=0, microsecond=0)
        if self.start <= self.end:
            return self.start <= current <= self.end
        # overnight window e.g. 22:00 – 06:00
        return current >= self.start or current <= self.end


def parse_schedule(raw: dict) -> ScheduleWindow:
    """Build a ScheduleWindow from a config dict.

    Expected keys: start ("HH:MM"), end ("HH:MM"), days (list[int], optional).
    """
    def _t(s: str) -> time:
        h, m = s.split(":")
        return time(int(h), int(m))

    kwargs: dict = {
        "start": _t(raw["start"]),
        "end": _t(raw["end"]),
    }
    if "days" in raw:
        kwargs["days"] = raw["days"]
    return ScheduleWindow(**kwargs)


def should_run(pipeline_cfg, now: Optional[datetime] = None) -> bool:
    """Return True if the pipeline should be checked right now.

    *pipeline_cfg* is a PipelineConfig (or any object with an optional
    ``schedule`` attribute that is a dict or None).
    """
    raw = getattr(pipeline_cfg, "schedule", None)
    if raw is None:
        return True
    window = parse_schedule(raw)
    return window.is_active(now)
