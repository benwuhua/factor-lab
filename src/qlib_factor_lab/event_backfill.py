from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class EventBackfillConfig:
    project_root: Path
    as_of_date: str
    days: int = 90
    universes: tuple[str, ...] = ("csi300", "csi500")
    output: str = "data/company_events.csv"
    delay: float = 0.2


def event_backfill_window(as_of_date: str, *, days: int) -> tuple[str, str]:
    if int(days) <= 0:
        raise ValueError("days must be positive")
    end = pd.Timestamp(as_of_date).normalize()
    start = end - pd.Timedelta(days=int(days) - 1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def build_event_backfill_command(config: EventBackfillConfig) -> tuple[str, ...]:
    start, end = event_backfill_window(config.as_of_date, days=config.days)
    return (
        sys.executable,
        "scripts/build_research_context_data.py",
        "--project-root",
        str(Path(config.project_root).expanduser().resolve()),
        "--as-of-date",
        end,
        "--notice-start",
        start,
        "--notice-end",
        end,
        "--company-events-output",
        config.output,
        "--universes",
        *tuple(str(universe) for universe in config.universes),
        "--merge-existing-events",
        "--delay",
        str(config.delay),
    )
