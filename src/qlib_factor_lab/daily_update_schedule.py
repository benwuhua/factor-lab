from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


def next_run_at(now: datetime, *, run_time: str = "15:45", timezone: str = "Asia/Shanghai") -> datetime:
    tzinfo = ZoneInfo(timezone)
    local_now = now.astimezone(tzinfo) if now.tzinfo is not None else now.replace(tzinfo=tzinfo)
    hour, minute = _parse_run_time(run_time)
    candidate = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if local_now >= candidate:
        candidate = candidate + timedelta(days=1)
    return candidate


def build_daily_update_command(
    *,
    python_bin: str = ".venv/bin/python",
    as_of_date: str | None = None,
    env_file: str | Path | None = ".env",
    fetch_fundamentals: bool = True,
    derive_valuation_fields: bool = True,
    fetch_cninfo_dividends: bool = False,
    dividend_provider: str = "tushare",
    fetch_disclosure_events: bool = True,
) -> tuple[str, ...]:
    command = [python_bin, "scripts/update_daily_data.py"]
    if as_of_date:
        command.extend(["--as-of-date", str(as_of_date)])
    if fetch_fundamentals:
        command.append("--fetch-fundamentals")
    if derive_valuation_fields:
        command.append("--derive-valuation-fields")
    if fetch_cninfo_dividends:
        command.append("--fetch-cninfo-dividends")
    command.extend(["--dividend-provider", str(dividend_provider)])
    if dividend_provider:
        command.append("--fetch-dividends")
    if fetch_disclosure_events:
        command.append("--fetch-disclosure-events")
    if env_file:
        command.extend(["--env-file", str(env_file)])
    return tuple(command)


def _parse_run_time(value: str) -> tuple[int, int]:
    hour_text, minute_text = str(value).split(":", 1)
    hour = int(hour_text)
    minute = int(minute_text)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"run_time must be HH:MM: {value}")
    return hour, minute
