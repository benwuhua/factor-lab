from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


COMPANY_EVENT_COLUMNS = [
    "event_id",
    "instrument",
    "event_type",
    "event_date",
    "source",
    "source_url",
    "title",
    "severity",
    "summary",
    "evidence",
    "active_until",
]

EVENT_RISK_SNAPSHOT_COLUMNS = [
    "date",
    "instrument",
    "event_count",
    "event_blocked",
    "max_event_severity",
    "active_event_types",
    "event_risk_summary",
    "event_source_urls",
]

_SEVERITY_RANK = {"info": 0, "watch": 1, "risk": 2, "block": 3}


@dataclass
class EventRiskConfig:
    events_path: Path | None = None
    security_master_path: Path | None = None
    default_lookback_days: int = 30
    event_type_lookbacks: dict[str, int] = field(default_factory=dict)
    block_event_types: tuple[str, ...] = (
        "disciplinary_action",
        "delisting_risk",
        "trading_suspension",
        "st_status",
    )
    block_severities: tuple[str, ...] = ("block",)
    max_events_per_name: int = 3


def load_event_risk_config(path: str | Path | None) -> EventRiskConfig:
    if path is None:
        return EventRiskConfig()

    config_path = Path(path)
    if not config_path.exists():
        return EventRiskConfig()

    with config_path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}

    raw_config = loaded.get("event_risk", loaded)
    return EventRiskConfig(
        events_path=_optional_path(raw_config.get("events_path")),
        security_master_path=_optional_path(raw_config.get("security_master_path")),
        default_lookback_days=int(raw_config.get("default_lookback_days", 30)),
        event_type_lookbacks={
            str(event_type): int(days)
            for event_type, days in (raw_config.get("event_type_lookbacks") or {}).items()
        },
        block_event_types=tuple(raw_config.get("block_event_types", EventRiskConfig().block_event_types)),
        block_severities=tuple(raw_config.get("block_severities", EventRiskConfig().block_severities)),
        max_events_per_name=int(raw_config.get("max_events_per_name", 3)),
    )


def load_company_events(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    events_path = Path(path)
    if not events_path.exists():
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    return pd.read_csv(events_path)


def build_event_risk_snapshot(
    signal: pd.DataFrame,
    events: pd.DataFrame,
    config: EventRiskConfig,
) -> pd.DataFrame:
    event_frame = _with_required_columns(events, COMPANY_EVENT_COLUMNS)
    snapshots = []

    for _, signal_row in signal.iterrows():
        active_events = _active_events_for_signal(signal_row, event_frame, config)
        snapshots.append(_snapshot_row(signal_row, active_events, config))

    return pd.DataFrame(snapshots, columns=EVENT_RISK_SNAPSHOT_COLUMNS)


def _active_events_for_signal(
    signal_row: pd.Series,
    events: pd.DataFrame,
    config: EventRiskConfig,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    signal_date = pd.to_datetime(signal_row["date"])
    matching_events = events[events["instrument"] == signal_row["instrument"]].copy()
    if matching_events.empty:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    active_mask = []
    for _, event_row in matching_events.iterrows():
        event_date = pd.to_datetime(event_row.get("event_date"))
        if pd.isna(event_date) or event_date > signal_date:
            active_mask.append(False)
            continue

        active_until = _event_active_until(event_row, config)
        active_mask.append(not pd.isna(active_until) and signal_date <= active_until)

    active = matching_events.loc[active_mask].copy()
    if active.empty:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    active["_event_date_sort"] = pd.to_datetime(active["event_date"])
    return active.sort_values("_event_date_sort", kind="mergesort").drop(columns=["_event_date_sort"])


def _snapshot_row(
    signal_row: pd.Series,
    active_events: pd.DataFrame,
    config: EventRiskConfig,
) -> dict[str, Any]:
    if active_events.empty:
        return {
            "date": signal_row["date"],
            "instrument": signal_row["instrument"],
            "event_count": 0,
            "event_blocked": False,
            "max_event_severity": "",
            "active_event_types": "",
            "event_risk_summary": "",
            "event_source_urls": "",
        }

    return {
        "date": signal_row["date"],
        "instrument": signal_row["instrument"],
        "event_count": int(len(active_events)),
        "event_blocked": _has_blocking_event(active_events, config),
        "max_event_severity": _max_event_severity(active_events),
        "active_event_types": _join_unique(active_events["event_type"]),
        "event_risk_summary": _event_summary(active_events, config.max_events_per_name),
        "event_source_urls": _join_unique(active_events["source_url"]),
    }


def _event_active_until(event_row: pd.Series, config: EventRiskConfig) -> pd.Timestamp:
    active_until = event_row.get("active_until")
    if not _is_blank(active_until):
        return pd.to_datetime(active_until)

    event_date = pd.to_datetime(event_row.get("event_date"))
    event_type = _clean(event_row.get("event_type"))
    lookback_days = int(config.event_type_lookbacks.get(event_type, config.default_lookback_days))
    return event_date + pd.Timedelta(days=lookback_days)


def _has_blocking_event(events: pd.DataFrame, config: EventRiskConfig) -> bool:
    block_severities = {_clean(severity) for severity in config.block_severities}
    block_event_types = {_clean(event_type) for event_type in config.block_event_types}

    for _, event_row in events.iterrows():
        if _clean(event_row.get("severity")) in block_severities:
            return True
        if _clean(event_row.get("event_type")) in block_event_types:
            return True
    return False


def _max_event_severity(events: pd.DataFrame) -> str:
    severities = [_clean(severity) for severity in events["severity"] if not _is_blank(severity)]
    if not severities:
        return ""

    return max(severities, key=lambda severity: _SEVERITY_RANK.get(severity, -1))


def _event_summary(events: pd.DataFrame, max_events: int) -> str:
    entries = []
    for _, event_row in events.head(max_events).iterrows():
        parts = [
            _clean(event_row.get("event_type")),
            _clean(event_row.get("title")),
            _clean(event_row.get("summary")),
        ]
        entries.append(" | ".join(part for part in parts if part))
    return "; ".join(entry for entry in entries if entry)


def _with_required_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    prepared = frame.copy()
    for column in columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    return prepared


def _join_unique(values: pd.Series) -> str:
    seen = set()
    items = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        items.append(item)
    return "; ".join(items)


def _optional_path(value: Any) -> Path | None:
    if _is_blank(value):
        return None
    return Path(str(value))


def _clean(value: Any) -> str:
    if _is_blank(value):
        return ""
    return str(value).strip()


def _is_blank(value: Any) -> bool:
    return pd.isna(value) or str(value).strip() == ""
