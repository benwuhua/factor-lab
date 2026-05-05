from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
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
    "event_classes",
    "event_actions",
    "positive_event_types",
    "positive_event_summary",
    "risk_event_types",
    "risk_event_summary",
    "event_risk_summary",
    "event_source_urls",
]

_SEVERITY_RANK = {"info": 0, "watch": 1, "risk": 2, "block": 3}
_POSITIVE_CATALYSTS = {
    "buyback",
    "shareholder_increase",
    "order_contract",
    "earnings_preannouncement_up",
    "equity_incentive",
}
_WATCH_RISKS = {
    "shareholder_reduction",
    "large_unlock",
    "regulatory_inquiry",
    "pledge_risk",
    "guarantee",
    "lawsuit",
}
_BLOCK_RISKS = {
    "disciplinary_action",
    "investigation",
    "st_risk",
    "delisting_risk",
    "nonstandard_audit",
    "major_penalty",
}


def _default_event_taxonomy() -> dict[str, dict[str, str]]:
    taxonomy: dict[str, dict[str, str]] = {}
    for event_type in _POSITIVE_CATALYSTS:
        taxonomy[event_type] = {
            "event_class": "positive_catalyst",
            "default_severity": "info",
            "portfolio_action": "boost",
        }
    taxonomy["financial_report_disclosure"] = {
        "event_class": "information_event",
        "default_severity": "info",
        "portfolio_action": "review",
    }
    for event_type in _WATCH_RISKS:
        taxonomy[event_type] = {
            "event_class": "watch_risk",
            "default_severity": "watch",
            "portfolio_action": "watch",
        }
    for event_type in _BLOCK_RISKS:
        taxonomy[event_type] = {
            "event_class": "block_risk",
            "default_severity": "block",
            "portfolio_action": "block",
        }
    return taxonomy


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
    event_taxonomy: dict[str, dict[str, str]] = field(default_factory=_default_event_taxonomy)


def classify_event_type(event_type: str, taxonomy: dict[str, dict[str, str]] | None = None) -> dict[str, str]:
    normalized = _clean(event_type)
    lookup = taxonomy or _default_event_taxonomy()
    if normalized in lookup:
        entry = lookup[normalized]
        return {
            "event_class": str(entry.get("event_class", "watch_risk")),
            "default_severity": str(entry.get("default_severity", "watch")),
            "portfolio_action": str(entry.get("portfolio_action", "watch")),
        }
    return {
        "event_class": "watch_risk",
        "default_severity": "watch",
        "portfolio_action": "watch",
    }


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
        event_taxonomy=_event_taxonomy_from_config(raw_config.get("event_taxonomy")),
    )


def load_company_events(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    events_path = Path(path)
    if not events_path.exists():
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    return pd.read_csv(events_path, low_memory=False)


def build_event_risk_snapshot(
    signal: pd.DataFrame,
    events: pd.DataFrame,
    config: EventRiskConfig,
) -> pd.DataFrame:
    event_frame = _with_required_columns(events, COMPANY_EVENT_COLUMNS)
    event_frame = _with_event_taxonomy(event_frame, config)
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

    signal_date = _calendar_date(signal_row["date"])
    if signal_date is None:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    matching_events = events[events["instrument"] == signal_row["instrument"]].copy()
    if matching_events.empty:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    active_mask = []
    for _, event_row in matching_events.iterrows():
        event_date = _calendar_date(event_row.get("event_date"))
        if event_date is None or event_date > signal_date:
            active_mask.append(False)
            continue

        active_until = _event_active_until_date(event_row, config)
        active_mask.append(active_until is not None and signal_date <= active_until)

    active = matching_events.loc[active_mask].copy()
    if active.empty:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

    active["_event_date_sort"] = active["event_date"].map(_calendar_date)
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
            "event_classes": "",
            "event_actions": "",
            "positive_event_types": "",
            "positive_event_summary": "",
            "risk_event_types": "",
            "risk_event_summary": "",
            "event_risk_summary": "",
            "event_source_urls": "",
        }

    positive_events = active_events[active_events["event_class"] == "positive_catalyst"]
    risk_events = active_events[active_events["event_class"] != "positive_catalyst"]

    return {
        "date": signal_row["date"],
        "instrument": signal_row["instrument"],
        "event_count": int(len(active_events)),
        "event_blocked": _has_blocking_event(active_events, config),
        "max_event_severity": _max_event_severity(active_events),
        "active_event_types": _join_unique(active_events["event_type"]),
        "event_classes": _join_unique(active_events["event_class"]),
        "event_actions": _join_unique(active_events["event_action"]),
        "positive_event_types": _join_unique(positive_events["event_type"]),
        "positive_event_summary": _event_summary(positive_events, config.max_events_per_name),
        "risk_event_types": _join_unique(risk_events["event_type"]),
        "risk_event_summary": _event_summary(risk_events, config.max_events_per_name),
        "event_risk_summary": _event_summary(active_events, config.max_events_per_name),
        "event_source_urls": _join_unique(active_events["source_url"]),
    }


def _event_active_until_date(event_row: pd.Series, config: EventRiskConfig) -> date | None:
    active_until = event_row.get("active_until")
    if not _is_blank(active_until):
        return _calendar_date(active_until)

    event_date = _calendar_date(event_row.get("event_date"))
    if event_date is None:
        return None

    event_type = _clean(event_row.get("event_type"))
    lookback_days = int(config.event_type_lookbacks.get(event_type, config.default_lookback_days))
    return event_date + timedelta(days=lookback_days)


def _has_blocking_event(events: pd.DataFrame, config: EventRiskConfig) -> bool:
    block_severities = {_clean(severity) for severity in config.block_severities}
    block_event_types = {_clean(event_type) for event_type in config.block_event_types}

    for _, event_row in events.iterrows():
        if _clean(event_row.get("severity")) in block_severities:
            return True
        if _clean(event_row.get("event_action")) == "block":
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


def _with_event_taxonomy(events: pd.DataFrame, config: EventRiskConfig) -> pd.DataFrame:
    prepared = events.copy()
    classes = []
    actions = []
    severities = []
    for _, event_row in prepared.iterrows():
        taxonomy = classify_event_type(event_row.get("event_type"), config.event_taxonomy)
        classes.append(taxonomy["event_class"])
        actions.append(taxonomy["portfolio_action"])
        explicit_severity = _clean(event_row.get("severity"))
        severities.append(explicit_severity or taxonomy["default_severity"])

    prepared["event_class"] = classes
    prepared["event_action"] = actions
    prepared["severity"] = severities
    return prepared


def _event_taxonomy_from_config(raw: Any) -> dict[str, dict[str, str]]:
    taxonomy = _default_event_taxonomy()
    if not isinstance(raw, dict):
        return taxonomy
    for event_class, value in raw.items():
        if not isinstance(value, dict):
            continue
        default_severity = str(value.get("default_severity", "watch"))
        portfolio_action = str(value.get("portfolio_action", "watch"))
        for event_type in value.get("event_types") or []:
            normalized = _clean(event_type)
            if not normalized:
                continue
            taxonomy[normalized] = {
                "event_class": str(event_class),
                "default_severity": default_severity,
                "portfolio_action": portfolio_action,
            }
    return taxonomy


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


def _calendar_date(value: Any) -> date | None:
    if _is_blank(value):
        return None

    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return None
    return timestamp.date()


def _clean(value: Any) -> str:
    if _is_blank(value):
        return ""
    return str(value).strip()


def _is_blank(value: Any) -> bool:
    return pd.isna(value) or str(value).strip() == ""
