#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.company_events import (
    build_event_risk_snapshot,
    load_company_events,
    load_event_risk_config,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build a daily event-risk snapshot from signal and event CSVs.")
    parser.add_argument("--signals", default=None, help="Daily signal CSV.")
    parser.add_argument("--event-risk-config", default="configs/event_risk.yaml", help="Event risk config YAML.")
    parser.add_argument("--events", default=None, help="Optional override for the company events CSV.")
    parser.add_argument("--output", default=None, help="Output snapshot CSV path.")
    parser.add_argument("--project-root", default=str(default_root), help="Project root used to resolve relative paths.")
    args = parser.parse_args()

    if args.signals is None:
        parser.error("--signals is required")

    root = Path(args.project_root).expanduser().resolve()
    signals = pd.read_csv(_resolve(root, args.signals))
    config = load_event_risk_config(_resolve(root, args.event_risk_config))

    events_path = Path(args.events) if args.events is not None else config.events_path
    if events_path is not None:
        events_path = _resolve(root, events_path)
        config = replace(config, events_path=events_path)

    events = load_company_events(events_path)
    snapshot = build_event_risk_snapshot(signals, events, config)

    output = Path(args.output) if args.output is not None else _default_output_path(signals)
    output_path = _resolve(root, _materialize(output, _run_date(signals)))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot.to_csv(output_path, index=False)
    print(f"wrote: {output_path}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


def _default_output_path(signals: pd.DataFrame) -> Path:
    return Path("reports") / f"event_risk_snapshot_{_yyyymmdd(_run_date(signals))}.csv"


def _materialize(path: str | Path, run_date: str) -> Path:
    return Path(str(path).format(run_date=run_date, run_yyyymmdd=_yyyymmdd(run_date)))


def _run_date(signals: pd.DataFrame) -> str:
    if "date" not in signals.columns or signals.empty:
        return "unknown"

    max_date = pd.to_datetime(signals["date"], errors="coerce").max()
    if pd.isna(max_date):
        return "unknown"
    return str(max_date.date())


def _yyyymmdd(run_date: str) -> str:
    try:
        return pd.Timestamp(run_date).strftime("%Y%m%d")
    except (TypeError, ValueError):
        return str(run_date).replace("-", "")


if __name__ == "__main__":
    raise SystemExit(main())
