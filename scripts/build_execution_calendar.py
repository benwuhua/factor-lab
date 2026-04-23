#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.execution_calendar import (
    build_execution_calendar,
    fetch_execution_calendar_features,
    write_execution_calendar,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build A-share execution calendar from daily OHLCV features.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--provider-config", default="configs/provider_current.yaml")
    parser.add_argument("--run-date", default=None)
    parser.add_argument("--features-csv", default=None, help="Optional feature CSV for offline runs/tests.")
    parser.add_argument("--output", default=None)
    parser.add_argument("--limit-up-pct", type=float, default=0.098)
    parser.add_argument("--limit-down-pct", type=float, default=-0.098)
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    if args.features_csv:
        features = pd.read_csv(_resolve(root, args.features_csv))
        run_date = str(features["date"].max()) if "date" in features.columns and not features.empty else "unknown"
    else:
        project_config = load_project_config(_resolve(root, args.provider_config))
        run_date = args.run_date or project_config.end_time
        features = fetch_execution_calendar_features(project_config, run_date)

    calendar = build_execution_calendar(
        features,
        limit_up_pct=args.limit_up_pct,
        limit_down_pct=args.limit_down_pct,
    )
    output = _resolve(root, args.output or f"reports/execution_calendar_{run_date.replace('-', '')}.csv")
    path = write_execution_calendar(calendar, output)
    print(calendar.head(20).to_string(index=False))
    print(f"wrote: {path}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
