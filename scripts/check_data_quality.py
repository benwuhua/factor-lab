#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.data_quality import check_signal_quality, load_data_quality_config, write_quality_report


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Check daily signal data quality before portfolio construction.")
    parser.add_argument("--signal-csv", required=True, help="Daily signal CSV.")
    parser.add_argument("--config", default="configs/trading.yaml", help="YAML containing data_quality settings.")
    parser.add_argument("--project-root", default=str(default_root), help="Project root used to resolve relative paths.")
    parser.add_argument("--output", default="reports/data_quality_{run_yyyymmdd}.md", help="Markdown report output.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    signal = pd.read_csv(_resolve(root, args.signal_csv))
    config = load_data_quality_config(_resolve(root, args.config))
    report = check_signal_quality(signal, config)
    run_date = str(signal["date"].max()) if "date" in signal.columns and not signal.empty else "unknown"
    output = write_quality_report(report, _resolve(root, _materialize(args.output, run_date)))
    print(report.to_frame().to_string(index=False))
    print(f"wrote: {output}")
    return 0 if report.passed else 1


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


def _materialize(path: str | Path, run_date: str) -> Path:
    yyyymmdd = run_date.replace("-", "")
    return Path(str(path).format(run_date=run_date, run_yyyymmdd=yyyymmdd))


if __name__ == "__main__":
    raise SystemExit(main())
