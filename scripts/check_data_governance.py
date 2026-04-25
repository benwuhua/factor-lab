#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.data_governance import (
    build_data_governance_report,
    load_data_governance_config,
    write_data_governance_report,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Check point-in-time data governance coverage for research domains.")
    parser.add_argument("--config", default="configs/data_governance.yaml")
    parser.add_argument("--as-of-date", default="")
    parser.add_argument("--output", default="reports/data_governance_{run_yyyymmdd}.md")
    parser.add_argument("--project-root", default=str(default_root))
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    as_of_date = args.as_of_date or "unknown"
    config = load_data_governance_config(_resolve(root, args.config))
    report = build_data_governance_report(
        config,
        project_root=root,
        as_of_date=None if as_of_date == "unknown" else as_of_date,
    )
    output = write_data_governance_report(report, _resolve(root, _materialize(args.output, as_of_date)))
    print(report.to_frame().to_string(index=False))
    print(f"wrote: {output}")
    return 0 if report.passed else 1


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    return candidate if candidate.is_absolute() else root / candidate


def _materialize(path: str | Path, as_of_date: str) -> Path:
    yyyymmdd = str(as_of_date).replace("-", "")
    return Path(str(path).format(as_of_date=as_of_date, run_yyyymmdd=yyyymmdd))


if __name__ == "__main__":
    raise SystemExit(main())
