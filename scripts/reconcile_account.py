#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.reconcile import load_reconcile_config, reconcile_positions, write_reconciliation_report


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Reconcile expected paper positions against actual positions.")
    parser.add_argument("--expected-positions", required=True)
    parser.add_argument("--actual-positions", required=True)
    parser.add_argument("--execution-config", default="configs/execution.yaml")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    expected_path = _resolve(root, args.expected_positions)
    actual_path = _resolve(root, args.actual_positions)
    expected = pd.read_csv(expected_path)
    actual = pd.read_csv(actual_path)
    config = load_reconcile_config(_resolve(root, args.execution_config))
    report = reconcile_positions(expected, actual, config)
    output = _resolve(root, args.output) if args.output else expected_path.parent / "reconciliation.md"
    report_path = write_reconciliation_report(report, output)
    print(report.to_frame().to_string(index=False))
    print(f"wrote: {report_path}")
    return 0 if report.passed else 1


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
