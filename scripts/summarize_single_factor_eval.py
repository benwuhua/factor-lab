#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import yaml

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.factor_diagnostics import (
    build_single_factor_diagnostics,
    write_single_factor_diagnostics,
    write_single_factor_diagnostics_markdown,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Summarize raw and neutralized single-factor evaluation results.")
    parser.add_argument("--raw-eval", required=True, help="Raw factor evaluation CSV.")
    parser.add_argument("--neutral-eval", required=True, help="Size-neutral factor evaluation CSV.")
    parser.add_argument("--approved-factors", default=None, help="Optional approved_factors.yaml for family metadata.")
    parser.add_argument("--focus-horizon", type=int, default=20)
    parser.add_argument("--output-csv", default="reports/single_factor_diagnostics.csv")
    parser.add_argument("--output-md", default="reports/single_factor_diagnostics.md")
    parser.add_argument("--project-root", default=str(default_root), help="Project root used to resolve relative paths.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    raw_eval = pd.read_csv(_resolve(root, args.raw_eval))
    neutral_eval = pd.read_csv(_resolve(root, args.neutral_eval))
    metadata = _load_approved_metadata(_resolve(root, args.approved_factors)) if args.approved_factors else None
    diagnostics = build_single_factor_diagnostics(
        raw_eval,
        neutral_eval,
        metadata=metadata,
        focus_horizon=args.focus_horizon,
    )
    csv_path = write_single_factor_diagnostics(diagnostics, _resolve(root, args.output_csv))
    md_path = write_single_factor_diagnostics_markdown(diagnostics, _resolve(root, args.output_md))
    print(diagnostics.to_string(index=False))
    print(f"wrote: {csv_path}")
    print(f"wrote: {md_path}")
    return 0


def _load_approved_metadata(path: Path) -> pd.DataFrame:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows = []
    for raw in data.get("approved_factors", []):
        rows.append(
            {
                "factor": str(raw.get("name", "")),
                "family": str(raw.get("family", "")),
                "approval_status": str(raw.get("approval_status", "")),
            }
        )
    return pd.DataFrame(rows)


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
