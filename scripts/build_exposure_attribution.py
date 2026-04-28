#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.exposure_attribution import (
    build_exposure_attribution,
    load_factor_family_map,
    load_factor_logic_map,
    write_exposure_attribution_csv,
    write_exposure_attribution_markdown,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build exposure attribution reports for a signal or target portfolio CSV.")
    parser.add_argument("--input-csv", required=True, help="Daily signal or target portfolio CSV.")
    parser.add_argument("--approved-factors", default="reports/approved_factors.yaml")
    parser.add_argument("--weight-col", default="target_weight")
    parser.add_argument("--industry-col", default="industry")
    parser.add_argument("--style-col", action="append", default=None, help="Optional style exposure column. Can be repeated.")
    parser.add_argument("--output-dir", default="reports/exposure_attribution")
    parser.add_argument("--markdown-output", default=None)
    parser.add_argument("--prefix", default=None)
    parser.add_argument("--project-root", default=None)
    args = parser.parse_args()

    root = Path(args.project_root).resolve() if args.project_root else project_root()
    input_path = _resolve(root, args.input_csv)
    portfolio = pd.read_csv(input_path)
    family_map = {}
    logic_map = {}
    approved_path = _resolve(root, args.approved_factors)
    if approved_path.exists():
        family_map = load_factor_family_map(approved_path)
        logic_map = load_factor_logic_map(approved_path)

    result = build_exposure_attribution(
        portfolio,
        family_map=family_map,
        logic_map=logic_map,
        weight_col=args.weight_col,
        industry_col=args.industry_col,
        style_cols=args.style_col,
    )
    prefix = args.prefix or input_path.stem
    output_dir = _resolve(root, args.output_dir)
    csv_paths = write_exposure_attribution_csv(result, output_dir, prefix=prefix)
    markdown_path = _resolve(root, args.markdown_output) if args.markdown_output else output_dir / f"{prefix}.md"
    write_exposure_attribution_markdown(result, markdown_path)

    print(result.summary.to_string(index=False))
    for path in csv_paths:
        print(f"wrote: {path}")
    print(f"wrote: {markdown_path}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
