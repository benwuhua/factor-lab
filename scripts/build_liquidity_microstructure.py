#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.liquidity_microstructure import (
    build_liquidity_microstructure,
    fetch_liquidity_microstructure_features,
    write_liquidity_microstructure,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build persistent liquidity microstructure daily data.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--provider-config", default="configs/provider_current.yaml")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output", default="data/liquidity_microstructure.csv")
    parser.add_argument("--features-csv", default=None, help="Optional feature CSV for offline runs/tests.")
    parser.add_argument("--merge-existing", action="store_true", help="Merge with an existing output CSV by date+instrument.")
    parser.add_argument("--limit-up-pct", type=float, default=0.098)
    parser.add_argument("--limit-down-pct", type=float, default=-0.098)
    parser.add_argument("--preview", action="store_true", help="Print the first rows after building the data domain.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    if args.features_csv:
        features = pd.read_csv(_resolve(root, args.features_csv))
    else:
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date are required unless --features-csv is provided")
        features = fetch_liquidity_microstructure_features(
            _resolve(root, args.provider_config),
            start_date=args.start_date,
            end_date=args.end_date,
        )

    liquidity = build_liquidity_microstructure(
        features,
        limit_up_pct=args.limit_up_pct,
        limit_down_pct=args.limit_down_pct,
    )
    output = _resolve(root, args.output)
    path = write_liquidity_microstructure(liquidity, output, merge_existing=args.merge_existing)
    if args.preview:
        print(liquidity.head(20).to_string(index=False))
    print(f"wrote: {path}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
