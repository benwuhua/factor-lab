#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.emotion_atmosphere import build_emotion_atmosphere, write_emotion_atmosphere


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build daily emotion/atmosphere features from liquidity microstructure CSV.")
    parser.add_argument("--liquidity-csv", required=True, help="Input liquidity_microstructure CSV.")
    parser.add_argument("--start-date", default=None, help="Inclusive start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Inclusive end date, YYYY-MM-DD.")
    parser.add_argument("--output", default="data/emotion_atmosphere.csv", help="Output CSV path.")
    parser.add_argument("--merge-existing", action="store_true", help="Append/merge into an existing output CSV.")
    parser.add_argument("--project-root", default=str(default_root), help="Project root used to resolve relative paths.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    liquidity = pd.read_csv(_resolve(root, args.liquidity_csv))
    atmosphere = build_emotion_atmosphere(liquidity, start_date=args.start_date, end_date=args.end_date)
    output = write_emotion_atmosphere(atmosphere, _resolve(root, args.output), merge_existing=args.merge_existing)
    print(f"wrote: {_resolve(root, args.output)} ({len(output)} rows)")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
