#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.akshare_data import (
    INDUSTRY_OVERRIDE_COLUMNS,
    fetch_security_industry_overrides,
    fetch_universe_symbols,
    load_universe_symbols_csv,
    today_for_daily_data,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build local industry override CSV for CSI300/CSI500 research universe.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--as-of-date", default=today_for_daily_data())
    parser.add_argument("--output", default="data/security_industry_overrides.csv")
    parser.add_argument("--universes", nargs="+", default=["csi300", "csi500"], choices=["csi300", "csi500"])
    parser.add_argument("--universe-symbols-csv", default=None, help="Optional CSV with universe,instrument columns.")
    parser.add_argument("--fallback-qlib-dir", default="data/qlib/cn_data")
    parser.add_argument("--start-date", default="20000101")
    parser.add_argument("--delay", type=float, default=0.1)
    parser.add_argument("--limit", type=int, default=None, help="Limit symbols for smoke tests.")
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--merge-existing", action="store_true", help="Merge with existing output instead of replacing it.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    symbols = _load_symbols(root, args)
    fetched = fetch_security_industry_overrides(
        symbols,
        args.as_of_date,
        start_date=args.start_date,
        delay=args.delay,
        limit=args.limit,
        retries=args.retries,
    )
    output_path = _resolve(root, args.output)
    if args.merge_existing and output_path.exists():
        existing = pd.read_csv(output_path, dtype={"证券代码": str})
        fetched = pd.concat([existing, fetched], ignore_index=True)
    fetched = _dedupe(fetched)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fetched.to_csv(output_path, index=False)
    print(f"symbols: {len(symbols)}")
    print(f"rows: {len(fetched)}")
    print(f"wrote: {output_path}")
    return 0


def _load_symbols(root: Path, args: argparse.Namespace) -> list[str]:
    if args.universe_symbols_csv:
        universe_symbols = load_universe_symbols_csv(_resolve(root, args.universe_symbols_csv))
        selected = []
        for universe in args.universes:
            selected.extend(universe_symbols.get(universe, []))
        return sorted(set(selected))

    fallback_qlib_dir = _resolve(root, args.fallback_qlib_dir)
    selected = []
    for universe in args.universes:
        selected.extend(fetch_universe_symbols(universe, fallback_qlib_dir=fallback_qlib_dir).symbols)
    return sorted(set(selected))


def _dedupe(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=INDUSTRY_OVERRIDE_COLUMNS)
    output = frame.copy()
    output["证券代码"] = output["证券代码"].astype(str).str.extract(r"(\d+)", expand=False).fillna("").str.zfill(6)
    output = output[output["证券代码"].str.len() == 6]
    output = output.drop_duplicates("证券代码", keep="last").sort_values("证券代码")
    return output.loc[:, INDUSTRY_OVERRIDE_COLUMNS]


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
