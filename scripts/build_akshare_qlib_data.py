#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.akshare_data import (
    download_history_csvs,
    dump_csvs_to_qlib,
    fetch_universe_symbols,
    today_for_daily_data,
    write_provider_config,
    write_instrument_alias,
)


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Build a current CN daily Qlib dataset from AkShare.")
    parser.add_argument("--universe", default="csi500", choices=["csi300", "csi500", "csi800", "all"])
    parser.add_argument("--start", default="20150101", help="Start date in YYYYMMDD format.")
    parser.add_argument("--end", default=today_for_daily_data().replace("-", ""), help="End date in YYYYMMDD format.")
    parser.add_argument("--source-dir", default=str(root / "data/akshare/source"))
    parser.add_argument("--qlib-dir", default=str(root / "data/qlib/cn_data_current"))
    parser.add_argument("--provider-config", default=str(root / "configs/provider_current.yaml"))
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"], help="AkShare adjustment mode.")
    parser.add_argument("--history-source", default="sina", choices=["sina", "em", "tx"])
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--limit", type=int, default=None, help="Download only first N symbols for smoke tests.")
    parser.add_argument("--skip-download", action="store_true", help="Reuse existing source CSV files.")
    parser.add_argument("--skip-dump", action="store_true", help="Only download normalized CSV files.")
    parser.add_argument("--max-workers", type=int, default=4, help="Workers for Qlib dump_bin conversion.")
    args = parser.parse_args()

    fallback_qlib_dir = root / "data/qlib/cn_data"
    spec = fetch_universe_symbols(args.universe, fallback_qlib_dir=fallback_qlib_dir)
    symbols = spec.symbols[: args.limit] if args.limit else spec.symbols
    print(f"universe: {spec.name}")
    print(f"benchmark: {spec.benchmark}")
    print(f"symbols: {len(symbols)} of {len(spec.symbols)}")
    print(f"date range: {args.start} -> {args.end}")

    if not args.skip_download:
        paths = download_history_csvs(
            symbols,
            args.source_dir,
            start=args.start,
            end=args.end,
            adjust=args.adjust,
            delay=args.delay,
            limit=None,
            retries=args.retries,
            source=args.history_source,
        )
        if not paths:
            raise SystemExit("no AkShare histories were downloaded")

    end_time = f"{args.end[0:4]}-{args.end[4:6]}-{args.end[6:8]}"
    if not args.skip_dump:
        dump_csvs_to_qlib(
            args.source_dir,
            args.qlib_dir,
            root / "data/source/qlib/scripts/dump_bin.py",
            python_bin=sys.executable,
            max_workers=args.max_workers,
        )
    if not args.skip_dump:
        write_instrument_alias(args.qlib_dir, spec.name)
    write_provider_config(
        args.provider_config,
        args.qlib_dir,
        market=spec.name,
        benchmark=spec.benchmark,
        start_time=f"{args.start[0:4]}-{args.start[4:6]}-{args.start[6:8]}",
        end_time=end_time,
    )
    print(f"wrote provider config: {args.provider_config}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
