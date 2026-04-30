#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import shutil
import sys
from pathlib import Path

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.akshare_data import (
    dump_csvs_to_qlib,
    fetch_universe_symbols,
    read_latest_qlib_calendar_date,
    today_for_daily_data,
    write_instrument_alias,
    write_provider_config,
)
from qlib_factor_lab.tushare_data import download_tushare_history_csvs, resolve_latest_tushare_daily_date


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Build a current CN daily Qlib dataset from Tushare Pro.")
    parser.add_argument("--universe", default="csi500", choices=["csi300", "csi500"])
    parser.add_argument("--start", default="20150101", help="Start date in YYYYMMDD format.")
    parser.add_argument("--end", default=today_for_daily_data().replace("-", ""), help="Requested end date in YYYYMMDD format.")
    parser.add_argument("--source-dir", default=str(root / "data/tushare/source"))
    parser.add_argument("--qlib-dir", default=str(root / "data/qlib/cn_data_current"))
    parser.add_argument("--provider-config", default=str(root / "configs/provider_current.yaml"))
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--limit", type=int, default=None, help="Download only first N symbols for smoke tests.")
    parser.add_argument("--skip-download", action="store_true", help="Reuse existing source CSV files.")
    parser.add_argument("--skip-dump", action="store_true", help="Only download normalized CSV files.")
    parser.add_argument("--max-workers", type=int, default=4, help="Workers for Qlib dump_bin conversion.")
    parser.add_argument("--full-rebuild", action="store_true", help="Rebuild the Qlib directory from scratch instead of appending new dates.")
    parser.add_argument(
        "--no-resolve-end-date",
        action="store_true",
        help="Use --end directly instead of falling back to latest Tushare daily date with rows.",
    )
    args = parser.parse_args()

    fallback_qlib_dir = root / "data/qlib/cn_data"
    spec = fetch_universe_symbols(args.universe, fallback_qlib_dir=fallback_qlib_dir)
    symbols = spec.symbols[: args.limit] if args.limit else spec.symbols
    provider_qlib_dir = Path(args.qlib_dir).expanduser()
    qlib_dir = provider_qlib_dir.resolve()
    requested_end = _yyyymmdd(args.end)
    effective_end = requested_end if args.no_resolve_end_date else resolve_latest_tushare_daily_date(requested_end)
    existing_latest = read_latest_qlib_calendar_date(qlib_dir)
    incremental = bool(existing_latest) and not args.full_rebuild
    effective_start = _yyyymmdd(args.start)
    if incremental:
        effective_start = _next_yyyymmdd(existing_latest)
    print(f"universe: {spec.name}")
    print(f"benchmark: {spec.benchmark}")
    print(f"symbols: {len(symbols)} of {len(spec.symbols)}")
    print(f"requested end: {requested_end}")
    print(f"effective end: {effective_end}")
    print(f"date range: {effective_start} -> {effective_end}")
    print(f"update mode: {'incremental' if incremental else 'full'}")
    if existing_latest:
        print(f"existing latest calendar: {existing_latest}")

    if incremental and effective_start > effective_end:
        print("dataset is already up to date for effective end; skip download and dump")
    elif not args.skip_download:
        source_dir = _effective_source_dir(args.source_dir, spec.name, effective_start, effective_end, incremental)
        if incremental and source_dir.exists():
            shutil.rmtree(source_dir)
        paths = download_tushare_history_csvs(
            symbols,
            source_dir,
            start=effective_start,
            end=effective_end,
            delay=args.delay,
            limit=None,
        )
        if not paths:
            raise SystemExit("no Tushare histories were downloaded")
    else:
        source_dir = Path(args.source_dir).expanduser().resolve()

    if not args.skip_dump and not (incremental and effective_start > effective_end):
        dump_csvs_to_qlib(
            source_dir,
            qlib_dir,
            root / "data/source/qlib/scripts/dump_bin.py",
            python_bin=sys.executable,
            max_workers=args.max_workers,
            update=incremental,
        )
    if not args.skip_dump:
        write_instrument_alias(qlib_dir, spec.name)
    latest_after_dump = read_latest_qlib_calendar_date(qlib_dir) or _yyyy_mm_dd(effective_end)
    write_provider_config(
        args.provider_config,
        provider_qlib_dir,
        market=spec.name,
        benchmark=spec.benchmark,
        start_time=f"{args.start[0:4]}-{args.start[4:6]}-{args.start[6:8]}",
        end_time=latest_after_dump,
    )
    print(f"wrote provider config: {args.provider_config}")
    return 0


def _yyyymmdd(value: str) -> str:
    text = str(value).replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"date must be YYYYMMDD or YYYY-MM-DD: {value}")
    return text


def _yyyy_mm_dd(value: str) -> str:
    text = _yyyymmdd(value)
    return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"


def _next_yyyymmdd(value: str) -> str:
    current = dt.datetime.strptime(_yyyymmdd(value), "%Y%m%d").date()
    return (current + dt.timedelta(days=1)).strftime("%Y%m%d")


def _effective_source_dir(source_dir: str, universe: str, start: str, end: str, incremental: bool) -> Path:
    base = Path(source_dir).expanduser().resolve()
    if not incremental:
        return base
    return base / f"{universe}_{start}_{end}"


if __name__ == "__main__":
    raise SystemExit(main())
