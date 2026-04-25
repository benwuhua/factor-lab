#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.akshare_data import (
    fetch_company_notices,
    fetch_security_master_snapshot,
    fetch_universe_symbols,
    filter_frame_to_universes,
    load_universe_symbols_csv,
    normalize_akshare_notices,
    normalize_security_master_snapshot,
    today_for_daily_data,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build security master and company event CSVs for daily research risk.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--as-of-date", default=today_for_daily_data())
    parser.add_argument("--notice-start", default=None, help="Notice start date, YYYY-MM-DD or YYYYMMDD. Defaults to as-of date.")
    parser.add_argument("--notice-end", default=None, help="Notice end date, YYYY-MM-DD or YYYYMMDD. Defaults to as-of date.")
    parser.add_argument("--security-master-output", default="data/security_master.csv")
    parser.add_argument("--company-events-output", default="data/company_events.csv")
    parser.add_argument("--universes", nargs="+", default=["csi300", "csi500"], choices=["csi300", "csi500"])
    parser.add_argument("--universe-symbols-csv", default=None, help="Optional CSV with universe,instrument columns.")
    parser.add_argument("--security-master-source-csv", default=None, help="Offline raw security CSV to normalize instead of AkShare.")
    parser.add_argument("--notice-source-csv", default=None, help="Offline raw notice CSV to normalize instead of AkShare.")
    parser.add_argument("--skip-security-master", action="store_true")
    parser.add_argument("--skip-company-events", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="Limit security rows for smoke tests.")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between AkShare notice-date calls.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    universe_symbols = _load_universe_symbols(root, args)
    if not args.skip_security_master:
        if args.security_master_source_csv:
            raw_master = pd.read_csv(_resolve(root, args.security_master_source_csv))
            master = normalize_security_master_snapshot(raw_master, as_of_date=args.as_of_date)
        else:
            master = fetch_security_master_snapshot(args.as_of_date, limit=args.limit)
        master = filter_frame_to_universes(master, universe_symbols)
        _write_csv(master, _resolve(root, args.security_master_output))

    if not args.skip_company_events:
        if args.notice_source_csv:
            raw_notices = pd.read_csv(_resolve(root, args.notice_source_csv))
            events = normalize_akshare_notices(raw_notices)
        else:
            notice_start = _normalize_date_arg(args.notice_start or args.as_of_date)
            notice_end = _normalize_date_arg(args.notice_end or args.as_of_date)
            events = fetch_company_notices(notice_start, notice_end, delay=args.delay)
        events = filter_frame_to_universes(events, universe_symbols)
        _write_csv(events, _resolve(root, args.company_events_output))

    return 0


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    print(f"wrote: {path}")


def _load_universe_symbols(root: Path, args: argparse.Namespace) -> dict[str, list[str]]:
    if args.universe_symbols_csv:
        loaded = load_universe_symbols_csv(_resolve(root, args.universe_symbols_csv))
        return {universe: symbols for universe, symbols in loaded.items() if universe in set(args.universes)}
    fallback_qlib_dir = root / "data/qlib/cn_data"
    return {
        universe: fetch_universe_symbols(universe, fallback_qlib_dir=fallback_qlib_dir).symbols
        for universe in args.universes
    }


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


def _normalize_date_arg(value: str) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


if __name__ == "__main__":
    raise SystemExit(main())
