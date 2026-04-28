#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.akshare_data import today_for_daily_data
from qlib_factor_lab.data_update import DailyDataUpdateConfig, run_daily_data_update, write_update_manifest


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Incrementally refresh market data, research data domains, and governance reports.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--as-of-date", default=today_for_daily_data())
    parser.add_argument("--skip-market-data", action="store_true")
    parser.add_argument("--skip-research-context", action="store_true")
    parser.add_argument("--fetch-fundamentals", action="store_true")
    parser.add_argument("--fundamental-source", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--manifest", default=None)
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    config = DailyDataUpdateConfig(
        project_root=root,
        as_of_date=args.as_of_date,
        skip_market_data=args.skip_market_data,
        skip_research_context=args.skip_research_context,
        fetch_fundamentals=args.fetch_fundamentals,
        fundamental_source=Path(args.fundamental_source) if args.fundamental_source else None,
        limit=args.limit,
        delay=args.delay,
    )
    rows = run_daily_data_update(config, dry_run=args.dry_run)
    manifest = Path(args.manifest) if args.manifest else root / "reports" / f"daily_data_update_{args.as_of_date.replace('-', '')}.md"
    manifest_path = write_update_manifest(manifest, as_of_date=args.as_of_date, rows=rows)
    print(f"wrote: {manifest_path}")
    return 0 if rows and all(row[1] in {"pass", "dry_run"} for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
