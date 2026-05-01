#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.akshare_data import today_for_daily_data
from qlib_factor_lab.research_data_domains import write_research_data_domains


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build fundamental, shareholder-capital, and announcement evidence data domains.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--as-of-date", default=today_for_daily_data())
    parser.add_argument("--fundamental-source", default=None, help="Optional offline raw fundamental CSV to normalize.")
    parser.add_argument("--security-master-history-source", default=None, help="Optional PIT security master history CSV from a licensed vendor.")
    parser.add_argument("--fetch-fundamentals", action="store_true", help="Fetch fundamental quality data from the configured provider.")
    parser.add_argument("--fundamental-provider", default="tushare", choices=["akshare", "tushare"])
    parser.add_argument("--derive-valuation-fields", action="store_true", help="Derive EP/CFP/dividend yield from PIT close prices and dividend records.")
    parser.add_argument("--fetch-cninfo-dividends", action="store_true", help="Fetch dividend records from CNINFO via AkShare.")
    parser.add_argument("--evidence-lookback-days", type=int, default=180, help="Rolling announcement evidence window. Use a negative value to keep all visible events.")
    parser.add_argument("--limit", type=int, default=None, help="Limit instruments for smoke tests.")
    parser.add_argument("--offset", type=int, default=0, help="Skip the first N instruments for batched refreshes.")
    parser.add_argument("--delay", type=float, default=0.2)
    args = parser.parse_args()

    manifest = write_research_data_domains(
        Path(args.project_root),
        as_of_date=args.as_of_date,
        fundamental_source=args.fundamental_source,
        security_master_history_source=args.security_master_history_source,
        fetch_fundamentals=args.fetch_fundamentals,
        fundamental_provider=args.fundamental_provider,
        derive_valuation_fields=args.derive_valuation_fields,
        fetch_cninfo_dividends=args.fetch_cninfo_dividends,
        evidence_lookback_days=None if args.evidence_lookback_days < 0 else args.evidence_lookback_days,
        limit=args.limit,
        offset=args.offset,
        delay=args.delay,
    )
    for key, path in manifest.items():
        print(f"{key}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
