#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.portfolio_performance import (  # noqa: E402
    build_intraday_performance,
    fetch_akshare_spot_quotes,
    write_intraday_performance_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build formal intraday performance attribution for an execution portfolio.")
    parser.add_argument("--project-root", default=str(project_root()))
    parser.add_argument("--portfolio", default="", help="Execution portfolio CSV. Defaults to runs/<date>/execution_portfolio.csv.")
    parser.add_argument("--quote-csv", default="", help="Optional quote CSV. If omitted, AkShare A-share spot quotes are fetched.")
    parser.add_argument("--run-date", default="", help="Run date in YYYYMMDD or YYYY-MM-DD format.")
    parser.add_argument("--output-csv", default="", help="Output CSV path.")
    parser.add_argument("--output-md", default="", help="Output Markdown path.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    run_date = _normalize_run_date(args.run_date)
    portfolio_path = _resolve(root, args.portfolio) if args.portfolio else root / "runs" / run_date / "execution_portfolio.csv"
    if not portfolio_path.exists():
        raise FileNotFoundError(f"execution portfolio not found: {portfolio_path}")
    quote_frame = pd.read_csv(_resolve(root, args.quote_csv)) if args.quote_csv else fetch_akshare_spot_quotes()
    portfolio = pd.read_csv(portfolio_path)
    performance = build_intraday_performance(portfolio, quote_frame, run_date=run_date)
    output_csv = _resolve(root, args.output_csv) if args.output_csv else portfolio_path.parent / "portfolio_intraday_performance.csv"
    output_md = _resolve(root, args.output_md) if args.output_md else portfolio_path.parent / "portfolio_intraday_performance.md"
    csv_path, md_path = write_intraday_performance_report(performance, output_csv, output_md)
    reports_csv = root / "reports" / f"portfolio_intraday_performance_{run_date}.csv"
    reports_md = root / "reports" / f"portfolio_intraday_performance_{run_date}.md"
    if csv_path != reports_csv:
        write_intraday_performance_report(performance, reports_csv, reports_md)
    print(f"wrote: {csv_path}")
    print(f"wrote: {md_path}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root / candidate


def _normalize_run_date(value: str) -> str:
    if not str(value or "").strip():
        return pd.Timestamp.today().strftime("%Y%m%d")
    return pd.Timestamp(value).strftime("%Y%m%d")


if __name__ == "__main__":
    raise SystemExit(main())
