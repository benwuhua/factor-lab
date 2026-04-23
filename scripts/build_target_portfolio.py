#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.data_quality import check_signal_quality, load_data_quality_config, write_quality_report
from qlib_factor_lab.portfolio import (
    build_target_portfolio,
    load_portfolio_config,
    write_portfolio_summary,
    write_target_portfolio,
)
from qlib_factor_lab.risk import check_portfolio_risk, load_risk_config, write_risk_report
from qlib_factor_lab.tradability import apply_tradability_filter, load_trading_config


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build a TopK target portfolio from a daily signal.")
    parser.add_argument("--signal-csv", required=True, help="Daily signal CSV from build_daily_signal.py.")
    parser.add_argument("--trading-config", default="configs/trading.yaml")
    parser.add_argument("--portfolio-config", default="configs/portfolio.yaml")
    parser.add_argument("--risk-config", default="configs/risk.yaml")
    parser.add_argument("--project-root", default=str(default_root), help="Project root used to resolve relative paths.")
    parser.add_argument("--current-positions-csv", default=None, help="Optional current positions CSV.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    signal = pd.read_csv(_resolve(root, args.signal_csv))
    run_date = str(signal["date"].max()) if "date" in signal.columns and not signal.empty else "unknown"

    quality_config = load_data_quality_config(_resolve(root, args.trading_config))
    quality_report = check_signal_quality(signal, quality_config)
    write_quality_report(quality_report, _resolve(root, _materialize("reports/data_quality_{run_yyyymmdd}.md", run_date)))
    if not quality_report.passed:
        print(quality_report.to_frame().to_string(index=False))
        return 1

    trading_config = load_trading_config(_resolve(root, args.trading_config))
    portfolio_config = load_portfolio_config(_resolve(root, args.portfolio_config))
    risk_config = load_risk_config(_resolve(root, args.risk_config))
    current_positions = pd.read_csv(_resolve(root, args.current_positions_csv)) if args.current_positions_csv else None

    tradable_signal = apply_tradability_filter(signal, trading_config)
    portfolio = build_target_portfolio(tradable_signal, portfolio_config, current_positions=current_positions)
    risk_report = check_portfolio_risk(portfolio, tradable_signal, risk_config, current_positions=current_positions)

    target_path = write_target_portfolio(
        portfolio,
        _resolve(root, _materialize(portfolio_config.target_output_path, run_date)),
    )
    summary_path = write_portfolio_summary(
        portfolio,
        _resolve(root, _materialize(portfolio_config.summary_output_path, run_date)),
    )
    risk_path = write_risk_report(
        risk_report,
        _resolve(root, _materialize(risk_config.report_output_path, run_date)),
    )
    print(portfolio.to_string(index=False))
    print(risk_report.to_frame().to_string(index=False))
    print(f"wrote: {target_path}")
    print(f"wrote: {summary_path}")
    print(f"wrote: {risk_path}")
    return 0 if risk_report.passed else 1


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


def _materialize(path: str | Path, run_date: str) -> Path:
    yyyymmdd = run_date.replace("-", "")
    return Path(str(path).format(run_date=run_date, run_yyyymmdd=yyyymmdd))


if __name__ == "__main__":
    raise SystemExit(main())
