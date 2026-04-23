#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
from dataclasses import replace
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.historical_paper_batch import (
    BatchPaths,
    build_historical_targets,
    recent_trading_dates,
    run_historical_paper_batch,
    write_historical_batch_summary,
)
from qlib_factor_lab.orders import load_order_config
from qlib_factor_lab.paper_batch import load_paper_batch_config
from qlib_factor_lab.paper_broker import load_paper_fill_config
from qlib_factor_lab.portfolio import load_portfolio_config
from qlib_factor_lab.reconcile import load_reconcile_config
from qlib_factor_lab.risk import load_risk_config
from qlib_factor_lab.signal import load_approved_signal_factors, load_signal_config
from qlib_factor_lab.tradability import load_trading_config


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Generate historical signals, target portfolios, and a paper batch.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--provider-config", default="configs/provider_current.yaml")
    parser.add_argument("--signal-config", default="configs/signal.yaml")
    parser.add_argument("--trading-config", default="configs/trading.yaml")
    parser.add_argument("--portfolio-config", default="configs/portfolio.yaml")
    parser.add_argument("--risk-config", default="configs/risk.yaml")
    parser.add_argument("--execution-config", default="configs/execution.yaml")
    parser.add_argument("--current-positions", default="state/current_positions.csv")
    parser.add_argument("--signal-dir", default="reports/historical_signals")
    parser.add_argument("--target-dir", default="reports/historical_targets")
    parser.add_argument("--target-glob", default=None, help="Use existing target portfolios instead of generating from Qlib.")
    parser.add_argument("--summary-output", default="reports/historical_paper_batch.md")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    execution_path = _resolve(root, args.execution_config)
    current_path = _resolve(root, args.current_positions)
    current_positions = pd.read_csv(current_path) if current_path.exists() else pd.DataFrame(columns=["instrument", "current_weight"])
    batch_config = load_paper_batch_config(execution_path)
    batch_config = replace(
        batch_config,
        max_days=args.days,
        run_root=_resolve(root, batch_config.run_root),
        summary_csv_path=_resolve(root, batch_config.summary_csv_path),
        summary_md_path=_resolve(root, batch_config.summary_md_path),
    )

    if args.target_glob:
        target_paths = sorted(Path(path) for path in glob.glob(str(_resolve(root, args.target_glob))))[: args.days]
        paths = BatchPaths(signal_paths=[], target_paths=target_paths, batch_summary_csv=Path(), batch_summary_md=Path())
    else:
        project_config = load_project_config(_resolve(root, args.provider_config))
        from qlib_factor_lab.qlib_bootstrap import init_qlib

        init_qlib(project_config)
        signal_config = load_signal_config(_resolve(root, args.signal_config))
        if signal_config.execution_calendar_path is not None:
            signal_config = replace(signal_config, execution_calendar_path=_resolve(root, signal_config.execution_calendar_path))
        factors = load_approved_signal_factors(_resolve(root, signal_config.approved_factors_path))
        run_dates = recent_trading_dates(project_config, args.days, args.end_date)
        paths = build_historical_targets(
            project_config,
            factors,
            signal_config,
            load_trading_config(_resolve(root, args.trading_config)),
            load_portfolio_config(_resolve(root, args.portfolio_config)),
            load_risk_config(_resolve(root, args.risk_config)),
            run_dates,
            _resolve(root, args.signal_dir),
            _resolve(root, args.target_dir),
            current_positions=current_positions,
        )
        target_paths = paths.target_paths

    batch_paths = run_historical_paper_batch(
        target_paths,
        current_positions,
        load_order_config(execution_path),
        load_paper_fill_config(execution_path),
        load_reconcile_config(execution_path),
        batch_config,
    )
    paths = BatchPaths(
        signal_paths=paths.signal_paths,
        target_paths=target_paths,
        batch_summary_csv=batch_paths.batch_summary_csv,
        batch_summary_md=batch_paths.batch_summary_md,
    )
    summary = write_historical_batch_summary(paths, _resolve(root, args.summary_output))
    print(f"wrote: {summary}")
    print(f"wrote: {paths.batch_summary_csv}")
    print(f"wrote: {paths.batch_summary_md}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
