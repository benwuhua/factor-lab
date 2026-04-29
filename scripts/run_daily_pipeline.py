#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.daily_pipeline import DailyPipelineInputs, run_daily_pipeline


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Run the daily alpha-to-paper pipeline.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--signal-config", default="configs/signal.yaml")
    parser.add_argument("--trading-config", default="configs/trading.yaml")
    parser.add_argument("--portfolio-config", default="configs/portfolio.yaml")
    parser.add_argument("--risk-config", default="configs/risk.yaml")
    parser.add_argument("--execution-config", default="configs/execution.yaml")
    parser.add_argument("--event-risk-config", default="configs/event_risk.yaml")
    parser.add_argument("--data-governance-config", default="configs/data_governance.yaml")
    parser.add_argument("--combo-spec", default=None, help="Optional governed combo spec to score instead of the approved factor list.")
    parser.add_argument("--exposures-csv", default=None, help="Optional precomputed exposures CSV.")
    parser.add_argument("--current-positions-csv", default="state/current_positions.csv")
    parser.add_argument("--expert-manual-confirm", action="store_true", help="Confirm an expert caution/manual gate for this run.")
    parser.add_argument("--expert-reviewer", default="", help="Reviewer name for --expert-manual-confirm.")
    parser.add_argument("--expert-confirm-reason", default="", help="Reason recorded for --expert-manual-confirm.")
    parser.add_argument("--run-date", default=None)
    parser.add_argument("--active-regime", default=None)
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    result = run_daily_pipeline(
        root,
        DailyPipelineInputs(
            signal_config_path=Path(args.signal_config),
            trading_config_path=Path(args.trading_config),
            portfolio_config_path=Path(args.portfolio_config),
            risk_config_path=Path(args.risk_config),
            execution_config_path=Path(args.execution_config),
            event_risk_config_path=Path(args.event_risk_config) if args.event_risk_config else None,
            data_governance_config_path=Path(args.data_governance_config) if args.data_governance_config else None,
            combo_spec_path=Path(args.combo_spec) if args.combo_spec else None,
            exposures_csv=Path(args.exposures_csv) if args.exposures_csv else None,
            current_positions_csv=Path(args.current_positions_csv) if args.current_positions_csv else None,
            expert_manual_confirm=args.expert_manual_confirm,
            expert_reviewer=args.expert_reviewer,
            expert_confirm_reason=args.expert_confirm_reason,
            run_date=args.run_date,
            active_regime=args.active_regime,
        ),
    )
    print(f"run_date: {result.run_date}")
    print(f"status: {result.status}")
    print(f"risk_passed: {result.risk_passed}")
    print(f"wrote: {result.run_dir}")
    print(f"wrote: {result.manifest_path}")
    return 0 if result.status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
