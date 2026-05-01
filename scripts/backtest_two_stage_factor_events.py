#!/usr/bin/env python3
from __future__ import annotations

import argparse

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.event_backtest import TwoStageEventBacktestConfig, build_two_stage_event_trades, summarize_trades
from qlib_factor_lab.factor_eval import write_eval_report
from qlib_factor_lab.factor_mining import generate_candidate_factors, load_mining_config
from qlib_factor_lab.qlib_bootstrap import init_qlib


def fetch_two_stage_frame(config, setup_factor, confirm_factor):
    from qlib.data import D

    frame = D.features(
        D.instruments(config.market),
        [setup_factor.expression, confirm_factor.expression, "$open", "$high", "$low", "$close"],
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    frame.columns = [setup_factor.name, confirm_factor.name, "open", "high", "low", "close"]
    return frame.dropna(subset=[setup_factor.name, confirm_factor.name, "open", "high", "low", "close"])


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Backtest setup factor events gated by a delayed confirmation factor.")
    parser.add_argument("--setup-factor", required=True, help="Setup factor name in the mining config.")
    parser.add_argument("--confirm-factor", required=True, help="Confirmation factor name in the mining config.")
    parser.add_argument("--config", default=str(root / "configs/factor_mining.yaml"))
    parser.add_argument("--provider-config", default=str(root / "configs/provider.yaml"))
    parser.add_argument("--trades-output", default=None)
    parser.add_argument("--summary-output", default=None)
    parser.add_argument("--yearly-output", default=None)
    parser.add_argument("--horizon", type=int, action="append", help="Holding horizon from confirmation date; may be repeated.")
    parser.add_argument("--confirmation-delay", type=int, default=3, help="Trading bars after setup date to evaluate confirmation.")
    parser.add_argument("--confirmation-min-score", type=float, default=None, help="Minimum directional confirmation score.")
    parser.add_argument("--confirmation-min-percentile", type=float, default=None, help="Minimum confirmation cross-sectional percentile.")
    args = parser.parse_args()

    config = load_project_config(args.provider_config)
    factors = generate_candidate_factors(load_mining_config(args.config))
    factor_by_name = {factor.name: factor for factor in factors}
    if args.setup_factor not in factor_by_name:
        raise SystemExit(f"unknown setup factor in mining config: {args.setup_factor}")
    if args.confirm_factor not in factor_by_name:
        raise SystemExit(f"unknown confirmation factor in mining config: {args.confirm_factor}")
    setup_factor = factor_by_name[args.setup_factor]
    confirm_factor = factor_by_name[args.confirm_factor]

    init_qlib(config)
    frame = fetch_two_stage_frame(config, setup_factor, confirm_factor)
    backtest_config = TwoStageEventBacktestConfig(
        horizons=tuple(args.horizon or [5, 20]),
        confirmation_delay=args.confirmation_delay,
        confirmation_min_score=args.confirmation_min_score,
        confirmation_min_percentile=args.confirmation_min_percentile,
    )
    trades = build_two_stage_event_trades(
        frame,
        setup_col=setup_factor.name,
        confirm_col=confirm_factor.name,
        config=backtest_config,
        setup_direction=setup_factor.direction,
        confirm_direction=confirm_factor.direction,
    )
    summary = summarize_trades(trades)
    yearly = summarize_trades(trades, by_year=True)

    pair_name = f"{setup_factor.name}_then_{confirm_factor.name}"
    trades_output = args.trades_output or root / "reports" / f"factor_{pair_name}_two_stage_trades.csv"
    summary_output = args.summary_output or root / "reports" / f"factor_{pair_name}_two_stage_summary.csv"
    yearly_output = args.yearly_output or root / "reports" / f"factor_{pair_name}_two_stage_yearly.csv"
    write_eval_report(trades, trades_output)
    write_eval_report(summary, summary_output)
    write_eval_report(yearly, yearly_output)
    print(summary.to_string(index=False))
    print(f"wrote: {trades_output}")
    print(f"wrote: {summary_output}")
    print(f"wrote: {yearly_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
