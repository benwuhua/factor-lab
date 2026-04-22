#!/usr/bin/env python3
from __future__ import annotations

import argparse

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.event_backtest import EventBacktestConfig, build_event_trades, summarize_trades
from qlib_factor_lab.factor_eval import write_eval_report
from qlib_factor_lab.factor_mining import generate_candidate_factors, load_mining_config
from qlib_factor_lab.market_regime import annotate_trades_with_market_regime, compute_equal_weight_market_regime
from qlib_factor_lab.qlib_bootstrap import init_qlib


def fetch_backtest_frame(config, factor):
    from qlib.data import D

    frame = D.features(
        D.instruments(config.market),
        [factor.expression, "$open", "$high", "$low", "$close", "$volume"],
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    frame.columns = [factor.name, "open", "high", "low", "close", "volume"]
    return frame.dropna(subset=[factor.name, "open", "high", "low", "close", "volume"])


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Backtest absolute factor percentile events.")
    parser.add_argument("--factor", required=True, help="Factor name in the mining config.")
    parser.add_argument("--config", default=str(root / "configs/factor_mining.yaml"))
    parser.add_argument("--provider-config", default=str(root / "configs/provider.yaml"))
    parser.add_argument("--trades-output", default=None)
    parser.add_argument("--summary-output", default=None)
    parser.add_argument("--yearly-output", default=None)
    parser.add_argument("--horizon", type=int, action="append", help="Holding horizon; may be repeated.")
    parser.add_argument("--confirm-window", type=int, default=0, help="Require breakout-volume confirmation within N days.")
    parser.add_argument("--confirm-breakout-lookback", type=int, default=14, help="Lookback bars for breakout high.")
    parser.add_argument("--confirm-volume-lookback", type=int, default=5, help="Lookback bars for confirmation volume average.")
    parser.add_argument("--confirm-volume-ratio", type=float, default=1.2, help="Minimum confirmation volume / prior average volume.")
    parser.add_argument("--market-regime-output", default=None, help="Optional output for event summary by market regime.")
    parser.add_argument("--market-regime-trades-output", default=None, help="Optional output for trades annotated by market regime.")
    parser.add_argument("--market-proxy-output", default=None, help="Optional output for the equal-weight market proxy and regime.")
    parser.add_argument("--market-fast-window", type=int, default=20, help="Fast moving-average window for market regime.")
    parser.add_argument("--market-slow-window", type=int, default=60, help="Slow moving-average window for market regime.")
    parser.add_argument("--market-trend-window", type=int, default=20, help="Return lookback window for market regime.")
    parser.add_argument("--market-trend-threshold", type=float, default=0.02, help="Minimum trend return magnitude for up/down regimes.")
    args = parser.parse_args()

    config = load_project_config(args.provider_config)
    factors = generate_candidate_factors(load_mining_config(args.config))
    matches = [factor for factor in factors if factor.name == args.factor]
    if not matches:
        raise SystemExit(f"unknown factor in mining config: {args.factor}")
    factor = matches[0]

    init_qlib(config)
    frame = fetch_backtest_frame(config, factor)
    backtest_config = EventBacktestConfig(
        horizons=tuple(args.horizon or [5, 20]),
        confirmation_window=args.confirm_window,
        confirmation_breakout_lookback=args.confirm_breakout_lookback,
        confirmation_volume_lookback=args.confirm_volume_lookback,
        confirmation_volume_ratio=args.confirm_volume_ratio,
    )
    trades = build_event_trades(frame, factor.name, backtest_config, signal_direction=factor.direction)
    summary = summarize_trades(trades)
    yearly = summarize_trades(trades, by_year=True)
    regime_summary = None
    regime_trades = None
    market_proxy = None
    if args.market_regime_output or args.market_regime_trades_output or args.market_proxy_output:
        market_proxy = compute_equal_weight_market_regime(
            frame,
            close_col="close",
            fast_window=args.market_fast_window,
            slow_window=args.market_slow_window,
            trend_window=args.market_trend_window,
            trend_threshold=args.market_trend_threshold,
        )
        regime_trades = annotate_trades_with_market_regime(trades, market_proxy)
        regime_summary = summarize_trades(regime_trades, group_cols=("market_regime", "bucket", "horizon"))

    trades_output = args.trades_output or root / "reports" / f"factor_{factor.name}_event_backtest_trades.csv"
    summary_output = args.summary_output or root / "reports" / f"factor_{factor.name}_event_backtest_summary.csv"
    yearly_output = args.yearly_output or root / "reports" / f"factor_{factor.name}_event_backtest_yearly.csv"
    write_eval_report(trades, trades_output)
    write_eval_report(summary, summary_output)
    write_eval_report(yearly, yearly_output)
    print(summary.to_string(index=False))
    print(f"wrote: {trades_output}")
    print(f"wrote: {summary_output}")
    print(f"wrote: {yearly_output}")
    if regime_summary is not None and args.market_regime_output:
        write_eval_report(regime_summary, args.market_regime_output)
        print(regime_summary.to_string(index=False))
        print(f"wrote: {args.market_regime_output}")
    if regime_trades is not None and args.market_regime_trades_output:
        write_eval_report(regime_trades, args.market_regime_trades_output)
        print(f"wrote: {args.market_regime_trades_output}")
    if market_proxy is not None and args.market_proxy_output:
        write_eval_report(market_proxy.reset_index(), args.market_proxy_output)
        print(f"wrote: {args.market_proxy_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
