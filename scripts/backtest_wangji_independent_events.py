#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.event_backtest import (
    IndependentEventBacktestConfig,
    build_independent_event_trades,
    summarize_trades,
)
from qlib_factor_lab.factor_eval import write_eval_report
from qlib_factor_lab.factor_mining import FactorDef, generate_candidate_factors, load_mining_config
from qlib_factor_lab.qlib_bootstrap import init_qlib
from qlib_factor_lab.wangji_diagnostics import write_mfe_mae_distribution_plot
from qlib_factor_lab.wangji_patterns import FACTOR2_SIGNAL_COLUMN, compute_wangji_factor2_events


DEFAULT_WANGJI_FACTORS = (
    "wangji-factor1",
    "wangji-factor2",
)
WANGJI_TRADE_SIDE = {
    "wangji-factor1": "left",
    "wangji-reversal20-combo": "left",
    "wangji-factor2": "right",
}


def fetch_backtest_frame(config, factor: FactorDef) -> pd.DataFrame:
    from qlib.data import D

    if factor.name == FACTOR2_SIGNAL_COLUMN:
        frame = D.features(
            D.instruments(config.market),
            ["$open", "$high", "$low", "$close", "$volume"],
            start_time=config.start_time,
            end_time=config.end_time,
            freq=config.freq,
        )
        frame.columns = ["open", "high", "low", "close", "volume"]
        events = compute_wangji_factor2_events(frame.dropna(subset=["open", "high", "low", "close", "volume"]))
        return events.dropna(subset=[factor.name, "open", "high", "low", "close"])

    frame = D.features(
        D.instruments(config.market),
        [factor.expression, "$open", "$high", "$low", "$close"],
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    frame.columns = [factor.name, "open", "high", "low", "close"]
    return frame.dropna(subset=[factor.name, "open", "high", "low", "close"])


def resolve_trade_side(factor_name: str) -> str:
    return WANGJI_TRADE_SIDE.get(factor_name, "independent")


def run_provider_backtest(
    provider_config_path: Path,
    factors: list[FactorDef],
    config: IndependentEventBacktestConfig,
    start_time: str | None = None,
    end_time: str | None = None,
) -> pd.DataFrame:
    provider_config = load_project_config(provider_config_path)
    if end_time:
        provider_config = replace(
            provider_config,
            end_time=end_time,
        )
    init_qlib(provider_config)

    frames = []
    for factor in factors:
        frame = fetch_backtest_frame(provider_config, factor)
        trades = build_independent_event_trades(
            frame,
            factor.name,
            factor_name=factor.name,
            trade_side=resolve_trade_side(factor.name),
            config=config,
            signal_direction=factor.direction,
        )
        if trades.empty:
            continue
        trades = _filter_trades_by_signal_date(trades, start_time=start_time, end_time=end_time)
        if trades.empty:
            continue
        trades.insert(0, "universe", provider_config.market)
        trades.insert(1, "provider_config", str(provider_config_path))
        frames.append(trades)
    if not frames:
        return pd.DataFrame(
            columns=[
                "universe",
                "provider_config",
                "factor",
                "trade_side",
                "horizon",
                "instrument",
                "signal_date",
                "entry_date",
                "exit_date",
                "score",
                "rank_signal",
                "entry_open",
                "exit_close",
                "return",
                "mfe",
                "mae",
            ]
        )
    return pd.concat(frames, ignore_index=True)


def _filter_trades_by_signal_date(
    trades: pd.DataFrame,
    start_time: str | None = None,
    end_time: str | None = None,
) -> pd.DataFrame:
    if trades.empty or (start_time is None and end_time is None):
        return trades
    data = trades.copy()
    signal_date = pd.to_datetime(data["signal_date"])
    if start_time is not None:
        data = data[signal_date >= pd.Timestamp(start_time)]
        signal_date = pd.to_datetime(data["signal_date"])
    if end_time is not None:
        data = data[signal_date <= pd.Timestamp(end_time)]
    return data.reset_index(drop=True)


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(
        description="Backtest independent Wangji price-volume event factors without factor chaining."
    )
    parser.add_argument("--factor", action="append", help="Wangji factor name in the mining config; may be repeated.")
    parser.add_argument("--config", default=str(root / "configs/factor_mining.yaml"))
    parser.add_argument(
        "--provider-config",
        action="append",
        default=None,
        help="Provider config to backtest; may be repeated. Defaults to CSI500 current.",
    )
    parser.add_argument("--trades-output", default=str(root / "reports/wangji_independent_event_trades.csv"))
    parser.add_argument("--summary-output", default=str(root / "reports/wangji_independent_event_summary.csv"))
    parser.add_argument("--yearly-output", default=str(root / "reports/wangji_independent_event_yearly.csv"))
    parser.add_argument("--mfe-mae-plot-output", default=str(root / "reports/wangji_independent_event_mfe_mae.png"))
    parser.add_argument("--horizon", type=int, action="append", help="Holding horizon; may be repeated.")
    parser.add_argument("--start-time", default=None, help="Optional event backtest start date override, e.g. 2026-01-01.")
    parser.add_argument("--end-time", default=None, help="Optional event backtest end date override, e.g. 2026-05-06.")
    parser.add_argument("--signal-threshold", type=float, default=0.5, help="Minimum directional signal value to trade.")
    args = parser.parse_args()

    factor_names = tuple(args.factor or DEFAULT_WANGJI_FACTORS)
    mining_factors = generate_candidate_factors(load_mining_config(args.config))
    factor_by_name = {factor.name: factor for factor in mining_factors}
    unknown = [name for name in factor_names if name not in factor_by_name]
    if unknown:
        raise SystemExit(f"unknown factor(s) in mining config: {', '.join(unknown)}")
    factors = [factor_by_name[name] for name in factor_names]

    provider_configs = [Path(path) for path in (args.provider_config or [root / "configs/provider_current.yaml"])]
    backtest_config = IndependentEventBacktestConfig(
        horizons=tuple(args.horizon or [3, 5, 10, 20]),
        signal_threshold=args.signal_threshold,
    )
    trades = pd.concat(
        [
            run_provider_backtest(
                path,
                factors,
                backtest_config,
                start_time=args.start_time,
                end_time=args.end_time,
            )
            for path in provider_configs
        ],
        ignore_index=True,
    )
    summary = summarize_trades(trades, group_cols=("universe", "factor", "trade_side", "horizon"))
    yearly = summarize_trades(trades, by_year=True, group_cols=("universe", "factor", "trade_side", "horizon"))

    write_eval_report(trades, args.trades_output)
    write_eval_report(summary, args.summary_output)
    write_eval_report(yearly, args.yearly_output)
    if args.mfe_mae_plot_output:
        write_mfe_mae_distribution_plot(trades, args.mfe_mae_plot_output)
    print(summary.to_string(index=False))
    print(f"wrote: {args.trades_output}")
    print(f"wrote: {args.summary_output}")
    print(f"wrote: {args.yearly_output}")
    if args.mfe_mae_plot_output:
        print(f"wrote: {args.mfe_mae_plot_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
