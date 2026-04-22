#!/usr/bin/env python3
from __future__ import annotations

import argparse

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_eval import EvalConfig, evaluate_factor, write_eval_report
from qlib_factor_lab.factor_registry import load_factor_registry, select_factors
from qlib_factor_lab.reports import plot_quantile_returns
from qlib_factor_lab.qlib_bootstrap import init_qlib


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Evaluate multiple Qlib expression factors.")
    parser.add_argument("--category", action="append", help="Filter by category; may be repeated.")
    parser.add_argument("--factor", action="append", help="Filter by factor name; may be repeated.")
    parser.add_argument("--provider-config", default=str(root / "configs/provider.yaml"))
    parser.add_argument("--output", default=str(root / "reports/factor_batch.csv"))
    parser.add_argument("--neutralize-size-proxy", action="store_true", help="Neutralize each factor by log(close * volume).")
    parser.add_argument("--industry-map", default=None, help="CSV with instrument,industry columns for industry neutralization.")
    parser.add_argument("--plot-top", action="store_true", help="Plot the best absolute RankIC row after evaluation.")
    args = parser.parse_args()

    config = load_project_config(args.provider_config)
    factors = select_factors(
        load_factor_registry(root / "factors/registry.yaml"),
        names=args.factor,
        categories=args.category,
    )
    if not factors:
        raise SystemExit("no factors selected")

    init_qlib(config)
    eval_config = EvalConfig(
        neutralize_size=args.neutralize_size_proxy,
        industry_map_path=args.industry_map,
    )
    frames = [evaluate_factor(config, factor, eval_config, initialize=False) for factor in factors]
    result = pd.concat(frames, ignore_index=True)
    write_eval_report(result, args.output)
    print(result.to_string(index=False))
    print(f"wrote: {args.output}")
    if args.plot_top:
        ranked = result.assign(abs_rank_ic_mean=result["rank_ic_mean"].abs()).sort_values("abs_rank_ic_mean", ascending=False)
        top = ranked.head(1)
        png_path = str(args.output).replace(".csv", "_top_quantile.png")
        plot_quantile_returns(top, png_path)
        print(f"wrote: {png_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
