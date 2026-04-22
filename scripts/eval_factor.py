#!/usr/bin/env python3
from __future__ import annotations

import argparse

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_eval import EvalConfig, evaluate_factor, write_eval_report
from qlib_factor_lab.factor_registry import load_factor_registry, select_factors
from qlib_factor_lab.reports import plot_quantile_returns


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Evaluate one Qlib expression factor.")
    parser.add_argument("--factor", required=True, help="Factor name in factors/registry.yaml.")
    parser.add_argument("--provider-config", default=str(root / "configs/provider.yaml"))
    parser.add_argument("--output", default=None, help="CSV output path.")
    parser.add_argument("--neutralize-size-proxy", action="store_true", help="Neutralize by log(close * volume).")
    parser.add_argument("--industry-map", default=None, help="CSV with instrument,industry columns for industry neutralization.")
    parser.add_argument("--plot", action="store_true", help="Write a quantile return PNG next to the CSV report.")
    parser.add_argument("--plot-horizon", type=int, default=5, help="Horizon row to plot when --plot is set.")
    args = parser.parse_args()

    config = load_project_config(args.provider_config)
    factors = select_factors(load_factor_registry(root / "factors/registry.yaml"), names=[args.factor])
    if not factors:
        raise SystemExit(f"unknown factor: {args.factor}")
    result = evaluate_factor(
        config,
        factors[0],
        EvalConfig(
            neutralize_size=args.neutralize_size_proxy,
            industry_map_path=args.industry_map,
        ),
    )
    output = args.output or root / "reports" / f"factor_{args.factor}.csv"
    write_eval_report(result, output)
    print(result.to_string(index=False))
    print(f"wrote: {output}")
    if args.plot:
        output_path = root / output if isinstance(output, str) and not output.startswith("/") else output
        plot_frame = result[result["horizon"] == args.plot_horizon]
        if plot_frame.empty:
            raise SystemExit(f"no result for plot horizon: {args.plot_horizon}")
        png_path = str(output_path).replace(".csv", f"_h{args.plot_horizon}_quantile.png")
        plot_quantile_returns(plot_frame.head(1), png_path)
        print(f"wrote: {png_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
