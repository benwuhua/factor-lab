#!/usr/bin/env python3
from __future__ import annotations

import argparse

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.event_eval import EventEvalConfig, evaluate_event_buckets
from qlib_factor_lab.factor_eval import write_eval_report
from qlib_factor_lab.factor_mining import generate_candidate_factors, load_mining_config
from qlib_factor_lab.qlib_bootstrap import init_qlib


def fetch_event_frame(config, factor):
    from qlib.data import D

    frame = D.features(
        D.instruments(config.market),
        [factor.expression, "$close", "$high", "$low"],
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    frame.columns = [factor.name, "close", "high", "low"]
    return frame.dropna(subset=[factor.name, "close", "high", "low"])


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Evaluate absolute factor event buckets.")
    parser.add_argument("--factor", required=True, help="Factor name in the mining config.")
    parser.add_argument("--config", default=str(root / "configs/factor_mining.yaml"))
    parser.add_argument("--provider-config", default=str(root / "configs/provider.yaml"))
    parser.add_argument("--output", default=None)
    parser.add_argument("--yearly-output", default=None)
    parser.add_argument("--horizon", type=int, action="append", help="Forward horizon; may be repeated.")
    args = parser.parse_args()

    config = load_project_config(args.provider_config)
    factors = generate_candidate_factors(load_mining_config(args.config))
    matches = [factor for factor in factors if factor.name == args.factor]
    if not matches:
        raise SystemExit(f"unknown factor in mining config: {args.factor}")
    factor = matches[0]

    init_qlib(config)
    frame = fetch_event_frame(config, factor)
    horizons = tuple(args.horizon or [5, 20])
    event_config = EventEvalConfig(horizons=horizons)
    result = evaluate_event_buckets(frame, factor.name, event_config)
    yearly = evaluate_event_buckets(frame, factor.name, EventEvalConfig(horizons=horizons, by_year=True))

    output = args.output or root / "reports" / f"factor_{factor.name}_event_buckets.csv"
    yearly_output = args.yearly_output or root / "reports" / f"factor_{factor.name}_event_buckets_yearly.csv"
    write_eval_report(result, output)
    write_eval_report(yearly, yearly_output)
    print(result.to_string(index=False))
    print(f"wrote: {output}")
    print(f"wrote: {yearly_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
