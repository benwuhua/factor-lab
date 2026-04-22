#!/usr/bin/env python3
from __future__ import annotations

import argparse

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_eval import EvalConfig, evaluate_factor, write_eval_report
from qlib_factor_lab.factor_mining import factors_to_frame, generate_candidate_factors, load_mining_config, rank_factor_results
from qlib_factor_lab.qlib_bootstrap import init_qlib


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Generate and screen candidate Qlib expression factors.")
    parser.add_argument("--config", default=str(root / "configs/factor_mining.yaml"))
    parser.add_argument("--provider-config", default=str(root / "configs/provider.yaml"))
    parser.add_argument("--output", default=str(root / "reports/factor_mining_results.csv"))
    parser.add_argument("--candidates-output", default=str(root / "reports/factor_mining_candidates.csv"))
    parser.add_argument("--metric", default="rank_ic_mean")
    parser.add_argument("--limit", type=int, default=None, help="Evaluate only the first N candidates.")
    parser.add_argument("--horizon", type=int, action="append", help="Forward return horizon; may be repeated.")
    parser.add_argument("--generate-only", action="store_true", help="Only write the generated candidate formula catalog.")
    parser.add_argument("--neutralize-size-proxy", action="store_true")
    args = parser.parse_args()

    candidates = generate_candidate_factors(load_mining_config(args.config))
    if args.limit is not None:
        candidates = candidates[: args.limit]
    candidate_frame = factors_to_frame(candidates)
    write_eval_report(candidate_frame, args.candidates_output)
    print(f"wrote: {args.candidates_output}")
    if args.generate_only:
        print(candidate_frame.to_string(index=False))
        return 0

    config = load_project_config(args.provider_config)
    horizons = tuple(args.horizon or [5, 10, 20])
    eval_config = EvalConfig(horizons=horizons, neutralize_size=args.neutralize_size_proxy)
    init_qlib(config)
    frames = [evaluate_factor(config, factor, eval_config, initialize=False) for factor in candidates]
    result = pd.concat(frames, ignore_index=True)
    ranked = rank_factor_results(result, metric=args.metric)
    write_eval_report(ranked, args.output)
    print(ranked.to_string(index=False))
    print(f"wrote: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
