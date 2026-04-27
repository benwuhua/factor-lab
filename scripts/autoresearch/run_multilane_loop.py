#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.autoresearch.multilane_loop import (
    parse_multilane_deadline,
    resolve_multilane_max_iterations,
    run_multilane_loop,
)


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Run multilane autoresearch repeatedly until a stop condition.")
    parser.add_argument("--lane-space", default=str(root / "configs/autoresearch/lane_space.yaml"))
    parser.add_argument("--contract", default=str(root / "configs/autoresearch/contracts/csi500_current_v1.yaml"))
    parser.add_argument("--expression-space", default=str(root / "configs/autoresearch/expression_space.yaml"))
    parser.add_argument("--expression-candidate", default=str(root / "configs/autoresearch/candidates/example_expression.yaml"))
    parser.add_argument("--mining-config", default=str(root / "configs/factor_mining.yaml"))
    parser.add_argument("--provider-config", default=str(root / "configs/provider_current.yaml"))
    parser.add_argument("--output-root", default="reports/autoresearch/multilane_loop")
    parser.add_argument("--data-governance-report", default="")
    parser.add_argument("--include-shadow", action="store_true")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--start-time", default="")
    parser.add_argument("--end-time", default="")
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Maximum iterations. Omit or set 0 to run until --until or --max-hours.",
    )
    parser.add_argument("--max-hours", type=float, default=None)
    parser.add_argument("--until", default="08:30", help='Stop at HH:MM or "YYYY-MM-DD HH:MM" in --timezone.')
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--sleep-sec", type=float, default=60.0)
    parser.add_argument("--max-crashes", type=int, default=5)
    args = parser.parse_args()

    deadline = parse_multilane_deadline(args.until, timezone=args.timezone)
    max_iterations = resolve_multilane_max_iterations(
        args.max_iterations,
        has_deadline=deadline is not None,
        max_hours=args.max_hours,
    )
    result = run_multilane_loop(
        project_root=root,
        lane_space_path=args.lane_space,
        contract_path=args.contract,
        expression_space_path=args.expression_space,
        expression_candidate_path=args.expression_candidate,
        mining_config_path=args.mining_config,
        provider_config_path=args.provider_config,
        output_root=args.output_root,
        data_governance_report_path=args.data_governance_report or None,
        include_shadow=args.include_shadow,
        max_workers=args.max_workers,
        start_time=args.start_time or None,
        end_time=args.end_time or None,
        deadline=deadline,
        max_hours=args.max_hours,
        max_iterations=max_iterations,
        max_crashes=args.max_crashes,
        sleep_sec=args.sleep_sec,
    )
    print(f"iterations_started: {result.iterations_started}")
    print(f"crash_count: {result.crash_count}")
    print(f"stop_reason: {result.stop_reason}")
    print(f"log_dir: {result.log_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
