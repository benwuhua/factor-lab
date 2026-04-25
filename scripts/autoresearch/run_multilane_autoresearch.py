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

from qlib_factor_lab.autoresearch.multilane import run_multilane_autoresearch


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Run the configured autoresearch lane set.")
    parser.add_argument("--lane-space", default=str(root / "configs/autoresearch/lane_space.yaml"))
    parser.add_argument("--contract", default=str(root / "configs/autoresearch/contracts/csi500_current_v1.yaml"))
    parser.add_argument("--expression-space", default=str(root / "configs/autoresearch/expression_space.yaml"))
    parser.add_argument("--expression-candidate", default=str(root / "configs/autoresearch/candidates/example_expression.yaml"))
    parser.add_argument("--output", default="reports/autoresearch/multilane_summary.md")
    parser.add_argument("--include-shadow", action="store_true")
    parser.add_argument("--max-workers", type=int, default=4)
    args = parser.parse_args()

    report = run_multilane_autoresearch(
        lane_space_path=args.lane_space,
        project_root=root,
        contract_path=args.contract,
        expression_space_path=args.expression_space,
        expression_candidate_path=args.expression_candidate,
        output_path=args.output,
        include_shadow=args.include_shadow,
        max_workers=args.max_workers,
    )
    print(report.to_frame().to_string(index=False))
    print(f"wrote: {report.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
