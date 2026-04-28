#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from qlib_factor_lab.autoresearch.fundamental_oracle import run_fundamental_lane_oracle


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the point-in-time fundamental quality autoresearch lane.")
    parser.add_argument("--lane-name", default="fundamental_quality")
    parser.add_argument("--contract", required=True)
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--fundamental-path", default="data/fundamental_quality.csv")
    parser.add_argument("--space", default="configs/autoresearch/fundamental_space.yaml")
    parser.add_argument("--security-master", default="data/security_master.csv")
    parser.add_argument("--start-time")
    parser.add_argument("--end-time")
    parser.add_argument("--json", action="store_true", help="Print payload JSON instead of summary block.")
    args = parser.parse_args()

    payload, block = run_fundamental_lane_oracle(
        lane_name=args.lane_name,
        contract_path=args.contract,
        project_root=Path(args.project_root),
        fundamental_path=args.fundamental_path,
        space_path=args.space,
        security_master_path=args.security_master,
        start_time=args.start_time,
        end_time=args.end_time,
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(block)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
