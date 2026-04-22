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

from qlib_factor_lab.autoresearch.oracle import run_expression_oracle


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Run one controlled autoresearch expression-factor experiment.")
    parser.add_argument(
        "--contract",
        default=str(root / "configs/autoresearch/contracts/csi500_current_v1.yaml"),
        help="Locked autoresearch contract YAML.",
    )
    parser.add_argument(
        "--space",
        default=str(root / "configs/autoresearch/expression_space.yaml"),
        help="Allowed expression search-space YAML.",
    )
    parser.add_argument(
        "--candidate",
        default=str(root / "configs/autoresearch/candidates/example_expression.yaml"),
        help="Single expression candidate YAML.",
    )
    args = parser.parse_args()

    _, block = run_expression_oracle(
        contract_path=args.contract,
        space_path=args.space,
        candidate_path=args.candidate,
        project_root=root,
    )
    print(block, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
