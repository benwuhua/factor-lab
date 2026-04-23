#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.factor_selection import (
    build_factor_selection,
    load_factor_selection_config,
    write_approved_factors,
    write_factor_review,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build approved factor governance artifacts.")
    parser.add_argument("--config", default="configs/factor_selection.yaml")
    parser.add_argument("--project-root", default=None)
    parser.add_argument("--approved-output", default=None)
    parser.add_argument("--review-output", default=None)
    args = parser.parse_args()

    root = Path(args.project_root).resolve() if args.project_root else project_root()
    config = load_factor_selection_config(root / args.config)
    result = build_factor_selection(config, root=root)
    approved_output = root / (args.approved_output or config.output_approved_path)
    review_output = root / (args.review_output or config.output_review_path)
    write_approved_factors(result, approved_output)
    write_factor_review(result, review_output)
    print(f"approved_count: {len(result.approved_factors)}")
    print(f"wrote: {approved_output}")
    print(f"wrote: {review_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
