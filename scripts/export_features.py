#!/usr/bin/env python3
from __future__ import annotations

import argparse

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_registry import load_factor_registry, select_factors
from qlib_factor_lab.qlib_bootstrap import init_qlib


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Export selected factor features from Qlib to parquet/csv.")
    parser.add_argument("--factor", action="append", help="Factor name; may be repeated.")
    parser.add_argument("--category", action="append", help="Category name; may be repeated.")
    parser.add_argument("--output", default=str(root / "reports/features.csv"))
    args = parser.parse_args()

    config = load_project_config(root / "configs/provider.yaml")
    factors = select_factors(
        load_factor_registry(root / "factors/registry.yaml"),
        names=args.factor,
        categories=args.category,
    )
    if not factors:
        raise SystemExit("no factors selected")

    init_qlib(config)
    from qlib.data import D

    frame = D.features(
        D.instruments(config.market),
        [f.expression for f in factors],
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    frame.columns = [f.name for f in factors]
    output = root / args.output if not args.output.startswith("/") else args.output
    if str(output).endswith(".parquet"):
        frame.to_parquet(output)
    else:
        frame.to_csv(output)
    print(f"wrote: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
