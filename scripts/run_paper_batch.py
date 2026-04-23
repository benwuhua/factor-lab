#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
from dataclasses import replace
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.orders import load_order_config
from qlib_factor_lab.paper_batch import load_paper_batch_config, run_paper_batch, write_paper_batch_outputs
from qlib_factor_lab.paper_broker import load_paper_fill_config
from qlib_factor_lab.reconcile import load_reconcile_config


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Run a rolling paper-trading batch over dated target portfolios.")
    parser.add_argument("--target-glob", required=True, help="Glob for target_portfolio_YYYYMMDD.csv files.")
    parser.add_argument("--initial-positions", default="state/current_positions.csv")
    parser.add_argument("--execution-config", default="configs/execution.yaml")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--max-days", type=int, default=None)
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    execution_path = _resolve(root, args.execution_config)
    target_paths = sorted(Path(path) for path in glob.glob(str(_resolve(root, args.target_glob))))
    if not target_paths:
        raise SystemExit(f"no target portfolios matched: {args.target_glob}")
    initial_path = _resolve(root, args.initial_positions)
    initial = pd.read_csv(initial_path) if initial_path.exists() else pd.DataFrame(columns=["instrument", "current_weight"])

    batch_config = load_paper_batch_config(execution_path)
    if args.max_days is not None:
        batch_config = replace(batch_config, max_days=args.max_days)
    batch_config = replace(
        batch_config,
        run_root=_resolve(root, batch_config.run_root),
        summary_csv_path=_resolve(root, batch_config.summary_csv_path),
        summary_md_path=_resolve(root, batch_config.summary_md_path),
    )
    result = run_paper_batch(
        target_paths,
        initial,
        load_order_config(execution_path),
        load_paper_fill_config(execution_path),
        load_reconcile_config(execution_path),
        batch_config,
    )
    summary_csv, summary_md = write_paper_batch_outputs(result, batch_config)
    print(pd.DataFrame([result.summary]).to_string(index=False))
    print(f"wrote: {summary_csv}")
    print(f"wrote: {summary_md}")
    return 0 if result.summary["reconciliation_failures"] <= batch_config.max_reconciliation_failures else 1


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
