#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.broker_adapter import load_broker_adapter
from qlib_factor_lab.config import load_yaml
from qlib_factor_lab.orders import build_order_suggestions, load_order_config, write_orders
from qlib_factor_lab.paper_broker import write_fills
from qlib_factor_lab.reconcile import write_reconciliation_report
from qlib_factor_lab.state import write_positions_state


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Generate paper orders and fills from a target portfolio.")
    parser.add_argument("--target-portfolio", required=True)
    parser.add_argument("--current-positions", default="state/current_positions.csv")
    parser.add_argument("--execution-config", default="configs/execution.yaml")
    parser.add_argument("--project-root", default=str(default_root))
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    target = pd.read_csv(_resolve(root, args.target_portfolio))
    current_path = _resolve(root, args.current_positions)
    current = pd.read_csv(current_path) if current_path.exists() else pd.DataFrame(columns=["instrument", "current_weight"])
    run_date = str(target["date"].max()) if "date" in target.columns and not target.empty else ""
    execution_path = _resolve(root, args.execution_config)
    run_dir = _resolve(root, _run_dir(load_yaml(execution_path), run_date))
    run_dir.mkdir(parents=True, exist_ok=True)

    order_config = load_order_config(execution_path)
    broker = load_broker_adapter(load_yaml(execution_path), run_id=f"{run_date.replace('-', '')}-paper")
    orders = broker.submit_orders(broker.validate_orders(build_order_suggestions(target, current, order_config)))
    fills = broker.fetch_fills(orders)
    expected = broker.fetch_positions(current, fills)
    reconcile_report = broker.reconcile(expected, expected.copy())

    orders_path = write_orders(orders, run_dir / "orders.csv")
    fills_path = write_fills(fills, run_dir / "fills.csv")
    expected_path = write_positions_state(expected, run_dir / "positions_expected.csv")
    reconciliation_path = write_reconciliation_report(reconcile_report, run_dir / "reconciliation.md")
    manifest_path = _write_manifest(
        run_dir / "manifest.json",
        {
            "run_date": run_date,
            "target_portfolio": str(_resolve(root, args.target_portfolio)),
            "current_positions": str(current_path),
            "orders": str(orders_path),
            "fills": str(fills_path),
            "positions_expected": str(expected_path),
            "reconciliation": str(reconciliation_path),
        },
    )
    print(orders.to_string(index=False))
    print(f"wrote: {orders_path}")
    print(f"wrote: {fills_path}")
    print(f"wrote: {expected_path}")
    print(f"wrote: {reconciliation_path}")
    print(f"wrote: {manifest_path}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


def _run_dir(data: dict, run_date: str) -> Path:
    output = data.get("output", {})
    template = output.get("run_dir", "runs/{run_yyyymmdd}")
    return Path(str(template).format(run_date=run_date, run_yyyymmdd=run_date.replace("-", "")))


def _write_manifest(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


if __name__ == "__main__":
    raise SystemExit(main())
