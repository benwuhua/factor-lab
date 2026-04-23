#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.manual_live import build_manual_order_ticket, write_manual_order_ticket


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Generate a human-reviewed manual order ticket from paper orders.")
    parser.add_argument("--orders-csv", required=True)
    parser.add_argument("--fills-csv", default=None)
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--csv-output", default=None)
    parser.add_argument("--markdown-output", default=None)
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    orders_path = _resolve(root, args.orders_csv)
    orders = pd.read_csv(orders_path)
    fills = pd.read_csv(_resolve(root, args.fills_csv)) if args.fills_csv else None
    run_date = str(orders["date"].max()) if "date" in orders.columns and not orders.empty else _date_from_path(orders_path)
    run_yyyymmdd = run_date.replace("-", "")
    csv_output = _resolve(root, args.csv_output or f"reports/order_ticket_{run_yyyymmdd}.csv")
    markdown_output = _resolve(root, args.markdown_output or f"reports/order_ticket_{run_yyyymmdd}.md")

    ticket = build_manual_order_ticket(orders, fills)
    csv_path, md_path = write_manual_order_ticket(ticket, csv_output, markdown_output)
    print(ticket.to_string(index=False))
    print(f"wrote: {csv_path}")
    print(f"wrote: {md_path}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


def _date_from_path(path: Path) -> str:
    digits = "".join(ch for ch in path.stem if ch.isdigit())
    if len(digits) >= 8:
        value = digits[-8:]
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return "unknown"


if __name__ == "__main__":
    raise SystemExit(main())
