#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.manual_live import ManualTicketConfig, build_manual_order_ticket, write_manual_order_ticket


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Generate a human-reviewed manual order ticket from paper orders.")
    parser.add_argument("--orders-csv", required=True)
    parser.add_argument("--fills-csv", default=None)
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--csv-output", default=None)
    parser.add_argument("--markdown-output", default=None)
    parser.add_argument("--available-cash", type=float, default=None)
    parser.add_argument("--banned-instruments-csv", default=None)
    parser.add_argument("--max-order-value", type=float, default=None)
    parser.add_argument("--no-sells", action="store_true", help="Require review for every sell order.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    orders_path = _resolve(root, args.orders_csv)
    orders = pd.read_csv(orders_path)
    fills = pd.read_csv(_resolve(root, args.fills_csv)) if args.fills_csv else None
    run_date = str(orders["date"].max()) if "date" in orders.columns and not orders.empty else _date_from_path(orders_path)
    run_yyyymmdd = run_date.replace("-", "")
    csv_output = _resolve(root, args.csv_output or f"reports/order_ticket_{run_yyyymmdd}.csv")
    markdown_output = _resolve(root, args.markdown_output or f"reports/order_ticket_{run_yyyymmdd}.md")

    ticket = build_manual_order_ticket(
        orders,
        fills,
        pretrade_config=ManualTicketConfig(
            available_cash=args.available_cash,
            banned_instruments=_load_banned_instruments(root, args.banned_instruments_csv),
            max_order_value=args.max_order_value,
            allow_sells=not args.no_sells,
        ),
    )
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


def _load_banned_instruments(root: Path, path: str | None) -> tuple[str, ...]:
    if path is None:
        return ()
    frame = pd.read_csv(_resolve(root, path))
    if frame.empty:
        return ()
    column = "instrument" if "instrument" in frame.columns else frame.columns[0]
    return tuple(str(value) for value in frame[column].dropna())


if __name__ == "__main__":
    raise SystemExit(main())
