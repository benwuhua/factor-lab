#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.stock_cards import build_stock_cards, write_stock_card_report, write_stock_cards


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build JSONL stock research cards from a target portfolio.")
    parser.add_argument("--target-portfolio", required=True)
    parser.add_argument("--gate-checks-csv", default=None)
    parser.add_argument("--as-of-date", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--card-version", default="v1")
    parser.add_argument("--gate-decision", default="")
    parser.add_argument("--factor-version", default="")
    parser.add_argument("--output", default="reports/stock_cards_{run_yyyymmdd}.jsonl")
    parser.add_argument("--report-output", default="")
    parser.add_argument("--project-root", default=str(default_root))
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    portfolio = pd.read_csv(_resolve(root, args.target_portfolio))
    as_of_date = args.as_of_date or _infer_as_of_date(portfolio)
    run_id = args.run_id or f"stock_cards_{as_of_date.replace('-', '')}"
    gate_checks = pd.read_csv(_resolve(root, args.gate_checks_csv)) if args.gate_checks_csv else None
    cards = build_stock_cards(
        portfolio,
        run_id=run_id,
        as_of_date=as_of_date,
        card_version=args.card_version,
        gate_decision=args.gate_decision,
        gate_checks=gate_checks,
        factor_version=args.factor_version,
    )
    output = write_stock_cards(cards, _resolve(root, _materialize(args.output, as_of_date)))
    print(f"cards: {len(cards)}")
    print(f"wrote: {output}")
    if args.report_output:
        report_output = write_stock_card_report(
            cards,
            _resolve(root, _materialize(args.report_output, as_of_date)),
        )
        print(f"wrote report: {report_output}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    return candidate if candidate.is_absolute() else root / candidate


def _materialize(path: str | Path, as_of_date: str) -> Path:
    yyyymmdd = str(as_of_date).replace("-", "")
    return Path(str(path).format(as_of_date=as_of_date, run_yyyymmdd=yyyymmdd))


def _infer_as_of_date(portfolio: pd.DataFrame) -> str:
    if "date" in portfolio.columns and not portfolio.empty:
        return str(portfolio["date"].max())
    return "unknown"


if __name__ == "__main__":
    raise SystemExit(main())
