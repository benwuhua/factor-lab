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

from qlib_factor_lab.autoresearch.ledger import (
    render_expression_ledger_status_report,
    summarize_expression_ledger,
)


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Summarize expression autoresearch ledger rows by status.")
    parser.add_argument(
        "--ledger",
        default=str(root / "reports/autoresearch/expression_results.tsv"),
        help="Expression autoresearch ledger TSV.",
    )
    parser.add_argument("--top-n", type=int, default=10, help="Number of review candidates to show.")
    parser.add_argument("--output", default=None, help="Optional Markdown output path.")
    args = parser.parse_args()

    report = render_expression_ledger_status_report(summarize_expression_ledger(args.ledger, top_n=args.top_n))
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8")
        print(f"wrote: {output}")
    else:
        print(report, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
