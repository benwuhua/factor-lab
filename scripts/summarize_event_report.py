#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.reports import write_event_summary_markdown


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Render a Markdown summary from an event backtest summary CSV.")
    parser.add_argument("--summary", required=True, help="Input event backtest summary CSV.")
    parser.add_argument("--output", default=None, help="Markdown output path.")
    parser.add_argument("--name", default=None, help="Report title. Defaults to the summary filename stem.")
    parser.add_argument("--factor", default="", help="Related factor name.")
    parser.add_argument("--universe", default="", help="Universe or market name.")
    parser.add_argument("--provider-config", default="", help="Provider config used by the backtest.")
    parser.add_argument("--data-range", default="", help="Data range used by the backtest.")
    parser.add_argument("--command", default="", help="Command that generated the source report.")
    args = parser.parse_args()

    summary_path = Path(args.summary)
    output = Path(args.output) if args.output else root / "reports" / f"{summary_path.stem}.md"
    frame = pd.read_csv(summary_path)
    written = write_event_summary_markdown(
        frame,
        output,
        name=args.name or summary_path.stem,
        factor=args.factor,
        universe=args.universe,
        provider_config=args.provider_config,
        command=args.command,
        data_range=args.data_range,
    )
    print(f"wrote: {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
