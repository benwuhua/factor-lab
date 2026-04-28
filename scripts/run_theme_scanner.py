#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.theme_scanner import (
    build_theme_candidates,
    load_theme_universe,
    write_theme_candidate_report,
    write_theme_candidates,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Scan a hot theme universe with the latest daily signal.")
    parser.add_argument("--theme-config", required=True, help="Theme universe YAML.")
    parser.add_argument("--signal-csv", required=True, help="Daily signal CSV.")
    parser.add_argument("--top-k", type=int, default=30)
    parser.add_argument("--output-csv", default="reports/theme_scans/{theme_id}_{run_yyyymmdd}.csv")
    parser.add_argument("--output-md", default="reports/theme_scans/{theme_id}_{run_yyyymmdd}.md")
    parser.add_argument("--project-root", default=str(default_root))
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    universe = load_theme_universe(_resolve(root, args.theme_config))
    signal = pd.read_csv(_resolve(root, args.signal_csv))
    run_date = _infer_run_date(signal)
    candidates = build_theme_candidates(signal, universe, top_k=args.top_k)
    csv_path = write_theme_candidates(
        candidates,
        _resolve(root, _materialize(args.output_csv, universe.theme_id, run_date)),
    )
    md_path = write_theme_candidate_report(
        candidates,
        _resolve(root, _materialize(args.output_md, universe.theme_id, run_date)),
        theme_display_name=universe.display_name,
        thesis=universe.thesis,
        sources=universe.sources,
    )
    print(candidates.head(args.top_k).to_string(index=False))
    print(f"wrote: {csv_path}")
    print(f"wrote: {md_path}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    return candidate if candidate.is_absolute() else root / candidate


def _materialize(path: str | Path, theme_id: str, run_date: str) -> Path:
    yyyymmdd = run_date.replace("-", "")
    return Path(str(path).format(theme_id=theme_id, run_date=run_date, run_yyyymmdd=yyyymmdd))


def _infer_run_date(signal: pd.DataFrame) -> str:
    if "date" in signal.columns and not signal.empty:
        return str(signal["date"].max())
    return "unknown"


if __name__ == "__main__":
    raise SystemExit(main())
