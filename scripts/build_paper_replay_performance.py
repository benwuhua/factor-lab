#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.paper_replay_performance import (
    compute_paper_replay_returns,
    summarize_paper_replay_monthly_returns,
    summarize_paper_replay_returns,
    write_paper_replay_report,
)
from qlib_factor_lab.qlib_bootstrap import init_qlib


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build close-to-next-close paper replay performance from target portfolios.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--provider-config", default="configs/provider_current.yaml")
    parser.add_argument("--target-glob", required=True)
    parser.add_argument("--paper-run-root", default="runs/paper_batch")
    parser.add_argument("--close-csv", default=None, help="Optional fixture/source close CSV with date,instrument,close.")
    parser.add_argument("--output-dir", default="reports/paper_replay")
    parser.add_argument("--total-equity", type=float, default=1_000_000.0)
    parser.add_argument("--title", default="Paper Replay Performance")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    target_paths = sorted(Path(path) for path in glob.glob(str(_resolve(root, args.target_glob))))
    if not target_paths:
        raise SystemExit(f"no target portfolios matched: {args.target_glob}")
    close = pd.read_csv(_resolve(root, args.close_csv)) if args.close_csv else _fetch_qlib_close(root, args.provider_config, target_paths)
    daily = compute_paper_replay_returns(
        target_paths,
        close,
        paper_run_root=_resolve(root, args.paper_run_root),
        total_equity=args.total_equity,
    )
    summary = summarize_paper_replay_returns(daily)
    monthly = summarize_paper_replay_monthly_returns(daily)

    output_dir = _resolve(root, args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_path = output_dir / "paper_replay_daily_returns.csv"
    summary_path = output_dir / "paper_replay_summary.csv"
    monthly_path = output_dir / "paper_replay_monthly.csv"
    report_path = output_dir / "paper_replay_report.md"
    daily.to_csv(daily_path, index=False)
    pd.DataFrame([summary]).to_csv(summary_path, index=False)
    monthly.to_csv(monthly_path, index=False)
    write_paper_replay_report(daily, summary, monthly, report_path, title=args.title)

    print(f"wrote: {daily_path}")
    print(f"wrote: {summary_path}")
    print(f"wrote: {monthly_path}")
    print(f"wrote: {report_path}")
    return 0


def _fetch_qlib_close(root: Path, provider_config: str, target_paths: list[Path]) -> pd.DataFrame:
    project_config = load_project_config(_resolve(root, provider_config))
    init_qlib(project_config)
    from qlib.data import D

    dates = _target_dates(target_paths)
    start = min(dates)
    frame = D.features(
        D.instruments(project_config.market),
        ["$close"],
        start_time=start,
        end_time=project_config.end_time,
        freq=project_config.freq,
    )
    frame.columns = ["close"]
    frame = frame.reset_index()
    if "datetime" in frame.columns:
        frame = frame.rename(columns={"datetime": "date"})
    if "date" not in frame.columns or "instrument" not in frame.columns:
        raise ValueError("qlib close frame must include date/datetime and instrument columns")
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    return frame.loc[:, ["date", "instrument", "close"]].dropna(subset=["close"]).reset_index(drop=True)


def _target_dates(target_paths: list[Path]) -> list[str]:
    dates: list[str] = []
    for path in target_paths:
        frame = pd.read_csv(path, usecols=["date"])
        if not frame.empty:
            dates.append(pd.to_datetime(frame["date"].max()).strftime("%Y-%m-%d"))
    if not dates:
        raise ValueError("target portfolios did not contain any dates")
    return dates


def _resolve(root: Path, path: str | Path | None) -> Path:
    if path is None:
        return root
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
