#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.akshare_data import today_for_daily_data
from qlib_factor_lab.event_backfill import EventBackfillConfig, build_event_backfill_command


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Backfill company announcement events over a historical date window.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--as-of-date", default=today_for_daily_data())
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--universes", nargs="+", default=["csi300", "csi500"], choices=["csi300", "csi500"])
    parser.add_argument("--output", default="data/company_events.csv")
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    command = build_event_backfill_command(
        EventBackfillConfig(
            project_root=root,
            as_of_date=args.as_of_date,
            days=args.days,
            universes=tuple(args.universes),
            output=args.output,
            delay=args.delay,
        )
    )
    if args.dry_run:
        print(" ".join(command))
        return 0
    completed = subprocess.run(command, cwd=root, check=False)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
