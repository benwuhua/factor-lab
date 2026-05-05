#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.akshare_data import today_for_daily_data
from qlib_factor_lab.daily_update_schedule import build_daily_update_command, next_run_at


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Run the full daily data refresh after the A-share close.")
    parser.add_argument("--project-root", default=str(root))
    parser.add_argument("--run-time", default="15:45", help="Local wall-clock run time, HH:MM.")
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--as-of-date", default="", help="Defaults to the latest closed A-share session.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument("--run-now", action="store_true", help="Run immediately instead of waiting for --run-time.")
    parser.add_argument("--dry-run", action="store_true", help="Print the command and exit without running it.")
    args = parser.parse_args()

    run_at = next_run_at(datetime.now().astimezone(), run_time=args.run_time, timezone=args.timezone)
    if not args.run_now:
        wait_sec = max(0.0, (run_at - datetime.now(run_at.tzinfo)).total_seconds())
        print(f"scheduled_daily_data_update_at: {run_at.isoformat(timespec='seconds')}")
        if wait_sec > 0:
            time.sleep(wait_sec)

    as_of_date = args.as_of_date or today_for_daily_data()
    command = build_daily_update_command(
        python_bin=args.python_bin,
        as_of_date=as_of_date.replace("-", ""),
        env_file=args.env_file,
    )
    print("command: " + " ".join(command))
    if args.dry_run:
        return 0
    completed = subprocess.run(command, cwd=Path(args.project_root).expanduser().resolve(), check=False)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
