#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from qlib_factor_lab.workbench_tasks import run_workbench_task


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one allowlisted Factor Lab workbench task.")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--root", default=str(ROOT))
    args = parser.parse_args()
    return run_workbench_task(args.root, args.task_id, args.run_dir)


if __name__ == "__main__":
    raise SystemExit(main())
