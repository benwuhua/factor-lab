#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.replay import replay_daily_run, write_replay_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay and audit a daily Factor Lab run bundle.")
    parser.add_argument("--project-root", default=str(project_root()))
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    run_dir = _resolve(root, args.run_dir)
    report = replay_daily_run(run_dir)
    output = _resolve(root, args.output) if args.output else run_dir / "replay_report.md"
    path = write_replay_report(report, output)
    print(f"status: {'pass' if report.passed else 'fail'}")
    print(f"wrote: {path}")
    return 0 if report.passed else 1


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    return candidate if candidate.is_absolute() else root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
