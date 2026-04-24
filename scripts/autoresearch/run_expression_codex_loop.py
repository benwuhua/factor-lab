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

from qlib_factor_lab.autoresearch.codex_loop import (
    DEFAULT_ALLOWED_FAMILIES,
    parse_until_deadline,
    resolve_max_iterations,
    run_codex_autoloop,
)


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Run overnight expression autoresearch through Codex CLI.")
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Maximum iterations. Omit or set 0 to run until the time/hour stop condition.",
    )
    parser.add_argument("--max-hours", type=float, default=None)
    parser.add_argument("--until", default=None, help='Stop at HH:MM or "YYYY-MM-DD HH:MM" in --timezone.')
    parser.add_argument("--timezone", default="Asia/Shanghai")
    parser.add_argument("--sleep-sec", type=float, default=10.0)
    parser.add_argument("--max-crashes", type=int, default=5)
    parser.add_argument("--model", default="gpt-5.4")
    parser.add_argument("--sandbox", default="workspace-write")
    parser.add_argument("--codex-full-auto", action="store_true", help="Pass --full-auto to codex exec.")
    parser.add_argument(
        "--allow-protected-branch",
        action="store_true",
        help="Allow running on main/master. By default the loop refuses because it commits each candidate.",
    )
    parser.add_argument(
        "--candidate-file",
        default="configs/autoresearch/candidates/example_expression.yaml",
    )
    parser.add_argument("--ledger", default="reports/autoresearch/expression_results.tsv")
    parser.add_argument("--allowed-families", nargs="+", default=DEFAULT_ALLOWED_FAMILIES)
    args = parser.parse_args()

    deadline = parse_until_deadline(args.until, timezone=args.timezone)
    max_iterations = resolve_max_iterations(
        max_iterations=args.max_iterations,
        has_deadline=deadline is not None,
        max_hours=args.max_hours,
    )
    result = run_codex_autoloop(
        root=root,
        max_iterations=max_iterations,
        max_crashes=args.max_crashes,
        deadline=deadline,
        max_hours=args.max_hours,
        sleep_sec=args.sleep_sec,
        model=args.model,
        sandbox=args.sandbox,
        candidate_file=args.candidate_file,
        ledger_path=args.ledger,
        allowed_families=list(args.allowed_families),
        full_auto=args.codex_full_auto,
        allow_protected_branch=args.allow_protected_branch,
    )
    print(f"iterations_started: {result.iterations_started}")
    print(f"crash_count: {result.crash_count}")
    print(f"stop_reason: {result.stop_reason}")
    print(f"log_dir: {result.log_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
