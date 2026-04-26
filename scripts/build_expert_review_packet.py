#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.expert_review import (
    ExpertReviewRunConfig,
    build_expert_review_packet,
    load_stock_cards_jsonl,
    run_expert_review_command,
    write_expert_review_result,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build a Markdown packet for expert LLM portfolio review.")
    parser.add_argument("--target-portfolio", required=True, help="Target portfolio CSV.")
    parser.add_argument("--factor-diagnostics", default=None, help="Optional single factor diagnostics CSV.")
    parser.add_argument("--stock-cards", default=None, help="Optional stock card JSONL file.")
    parser.add_argument("--run-date", default="")
    parser.add_argument("--output", default="reports/expert_review_packet.md")
    parser.add_argument("--run-review", action="store_true", help="Run an expert LLM command with the packet on stdin.")
    parser.add_argument("--llm-command", default=None, help="Command string used when --run-review is set.")
    parser.add_argument("--review-output", default=None, help="Optional expert review result Markdown path.")
    parser.add_argument("--timeout-sec", type=int, default=300)
    parser.add_argument("--project-root", default=str(default_root), help="Project root used to resolve relative paths.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    target = pd.read_csv(_resolve(root, args.target_portfolio))
    diagnostics = pd.read_csv(_resolve(root, args.factor_diagnostics)) if args.factor_diagnostics else None
    stock_cards = load_stock_cards_jsonl(_resolve(root, args.stock_cards)) if args.stock_cards else None
    packet = build_expert_review_packet(target, diagnostics, run_date=args.run_date, stock_cards=stock_cards)
    output = _resolve(root, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(packet, encoding="utf-8")
    print(f"wrote: {output}")
    if args.run_review:
        result = run_expert_review_command(
            packet,
            ExpertReviewRunConfig(
                enabled=True,
                command=shlex.split(args.llm_command or ""),
                timeout_sec=args.timeout_sec,
            ),
            cwd=root,
        )
        review_output = _resolve(root, args.review_output) if args.review_output else output.with_name("expert_review_result.md")
        write_expert_review_result(result, review_output)
        print(f"expert_review_status: {result.status}")
        print(f"expert_review_decision: {result.decision}")
        print(f"wrote: {review_output}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
