#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.expert_review import build_expert_review_packet


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build a Markdown packet for expert LLM portfolio review.")
    parser.add_argument("--target-portfolio", required=True, help="Target portfolio CSV.")
    parser.add_argument("--factor-diagnostics", default=None, help="Optional single factor diagnostics CSV.")
    parser.add_argument("--run-date", default="")
    parser.add_argument("--output", default="reports/expert_review_packet.md")
    parser.add_argument("--project-root", default=str(default_root), help="Project root used to resolve relative paths.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    target = pd.read_csv(_resolve(root, args.target_portfolio))
    diagnostics = pd.read_csv(_resolve(root, args.factor_diagnostics)) if args.factor_diagnostics else None
    packet = build_expert_review_packet(target, diagnostics, run_date=args.run_date)
    output = _resolve(root, args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(packet, encoding="utf-8")
    print(f"wrote: {output}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
