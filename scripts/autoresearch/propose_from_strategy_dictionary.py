#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import yaml

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.combo_spec import load_combo_spec
from qlib_factor_lab.strategy_dictionary import (
    build_expression_candidate_from_strategy,
    load_strategy_dictionary,
    propose_strategy_ideas,
    render_strategy_proposals_markdown,
)


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Propose autoresearch ideas from a strategy dictionary.")
    parser.add_argument("--dictionary", default="configs/strategy_dictionary/151_trading_strategies_equity.yaml")
    parser.add_argument("--combo-spec", default="configs/combo_specs/balanced_multifactor_v1.yaml")
    parser.add_argument("--lane", default=None, help="Optional candidate_lane filter, e.g. expression, theme, combo.")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--output-md", default="reports/strategy_dictionary_proposals.md")
    parser.add_argument("--output-csv", default="reports/strategy_dictionary_proposals.csv")
    parser.add_argument("--write-expression-candidates", action="store_true")
    parser.add_argument("--candidate-output-dir", default="configs/autoresearch/candidates")
    args = parser.parse_args()

    dictionary_path = _resolve(root, args.dictionary)
    combo_path = _resolve(root, args.combo_spec)
    entries = load_strategy_dictionary(dictionary_path)
    proposals = propose_strategy_ideas(
        entries,
        combo_spec=load_combo_spec(combo_path) if combo_path.exists() else None,
        limit=args.limit,
        candidate_lane=args.lane,
    )

    md_path = _resolve(root, args.output_md)
    csv_path = _resolve(root, args.output_csv)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_strategy_proposals_markdown(proposals), encoding="utf-8")
    pd.DataFrame([proposal.__dict__ for proposal in proposals]).to_csv(csv_path, index=False)
    written_candidates = []
    if args.write_expression_candidates:
        entry_by_id = {entry.strategy_id: entry for entry in entries}
        output_dir = _resolve(root, args.candidate_output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        for proposal in proposals:
            if proposal.candidate_lane != "expression":
                continue
            try:
                candidate = build_expression_candidate_from_strategy(entry_by_id[proposal.strategy_id])
            except ValueError:
                continue
            candidate_path = output_dir / f"{candidate['name']}.yaml"
            candidate_path.write_text(yaml.safe_dump(candidate, allow_unicode=True, sort_keys=False), encoding="utf-8")
            written_candidates.append(candidate_path)
    print(render_strategy_proposals_markdown(proposals))
    print(f"wrote: {md_path}")
    print(f"wrote: {csv_path}")
    for candidate_path in written_candidates:
        print(f"wrote: {candidate_path}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    return candidate if candidate.is_absolute() else root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
