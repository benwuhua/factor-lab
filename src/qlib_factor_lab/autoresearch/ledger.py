from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


EXPRESSION_LEDGER_COLUMNS = [
    "timestamp",
    "run_id",
    "commit",
    "loop",
    "contract",
    "candidate_name",
    "candidate_file",
    "candidate_hash",
    "status",
    "decision_reason",
    "primary_metric",
    "secondary_metric",
    "guard_metric",
    "rank_ic_mean_h5",
    "rank_ic_mean_h20",
    "neutral_rank_ic_mean_h5",
    "neutral_rank_ic_mean_h20",
    "long_short_mean_return_h20",
    "top_quantile_turnover_h20",
    "observations_h20",
    "complexity_score",
    "artifact_dir",
    "elapsed_sec",
]


def append_expression_ledger_row(path: str | Path, row: dict[str, Any]) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output.exists() or output.stat().st_size == 0
    with output.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXPRESSION_LEDGER_COLUMNS, delimiter="\t", extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({column: row.get(column, "") for column in EXPRESSION_LEDGER_COLUMNS})
    return output
