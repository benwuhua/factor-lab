from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ExpressionLedgerSummary:
    total_runs: int
    status_counts: dict[str, int]
    top_review: list[dict[str, Any]]
    discard_reasons: dict[str, int]
    crash_reasons: dict[str, int]


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


def summarize_expression_ledger(path: str | Path, top_n: int = 10) -> ExpressionLedgerSummary:
    rows = _read_ledger_rows(path)
    status_counts = Counter(str(row.get("status", "") or "unknown") for row in rows)
    review_rows = [row for row in rows if row.get("status") == "review"]
    review_rows.sort(key=lambda row: _as_float(row.get("primary_metric")), reverse=True)
    discard_reasons = Counter(
        str(row.get("decision_reason", "") or "unspecified")
        for row in rows
        if row.get("status") == "discard_candidate"
    )
    crash_reasons = Counter(
        str(row.get("decision_reason", "") or "unspecified") for row in rows if row.get("status") == "crash"
    )
    return ExpressionLedgerSummary(
        total_runs=len(rows),
        status_counts=dict(status_counts),
        top_review=review_rows[:top_n],
        discard_reasons=dict(discard_reasons),
        crash_reasons=dict(crash_reasons),
    )


def render_expression_ledger_status_report(summary: ExpressionLedgerSummary) -> str:
    lines = [
        "# Expression Autoresearch Ledger",
        "",
        f"- Total runs: {summary.total_runs}",
        "",
        "## Status Counts",
        "",
        "| Status | Count |",
        "|---|---:|",
    ]
    for status, count in sorted(summary.status_counts.items()):
        lines.append(f"| {status} | {count} |")
    lines.extend(
        [
            "",
            "## Top Review Candidates",
            "",
            "| Candidate | Primary | Secondary | Guard | Run |",
            "|---|---:|---:|---:|---|",
        ]
    )
    if summary.top_review:
        for row in summary.top_review:
            lines.append(
                "| {candidate} | {primary} | {secondary} | {guard} | {run_id} |".format(
                    candidate=row.get("candidate_name", ""),
                    primary=_format_float(row.get("primary_metric")),
                    secondary=_format_float(row.get("secondary_metric")),
                    guard=_format_float(row.get("guard_metric")),
                    run_id=row.get("run_id", ""),
                )
            )
    else:
        lines.append("|  |  |  |  |  |")
    lines.extend(_reason_section("Discard Reasons", summary.discard_reasons))
    lines.extend(_reason_section("Crash Reasons", summary.crash_reasons))
    return "\n".join(lines) + "\n"


def _read_ledger_rows(path: str | Path) -> list[dict[str, str]]:
    ledger = Path(path)
    if not ledger.exists():
        return []
    with ledger.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _reason_section(title: str, reasons: dict[str, int]) -> list[str]:
    lines = ["", f"## {title}", "", "| Reason | Count |", "|---|---:|"]
    if reasons:
        for reason, count in sorted(reasons.items()):
            lines.append(f"| {reason} | {count} |")
    else:
        lines.append("|  |  |")
    return lines


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("-inf")


def _format_float(value: Any) -> str:
    try:
        return f"{float(value):.6f}"
    except (TypeError, ValueError):
        return "nan"
