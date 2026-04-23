from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_yaml


DEFAULT_SIGNAL_COLUMNS = (
    "date",
    "instrument",
    "tradable",
    "ensemble_score",
    "rule_score",
    "risk_flags",
)


@dataclass(frozen=True)
class DataQualityConfig:
    required_columns: tuple[str, ...] = DEFAULT_SIGNAL_COLUMNS
    score_column: str = "ensemble_score"
    min_coverage_ratio: float = 0.8
    require_single_date: bool = True


@dataclass(frozen=True)
class QualityReport:
    rows: tuple[dict[str, Any], ...]

    @property
    def passed(self) -> bool:
        return all(row["status"] == "pass" for row in self.rows)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=["check", "status", "value", "threshold", "detail"])


def load_data_quality_config(path: str | Path) -> DataQualityConfig:
    data = load_yaml(path)
    raw = data.get("data_quality", data)
    return DataQualityConfig(
        required_columns=tuple(raw.get("required_columns", DEFAULT_SIGNAL_COLUMNS)),
        score_column=str(raw.get("score_column", "ensemble_score")),
        min_coverage_ratio=float(raw.get("min_coverage_ratio", 0.8)),
        require_single_date=bool(raw.get("require_single_date", True)),
    )


def check_signal_quality(signal: pd.DataFrame, config: DataQualityConfig = DataQualityConfig()) -> QualityReport:
    rows: list[dict[str, Any]] = []
    missing = [column for column in config.required_columns if column not in signal.columns]
    rows.append(
        _row(
            "required_columns",
            not missing,
            len(config.required_columns) - len(missing),
            len(config.required_columns),
            f"missing={missing}" if missing else "",
        )
    )
    if signal.empty:
        rows.append(_row("non_empty", False, 0, 1, "signal file has no rows"))
        return QualityReport(tuple(rows))
    rows.append(_row("non_empty", True, len(signal), 1, ""))

    if config.score_column in signal.columns:
        scores = pd.to_numeric(signal[config.score_column], errors="coerce")
        finite = scores.map(lambda value: math.isfinite(float(value)) if pd.notna(value) else False)
        coverage = float(finite.mean()) if len(finite) else 0.0
        rows.append(
            _row(
                "score_coverage",
                coverage >= config.min_coverage_ratio,
                coverage,
                config.min_coverage_ratio,
                config.score_column,
            )
        )
        unique_scores = int(scores[finite].nunique())
        rows.append(_row("score_not_constant", unique_scores > 1, unique_scores, ">1", config.score_column))

    if "instrument" in signal.columns:
        duplicate_count = int(signal["instrument"].duplicated().sum())
        rows.append(_row("unique_instruments", duplicate_count == 0, duplicate_count, 0, "duplicates"))

    if config.require_single_date and "date" in signal.columns:
        date_count = int(signal["date"].nunique())
        rows.append(_row("single_date", date_count == 1, date_count, 1, ""))

    return QualityReport(tuple(rows))


def write_quality_report(report: QualityReport, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Data Quality Report",
        "",
        f"- status: {'pass' if report.passed else 'fail'}",
        "",
        "| check | status | value | threshold | detail |",
        "|---|---|---:|---:|---|",
    ]
    for row in report.rows:
        lines.append(
            f"| {row['check']} | {row['status']} | {row['value']} | {row['threshold']} | {row['detail']} |"
        )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def _row(check: str, passed: bool, value: Any, threshold: Any, detail: str) -> dict[str, Any]:
    return {
        "check": check,
        "status": "pass" if passed else "fail",
        "value": value,
        "threshold": threshold,
        "detail": detail,
    }
