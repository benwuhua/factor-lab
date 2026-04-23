from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_yaml


@dataclass(frozen=True)
class ReconcileConfig:
    weight_tolerance: float = 1e-4


@dataclass(frozen=True)
class ReconcileReport:
    rows: tuple[dict[str, Any], ...]

    @property
    def passed(self) -> bool:
        return all(row["status"] == "pass" for row in self.rows)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=["check", "status", "instrument", "expected", "actual", "detail"])


def load_reconcile_config(path: str | Path) -> ReconcileConfig:
    data = load_yaml(path)
    raw = data.get("reconcile", data)
    return ReconcileConfig(weight_tolerance=float(raw.get("weight_tolerance", 1e-4)))


def reconcile_positions(
    expected_positions: pd.DataFrame,
    actual_positions: pd.DataFrame,
    config: ReconcileConfig = ReconcileConfig(),
) -> ReconcileReport:
    expected = _weights(expected_positions)
    actual = _weights(actual_positions)
    rows = []
    for instrument in sorted(set(expected) | set(actual)):
        expected_weight = expected.get(instrument, 0.0)
        actual_weight = actual.get(instrument, 0.0)
        diff = actual_weight - expected_weight
        if instrument not in actual:
            rows.append(_row("missing_actual", False, instrument, expected_weight, 0.0, "missing from actual positions"))
        elif instrument not in expected:
            rows.append(_row("unexpected_actual", False, instrument, 0.0, actual_weight, "unexpected actual position"))
        elif abs(diff) > config.weight_tolerance:
            rows.append(_row("weight_mismatch", False, instrument, expected_weight, actual_weight, f"diff={diff:.6g}"))
    if not rows:
        rows.append(_row("positions_match", True, "", 0.0, 0.0, ""))
    return ReconcileReport(tuple(rows))


def write_reconciliation_report(report: ReconcileReport, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Reconciliation Report",
        "",
        f"- status: {'pass' if report.passed else 'fail'}",
        "",
        "| check | status | instrument | expected | actual | detail |",
        "|---|---|---|---:|---:|---|",
    ]
    for row in report.rows:
        lines.append(
            f"| {row['check']} | {row['status']} | {row['instrument']} | "
            f"{float(row['expected']):.6g} | {float(row['actual']):.6g} | {row['detail']} |"
        )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def _weights(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {}
    return {str(row["instrument"]): float(row["current_weight"]) for _, row in frame.iterrows()}


def _row(check: str, passed: bool, instrument: str, expected: float, actual: float, detail: str) -> dict[str, Any]:
    return {
        "check": check,
        "status": "pass" if passed else "fail",
        "instrument": instrument,
        "expected": expected,
        "actual": actual,
        "detail": detail,
    }
