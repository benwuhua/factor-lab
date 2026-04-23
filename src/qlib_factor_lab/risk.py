from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_yaml


@dataclass(frozen=True)
class RiskConfig:
    max_single_weight: float = 0.1
    min_positions: int = 10
    min_signal_coverage: float = 0.2
    max_turnover: float | None = None
    report_output_path: Path = Path("reports/portfolio_risk_{run_yyyymmdd}.md")


@dataclass(frozen=True)
class RiskReport:
    rows: tuple[dict[str, Any], ...]

    @property
    def passed(self) -> bool:
        return all(row["status"] == "pass" for row in self.rows)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=["check", "status", "value", "threshold", "detail"])


def load_risk_config(path: str | Path) -> RiskConfig:
    data = load_yaml(path)
    raw = data.get("risk", data)
    output = data.get("output", {})
    max_turnover = raw.get("max_turnover")
    return RiskConfig(
        max_single_weight=float(raw.get("max_single_weight", 0.1)),
        min_positions=int(raw.get("min_positions", 10)),
        min_signal_coverage=float(raw.get("min_signal_coverage", 0.2)),
        max_turnover=float(max_turnover) if max_turnover is not None else None,
        report_output_path=Path(output.get("report", "reports/portfolio_risk_{run_yyyymmdd}.md")),
    )


def check_portfolio_risk(
    portfolio: pd.DataFrame,
    signal: pd.DataFrame,
    config: RiskConfig = RiskConfig(),
    current_positions: pd.DataFrame | None = None,
) -> RiskReport:
    rows = []
    max_weight = float(portfolio["target_weight"].max()) if not portfolio.empty and "target_weight" in portfolio else 0.0
    rows.append(_row("max_single_weight", max_weight <= config.max_single_weight, max_weight, config.max_single_weight, ""))

    position_count = int(len(portfolio))
    rows.append(_row("min_positions", position_count >= config.min_positions, position_count, config.min_positions, ""))

    coverage = _signal_coverage(signal)
    rows.append(_row("min_signal_coverage", coverage >= config.min_signal_coverage, coverage, config.min_signal_coverage, ""))

    if config.max_turnover is not None:
        turnover = _turnover(portfolio, current_positions)
        rows.append(_row("max_turnover", turnover <= config.max_turnover, turnover, config.max_turnover, ""))

    negative_count = int((portfolio.get("target_weight", pd.Series(dtype=float)) < 0).sum())
    rows.append(_row("no_negative_weights", negative_count == 0, negative_count, 0, ""))
    return RiskReport(tuple(rows))


def write_risk_report(report: RiskReport, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Portfolio Risk Report",
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


def _signal_coverage(signal: pd.DataFrame) -> float:
    if signal.empty:
        return 0.0
    if "eligible" in signal.columns:
        return float(signal["eligible"].fillna(False).astype(bool).mean())
    if "tradable" in signal.columns:
        return float(signal["tradable"].fillna(False).astype(bool).mean())
    return 1.0


def _turnover(portfolio: pd.DataFrame, current_positions: pd.DataFrame | None) -> float:
    target = _weights_by_instrument(portfolio, "target_weight")
    current = _weights_by_instrument(current_positions, "current_weight") if current_positions is not None else {}
    instruments = set(target) | set(current)
    return float(sum(abs(target.get(instrument, 0.0) - current.get(instrument, 0.0)) for instrument in instruments))


def _weights_by_instrument(frame: pd.DataFrame | None, weight_col: str) -> dict[str, float]:
    if frame is None or frame.empty or weight_col not in frame.columns:
        return {}
    return {str(row["instrument"]): float(row[weight_col]) for _, row in frame.iterrows()}


def _row(check: str, passed: bool, value: Any, threshold: Any, detail: str) -> dict[str, Any]:
    return {
        "check": check,
        "status": "pass" if passed else "fail",
        "value": value,
        "threshold": threshold,
        "detail": detail,
    }
