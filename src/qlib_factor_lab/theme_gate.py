from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ThemeGateConfig:
    min_research_candidates: int = 3
    min_signal_coverage: float = 0.5
    min_amount_20d: float = 100_000_000.0
    min_theme_research_score: float | None = None
    min_theme_exposure: float | None = None


@dataclass(frozen=True)
class ThemeGateReport:
    rows: tuple[dict[str, Any], ...]

    @property
    def decision(self) -> str:
        statuses = {str(row["status"]) for row in self.rows}
        if "fail" in statuses:
            return "reject"
        if "caution" in statuses:
            return "caution"
        return "pass"

    @property
    def passed(self) -> bool:
        return self.decision == "pass"

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=["check", "status", "value", "threshold", "detail"])


def check_theme_gate(candidates: pd.DataFrame, config: ThemeGateConfig = ThemeGateConfig()) -> ThemeGateReport:
    frame = candidates.copy()
    if frame.empty:
        return ThemeGateReport(
            (
                _row("min_research_candidates", "fail", 0, config.min_research_candidates, "empty candidate set"),
            )
        )

    research = frame[frame.get("research_status", pd.Series("", index=frame.index)).astype(str) == "research_candidate"]
    rows = [
        _row(
            "min_research_candidates",
            "pass" if len(research) >= config.min_research_candidates else "caution",
            int(len(research)),
            config.min_research_candidates,
            "theme shortlist can be small, but fewer names reduce comparability",
        ),
    ]

    coverage = _signal_coverage(frame)
    rows.append(
        _row(
            "min_signal_coverage",
            "pass" if coverage >= config.min_signal_coverage else "caution",
            coverage,
            config.min_signal_coverage,
            "watch_only names need supplemental data before ranking",
        )
    )
    rows.append(
        _row(
            "watch_only_positions",
            "pass" if _watch_only_count(frame) == 0 else "caution",
            _watch_only_count(frame),
            0,
            _instrument_detail(frame, frame.get("research_status", pd.Series("", index=frame.index)).astype(str) == "watch_only"),
        )
    )

    blocked = _blocked_mask(frame, "event_blocked")
    rows.append(
        _row(
            "event_blocked_positions",
            "fail" if blocked.any() else "pass",
            int(blocked.sum()),
            0,
            _instrument_detail(frame, blocked),
        )
    )
    buy_blocked = _blocked_mask(frame, "buy_blocked") | _false_mask(frame, "tradable")
    rows.append(
        _row(
            "tradability_blocked_positions",
            "fail" if buy_blocked.any() else "pass",
            int(buy_blocked.sum()),
            0,
            _instrument_detail(frame, buy_blocked),
        )
    )

    low_liquidity = _numeric(frame, "amount_20d") < config.min_amount_20d
    low_liquidity = low_liquidity.fillna(False)
    rows.append(
        _row(
            "min_amount_20d",
            "pass" if not low_liquidity.any() else "caution",
            int(low_liquidity.sum()),
            config.min_amount_20d,
            _instrument_detail(frame, low_liquidity),
        )
    )

    if config.min_theme_research_score is not None:
        weak_score = _numeric(frame, "theme_research_score") < config.min_theme_research_score
        rows.append(
            _row(
                "min_theme_research_score",
                "pass" if not weak_score.fillna(False).any() else "caution",
                int(weak_score.fillna(False).sum()),
                config.min_theme_research_score,
                _instrument_detail(frame, weak_score.fillna(False)),
            )
        )
    if config.min_theme_exposure is not None:
        weak_exposure = _numeric(frame, "theme_exposure") < config.min_theme_exposure
        rows.append(
            _row(
                "min_theme_exposure",
                "pass" if not weak_exposure.fillna(False).any() else "caution",
                int(weak_exposure.fillna(False).sum()),
                config.min_theme_exposure,
                _instrument_detail(frame, weak_exposure.fillna(False)),
            )
        )

    return ThemeGateReport(tuple(rows))


def write_theme_gate_report(report: ThemeGateReport, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Theme Gate Report",
        "",
        f"- decision: {report.decision}",
        "",
        "| check | status | value | threshold | detail |",
        "|---|---|---:|---:|---|",
    ]
    for row in report.rows:
        lines.append(f"| {row['check']} | {row['status']} | {row['value']} | {row['threshold']} | {row['detail']} |")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def _signal_coverage(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    if "ensemble_score" not in frame.columns:
        return 0.0
    return float(pd.to_numeric(frame["ensemble_score"], errors="coerce").notna().mean())


def _watch_only_count(frame: pd.DataFrame) -> int:
    if "research_status" not in frame.columns:
        return 0
    return int((frame["research_status"].astype(str) == "watch_only").sum())


def _blocked_mask(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].map(_truthy)


def _false_mask(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index)
    return frame[column].map(_falsey)


def _truthy(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "blocked"}


def _falsey(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return not value
    return str(value).strip().lower() in {"0", "false", "no", "n"}


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _instrument_detail(frame: pd.DataFrame, mask: pd.Series) -> str:
    if "instrument" not in frame.columns:
        return ""
    instruments = frame.loc[mask.fillna(False), "instrument"].astype(str).tolist()
    return "; ".join(instruments)


def _row(check: str, status: str, value: Any, threshold: Any, detail: str) -> dict[str, Any]:
    return {
        "check": check,
        "status": status,
        "value": value,
        "threshold": threshold,
        "detail": detail,
    }
