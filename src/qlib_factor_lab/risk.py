from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_yaml
from .exposure_attribution import build_exposure_attribution, load_factor_family_map, load_factor_logic_map


@dataclass(frozen=True)
class RiskConfig:
    max_single_weight: float = 0.1
    min_positions: int = 10
    min_signal_coverage: float = 0.2
    max_turnover: float | None = None
    max_industry_weight: float | None = None
    min_factor_family_count: int | None = None
    max_factor_family_concentration: float | None = None
    min_factor_logic_count: int | None = None
    max_factor_logic_concentration: float | None = None
    portfolio_value: float | None = None
    min_amount_20d: float | None = None
    max_position_amount_share: float | None = None
    max_estimated_cost: float | None = None
    commission_bps: float = 0.0
    slippage_bps: float = 0.0
    stamp_tax_bps: float = 0.0
    max_risk_budget_per_position: float | None = None
    factor_family_map_path: Path | None = None
    factor_logic_map_path: Path | None = None
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
    max_industry_weight = raw.get("max_industry_weight")
    min_factor_family_count = raw.get("min_factor_family_count")
    max_factor_family_concentration = raw.get("max_factor_family_concentration")
    min_factor_logic_count = raw.get("min_factor_logic_count")
    max_factor_logic_concentration = raw.get("max_factor_logic_concentration")
    portfolio_value = raw.get("portfolio_value", raw.get("capital_base"))
    min_amount_20d = raw.get("min_amount_20d")
    max_position_amount_share = raw.get("max_position_amount_share")
    max_estimated_cost = raw.get("max_estimated_cost")
    max_risk_budget_per_position = raw.get("max_risk_budget_per_position")
    factor_family_map_path = raw.get("factor_family_map_path")
    factor_logic_map_path = raw.get("factor_logic_map_path", factor_family_map_path)
    return RiskConfig(
        max_single_weight=float(raw.get("max_single_weight", 0.1)),
        min_positions=int(raw.get("min_positions", 10)),
        min_signal_coverage=float(raw.get("min_signal_coverage", 0.2)),
        max_turnover=float(max_turnover) if max_turnover is not None else None,
        max_industry_weight=float(max_industry_weight) if max_industry_weight is not None else None,
        min_factor_family_count=int(min_factor_family_count) if min_factor_family_count is not None else None,
        max_factor_family_concentration=(
            float(max_factor_family_concentration) if max_factor_family_concentration is not None else None
        ),
        min_factor_logic_count=int(min_factor_logic_count) if min_factor_logic_count is not None else None,
        max_factor_logic_concentration=(
            float(max_factor_logic_concentration) if max_factor_logic_concentration is not None else None
        ),
        portfolio_value=float(portfolio_value) if portfolio_value is not None else None,
        min_amount_20d=float(min_amount_20d) if min_amount_20d is not None else None,
        max_position_amount_share=(
            float(max_position_amount_share) if max_position_amount_share is not None else None
        ),
        max_estimated_cost=float(max_estimated_cost) if max_estimated_cost is not None else None,
        commission_bps=float(raw.get("commission_bps", 0.0)),
        slippage_bps=float(raw.get("slippage_bps", 0.0)),
        stamp_tax_bps=float(raw.get("stamp_tax_bps", 0.0)),
        max_risk_budget_per_position=(
            float(max_risk_budget_per_position) if max_risk_budget_per_position is not None else None
        ),
        factor_family_map_path=Path(str(factor_family_map_path)) if factor_family_map_path else None,
        factor_logic_map_path=Path(str(factor_logic_map_path)) if factor_logic_map_path else None,
        report_output_path=Path(output.get("report", "reports/portfolio_risk_{run_yyyymmdd}.md")),
    )


def check_portfolio_risk(
    portfolio: pd.DataFrame,
    signal: pd.DataFrame,
    config: RiskConfig = RiskConfig(),
    current_positions: pd.DataFrame | None = None,
    factor_family_map: dict[str, str] | None = None,
    factor_logic_map: dict[str, str] | None = None,
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

    blocked_detail = _event_blocked_detail(portfolio)
    blocked_count = int(len(blocked_detail))
    rows.append(
        _row(
            "event_blocked_positions",
            blocked_count == 0,
            blocked_count,
            0,
            "; ".join(blocked_detail),
        )
    )
    rows.extend(_capacity_and_cost_rows(portfolio, config))
    rows.extend(_exposure_maturity_rows(portfolio, config, factor_family_map or {}, factor_logic_map or {}))
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


def _event_blocked_detail(portfolio: pd.DataFrame) -> list[str]:
    if portfolio.empty or "event_blocked" not in portfolio.columns:
        return []

    blocked = portfolio[portfolio["event_blocked"].map(_truthy_bool)]
    details = []
    for _, row in blocked.iterrows():
        summary = _clean_event_detail(row.get("event_risk_summary"))
        if not summary:
            summary = _clean_event_detail(row.get("active_event_types"))
        if not summary:
            summary = _clean_event_detail(row.get("max_event_severity"))
        details.append(f"{row['instrument']}: {summary}")
    return details


def _capacity_and_cost_rows(portfolio: pd.DataFrame, config: RiskConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if config.min_amount_20d is not None:
        rows.append(_min_amount_row(portfolio, config.min_amount_20d))
    if config.max_position_amount_share is not None:
        rows.append(_max_position_amount_share_row(portfolio, config))
    if config.max_estimated_cost is not None:
        rows.append(_max_estimated_cost_row(portfolio, config))
    if config.max_risk_budget_per_position is not None:
        rows.append(_max_risk_budget_row(portfolio, config.max_risk_budget_per_position))
    return rows


def _min_amount_row(portfolio: pd.DataFrame, threshold: float) -> dict[str, Any]:
    missing = _missing_columns(portfolio, ["amount_20d"])
    if missing:
        return _row("min_amount_20d", False, "", threshold, f"missing required column: {missing[0]}")
    values = pd.to_numeric(portfolio["amount_20d"], errors="coerce")
    min_amount = float(values.min()) if not values.empty else 0.0
    detail = _below_threshold_detail(portfolio, values, threshold, "amount_20d")
    return _row("min_amount_20d", bool((values >= threshold).all()), min_amount, threshold, detail)


def _max_position_amount_share_row(portfolio: pd.DataFrame, config: RiskConfig) -> dict[str, Any]:
    missing = _missing_columns(portfolio, ["target_weight", "amount_20d"])
    if missing:
        return _row("max_position_amount_share", False, "", config.max_position_amount_share, f"missing required column: {missing[0]}")
    if config.portfolio_value is None:
        return _row("max_position_amount_share", False, "", config.max_position_amount_share, "missing required config: portfolio_value")
    amount = pd.to_numeric(portfolio["amount_20d"], errors="coerce")
    notional = pd.to_numeric(portfolio["target_weight"], errors="coerce").abs() * config.portfolio_value
    share = notional / amount.replace(0, pd.NA)
    max_share = float(share.max()) if not share.empty else 0.0
    detail = _above_threshold_detail(portfolio, share, float(config.max_position_amount_share), "position_amount_share")
    return _row(
        "max_position_amount_share",
        bool((share <= float(config.max_position_amount_share)).all()),
        max_share,
        config.max_position_amount_share,
        detail,
    )


def _max_estimated_cost_row(portfolio: pd.DataFrame, config: RiskConfig) -> dict[str, Any]:
    missing = _missing_columns(portfolio, ["target_weight"])
    if missing:
        return _row("max_estimated_cost", False, "", config.max_estimated_cost, f"missing required column: {missing[0]}")
    if config.portfolio_value is None:
        return _row("max_estimated_cost", False, "", config.max_estimated_cost, "missing required config: portfolio_value")
    total_bps = config.commission_bps + config.slippage_bps + config.stamp_tax_bps
    turnover_notional = pd.to_numeric(portfolio["target_weight"], errors="coerce").abs().sum() * config.portfolio_value
    estimated_cost = float(turnover_notional * total_bps / 10_000.0)
    return _row(
        "max_estimated_cost",
        estimated_cost <= float(config.max_estimated_cost),
        estimated_cost,
        config.max_estimated_cost,
        f"total_bps={total_bps:.6g}",
    )


def _max_risk_budget_row(portfolio: pd.DataFrame, threshold: float) -> dict[str, Any]:
    missing = _missing_columns(portfolio, ["target_weight", "turnover_20d"])
    if missing:
        return _row("max_risk_budget_per_position", False, "", threshold, f"missing required column: {missing[0]}")
    risk_budget = (
        pd.to_numeric(portfolio["target_weight"], errors="coerce").abs()
        * pd.to_numeric(portfolio["turnover_20d"], errors="coerce").abs()
    )
    max_budget = float(risk_budget.max()) if not risk_budget.empty else 0.0
    detail = _above_threshold_detail(portfolio, risk_budget, threshold, "risk_budget")
    return _row("max_risk_budget_per_position", bool((risk_budget <= threshold).all()), max_budget, threshold, detail)


def _missing_columns(frame: pd.DataFrame, columns: list[str]) -> list[str]:
    return [column for column in columns if column not in frame.columns]


def _below_threshold_detail(frame: pd.DataFrame, values: pd.Series, threshold: float, label: str) -> str:
    details = []
    for index, value in values.items():
        if pd.notna(value) and float(value) < threshold:
            details.append(f"{frame.loc[index, 'instrument']}: {label}={float(value):.6g}")
    return "; ".join(details)


def _above_threshold_detail(frame: pd.DataFrame, values: pd.Series, threshold: float, label: str) -> str:
    details = []
    for index, value in values.items():
        if pd.notna(value) and float(value) > threshold:
            details.append(f"{frame.loc[index, 'instrument']}: {label}={float(value):.6g}")
    return "; ".join(details)


def load_configured_factor_family_map(config: RiskConfig, root: str | Path = ".") -> dict[str, str]:
    if config.factor_family_map_path is None:
        return {}
    path = config.factor_family_map_path
    resolved = path if path.is_absolute() else Path(root) / path
    if not resolved.exists():
        return {}
    return load_factor_family_map(resolved)


def load_configured_factor_logic_map(config: RiskConfig, root: str | Path = ".") -> dict[str, str]:
    if config.factor_logic_map_path is None:
        return {}
    path = config.factor_logic_map_path
    resolved = path if path.is_absolute() else Path(root) / path
    if not resolved.exists():
        return {}
    return load_factor_logic_map(resolved)


def _exposure_maturity_rows(
    portfolio: pd.DataFrame,
    config: RiskConfig,
    factor_family_map: dict[str, str],
    factor_logic_map: dict[str, str],
) -> list[dict[str, Any]]:
    checks_enabled = any(
        value is not None
        for value in (
            config.max_industry_weight,
            config.min_factor_family_count,
            config.max_factor_family_concentration,
            config.min_factor_logic_count,
            config.max_factor_logic_concentration,
        )
    )
    if not checks_enabled:
        return []

    attribution = build_exposure_attribution(portfolio, family_map=factor_family_map, logic_map=factor_logic_map)
    rows: list[dict[str, Any]] = []
    if config.max_industry_weight is not None:
        max_industry_weight = float(attribution.industry["weight"].max()) if not attribution.industry.empty else 0.0
        top_industry = _top_label(attribution.industry, "industry", "weight")
        rows.append(
            _row(
                "max_industry_weight",
                max_industry_weight <= config.max_industry_weight,
                max_industry_weight,
                config.max_industry_weight,
                top_industry,
            )
        )
    if config.min_factor_family_count is not None:
        family_count = int((attribution.family["weighted_contribution"].abs() > 0).sum()) if not attribution.family.empty else 0
        rows.append(
            _row(
                "min_factor_family_count",
                family_count >= config.min_factor_family_count,
                family_count,
                config.min_factor_family_count,
                "",
            )
        )
    if config.max_factor_family_concentration is not None:
        concentration = _family_concentration(attribution.family)
        top_family = _top_label(attribution.family, "family", "abs_weighted_contribution")
        rows.append(
            _row(
                "max_factor_family_concentration",
                concentration <= config.max_factor_family_concentration,
                concentration,
                config.max_factor_family_concentration,
                top_family,
            )
        )
    if config.min_factor_logic_count is not None:
        logic_count = (
            int((attribution.logic["weighted_contribution"].abs() > 0).sum()) if not attribution.logic.empty else 0
        )
        rows.append(
            _row(
                "min_factor_logic_count",
                logic_count >= config.min_factor_logic_count,
                logic_count,
                config.min_factor_logic_count,
                "",
            )
        )
    if config.max_factor_logic_concentration is not None:
        concentration = _family_concentration(attribution.logic.rename(columns={"logic_bucket": "family"}))
        top_logic = _top_label(attribution.logic, "logic_bucket", "abs_weighted_contribution")
        rows.append(
            _row(
                "max_factor_logic_concentration",
                concentration <= config.max_factor_logic_concentration,
                concentration,
                config.max_factor_logic_concentration,
                top_logic,
            )
        )
    return rows


def _family_concentration(family: pd.DataFrame) -> float:
    if family.empty or "abs_weighted_contribution" not in family.columns:
        return 0.0
    total = float(family["abs_weighted_contribution"].sum())
    if total <= 0:
        return 0.0
    return float(family["abs_weighted_contribution"].max() / total)


def _top_label(frame: pd.DataFrame, label_col: str, value_col: str) -> str:
    if frame.empty or label_col not in frame.columns or value_col not in frame.columns:
        return ""
    row = frame.sort_values(value_col, ascending=False).iloc[0]
    return f"{row[label_col]}={float(row[value_col]):.6g}"


def _truthy_bool(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _clean_event_detail(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().replace("|", "/")


def _row(check: str, passed: bool, value: Any, threshold: Any, detail: str) -> dict[str, Any]:
    return {
        "check": check,
        "status": "pass" if passed else "fail",
        "value": value,
        "threshold": threshold,
        "detail": detail,
    }
