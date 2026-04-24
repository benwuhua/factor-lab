from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import load_yaml


EXECUTION_PASSTHROUGH_COLUMNS = (
    "last_price",
    "amount_20d",
    "tradable",
    "suspended",
    "limit_up",
    "limit_down",
    "buy_blocked",
    "sell_blocked",
    "turnover_20d",
    "industry",
    "abnormal_event",
    "announcement_flag",
)
EXPLANATION_COLUMNS = (
    "top_factor_1",
    "top_factor_1_contribution",
    "top_factor_2",
    "top_factor_2_contribution",
)


@dataclass(frozen=True)
class PortfolioConfig:
    top_k: int = 20
    cash_buffer: float = 0.05
    max_single_weight: float = 0.1
    max_new_buys: int | None = None
    dropout_rank: int | None = None
    score_column: str = "ensemble_score"
    target_output_path: Path = Path("reports/target_portfolio_{run_yyyymmdd}.csv")
    summary_output_path: Path = Path("reports/target_portfolio_summary_{run_yyyymmdd}.md")


def load_portfolio_config(path: str | Path) -> PortfolioConfig:
    data = load_yaml(path)
    raw = data.get("portfolio", data)
    output = data.get("output", {})
    max_new_buys = raw.get("max_new_buys")
    return PortfolioConfig(
        top_k=int(raw.get("top_k", 20)),
        cash_buffer=float(raw.get("cash_buffer", 0.05)),
        max_single_weight=float(raw.get("max_single_weight", 0.1)),
        max_new_buys=int(max_new_buys) if max_new_buys is not None else None,
        dropout_rank=int(raw["dropout_rank"]) if raw.get("dropout_rank") is not None else None,
        score_column=str(raw.get("score_column", "ensemble_score")),
        target_output_path=Path(output.get("target_portfolio", "reports/target_portfolio_{run_yyyymmdd}.csv")),
        summary_output_path=Path(output.get("summary", "reports/target_portfolio_summary_{run_yyyymmdd}.md")),
    )


def build_target_portfolio(
    signal: pd.DataFrame,
    config: PortfolioConfig = PortfolioConfig(),
    current_positions: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if config.top_k <= 0:
        raise ValueError("top_k must be positive")
    if config.cash_buffer < 0 or config.cash_buffer >= 1:
        raise ValueError("cash_buffer must be in [0, 1)")

    eligible = signal.copy()
    if "eligible" in eligible.columns:
        eligible = eligible[eligible["eligible"]]
    if config.score_column not in eligible.columns:
        raise ValueError(f"signal is missing score column: {config.score_column}")
    eligible = eligible.dropna(subset=[config.score_column]).sort_values(config.score_column, ascending=False)
    selected = _select_candidates(eligible, config, current_positions)
    if selected.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "instrument",
                "rank",
                "target_weight",
                config.score_column,
                "risk_flags",
                "rejection_reason",
                *EXECUTION_PASSTHROUGH_COLUMNS,
            ]
        )

    investable_weight = max(0.0, 1.0 - config.cash_buffer)
    equal_weight = investable_weight / len(selected)
    target_weight = min(equal_weight, config.max_single_weight)
    output = selected.copy()
    output["rank"] = range(1, len(output) + 1)
    output["target_weight"] = target_weight
    output["selection_explanation"] = output.apply(lambda row: _selection_explanation(row, config.score_column), axis=1)
    cols = ["date", "instrument", "rank", "target_weight", config.score_column]
    for optional in ["rule_score", "model_score", "active_regime", "risk_flags", "rejection_reason"]:
        if optional in output.columns:
            cols.append(optional)
    if "selection_reason" in output.columns:
        cols.append("selection_reason")
    cols.append("selection_explanation")
    for optional in EXPLANATION_COLUMNS:
        if optional in output.columns and optional not in cols:
            cols.append(optional)
    for optional in EXECUTION_PASSTHROUGH_COLUMNS:
        if optional in output.columns and optional not in cols:
            cols.append(optional)
    return output.loc[:, cols].reset_index(drop=True)


def write_target_portfolio(portfolio: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    portfolio.to_csv(output, index=False)
    return output


def write_portfolio_summary(portfolio: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Target Portfolio Summary",
        "",
        f"- positions: {len(portfolio)}",
        f"- gross_target_weight: {float(portfolio['target_weight'].sum()) if 'target_weight' in portfolio else 0.0:.6g}",
        "",
        "| rank | instrument | target_weight | ensemble_score |",
        "|---:|---|---:|---:|",
    ]
    for _, row in portfolio.iterrows():
        lines.append(
            f"| {int(row['rank'])} | {row['instrument']} | {float(row['target_weight']):.6g} | "
            f"{float(row.get('ensemble_score', 0.0)):.6g} |"
        )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def _select_candidates(
    eligible: pd.DataFrame,
    config: PortfolioConfig,
    current_positions: pd.DataFrame | None,
) -> pd.DataFrame:
    if config.max_new_buys is None or current_positions is None or current_positions.empty:
        if config.dropout_rank is None or current_positions is None or current_positions.empty:
            selected = eligible.head(config.top_k).copy()
            selected["selection_reason"] = "top_ranked"
            return selected

    current = set(current_positions["instrument"].astype(str))
    ranked = eligible.copy()
    ranked["signal_rank"] = range(1, len(ranked) + 1)
    keep = _dropout_keep(ranked, current, config)
    selected_rows = [row for _, row in keep.iterrows()]
    selected_instruments = {str(row["instrument"]) for row in selected_rows}
    new_buys = 0
    if config.max_new_buys is not None:
        new_buys = sum(1 for row in selected_rows if str(row["instrument"]) not in current)
    for _, row in ranked.iterrows():
        instrument = str(row["instrument"])
        if instrument in selected_instruments:
            continue
        is_new = instrument not in current
        if config.max_new_buys is not None and is_new and new_buys >= config.max_new_buys:
            continue
        candidate = row.copy()
        candidate["selection_reason"] = "new_buy" if is_new else "current_ranked"
        selected_rows.append(candidate)
        selected_instruments.add(instrument)
        if is_new:
            new_buys += 1
        if len(selected_rows) >= config.top_k:
            break
    selected = pd.DataFrame(selected_rows)
    if selected.empty:
        return selected
    return selected.sort_values(config.score_column, ascending=False).head(config.top_k)


def _dropout_keep(ranked: pd.DataFrame, current: set[str], config: PortfolioConfig) -> pd.DataFrame:
    if config.dropout_rank is None:
        return ranked.iloc[0:0].copy()
    keep = ranked[
        ranked["instrument"].astype(str).isin(current)
        & (ranked["signal_rank"] <= config.dropout_rank)
    ].copy()
    keep["selection_reason"] = "held_by_dropout"
    return keep.sort_values(config.score_column, ascending=False).head(config.top_k)


def _selection_explanation(row: pd.Series, score_column: str) -> str:
    drivers = []
    for factor_col, contribution_col in [
        ("top_factor_1", "top_factor_1_contribution"),
        ("top_factor_2", "top_factor_2_contribution"),
    ]:
        factor = str(row.get(factor_col, "") or "").strip()
        if not factor:
            continue
        contribution = row.get(contribution_col)
        if pd.isna(contribution):
            continue
        drivers.append(f"{factor} {_format_float(float(contribution))}")
    if drivers:
        return f"selected by {score_column} {_format_float(float(row[score_column]))}; main drivers: {', '.join(drivers)}"
    return f"selected by {score_column} {_format_float(float(row[score_column]))}"


def _format_float(value: float) -> str:
    return f"{value:.6g}"
