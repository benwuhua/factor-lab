from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import pandas as pd
import yaml


FACTOR_DRIVER_COLUMNS = (
    ("top_factor_1", "top_factor_1_contribution"),
    ("top_factor_2", "top_factor_2_contribution"),
)


@dataclass(frozen=True)
class ExposureAttribution:
    summary: pd.DataFrame
    family: pd.DataFrame
    industry: pd.DataFrame
    style: pd.DataFrame


def load_factor_family_map(path: str | Path) -> dict[str, str]:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    result: dict[str, str] = {}
    for item in data.get("approved_factors", []):
        name = str(item.get("name", "")).strip()
        family = str(item.get("family", "")).strip()
        if name and family:
            result[name] = family
    return result


def build_exposure_attribution(
    portfolio: pd.DataFrame,
    *,
    family_map: Mapping[str, str] | None = None,
    weight_col: str = "target_weight",
    industry_col: str = "industry",
    style_cols: list[str] | None = None,
) -> ExposureAttribution:
    if portfolio.empty:
        return ExposureAttribution(
            summary=_summary_frame(portfolio, weight_col),
            family=pd.DataFrame(columns=["family", "weighted_contribution", "abs_weighted_contribution", "driver_count"]),
            industry=pd.DataFrame(columns=["industry", "weight", "position_count"]),
            style=pd.DataFrame(columns=["style", "weighted_average", "available_weight"]),
        )

    weights = _weights(portfolio, weight_col)
    return ExposureAttribution(
        summary=_summary_frame(portfolio, weight_col, weights=weights),
        family=_factor_family_exposure(portfolio, family_map or {}, weights),
        industry=_industry_exposure(portfolio, weights, industry_col),
        style=_style_exposure(portfolio, weights, style_cols or _default_style_cols(portfolio)),
    )


def write_exposure_attribution_csv(result: ExposureAttribution, output_dir: str | Path, *, prefix: str = "exposure") -> list[Path]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, frame in [
        ("summary", result.summary),
        ("families", result.family),
        ("industry", result.industry),
        ("style", result.style),
    ]:
        path = output / f"{prefix}_{name}.csv"
        frame.to_csv(path, index=False)
        paths.append(path)
    return paths


def write_exposure_attribution_markdown(result: ExposureAttribution, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Exposure Attribution",
        "",
        "## Summary",
        "",
        _markdown_table(result.summary),
        "",
        "## Factor Families",
        "",
        _markdown_table(result.family),
        "",
        "## Industry",
        "",
        _markdown_table(result.industry),
        "",
        "## Style",
        "",
        _markdown_table(result.style),
    ]
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def _weights(portfolio: pd.DataFrame, weight_col: str) -> pd.Series:
    if weight_col in portfolio.columns:
        return pd.to_numeric(portfolio[weight_col], errors="coerce").fillna(0.0)
    if portfolio.empty:
        return pd.Series(dtype=float)
    return pd.Series(1.0 / len(portfolio), index=portfolio.index, dtype=float)


def _summary_frame(portfolio: pd.DataFrame, weight_col: str, *, weights: pd.Series | None = None) -> pd.DataFrame:
    weights = weights if weights is not None else _weights(portfolio, weight_col)
    return pd.DataFrame(
        [
            {
                "positions": int(len(portfolio)),
                "gross_weight": float(weights.abs().sum()),
                "net_weight": float(weights.sum()),
                "max_single_weight": float(weights.max()) if len(weights) else 0.0,
            }
        ]
    )


def _factor_family_exposure(
    portfolio: pd.DataFrame,
    family_map: Mapping[str, str],
    weights: pd.Series,
) -> pd.DataFrame:
    rows = []
    for factor_col, contribution_col in FACTOR_DRIVER_COLUMNS:
        if factor_col not in portfolio.columns or contribution_col not in portfolio.columns:
            continue
        factors = portfolio[factor_col].fillna("").astype(str).str.strip()
        contributions = pd.to_numeric(portfolio[contribution_col], errors="coerce").fillna(0.0)
        for index, factor in factors.items():
            if not factor:
                continue
            family = str(family_map.get(factor, factor))
            weighted = float(weights.loc[index] * contributions.loc[index])
            rows.append({"family": family, "weighted_contribution": weighted})
    if not rows:
        return pd.DataFrame(columns=["family", "weighted_contribution", "abs_weighted_contribution", "driver_count"])
    frame = pd.DataFrame(rows)
    grouped = frame.groupby("family", as_index=False).agg(
        weighted_contribution=("weighted_contribution", "sum"),
        abs_weighted_contribution=("weighted_contribution", lambda values: float(values.abs().sum())),
        driver_count=("weighted_contribution", "size"),
    )
    return grouped.sort_values("abs_weighted_contribution", ascending=False).reset_index(drop=True)


def _industry_exposure(portfolio: pd.DataFrame, weights: pd.Series, industry_col: str) -> pd.DataFrame:
    if industry_col not in portfolio.columns:
        return pd.DataFrame(columns=["industry", "weight", "position_count"])
    frame = pd.DataFrame(
        {
            "industry": portfolio[industry_col].fillna("unknown").astype(str),
            "weight": weights,
        }
    )
    return (
        frame.groupby("industry", as_index=False)
        .agg(weight=("weight", "sum"), position_count=("weight", "size"))
        .sort_values("weight", ascending=False)
        .reset_index(drop=True)
    )


def _style_exposure(portfolio: pd.DataFrame, weights: pd.Series, style_cols: list[str]) -> pd.DataFrame:
    rows = []
    total_weight = float(weights.abs().sum())
    for column in style_cols:
        if column not in portfolio.columns:
            continue
        values = pd.to_numeric(portfolio[column], errors="coerce")
        valid = values.notna() & weights.notna()
        available_weight = float(weights.loc[valid].abs().sum())
        denominator = available_weight if available_weight else total_weight
        weighted_average = float((values.loc[valid] * weights.loc[valid]).sum() / denominator) if denominator else float("nan")
        rows.append(
            {
                "style": column,
                "weighted_average": weighted_average,
                "available_weight": available_weight,
            }
        )
    return pd.DataFrame(rows, columns=["style", "weighted_average", "available_weight"])


def _default_style_cols(portfolio: pd.DataFrame) -> list[str]:
    candidates = ["amount_20d", "turnover_20d", "last_price", "ensemble_score", "rule_score", "model_score"]
    return [column for column in candidates if column in portfolio.columns]


def _markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    columns = list(frame.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in frame.iterrows():
        lines.append("| " + " | ".join(_format_value(row[column]) for column in columns) + " |")
    return "\n".join(lines)


def _format_value(value) -> str:
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)
