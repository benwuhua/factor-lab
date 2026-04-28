from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


@dataclass(frozen=True)
class ThemeUniverse:
    theme_id: str
    display_name: str
    as_of_date: str
    thesis: str
    members: pd.DataFrame
    sources: list[str]
    signal_weight: float = 0.7
    theme_weight: float = 0.3


def load_theme_universe(path: str | Path) -> ThemeUniverse:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    score = data.get("score") or {}
    members = pd.DataFrame(data.get("members") or [])
    if members.empty:
        members = pd.DataFrame(columns=["instrument", "name", "supply_chain_role", "theme_exposure", "confidence"])
    if "instrument" not in members.columns:
        raise ValueError("theme members must include instrument")
    for column, default in [
        ("name", ""),
        ("supply_chain_role", ""),
        ("theme_exposure", 1.0),
        ("confidence", "medium"),
        ("notes", ""),
    ]:
        if column not in members.columns:
            members[column] = default
    members["instrument"] = members["instrument"].astype(str).str.strip()
    members["theme_exposure"] = pd.to_numeric(members["theme_exposure"], errors="coerce").fillna(1.0)
    return ThemeUniverse(
        theme_id=str(data.get("theme_id") or Path(path).stem),
        display_name=str(data.get("display_name") or data.get("theme_id") or Path(path).stem),
        as_of_date=str(data.get("as_of_date") or ""),
        thesis=str(data.get("thesis") or ""),
        members=members,
        sources=[str(item) for item in data.get("sources", [])],
        signal_weight=float(score.get("signal_weight", 0.7)),
        theme_weight=float(score.get("theme_weight", 0.3)),
    )


def build_theme_candidates(
    signal: pd.DataFrame,
    universe: ThemeUniverse,
    *,
    top_k: int = 30,
) -> pd.DataFrame:
    if top_k <= 0:
        raise ValueError("top_k must be positive")
    if "instrument" not in signal.columns:
        raise ValueError("signal must include instrument")
    if universe.members.empty:
        return _empty_candidates()

    signal_frame = signal.copy()
    signal_frame["instrument"] = signal_frame["instrument"].astype(str).str.strip()
    merged = universe.members.merge(signal_frame, on="instrument", how="left", suffixes=("", "_signal"))
    if "ensemble_score" not in merged.columns:
        merged["ensemble_score"] = pd.NA
    merged["ensemble_score"] = pd.to_numeric(merged["ensemble_score"], errors="coerce")
    merged["theme_signal_score"] = _minmax_score(merged["ensemble_score"])
    merged["theme_exposure_score"] = _minmax_score(merged["theme_exposure"])
    merged["theme_research_score"] = (
        universe.signal_weight * merged["theme_signal_score"]
        + universe.theme_weight * merged["theme_exposure_score"]
    )
    merged["theme_id"] = universe.theme_id
    merged["theme_display_name"] = universe.display_name
    merged["theme_as_of_date"] = universe.as_of_date
    merged["research_status"] = merged.apply(_research_status, axis=1)
    merged.loc[merged["research_status"] == "risk_review", "theme_research_score"] *= 0.25
    merged["recommendation_type"] = "research_candidate_not_advice"

    sort_cols = ["research_status_rank", "theme_research_score", "theme_exposure"]
    merged["research_status_rank"] = merged["research_status"].map(
        {"research_candidate": 0, "watch_only": 1, "risk_review": 2}
    ).fillna(3)
    ordered = merged.sort_values(sort_cols, ascending=[True, False, False]).head(top_k).copy()
    ordered = ordered.drop(columns=["research_status_rank"])
    return ordered.loc[:, _candidate_columns(ordered)].reset_index(drop=True)


def write_theme_candidates(candidates: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_csv(output, index=False)
    return output


def write_theme_candidate_report(
    candidates: pd.DataFrame,
    output_path: str | Path,
    *,
    theme_display_name: str,
    thesis: str = "",
    sources: list[str] | None = None,
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# {theme_display_name} 主题扫描",
        "",
        "> 输出是研究候选和复核清单，非投资建议。",
        "",
    ]
    if thesis:
        lines.extend(["## 主题假设", "", thesis, ""])
    lines.extend(
        [
            "## 候选列表",
            "",
            "| rank | instrument | name | role | status | score | ensemble |",
            "|---:|---|---|---|---|---:|---:|",
        ]
    )
    if candidates.empty:
        lines.append("| - | - | - | - | - | - | - |")
    else:
        for rank, (_, row) in enumerate(candidates.iterrows(), start=1):
            lines.append(
                f"| {rank} | {row.get('instrument', '')} | {row.get('name', '')} | "
                f"{row.get('supply_chain_role', '')} | {row.get('research_status', '')} | "
                f"{_format_number(row.get('theme_research_score'))} | {_format_number(row.get('ensemble_score'))} |"
            )
    if sources:
        lines.extend(["", "## 参考来源", ""])
        for source in sources:
            lines.append(f"- {source}")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def _empty_candidates() -> pd.DataFrame:
    return pd.DataFrame(columns=_candidate_columns(pd.DataFrame()))


def _minmax_score(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    if values.notna().sum() == 0:
        return pd.Series(0.0, index=series.index)
    min_value = float(values.min(skipna=True))
    max_value = float(values.max(skipna=True))
    if max_value == min_value:
        return values.notna().astype(float)
    return ((values - min_value) / (max_value - min_value)).fillna(0.0)


def _research_status(row: pd.Series) -> str:
    if _truthy(row.get("event_blocked")) or _truthy(row.get("buy_blocked")) or _falsey(row.get("tradable")):
        return "risk_review"
    if pd.isna(row.get("ensemble_score")):
        return "watch_only"
    return "research_candidate"


def _truthy(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "blocked"}
    return bool(value)


def _falsey(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"0", "false", "no", "n"}
    return value is False


def _candidate_columns(frame: pd.DataFrame) -> list[str]:
    preferred = [
        "date",
        "instrument",
        "name",
        "supply_chain_role",
        "theme_id",
        "theme_display_name",
        "theme_as_of_date",
        "theme_exposure",
        "confidence",
        "ensemble_score",
        "theme_signal_score",
        "theme_exposure_score",
        "theme_research_score",
        "research_status",
        "recommendation_type",
        "risk_flags",
        "event_blocked",
        "amount_20d",
        "industry",
        "notes",
    ]
    return [column for column in preferred if column in frame.columns] or preferred


def _format_number(value: Any) -> str:
    try:
        if pd.isna(value):
            return ""
        return f"{float(value):.4g}"
    except (TypeError, ValueError):
        return ""
