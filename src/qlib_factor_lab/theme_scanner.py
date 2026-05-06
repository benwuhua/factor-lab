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
        ("sub_chain", ""),
        ("theme_exposure", 1.0),
        ("confidence", "medium"),
        ("notes", ""),
    ]:
        if column not in members.columns:
            members[column] = default
    members["sub_chain"] = members["sub_chain"].where(
        members["sub_chain"].astype(str).str.strip() != "",
        members["supply_chain_role"],
    )
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
    merged["theme_score"] = merged["theme_exposure_score"]
    merged["quality_score"] = _component_score(
        merged,
        [
            "family_quality_score",
            "logic_fundamental_quality_score",
            "family_cashflow_quality_score",
            "fundamental_roe_contribution",
            "fundamental_roic_contribution",
            "fundamental_cfo_to_ni_contribution",
            "fundamental_low_debt_contribution",
            "cashflow_quality_contribution",
        ],
    )
    merged["growth_score"] = _component_score(
        merged,
        [
            "family_growth_improvement_score",
            "logic_fundamental_growth_score",
            "fundamental_revenue_growth_change_contribution",
            "fundamental_profit_growth_change_contribution",
            "fundamental_margin_change_contribution",
            "growth_improvement_contribution",
        ],
    )
    merged["momentum_score"] = _component_score(
        merged,
        [
            "logic_reversal_repair_score",
            "family_quiet_breakout_score",
            "quiet_breakout_20_contribution",
            "ensemble_score",
        ],
    )
    merged["event_score"] = _component_score(
        merged,
        [
            "family_event_catalyst_score",
            "logic_financial_disclosure_score",
            "financial_disclosure_event_score_90d_contribution",
            "financial_disclosure_net_intensity_score_90d_contribution",
        ],
    )
    merged["theme_id"] = universe.theme_id
    merged["theme_display_name"] = universe.display_name
    merged["theme_as_of_date"] = universe.as_of_date
    merged["research_status"] = merged.apply(_research_status, axis=1)
    merged.loc[merged["research_status"] == "risk_review", "theme_research_score"] *= 0.25
    merged["risk_penalty"] = merged.apply(_risk_penalty, axis=1)
    merged["total_score"] = (
        0.30 * merged["theme_score"]
        + 0.20 * merged["quality_score"]
        + 0.20 * merged["growth_score"]
        + 0.15 * merged["momentum_score"]
        + 0.10 * merged["event_score"]
        - 0.15 * merged["risk_penalty"]
    ).clip(lower=0.0)
    merged["tier"] = merged.apply(_tier, axis=1)
    merged["reason"] = merged.apply(_reason, axis=1)
    merged["recommendation_type"] = "research_candidate_not_advice"

    sort_cols = ["tier_rank", "total_score", "theme_research_score", "theme_exposure"]
    merged["research_status_rank"] = merged["research_status"].map(
        {"research_candidate": 0, "watch_only": 1, "risk_review": 2}
    ).fillna(3)
    merged["tier_rank"] = merged["tier"].map({"A重点研究": 0, "B观察跟踪": 1, "C风险复核": 2}).fillna(3)
    ordered = merged.sort_values(sort_cols, ascending=[True, False, False, False]).head(top_k).copy()
    ordered = ordered.drop(columns=["research_status_rank", "tier_rank"])
    return ordered.loc[:, _candidate_columns(ordered)].reset_index(drop=True)


def missing_theme_instruments(signal: pd.DataFrame, universe: ThemeUniverse) -> list[str]:
    if "instrument" not in signal.columns:
        raise ValueError("signal must include instrument")
    existing = set(signal["instrument"].dropna().astype(str).str.strip())
    members = universe.members["instrument"].dropna().astype(str).str.strip()
    return [instrument for instrument in members.tolist() if instrument and instrument not in existing]


def combine_signal_with_supplemental(signal: pd.DataFrame, supplemental: pd.DataFrame | None) -> pd.DataFrame:
    if supplemental is None or supplemental.empty:
        return signal.copy()
    if "instrument" not in signal.columns or "instrument" not in supplemental.columns:
        raise ValueError("signal and supplemental signal must include instrument")
    primary = signal.copy()
    extra = supplemental.copy()
    primary["instrument"] = primary["instrument"].astype(str).str.strip()
    extra["instrument"] = extra["instrument"].astype(str).str.strip()
    existing = set(primary["instrument"])
    extra = extra[~extra["instrument"].isin(existing)]
    if extra.empty:
        return primary.reset_index(drop=True)
    return pd.concat([primary, extra], ignore_index=True, sort=False)


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
            "| rank | tier | instrument | name | role | total | theme | quality | growth | momentum | event | risk | reason |",
            "|---:|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    if candidates.empty:
        lines.append("| - | - | - | - | - | - | - | - | - | - | - | - | - |")
    else:
        for rank, (_, row) in enumerate(candidates.iterrows(), start=1):
            lines.append(
                f"| {rank} | {row.get('tier', row.get('research_status', ''))} | "
                f"{row.get('instrument', '')} | {row.get('name', '')} | "
                f"{row.get('supply_chain_role', '')} | {_format_number(row.get('total_score'))} | "
                f"{_format_number(row.get('theme_score'))} | {_format_number(row.get('quality_score'))} | "
                f"{_format_number(row.get('growth_score'))} | {_format_number(row.get('momentum_score'))} | "
                f"{_format_number(row.get('event_score'))} | {_format_number(row.get('risk_penalty'))} | "
                f"{row.get('reason', '')} |"
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


def _component_score(frame: pd.DataFrame, columns: list[str]) -> pd.Series:
    parts = [_minmax_score(frame[column]) for column in columns if column in frame.columns]
    if not parts:
        return pd.Series(0.0, index=frame.index)
    return pd.concat(parts, axis=1).mean(axis=1).fillna(0.0)


def _research_status(row: pd.Series) -> str:
    if _truthy(row.get("event_blocked")) or _truthy(row.get("buy_blocked")) or _falsey(row.get("tradable")):
        return "risk_review"
    if pd.isna(row.get("ensemble_score")):
        return "watch_only"
    return "research_candidate"


def _risk_penalty(row: pd.Series) -> float:
    penalty = 0.0
    if row.get("research_status") == "risk_review":
        penalty += 1.0
    elif row.get("research_status") == "watch_only":
        penalty += 0.35
    flags = str(row.get("risk_flags") or "").lower()
    risk_summary = str(row.get("risk_event_summary") or "").lower()
    risk_types = str(row.get("risk_event_types") or "").lower()
    if any(token in flags for token in ["risk", "blocked", "event_blocked"]):
        penalty += 0.35
    if risk_summary or risk_types:
        penalty += 0.25
    severity = str(row.get("max_event_severity") or "").lower()
    if severity in {"block", "blocked", "high"}:
        penalty += 0.5
    elif severity in {"risk", "watch"}:
        penalty += 0.2
    return min(penalty, 1.0)


def _tier(row: pd.Series) -> str:
    if row.get("research_status") == "risk_review" or float(row.get("risk_penalty") or 0.0) >= 0.8:
        return "C风险复核"
    score = float(row.get("total_score") or 0.0)
    if score >= 0.58 and row.get("research_status") == "research_candidate":
        return "A重点研究"
    return "B观察跟踪"


def _reason(row: pd.Series) -> str:
    role = str(row.get("supply_chain_role") or row.get("sub_chain") or "AI产业链").strip()
    pieces = [role]
    top_factors = [str(row.get("top_factor_1") or "").strip(), str(row.get("top_factor_2") or "").strip()]
    top_factors = [item for item in top_factors if item and item.lower() != "nan"]
    if top_factors:
        pieces.append("驱动: " + " / ".join(top_factors[:2]))
    if row.get("tier") == "C风险复核":
        pieces.append("事件/交易风险需先核查")
    elif row.get("tier") == "A重点研究":
        pieces.append("主题相关性和当前信号同时靠前")
    else:
        pieces.append("进入观察池，等待信号或证据增强")
    return "；".join(pieces)


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
        "sub_chain",
        "theme_id",
        "theme_display_name",
        "theme_as_of_date",
        "theme_exposure",
        "confidence",
        "ensemble_score",
        "theme_signal_score",
        "theme_exposure_score",
        "theme_research_score",
        "theme_score",
        "quality_score",
        "growth_score",
        "momentum_score",
        "event_score",
        "risk_penalty",
        "total_score",
        "tier",
        "reason",
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
