from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def build_stock_cards(
    portfolio: pd.DataFrame,
    *,
    run_id: str,
    as_of_date: str,
    card_version: str = "v1",
    gate_decision: str = "",
    gate_checks: pd.DataFrame | None = None,
    factor_version: str = "",
) -> list[dict[str, Any]]:
    cards = []
    checks = _gate_reason(gate_checks)
    for _, row in portfolio.iterrows():
        cards.append(
            {
                "instrument": _text(row.get("instrument")),
                "name": _text(row.get("name")),
                "identity": {
                    "research_universes": _text(row.get("research_universes")),
                    "industry": _text(row.get("industry")),
                    "industry_sw": _text(row.get("industry_sw")),
                    "board": _text(row.get("board")),
                    "is_st": _bool_or_text(row.get("is_st")),
                    "listing_status": _listing_status(row),
                },
                "current_signal": {
                    "date": _text(row.get("date", as_of_date)),
                    "rank": _number(row.get("rank")),
                    "target_weight": _number(row.get("target_weight")),
                    "ensemble_score": _number(row.get("ensemble_score")),
                    "rule_score": _number(row.get("rule_score")),
                    "model_score": _number(row.get("model_score")),
                    "top_factor_1": _text(row.get("top_factor_1")),
                    "top_factor_1_contribution": _number(row.get("top_factor_1_contribution")),
                    "top_factor_2": _text(row.get("top_factor_2")),
                    "top_factor_2_contribution": _number(row.get("top_factor_2_contribution")),
                    "selection_explanation": _text(row.get("selection_explanation")),
                },
                "factor_profile": _factor_profile(row),
                "trading_state": {
                    "amount_20d": _number(row.get("amount_20d")),
                    "turnover_20d": _number(row.get("turnover_20d")),
                    "tradable": _bool_or_text(row.get("tradable")),
                    "suspended": _bool_or_text(row.get("suspended")),
                    "limit_up": _bool_or_text(row.get("limit_up")),
                    "limit_down": _bool_or_text(row.get("limit_down")),
                    "buy_blocked": _bool_or_text(row.get("buy_blocked")),
                    "sell_blocked": _bool_or_text(row.get("sell_blocked")),
                },
                "evidence": {
                    "event_count": int(_number(row.get("event_count")) or 0),
                    "max_event_severity": _text(row.get("max_event_severity")),
                    "event_types": _text(row.get("active_event_types")),
                    "event_risk_summary": _text(row.get("event_risk_summary")),
                    "source_urls": _split_semicolon(row.get("event_source_urls")),
                    "risk_flags": _text(row.get("risk_flags")),
                },
                "portfolio_role": {
                    "selection_reason": _text(row.get("selection_reason")),
                    "top_factor_family": _text(row.get("top_factor_family")),
                    "industry_contribution": _number(row.get("industry_contribution")),
                    "family_contribution": _number(row.get("family_contribution")),
                },
                "review_questions": {
                    "why_selected": _text(row.get("selection_explanation")),
                    "key_risk": _text(row.get("risk_flags")),
                    "manual_chart_needed": bool(_text(row.get("risk_flags"))),
                    "manual_announcement_needed": int(_number(row.get("event_count")) or 0) > 0,
                    "gate_reason": checks,
                },
                "audit": {
                    "run_id": run_id,
                    "as_of_date": as_of_date,
                    "card_version": card_version,
                    "factor_version": factor_version,
                    "evidence_id": _text(row.get("evidence_id")),
                    "review_decision": gate_decision,
                    "reviewer": "",
                    "reviewed_at": "",
                    "override_reason": "",
                },
            }
        )
    return cards


def write_stock_cards(cards: list[dict[str, Any]], output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        "".join(json.dumps(card, ensure_ascii=False, sort_keys=True) + "\n" for card in cards),
        encoding="utf-8",
    )
    return output


def _factor_profile(row: pd.Series) -> dict[str, Any]:
    profile = {}
    for name in ["expression", "pattern", "emotion", "liquidity", "risk", "fundamental", "shareholder"]:
        column = f"{name}_score"
        if column in row.index:
            profile[column] = _number(row.get(column))
    return profile


def _gate_reason(gate_checks: pd.DataFrame | None) -> str:
    if gate_checks is None or gate_checks.empty:
        return ""
    parts = []
    for _, row in gate_checks.iterrows():
        check = _text(row.get("check"))
        status = _text(row.get("status"))
        if check:
            parts.append(f"{check}:{status}")
    return "; ".join(parts)


def _listing_status(row: pd.Series) -> str:
    if _text(row.get("delisting_date")):
        return "delisted"
    if _text(row.get("listing_date")):
        return "listed"
    return ""


def _split_semicolon(value: Any) -> list[str]:
    text = _text(value)
    if not text:
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def _text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _number(value: Any) -> float | int | None:
    if value is None or pd.isna(value):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric.is_integer():
        return int(numeric)
    return numeric


def _bool_or_text(value: Any) -> bool | str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, bool):
        return value
    text = str(value).strip()
    if text.lower() in {"true", "1", "yes"}:
        return True
    if text.lower() in {"false", "0", "no"}:
        return False
    return text
