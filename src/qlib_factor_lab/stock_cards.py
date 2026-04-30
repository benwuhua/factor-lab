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
        event_count = int(_number(row.get("event_count")) or 0)
        source_urls = _split_semicolon(row.get("event_source_urls"))
        risk_flags = _text(row.get("risk_flags"))
        factor_contributions = _factor_contributions(row)
        financial_anomalies = _split_list(row.get("financial_anomaly_flags") or row.get("anomaly_flags"))
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
                    "event_count": event_count,
                    "max_event_severity": _text(row.get("max_event_severity")),
                    "event_types": _text(row.get("active_event_types")),
                    "positive_event_types": _text(row.get("positive_event_types")),
                    "positive_event_summary": _text(row.get("positive_event_summary")),
                    "risk_event_types": _text(row.get("risk_event_types")),
                    "risk_event_summary": _text(row.get("risk_event_summary")),
                    "event_risk_summary": _text(row.get("event_risk_summary")),
                    "source_urls": source_urls,
                    "risk_flags": risk_flags,
                },
                "portfolio_role": {
                    "selection_reason": _text(row.get("selection_reason")),
                    "top_factor_family": _text(row.get("top_factor_family")),
                    "industry_contribution": _number(row.get("industry_contribution")),
                    "family_contribution": _number(row.get("family_contribution")),
                },
                "review_questions": {
                    "why_selected": _text(row.get("selection_explanation")),
                    "key_risk": risk_flags,
                    "manual_chart_needed": bool(risk_flags),
                    "manual_announcement_needed": event_count > 0,
                    "gate_reason": checks,
                },
                "selection_thesis": {
                    "why_selected": _text(row.get("selection_explanation")),
                    "selection_reason": _text(row.get("selection_reason")),
                    "portfolio_role": _text(row.get("top_factor_family")),
                    "gate_decision": gate_decision,
                    "gate_reason": checks,
                },
                "factor_contributions": factor_contributions,
                "counter_evidence": {
                    "risks": risk_flags,
                    "risk_event_types": _text(row.get("risk_event_types")),
                    "risk_event_summary": _text(row.get("risk_event_summary")),
                    "max_event_severity": _text(row.get("max_event_severity")),
                    "financial_anomalies": financial_anomalies,
                },
                "announcement_evidence": {
                    "positive_event_types": _text(row.get("positive_event_types")),
                    "positive_event_summary": _text(row.get("positive_event_summary")),
                    "risk_event_types": _text(row.get("risk_event_types")),
                    "risk_event_summary": _text(row.get("risk_event_summary")),
                    "source_urls": source_urls,
                },
                "financial_anomalies": financial_anomalies,
                "manual_review_actions": {
                    "announcement_review": event_count > 0
                    or bool(_text(row.get("positive_event_types")) or _text(row.get("risk_event_types"))),
                    "financial_review": bool(financial_anomalies),
                    "manual_chart_needed": bool(risk_flags),
                    "actions": _manual_review_actions(row, event_count, risk_flags, financial_anomalies),
                },
                "tracking": {
                    "status": _text(row.get("tracking_status")),
                    "next_review_date": _text(row.get("next_review_date")),
                    "owner": _text(row.get("tracking_owner") or row.get("owner")),
                    "notes": _text(row.get("tracking_notes")),
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


def write_stock_card_report(cards: list[dict[str, Any]], output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Stock Candidate Report", ""]
    for card in cards:
        title = " ".join(part for part in [card.get("instrument", ""), card.get("name", "")] if part).strip()
        lines.extend(
            [
                f"## {title or 'Unknown'}",
                "",
                f"Why selected: {_report_text(card.get('selection_thesis', {}).get('why_selected'))}",
                f"Top drivers: {_format_drivers(card.get('factor_contributions', []))}",
                f"Risks: {_format_risks(card)}",
                f"Evidence urls: {_format_urls(card.get('announcement_evidence', {}).get('source_urls', []))}",
                f"Manual review action: {_format_manual_actions(card)}",
                f"Tracking: {_format_tracking(card.get('tracking', {}))}",
                "",
            ]
        )
    output.write_text("\n".join(lines), encoding="utf-8")
    return output


def write_stock_cards(cards: list[dict[str, Any]], output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        "".join(json.dumps(card, ensure_ascii=False, sort_keys=True) + "\n" for card in cards),
        encoding="utf-8",
    )
    return output


def _factor_contributions(row: pd.Series) -> list[dict[str, Any]]:
    contributions = []
    index = 1
    while f"top_factor_{index}" in row.index:
        factor = _text(row.get(f"top_factor_{index}"))
        if factor:
            contributions.append(
                {
                    "factor": factor,
                    "contribution": _number(row.get(f"top_factor_{index}_contribution")),
                }
            )
        index += 1
    return contributions


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


def _split_list(value: Any) -> list[str]:
    text = _text(value)
    if not text:
        return []
    normalized = text.replace(",", ";")
    return [part.strip() for part in normalized.split(";") if part.strip()]


def _split_semicolon(value: Any) -> list[str]:
    text = _text(value)
    if not text:
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def _manual_review_actions(
    row: pd.Series,
    event_count: int,
    risk_flags: str,
    financial_anomalies: list[str],
) -> list[str]:
    actions = []
    if event_count > 0 or _text(row.get("positive_event_types")) or _text(row.get("risk_event_types")):
        actions.append("Review announcement evidence")
    if financial_anomalies:
        actions.append("Review financial anomalies")
    if risk_flags:
        actions.append("Review chart/risk flags")
    if not actions:
        actions.append("Standard monitoring")
    return actions


def _report_text(value: Any) -> str:
    return _text(value) or "n/a"


def _format_drivers(contributions: Any) -> str:
    if not isinstance(contributions, list) or not contributions:
        return "n/a"
    parts = []
    for item in contributions:
        if not isinstance(item, dict):
            continue
        factor = _text(item.get("factor"))
        contribution = item.get("contribution")
        if factor:
            parts.append(f"{factor} ({_format_signed_number(contribution)})")
    return "; ".join(parts) if parts else "n/a"


def _format_risks(card: dict[str, Any]) -> str:
    counter_evidence = card.get("counter_evidence", {})
    parts = [
        _text(counter_evidence.get("risks")),
        _text(counter_evidence.get("risk_event_summary")),
    ]
    parts.extend(_text(item) for item in counter_evidence.get("financial_anomalies", []) or [])
    return "; ".join(part for part in parts if part) or "n/a"


def _format_urls(urls: Any) -> str:
    if not isinstance(urls, list):
        return "n/a"
    return "; ".join(_text(url) for url in urls if _text(url)) or "n/a"


def _format_manual_actions(card: dict[str, Any]) -> str:
    actions = card.get("manual_review_actions", {}).get("actions", [])
    if not isinstance(actions, list):
        return "n/a"
    return "; ".join(_text(action) for action in actions if _text(action)) or "n/a"


def _format_tracking(tracking: Any) -> str:
    if not isinstance(tracking, dict):
        return "status=; next_review_date=; owner="
    return (
        f"status={_text(tracking.get('status'))}; "
        f"next_review_date={_text(tracking.get('next_review_date'))}; "
        f"owner={_text(tracking.get('owner'))}"
    )


def _format_signed_number(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "n/a"
    return f"{number:+g}"


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
