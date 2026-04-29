from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .combo_spec import ComboSpec


@dataclass(frozen=True)
class StrategyEntry:
    strategy_id: str
    strategy_name: str
    strategy_family: str
    candidate_lane: str = ""
    template_formula: str = ""
    a_share_transferability: str = ""
    source: str = ""
    asset_class: str = ""
    signal_type: str = ""
    required_data: tuple[str, ...] = ()
    risk_notes: tuple[str, ...] = ()
    related_factors: tuple[str, ...] = ()
    implementation_status: str = "candidate"


@dataclass(frozen=True)
class StrategyProposal:
    strategy_id: str
    strategy_name: str
    strategy_family: str
    candidate_lane: str
    template_formula: str
    a_share_transferability: str
    reason: str


def load_strategy_dictionary(path: str | Path) -> list[StrategyEntry]:
    source_path = Path(path)
    data = yaml.safe_load(source_path.read_text(encoding="utf-8")) or {}
    default_source = str(data.get("source", ""))
    entries = []
    seen = set()
    for raw in data.get("strategies", []):
        strategy_id = str(raw.get("strategy_id", "")).strip()
        if not strategy_id:
            raise ValueError("strategy entry is missing strategy_id")
        if strategy_id in seen:
            raise ValueError(f"duplicate strategy_id: {strategy_id}")
        seen.add(strategy_id)
        entries.append(_strategy_entry(raw, default_source=default_source))
    return entries


def filter_strategy_entries(
    entries: list[StrategyEntry],
    *,
    candidate_lane: str | None = None,
    strategy_family: str | None = None,
    min_transferability: str | None = None,
) -> list[StrategyEntry]:
    filtered = entries
    if candidate_lane:
        filtered = [entry for entry in filtered if entry.candidate_lane == candidate_lane]
    if strategy_family:
        filtered = [entry for entry in filtered if entry.strategy_family == strategy_family]
    if min_transferability:
        minimum = _transferability_rank(min_transferability)
        filtered = [
            entry
            for entry in filtered
            if _transferability_rank(entry.a_share_transferability) >= minimum
        ]
    return filtered


def propose_strategy_ideas(
    entries: list[StrategyEntry],
    *,
    combo_spec: ComboSpec | None = None,
    limit: int = 10,
    candidate_lane: str | None = None,
) -> list[StrategyProposal]:
    candidates = filter_strategy_entries(entries, candidate_lane=candidate_lane)
    family_counts = _combo_family_counts(combo_spec)
    ranked = sorted(
        candidates,
        key=lambda entry: (
            family_counts.get(entry.strategy_family, 0),
            -_transferability_rank(entry.a_share_transferability),
            entry.strategy_id,
        ),
    )
    return [
        StrategyProposal(
            strategy_id=entry.strategy_id,
            strategy_name=entry.strategy_name,
            strategy_family=entry.strategy_family,
            candidate_lane=entry.candidate_lane,
            template_formula=entry.template_formula,
            a_share_transferability=entry.a_share_transferability,
            reason=_proposal_reason(entry, family_counts),
        )
        for entry in ranked[:limit]
    ]


def render_strategy_proposals_markdown(proposals: list[StrategyProposal]) -> str:
    lines = [
        "# Strategy Dictionary Proposals",
        "",
        "| strategy_id | family | lane | transferability | template | reason |",
        "|---|---|---|---|---|---|",
    ]
    for proposal in proposals:
        lines.append(
            "| "
            + " | ".join(
                [
                    proposal.strategy_id,
                    proposal.strategy_family,
                    proposal.candidate_lane,
                    proposal.a_share_transferability,
                    proposal.template_formula,
                    proposal.reason,
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def build_expression_candidate_from_strategy(entry: StrategyEntry) -> dict[str, Any]:
    if entry.strategy_id == "stock_low_volatility":
        return {
            "name": "low_vol_120_v1",
            "family": "volatility",
            "expression": "Std($close / Ref($close, 1) - 1, 120)",
            "direction": -1,
            "description": "120 day close-to-close volatility inspired by the low-volatility strategy archetype. Lower volatility ranks higher.",
            "expected_behavior": "Stocks with lower 120 day realized volatility should receive higher scores after applying direction -1.",
            "strategy_dictionary_id": entry.strategy_id,
        }
    if entry.strategy_id == "stock_price_momentum":
        return {
            "name": "momentum_120_skip_20_v1",
            "family": "momentum",
            "expression": "Ref($close, 20) / Ref($close, 120) - 1",
            "direction": 1,
            "description": "120 day price momentum skipping the latest 20 sessions, inspired by the price momentum strategy archetype.",
            "expected_behavior": "Stocks with stronger medium-term returns before the latest 20 day skip window should rank higher.",
            "strategy_dictionary_id": entry.strategy_id,
        }
    raise ValueError(f"no expression candidate mapping for strategy_id: {entry.strategy_id}")


def _strategy_entry(raw: dict[str, Any], *, default_source: str) -> StrategyEntry:
    return StrategyEntry(
        strategy_id=str(raw["strategy_id"]),
        strategy_name=str(raw.get("strategy_name", raw["strategy_id"])),
        strategy_family=str(raw.get("strategy_family", "")),
        candidate_lane=str(raw.get("candidate_lane", "")),
        template_formula=str(raw.get("template_formula", "")),
        a_share_transferability=str(raw.get("a_share_transferability", "")),
        source=str(raw.get("source", default_source)),
        asset_class=str(raw.get("asset_class", "")),
        signal_type=str(raw.get("signal_type", "")),
        required_data=tuple(str(item) for item in raw.get("required_data", [])),
        risk_notes=tuple(str(item) for item in raw.get("risk_notes", [])),
        related_factors=tuple(str(item) for item in raw.get("related_factors", [])),
        implementation_status=str(raw.get("implementation_status", "candidate")),
    )


def _combo_family_counts(combo_spec: ComboSpec | None) -> dict[str, int]:
    if combo_spec is None:
        return {}
    counts: dict[str, int] = {}
    for member in combo_spec.members:
        if not member.active:
            continue
        family = member.family or member.name
        counts[family] = counts.get(family, 0) + 1
    return counts


def _proposal_reason(entry: StrategyEntry, family_counts: dict[str, int]) -> str:
    count = family_counts.get(entry.strategy_family, 0)
    if count == 0:
        return f"missing family: {entry.strategy_family}"
    return f"family already present {count} time(s); use as variant or benchmark"


def _transferability_rank(value: str) -> int:
    mapping = {"low": 1, "medium": 2, "mid": 2, "high": 3}
    return mapping.get(str(value).lower(), 0)
