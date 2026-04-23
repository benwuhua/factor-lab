from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from .config import load_yaml
from .factor_registry import FactorDef, load_factor_registry


REQUIRED_APPROVAL_FIELDS = {
    "name",
    "family",
    "type",
    "primary_horizon",
    "supported_universes",
    "regime_profile",
    "turnover_profile",
    "approval_status",
    "evidence_paths",
    "evidence",
    "review_notes",
}

REQUIRED_EVIDENCE_FIELDS = {
    "csi500_neutral_rank_ic_h20",
    "csi300_neutral_rank_ic_h20",
    "weakest_year",
    "weakest_year_neutral_rank_ic_h20",
}


@dataclass(frozen=True)
class FactorSelectionConfig:
    registry_path: Path
    approval_date: str
    generated_at: str | None
    output_approved_path: Path
    output_review_path: Path
    redundancy_similarity_threshold: float
    approved_specs: list[dict[str, Any]]


@dataclass(frozen=True)
class ApprovedFactor:
    name: str
    expression: str
    direction: int
    category: str
    description: str
    family: str
    factor_type: str
    primary_horizon: int
    supported_universes: list[str]
    regime_profile: str
    turnover_profile: str
    approval_status: str
    approval_date: str
    evidence_paths: list[str]
    evidence: dict[str, Any]
    review_notes: str
    redundancy_group: str
    redundancy_representative: str
    similarity_to_representative: float


@dataclass(frozen=True)
class FactorSelectionResult:
    approved_factors: list[ApprovedFactor]
    redundancy_rows: list[dict[str, Any]]
    generated_at: str


def load_factor_selection_config(path: str | Path) -> FactorSelectionConfig:
    data = load_yaml(path)
    output = data.get("output", {})
    redundancy = data.get("redundancy", {})
    return FactorSelectionConfig(
        registry_path=Path(data.get("registry_path", "factors/registry.yaml")),
        approval_date=str(data.get("approval_date", datetime.now().date().isoformat())),
        generated_at=str(data["generated_at"]) if data.get("generated_at") else None,
        output_approved_path=Path(output.get("approved_factors", "reports/approved_factors.yaml")),
        output_review_path=Path(output.get("review_markdown", "reports/factor_review.md")),
        redundancy_similarity_threshold=float(redundancy.get("similarity_threshold", 0.45)),
        approved_specs=list(data.get("approved_factors", [])),
    )


def build_factor_selection(config: FactorSelectionConfig, root: str | Path = ".") -> FactorSelectionResult:
    root_path = Path(root)
    registry = {factor.name: factor for factor in load_factor_registry(root_path / config.registry_path)}
    specs = [_validate_approval_spec(spec, root_path) for spec in config.approved_specs]
    approved = _build_approved_factors(specs, registry, config.approval_date)
    grouped = _assign_redundancy_groups(approved, threshold=config.redundancy_similarity_threshold)
    return FactorSelectionResult(
        approved_factors=grouped,
        redundancy_rows=_redundancy_rows(grouped),
        generated_at=config.generated_at or datetime.now().isoformat(timespec="seconds"),
    )


def write_approved_factors(result: FactorSelectionResult, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": result.generated_at,
        "approved_factors": [_approved_factor_payload(factor) for factor in result.approved_factors],
        "redundancy": result.redundancy_rows,
    }
    output.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return output


def write_factor_review(result: FactorSelectionResult, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Factor Review",
        "",
        f"- generated_at: {result.generated_at}",
        f"- approved_count: {len(result.approved_factors)}",
        "",
        "## Approved Factors",
        "",
        "| factor | status | family | regime | universes | neutral_h20_500 | neutral_h20_300 | weakest_year | redundancy |",
        "|---|---|---|---|---|---:|---:|---|---|",
    ]
    for factor in result.approved_factors:
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{factor.name}`",
                    factor.approval_status,
                    factor.family,
                    factor.regime_profile,
                    ",".join(factor.supported_universes),
                    _format_float(factor.evidence.get("csi500_neutral_rank_ic_h20")),
                    _format_float(factor.evidence.get("csi300_neutral_rank_ic_h20")),
                    str(factor.evidence.get("weakest_year", "")),
                    factor.redundancy_group,
                ]
            )
            + " |"
        )
    lines.extend(["", "## Redundancy Groups", ""])
    for row in result.redundancy_rows:
        lines.append(
            "- "
            f"{row['group']}: `{row['name']}` "
            f"representative=`{row['representative']}` "
            f"similarity={row['similarity_to_representative']:.3f}"
        )
    lines.extend(["", "## Review Notes", ""])
    for factor in result.approved_factors:
        lines.append(f"### {factor.name}")
        lines.append("")
        lines.append(factor.review_notes)
        lines.append("")
    output.write_text("\n".join(lines), encoding="utf-8")
    return output


def _validate_approval_spec(spec: dict[str, Any], root: Path) -> dict[str, Any]:
    missing = REQUIRED_APPROVAL_FIELDS - set(spec)
    if missing:
        raise ValueError(f"approval spec is missing fields: {sorted(missing)}")
    evidence = spec.get("evidence") or {}
    missing_evidence = REQUIRED_EVIDENCE_FIELDS - set(evidence)
    if missing_evidence:
        raise ValueError(f"approval evidence is missing fields: {sorted(missing_evidence)}")
    evidence_paths = list(spec.get("evidence_paths") or [])
    if not evidence_paths:
        raise ValueError(f"{spec['name']} has no evidence_paths")
    for evidence_path in evidence_paths:
        resolved = root / evidence_path
        if not resolved.exists():
            raise ValueError(f"missing evidence path for {spec['name']}: {evidence_path}")
    return spec


def _build_approved_factors(
    specs: list[dict[str, Any]],
    registry: dict[str, FactorDef],
    approval_date: str,
) -> list[ApprovedFactor]:
    approved: list[ApprovedFactor] = []
    for spec in specs:
        factor = registry.get(str(spec["name"]))
        if factor is None:
            raise ValueError(f"approved factor is not in registry: {spec['name']}")
        approved.append(
            ApprovedFactor(
                name=factor.name,
                expression=factor.expression,
                direction=factor.direction,
                category=factor.category,
                description=factor.description,
                family=str(spec["family"]),
                factor_type=str(spec["type"]),
                primary_horizon=int(spec["primary_horizon"]),
                supported_universes=[str(value) for value in spec["supported_universes"]],
                regime_profile=str(spec["regime_profile"]),
                turnover_profile=str(spec["turnover_profile"]),
                approval_status=str(spec["approval_status"]),
                approval_date=str(spec.get("approval_date", approval_date)),
                evidence_paths=[str(value) for value in spec["evidence_paths"]],
                evidence=dict(spec["evidence"]),
                review_notes=str(spec["review_notes"]),
                redundancy_group="",
                redundancy_representative="",
                similarity_to_representative=1.0,
            )
        )
    return approved


def _assign_redundancy_groups(factors: list[ApprovedFactor], threshold: float) -> list[ApprovedFactor]:
    representatives: list[ApprovedFactor] = []
    result: list[ApprovedFactor] = []
    for factor in factors:
        best_representative: ApprovedFactor | None = None
        best_similarity = 0.0
        for representative in representatives:
            if factor.family != representative.family:
                continue
            similarity = expression_similarity(factor.expression, representative.expression)
            if similarity > best_similarity:
                best_representative = representative
                best_similarity = similarity
        if best_representative is None or best_similarity < threshold:
            group = f"F{len(representatives) + 1:03d}"
            representatives.append(factor)
            result.append(
                _replace_redundancy(
                    factor,
                    group=group,
                    representative=factor.name,
                    similarity=1.0,
                )
            )
        else:
            group = next(item.redundancy_group for item in result if item.name == best_representative.name)
            result.append(
                _replace_redundancy(
                    factor,
                    group=group,
                    representative=best_representative.name,
                    similarity=best_similarity,
                )
            )
    return result


def expression_similarity(left: str, right: str) -> float:
    left_tokens = _expression_tokens(left)
    right_tokens = _expression_tokens(right)
    left_structure = _expression_structure_tokens(left)
    right_structure = _expression_structure_tokens(right)
    if not left_tokens and not right_tokens:
        return 1.0
    semantic = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    structural = len(left_structure & right_structure) / len(left_structure | right_structure)
    return max(semantic, structural)


def _expression_tokens(expression: str) -> set[str]:
    fields = {f"field:{value.lower()}" for value in re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", expression)}
    operators = {f"op:{value.lower()}" for value in re.findall(r"\b([A-Z][A-Za-z0-9_]*)\s*\(", expression)}
    windows = {f"w:{value}" for value in re.findall(r",\s*(\d+)\s*\)", expression)}
    concepts = {
        value.lower()
        for value in re.findall(r"[A-Za-z]+", expression)
        if value.lower() not in {"mean", "corr", "ref", "max", "min", "std", "abs"}
    }
    return fields | operators | windows | concepts


def _expression_structure_tokens(expression: str) -> set[str]:
    normalized = re.sub(r"\$[A-Za-z_][A-Za-z0-9_]*", "$field", expression)
    normalized = re.sub(r"\s+", "", normalized)
    return set(re.findall(r"\$field|[A-Za-z]+(?=\()|\d+|[+\-*/()]", normalized))


def _replace_redundancy(
    factor: ApprovedFactor,
    group: str,
    representative: str,
    similarity: float,
) -> ApprovedFactor:
    return ApprovedFactor(
        name=factor.name,
        expression=factor.expression,
        direction=factor.direction,
        category=factor.category,
        description=factor.description,
        family=factor.family,
        factor_type=factor.factor_type,
        primary_horizon=factor.primary_horizon,
        supported_universes=factor.supported_universes,
        regime_profile=factor.regime_profile,
        turnover_profile=factor.turnover_profile,
        approval_status=factor.approval_status,
        approval_date=factor.approval_date,
        evidence_paths=factor.evidence_paths,
        evidence=factor.evidence,
        review_notes=factor.review_notes,
        redundancy_group=group,
        redundancy_representative=representative,
        similarity_to_representative=round(float(similarity), 6),
    )


def _redundancy_rows(factors: list[ApprovedFactor]) -> list[dict[str, Any]]:
    return [
        {
            "group": factor.redundancy_group,
            "name": factor.name,
            "representative": factor.redundancy_representative,
            "similarity_to_representative": factor.similarity_to_representative,
            "family": factor.family,
            "approval_status": factor.approval_status,
        }
        for factor in factors
    ]


def _approved_factor_payload(factor: ApprovedFactor) -> dict[str, Any]:
    return {
        "name": factor.name,
        "expression": factor.expression,
        "direction": factor.direction,
        "category": factor.category,
        "family": factor.family,
        "type": factor.factor_type,
        "primary_horizon": factor.primary_horizon,
        "supported_universes": factor.supported_universes,
        "regime_profile": factor.regime_profile,
        "turnover_profile": factor.turnover_profile,
        "approval_status": factor.approval_status,
        "approval_date": factor.approval_date,
        "evidence_paths": factor.evidence_paths,
        "evidence": factor.evidence,
        "redundancy_group": factor.redundancy_group,
        "redundancy_representative": factor.redundancy_representative,
        "similarity_to_representative": factor.similarity_to_representative,
        "review_notes": factor.review_notes,
    }


def _format_float(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if not math.isfinite(number):
        return "nan"
    return f"{number:.5f}"
