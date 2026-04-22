from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import load_yaml


@dataclass(frozen=True)
class FactorDef:
    name: str
    expression: str
    direction: int = 1
    category: str = "custom"
    description: str = ""


def load_factor_registry(path: str | Path = "factors/registry.yaml") -> list[FactorDef]:
    data = load_yaml(path)
    raw_factors = data.get("factors", [])
    if not isinstance(raw_factors, list):
        raise ValueError("factors must be a list")

    factors: list[FactorDef] = []
    seen: set[str] = set()
    for raw in raw_factors:
        if not isinstance(raw, dict):
            raise ValueError("each factor must be a mapping")
        name = str(raw["name"])
        if name in seen:
            raise ValueError(f"duplicate factor name: {name}")
        seen.add(name)
        factors.append(
            FactorDef(
                name=name,
                expression=str(raw["expression"]),
                direction=int(raw.get("direction", 1)),
                category=str(raw.get("category", "custom")),
                description=str(raw.get("description", "")),
            )
        )
    return factors


def select_factors(
    factors: Iterable[FactorDef],
    names: Iterable[str] | None = None,
    categories: Iterable[str] | None = None,
) -> list[FactorDef]:
    name_set = set(names or [])
    category_set = set(categories or [])
    selected = []
    for factor in factors:
        if name_set and factor.name not in name_set:
            continue
        if category_set and factor.category not in category_set:
            continue
        selected.append(factor)
    return selected
