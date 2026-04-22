from __future__ import annotations

from dataclasses import asdict
from itertools import product
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_yaml
from .factor_registry import FactorDef


def load_mining_config(path: str | Path = "configs/factor_mining.yaml") -> dict[str, Any]:
    return load_yaml(path)


def generate_candidate_factors(config: dict[str, Any]) -> list[FactorDef]:
    candidates: list[FactorDef] = []
    seen: set[str] = set()
    for template in config.get("templates", []):
        for values in _iter_template_values(template):
            name = str(template["name"]).format(**values)
            if name in seen:
                raise ValueError(f"duplicate candidate factor name: {name}")
            seen.add(name)
            candidates.append(
                FactorDef(
                    name=name,
                    expression=str(template["expression"]).format(**values),
                    direction=int(template.get("direction", 1)),
                    category=str(template.get("category", "candidate")),
                    description=str(template.get("description", "")).format(**values),
                )
            )
    return candidates


def _iter_template_values(template: dict[str, Any]) -> list[dict[str, Any]]:
    params = template.get("params")
    if params is not None:
        if not isinstance(params, dict):
            raise ValueError("template params must be a mapping")
        keys = list(params)
        value_lists = [values if isinstance(values, list) else [values] for values in params.values()]
        return [dict(zip(keys, values)) for values in product(*value_lists)]

    return [{"window": window} for window in template.get("windows", [None])]


def rank_factor_results(
    results: pd.DataFrame,
    metric: str = "rank_ic_mean",
    min_observations: int = 0,
) -> pd.DataFrame:
    if metric not in results.columns:
        raise ValueError(f"metric is missing from results: {metric}")
    ranked = results.copy()
    if min_observations and "observations" in ranked.columns:
        ranked = ranked[ranked["observations"] >= min_observations]
    score_col = f"abs_{metric}"
    ranked[score_col] = ranked[metric].abs()
    return ranked.sort_values(score_col, ascending=False).reset_index(drop=True)


def factors_to_frame(factors: list[FactorDef]) -> pd.DataFrame:
    return pd.DataFrame([asdict(factor) for factor in factors])
