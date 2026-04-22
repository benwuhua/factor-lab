from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from qlib_factor_lab.config import load_yaml


@dataclass(frozen=True)
class ExpressionContract:
    name: str
    provider_config: Path
    universe: str
    benchmark: str
    start_time: str
    end_time: str
    horizons: tuple[int, ...]
    metric: str
    write_raw: bool
    neutralize_size_proxy: bool
    minimum_observations: int
    artifact_root: Path
    ledger_path: Path


_REQUIRED_FIELDS = (
    "name",
    "provider_config",
    "universe",
    "benchmark",
    "start_time",
    "end_time",
    "horizons",
    "metric",
    "neutralization",
    "minimum_observations",
    "artifact_root",
    "ledger_path",
)


def load_expression_contract(path: str | Path) -> ExpressionContract:
    data = load_yaml(path)
    for field in _REQUIRED_FIELDS:
        if field not in data:
            raise ValueError(f"missing required contract field: {field}")

    horizons = _parse_horizons(data["horizons"])
    neutralization = data["neutralization"]
    if not isinstance(neutralization, dict):
        raise ValueError("neutralization must be a mapping")

    return ExpressionContract(
        name=str(data["name"]),
        provider_config=Path(str(data["provider_config"])),
        universe=str(data["universe"]),
        benchmark=str(data["benchmark"]),
        start_time=str(data["start_time"]),
        end_time=str(data["end_time"]),
        horizons=horizons,
        metric=str(data["metric"]),
        write_raw=bool(neutralization.get("raw", True)),
        neutralize_size_proxy=bool(neutralization.get("size_proxy", True)),
        minimum_observations=int(data["minimum_observations"]),
        artifact_root=Path(str(data["artifact_root"])),
        ledger_path=Path(str(data["ledger_path"])),
    )


def _parse_horizons(value: Any) -> tuple[int, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError("horizons must be positive integers")
    horizons: list[int] = []
    for item in value:
        if not isinstance(item, int) or item <= 0:
            raise ValueError("horizons must be positive integers")
        horizons.append(item)
    return tuple(horizons)
