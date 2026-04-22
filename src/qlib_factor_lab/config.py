from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class ProjectConfig:
    provider_uri: Path
    region: str = "cn"
    market: str = "csi500"
    benchmark: str = "SH000905"
    freq: str = "day"
    start_time: str = "2010-01-01"
    end_time: str = "2020-12-31"


def _project_root_from_config(config_path: Path) -> Path:
    # configs/provider.yaml lives directly under the project root in this scaffold.
    return config_path.resolve().parent.parent if config_path.parent.name == "configs" else config_path.resolve().parent


def load_yaml(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return data


def load_project_config(path: str | Path = "configs/provider.yaml") -> ProjectConfig:
    config_path = Path(path)
    data = load_yaml(config_path)
    root = _project_root_from_config(config_path)
    provider_uri = Path(data.get("provider_uri", "data/qlib/cn_data")).expanduser()
    if not provider_uri.is_absolute():
        provider_uri = root / provider_uri
    provider_uri = provider_uri.resolve()

    return ProjectConfig(
        provider_uri=provider_uri,
        region=str(data.get("region", "cn")),
        market=str(data.get("market", "csi500")),
        benchmark=str(data.get("benchmark", "SH000905")),
        freq=str(data.get("freq", "day")),
        start_time=str(data.get("start_time", "2010-01-01")),
        end_time=str(data.get("end_time", "2020-12-31")),
    )
