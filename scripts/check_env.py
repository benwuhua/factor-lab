#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import argparse
from pathlib import Path

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_registry import load_factor_registry


def has_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Check Qlib Factor Lab environment and data markers.")
    parser.add_argument("--provider-config", default=str(root / "configs/provider.yaml"))
    args = parser.parse_args()

    config = load_project_config(args.provider_config)
    factors = load_factor_registry(root / "factors/registry.yaml")

    print(f"project_root: {root}")
    print(f"provider_uri: {config.provider_uri}")
    print(f"region: {config.region}")
    print(f"market: {config.market}")
    print(f"factors: {len(factors)}")

    for name in ["qlib", "pandas", "numpy", "lightgbm", "sklearn", "yaml"]:
        print(f"{name}: {'yes' if has_module(name) else 'no'}")

    data_markers = ["calendars", "features", "instruments"]
    for marker in data_markers:
        path = config.provider_uri / marker
        print(f"data/{marker}: {'yes' if path.exists() else 'no'} ({path})")

    if not config.provider_uri.exists():
        print("next: run `python scripts/download_qlib_data.py` to create the Qlib data directory.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
