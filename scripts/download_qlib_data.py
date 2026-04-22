#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config


QLIB_REPO = "https://github.com/microsoft/qlib.git"


def run(cmd: list[str], cwd: Path | None = None) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)


def ensure_qlib_source(source_dir: Path) -> Path:
    get_data = source_dir / "scripts/get_data.py"
    if get_data.exists():
        return get_data
    if source_dir.exists() and any(source_dir.iterdir()):
        raise RuntimeError(f"{source_dir} exists but does not look like a Qlib checkout")
    source_dir.parent.mkdir(parents=True, exist_ok=True)
    if shutil.which("git") is None:
        raise RuntimeError("git is required to fetch Qlib's official data script")
    run(["git", "clone", "--depth", "1", QLIB_REPO, str(source_dir)])
    if not get_data.exists():
        raise RuntimeError(f"expected {get_data} after cloning Qlib")
    return get_data


def qlib_data_exists(target_dir: Path) -> bool:
    return all((target_dir / name).exists() for name in ["calendars", "features", "instruments"])


def main() -> int:
    root = project_root()
    config = load_project_config(root / "configs/provider.yaml")
    parser = argparse.ArgumentParser(description="Download Qlib public stock data into this project.")
    parser.add_argument("--target-dir", default=str(config.provider_uri), help="Qlib data target directory.")
    parser.add_argument("--region", default=config.region, choices=["cn", "us"], help="Qlib market region.")
    parser.add_argument("--interval", default="1d", help="Data interval, e.g. 1d or 1min.")
    parser.add_argument("--force", action="store_true", help="Download even if data markers already exist.")
    parser.add_argument("--source-dir", default=str(root / "data/source/qlib"), help="Local Qlib source checkout.")
    args = parser.parse_args()

    target_dir = Path(args.target_dir).expanduser().resolve()
    if qlib_data_exists(target_dir) and not args.force:
        print(f"Qlib data already exists at {target_dir}")
        return 0

    get_data = ensure_qlib_source(Path(args.source_dir).expanduser().resolve())
    target_dir.mkdir(parents=True, exist_ok=True)
    run(
        [
            sys.executable,
            str(get_data),
            "qlib_data",
            "--target_dir",
            str(target_dir),
            "--region",
            args.region,
            "--interval",
            args.interval,
        ],
        cwd=get_data.parents[1],
    )
    print(f"Qlib data target: {target_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
