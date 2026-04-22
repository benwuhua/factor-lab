#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.model_workflow import build_qrun_command, render_lgb_workflow_config


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Render and optionally run a Qlib LightGBM Alpha158 workflow.")
    parser.add_argument("--output", default=str(root / "configs/qlib_lgb_workflow.yaml"))
    parser.add_argument("--provider-config", default=str(root / "configs/provider.yaml"))
    parser.add_argument("--qrun-bin", default=str(root / ".venv/bin/qrun"))
    parser.add_argument("--dry-run", action="store_true", help="Render config and print the qrun command without running it.")
    args = parser.parse_args()

    config = load_project_config(args.provider_config)
    workflow_path = render_lgb_workflow_config(config, args.output)
    command = build_qrun_command(workflow_path, qrun_bin=args.qrun_bin)
    print(f"wrote: {workflow_path}")
    print("command:", " ".join(command))
    if args.dry_run:
        return 0
    completed = subprocess.run(command, cwd=root, check=False)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
