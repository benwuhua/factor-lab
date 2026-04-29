#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.combo_diagnostics import evaluate_combo_member_diagnostics, fetch_combo_member_frame
from qlib_factor_lab.combo_spec import load_combo_spec
from qlib_factor_lab.config import load_project_config


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Evaluate recent IC/long-short diagnostics for a governed combo spec.")
    parser.add_argument("--combo-spec", default="configs/combo_specs/balanced_multifactor_v1.yaml")
    parser.add_argument("--provider-config", default="configs/provider_current.yaml")
    parser.add_argument("--start-time", default=None)
    parser.add_argument("--end-time", default=None)
    parser.add_argument("--horizon", action="append", type=int, default=None)
    parser.add_argument("--output-csv", default=None)
    args = parser.parse_args()

    spec_path = _resolve(root, args.combo_spec)
    provider_path = _resolve(root, args.provider_config)
    spec = load_combo_spec(spec_path)
    config = load_project_config(provider_path)
    if args.start_time or args.end_time:
        config = config.__class__(
            **{
                **config.__dict__,
                "start_time": args.start_time or config.start_time,
                "end_time": args.end_time or config.end_time,
            }
        )

    frame = fetch_combo_member_frame(config, spec, root=root)
    horizons = tuple(args.horizon or [20])
    diagnostics = evaluate_combo_member_diagnostics(frame, spec, horizons=horizons)
    output = _resolve(root, args.output_csv or f"reports/combo_member_diagnostics_{spec.name}_{config.end_time.replace('-', '')}.csv")
    output.parent.mkdir(parents=True, exist_ok=True)
    diagnostics.to_csv(output, index=False)
    print(diagnostics.to_string(index=False))
    print(f"wrote: {output}")
    return 0


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    return candidate if candidate.is_absolute() else root / candidate


if __name__ == "__main__":
    raise SystemExit(main())
