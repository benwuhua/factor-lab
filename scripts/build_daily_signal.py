#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.combo_spec import (
    build_combo_exposures,
    load_combo_spec,
    market_signal_factors_from_combo_spec,
    signal_config_for_combo_spec,
    signal_factors_from_combo_spec,
)
from qlib_factor_lab.signal import (
    build_daily_signal,
    fetch_daily_factor_exposures,
    load_approved_signal_factors,
    load_signal_config,
    write_daily_signal,
    write_signal_summary,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build an explainable daily signal from approved factors.")
    parser.add_argument("--config", default="configs/signal.yaml", help="Signal config YAML.")
    parser.add_argument("--project-root", default=str(default_root), help="Project root used to resolve relative paths.")
    parser.add_argument("--run-date", default=None, help="Override config run date. Use latest for provider end date.")
    parser.add_argument("--active-regime", default=None, help="Override config active market regime.")
    parser.add_argument("--provider-config", default=None, help="Override provider config used for Qlib exposure fetch.")
    parser.add_argument("--combo-spec", default=None, help="Optional governed combo spec to include fundamental/event/market combo members.")
    parser.add_argument("--exposures-csv", default=None, help="Optional precomputed exposure CSV for tests or offline runs.")
    parser.add_argument("--model-scores-csv", default=None, help="Optional CSV with instrument,model_score columns.")
    parser.add_argument("--signals-output", default=None, help="Override signal CSV output path.")
    parser.add_argument("--summary-output", default=None, help="Override Markdown summary output path.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    config = load_signal_config(_resolve_path(root, args.config))
    if config.execution_calendar_path is not None:
        config = replace(config, execution_calendar_path=_resolve_path(root, config.execution_calendar_path))
    if args.run_date is not None:
        config = replace(config, run_date=args.run_date)
    if args.active_regime is not None:
        config = replace(config, active_regime=args.active_regime)

    approved_factors = load_approved_signal_factors(_resolve_path(root, config.approved_factors_path))
    combo_spec = load_combo_spec(_resolve_path(root, args.combo_spec)) if args.combo_spec else None
    if combo_spec is None:
        factors = approved_factors
        if args.exposures_csv:
            exposures = pd.read_csv(_resolve_path(root, args.exposures_csv))
        else:
            provider_config = Path(args.provider_config) if args.provider_config is not None else config.provider_config
            project_config = load_project_config(_resolve_path(root, provider_config))
            exposures = fetch_daily_factor_exposures(project_config, factors, config.run_date)
    else:
        market_factors = market_signal_factors_from_combo_spec(combo_spec, approved_factors)
        if args.exposures_csv:
            base_exposures = pd.read_csv(_resolve_path(root, args.exposures_csv))
        else:
            provider_config = Path(args.provider_config) if args.provider_config is not None else config.provider_config
            project_config = load_project_config(_resolve_path(root, provider_config))
            base_exposures = fetch_daily_factor_exposures(project_config, market_factors, config.run_date)
        config = signal_config_for_combo_spec(config, combo_spec)
        factors = signal_factors_from_combo_spec(combo_spec, approved_factors)
        exposures = build_combo_exposures(root, combo_spec, base_exposures, config)

    if args.model_scores_csv:
        model_scores = pd.read_csv(_resolve_path(root, args.model_scores_csv))
        exposures = _merge_model_scores(exposures, model_scores)

    if config.run_date == "latest":
        config = replace(config, run_date=str(exposures["date"].max()))

    signal = build_daily_signal(exposures, factors, config)
    signals_output = _materialize_output_path(config.signals_output_path, config.run_date)
    summary_output = _materialize_output_path(config.summary_output_path, config.run_date)
    if args.signals_output is not None:
        signals_output = Path(args.signals_output)
    if args.summary_output is not None:
        summary_output = Path(args.summary_output)

    signals_path = write_daily_signal(signal, _resolve_path(root, signals_output))
    summary_path = write_signal_summary(signal, factors, config, _resolve_path(root, summary_output))
    print(signal.head(20).to_string(index=False))
    print(f"wrote: {signals_path}")
    print(f"wrote: {summary_path}")
    return 0


def _resolve_path(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


def _materialize_output_path(path: Path, run_date: str) -> Path:
    run_yyyymmdd = _yyyymmdd(run_date)
    return Path(str(path).format(run_date=run_date, run_yyyymmdd=run_yyyymmdd))


def _yyyymmdd(run_date: str) -> str:
    try:
        return pd.Timestamp(run_date).strftime("%Y%m%d")
    except (TypeError, ValueError):
        return str(run_date).replace("-", "")


def _merge_model_scores(exposures: pd.DataFrame, model_scores: pd.DataFrame) -> pd.DataFrame:
    if "instrument" not in model_scores.columns or "model_score" not in model_scores.columns:
        raise ValueError("model scores CSV must include instrument and model_score columns")
    join_cols = ["instrument"]
    if "date" in exposures.columns and "date" in model_scores.columns:
        join_cols.insert(0, "date")
    return exposures.merge(model_scores[join_cols + ["model_score"]], on=join_cols, how="left")


if __name__ == "__main__":
    raise SystemExit(main())
