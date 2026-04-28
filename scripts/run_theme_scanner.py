#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.signal import (
    build_daily_signal,
    fetch_daily_factor_exposures,
    load_approved_signal_factors,
    load_signal_config,
)
from qlib_factor_lab.theme_gate import ThemeGateConfig, check_theme_gate, write_theme_gate_report
from qlib_factor_lab.theme_scanner import (
    build_theme_candidates,
    combine_signal_with_supplemental,
    load_theme_universe,
    missing_theme_instruments,
    write_theme_candidate_report,
    write_theme_candidates,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Scan a hot theme universe with the latest daily signal.")
    parser.add_argument("--theme-config", required=True, help="Theme universe YAML.")
    parser.add_argument("--signal-csv", required=True, help="Daily signal CSV.")
    parser.add_argument("--supplemental-signal-csv", default=None, help="Optional extra signal CSV for theme-only names.")
    parser.add_argument("--fill-missing-from-provider", action="store_true")
    parser.add_argument("--signal-config", default="configs/signal.yaml")
    parser.add_argument(
        "--provider-config",
        action="append",
        default=None,
        help="Provider config used to fill missing theme names. Repeat to try multiple datasets.",
    )
    parser.add_argument("--top-k", type=int, default=30)
    parser.add_argument("--output-csv", default="reports/theme_scans/{theme_id}_{run_yyyymmdd}.csv")
    parser.add_argument("--output-md", default="reports/theme_scans/{theme_id}_{run_yyyymmdd}.md")
    parser.add_argument("--theme-gate-output", default="reports/theme_scans/{theme_id}_{run_yyyymmdd}_theme_gate.md")
    parser.add_argument("--theme-gate-min-candidates", type=int, default=3)
    parser.add_argument("--theme-gate-min-signal-coverage", type=float, default=0.5)
    parser.add_argument("--theme-gate-min-amount-20d", type=float, default=100_000_000.0)
    parser.add_argument("--project-root", default=str(default_root))
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    universe = load_theme_universe(_resolve(root, args.theme_config))
    signal = pd.read_csv(_resolve(root, args.signal_csv))
    run_date = _infer_run_date(signal)
    if args.supplemental_signal_csv:
        supplemental = pd.read_csv(_resolve(root, args.supplemental_signal_csv))
        signal = combine_signal_with_supplemental(signal, supplemental)
    if args.fill_missing_from_provider:
        signal = _fill_missing_signal_from_providers(
            root=root,
            signal=signal,
            universe=universe,
            run_date=run_date,
            signal_config_path=args.signal_config,
            provider_config_paths=args.provider_config,
        )
    candidates = build_theme_candidates(signal, universe, top_k=args.top_k)
    csv_path = write_theme_candidates(
        candidates,
        _resolve(root, _materialize(args.output_csv, universe.theme_id, run_date)),
    )
    md_path = write_theme_candidate_report(
        candidates,
        _resolve(root, _materialize(args.output_md, universe.theme_id, run_date)),
        theme_display_name=universe.display_name,
        thesis=universe.thesis,
        sources=universe.sources,
    )
    theme_gate = check_theme_gate(
        candidates,
        ThemeGateConfig(
            min_research_candidates=args.theme_gate_min_candidates,
            min_signal_coverage=args.theme_gate_min_signal_coverage,
            min_amount_20d=args.theme_gate_min_amount_20d,
        ),
    )
    theme_gate_path = write_theme_gate_report(
        theme_gate,
        _resolve(root, _materialize(args.theme_gate_output, universe.theme_id, run_date)),
    )
    print(candidates.head(args.top_k).to_string(index=False))
    print(f"wrote: {csv_path}")
    print(f"wrote: {md_path}")
    print(f"theme_gate: {theme_gate.decision}")
    print(f"wrote: {theme_gate_path}")
    return 0


def _fill_missing_signal_from_provider(
    *,
    root: Path,
    signal: pd.DataFrame,
    universe,
    run_date: str,
    signal_config_path: str | Path,
    provider_config_path: str | Path | None,
) -> pd.DataFrame:
    paths = [provider_config_path] if provider_config_path is not None else None
    return _fill_missing_signal_from_providers(
        root=root,
        signal=signal,
        universe=universe,
        run_date=run_date,
        signal_config_path=signal_config_path,
        provider_config_paths=paths,
    )


def _fill_missing_signal_from_providers(
    *,
    root: Path,
    signal: pd.DataFrame,
    universe,
    run_date: str,
    signal_config_path: str | Path,
    provider_config_paths: list[str | Path] | None,
) -> pd.DataFrame:
    output = signal
    missing = missing_theme_instruments(output, universe)
    if not missing:
        return output
    config = load_signal_config(_resolve(root, signal_config_path))
    if config.execution_calendar_path is not None:
        config = replace(config, execution_calendar_path=_resolve(root, config.execution_calendar_path))
    config = replace(config, run_date=run_date)
    factors = load_approved_signal_factors(_resolve(root, config.approved_factors_path))
    paths = provider_config_paths or [config.provider_config]
    for raw_provider_path in paths:
        missing = missing_theme_instruments(output, universe)
        if not missing:
            break
        provider_path = Path(raw_provider_path) if raw_provider_path is not None else config.provider_config
        project_config = load_project_config(_resolve(root, provider_path))
        try:
            exposures = fetch_daily_factor_exposures(project_config, factors, run_date, instruments=missing)
        except ValueError as exc:
            print(f"warning: theme provider fill skipped for {provider_path}: {exc}")
            continue
        supplemental_signal = build_daily_signal(exposures, factors, config)
        output = combine_signal_with_supplemental(output, supplemental_signal)
    return output


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    return candidate if candidate.is_absolute() else root / candidate


def _materialize(path: str | Path, theme_id: str, run_date: str) -> Path:
    yyyymmdd = run_date.replace("-", "")
    return Path(str(path).format(theme_id=theme_id, run_date=run_date, run_yyyymmdd=yyyymmdd))


def _infer_run_date(signal: pd.DataFrame) -> str:
    if "date" in signal.columns and not signal.empty:
        return str(signal["date"].max())
    return "unknown"


if __name__ == "__main__":
    raise SystemExit(main())
