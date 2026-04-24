from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_project_config, load_yaml
from .data_quality import check_signal_quality, load_data_quality_config, write_quality_report
from .expert_review import build_expert_review_packet
from .orders import build_order_suggestions, load_order_config, write_orders
from .paper_broker import load_paper_fill_config, simulate_paper_fills, write_fills
from .portfolio import build_target_portfolio, load_portfolio_config, write_portfolio_summary, write_target_portfolio
from .reconcile import load_reconcile_config, reconcile_positions, write_reconciliation_report
from .risk import check_portfolio_risk, load_risk_config, write_risk_report
from .signal import (
    build_daily_signal,
    fetch_daily_factor_exposures,
    load_approved_signal_factors,
    load_signal_config,
    write_daily_signal,
    write_signal_summary,
)
from .state import apply_fills_to_positions, write_positions_state
from .tradability import apply_tradability_filter, load_trading_config


@dataclass(frozen=True)
class DailyPipelineInputs:
    signal_config_path: Path
    trading_config_path: Path
    portfolio_config_path: Path
    risk_config_path: Path
    execution_config_path: Path
    exposures_csv: Path | None = None
    current_positions_csv: Path | None = None
    run_date: str | None = None
    active_regime: str | None = None


@dataclass(frozen=True)
class DailyPipelineResult:
    run_date: str
    run_dir: Path
    status: str
    risk_passed: bool
    manifest_path: Path
    artifacts: dict[str, str]


def run_daily_pipeline(root: str | Path, inputs: DailyPipelineInputs) -> DailyPipelineResult:
    root_path = Path(root).expanduser().resolve()
    signal_config = load_signal_config(_resolve(root_path, inputs.signal_config_path))
    if inputs.run_date is not None:
        signal_config = signal_config.__class__(**{**signal_config.__dict__, "run_date": inputs.run_date})
    if inputs.active_regime is not None:
        signal_config = signal_config.__class__(**{**signal_config.__dict__, "active_regime": inputs.active_regime})
    if signal_config.execution_calendar_path is not None:
        signal_config = signal_config.__class__(
            **{**signal_config.__dict__, "execution_calendar_path": _resolve(root_path, signal_config.execution_calendar_path)}
        )

    factors = load_approved_signal_factors(_resolve(root_path, signal_config.approved_factors_path))
    exposures = _load_exposures(root_path, inputs.exposures_csv, signal_config, factors)
    if signal_config.run_date == "latest":
        signal_config = signal_config.__class__(**{**signal_config.__dict__, "run_date": str(exposures["date"].max())})

    signal = build_daily_signal(exposures, factors, signal_config)
    run_date = str(signal["date"].max()) if not signal.empty else signal_config.run_date
    run_dir = _resolve(root_path, _run_dir(load_yaml(_resolve(root_path, inputs.execution_config_path)), run_date))
    run_dir.mkdir(parents=True, exist_ok=True)

    artifacts: dict[str, str] = {}
    signal_path = write_daily_signal(signal, run_dir / "signals.csv")
    signal_summary_path = write_signal_summary(signal, factors, signal_config, run_dir / "signal_summary.md")
    artifacts.update({"signals": str(signal_path), "signal_summary": str(signal_summary_path)})
    _copy_if_exists(_resolve(root_path, signal_config.approved_factors_path), run_dir / "approved_factors.yaml", artifacts, "approved_factors")
    _copy_configs(root_path, run_dir, inputs, artifacts)

    quality_report = check_signal_quality(signal, load_data_quality_config(_resolve(root_path, inputs.trading_config_path)))
    quality_path = write_quality_report(quality_report, run_dir / "data_quality.md")
    artifacts["data_quality"] = str(quality_path)
    if not quality_report.passed:
        manifest_path = _write_manifest(root_path, run_dir, run_date, "quality_failed", False, artifacts, inputs)
        return DailyPipelineResult(run_date, run_dir, "quality_failed", False, manifest_path, artifacts)

    current_positions = _load_current_positions(root_path, inputs.current_positions_csv)
    tradable_signal = apply_tradability_filter(signal, load_trading_config(_resolve(root_path, inputs.trading_config_path)))
    portfolio = build_target_portfolio(
        tradable_signal,
        load_portfolio_config(_resolve(root_path, inputs.portfolio_config_path)),
        current_positions=current_positions,
    )
    portfolio_config = load_portfolio_config(_resolve(root_path, inputs.portfolio_config_path))
    portfolio_path = write_target_portfolio(portfolio, run_dir / "target_portfolio.csv")
    portfolio_summary_path = write_portfolio_summary(portfolio, run_dir / "target_portfolio_summary.md")
    artifacts.update({"target_portfolio": str(portfolio_path), "target_portfolio_summary": str(portfolio_summary_path)})

    diagnostics = _load_factor_diagnostics(root_path, run_date)
    expert_review_path = run_dir / "expert_review_packet.md"
    expert_review_path.write_text(
        build_expert_review_packet(portfolio, diagnostics, run_date=run_date),
        encoding="utf-8",
    )
    artifacts["expert_review_packet"] = str(expert_review_path)

    risk_report = check_portfolio_risk(
        portfolio,
        tradable_signal,
        load_risk_config(_resolve(root_path, inputs.risk_config_path)),
        current_positions=current_positions,
    )
    risk_path = write_risk_report(risk_report, run_dir / "risk_report.md")
    artifacts["risk_report"] = str(risk_path)
    if not risk_report.passed:
        manifest_path = _write_manifest(root_path, run_dir, run_date, "risk_failed", False, artifacts, inputs)
        return DailyPipelineResult(run_date, run_dir, "risk_failed", False, manifest_path, artifacts)

    execution_path = _resolve(root_path, inputs.execution_config_path)
    orders = build_order_suggestions(portfolio, current_positions, load_order_config(execution_path))
    fills = simulate_paper_fills(orders, load_paper_fill_config(execution_path))
    expected = apply_fills_to_positions(current_positions, fills)
    reconcile_report = reconcile_positions(expected, expected.copy(), load_reconcile_config(execution_path))
    orders_path = write_orders(orders, run_dir / "orders.csv")
    fills_path = write_fills(fills, run_dir / "fills.csv")
    expected_path = write_positions_state(expected, run_dir / "positions_expected.csv")
    reconciliation_path = write_reconciliation_report(reconcile_report, run_dir / "reconciliation.md")
    artifacts.update(
        {
            "orders": str(orders_path),
            "fills": str(fills_path),
            "positions_expected": str(expected_path),
            "reconciliation": str(reconciliation_path),
        }
    )
    manifest_path = _write_manifest(root_path, run_dir, run_date, "pass", True, artifacts, inputs)
    return DailyPipelineResult(run_date, run_dir, "pass", True, manifest_path, artifacts)


def _load_exposures(root: Path, exposures_csv: Path | None, signal_config: Any, factors: Any) -> pd.DataFrame:
    if exposures_csv is not None:
        return pd.read_csv(_resolve(root, exposures_csv))
    project_config = load_project_config(_resolve(root, signal_config.provider_config))
    return fetch_daily_factor_exposures(project_config, factors, signal_config.run_date)


def _load_current_positions(root: Path, current_positions_csv: Path | None) -> pd.DataFrame:
    columns = ["instrument", "current_weight"]
    if current_positions_csv is None:
        return pd.DataFrame(columns=columns)
    path = _resolve(root, current_positions_csv)
    if not path.exists():
        return pd.DataFrame(columns=columns)
    return pd.read_csv(path)


def _load_factor_diagnostics(root: Path, run_date: str) -> pd.DataFrame | None:
    candidates = [
        root / "reports" / f"single_factor_diagnostics_{run_date.replace('-', '')}.csv",
        root / "reports" / f"single_factor_diagnostics_{run_date}.csv",
        root / "reports" / "single_factor_diagnostics.csv",
    ]
    for path in candidates:
        if path.exists():
            return pd.read_csv(path)
    return None


def _copy_configs(root: Path, run_dir: Path, inputs: DailyPipelineInputs, artifacts: dict[str, str]) -> None:
    config_dir = run_dir / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    for name, path in [
        ("signal_config", inputs.signal_config_path),
        ("trading_config", inputs.trading_config_path),
        ("portfolio_config", inputs.portfolio_config_path),
        ("risk_config", inputs.risk_config_path),
        ("execution_config", inputs.execution_config_path),
    ]:
        _copy_if_exists(_resolve(root, path), config_dir / Path(path).name, artifacts, name)


def _copy_if_exists(source: Path, dest: Path, artifacts: dict[str, str], key: str) -> None:
    if not source.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)
    artifacts[key] = str(dest)


def _write_manifest(
    root: Path,
    run_dir: Path,
    run_date: str,
    status: str,
    risk_passed: bool,
    artifacts: dict[str, str],
    inputs: DailyPipelineInputs,
) -> Path:
    payload = {
        "run_date": run_date,
        "status": status,
        "risk_passed": risk_passed,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "git_commit": _git_commit(root),
        "inputs": {
            "signal_config": str(inputs.signal_config_path),
            "trading_config": str(inputs.trading_config_path),
            "portfolio_config": str(inputs.portfolio_config_path),
            "risk_config": str(inputs.risk_config_path),
            "execution_config": str(inputs.execution_config_path),
            "exposures_csv": str(inputs.exposures_csv) if inputs.exposures_csv else None,
            "current_positions_csv": str(inputs.current_positions_csv) if inputs.current_positions_csv else None,
        },
        "artifacts": artifacts,
    }
    path = run_dir / "manifest.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _git_commit(root: Path) -> str:
    result = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=root, capture_output=True, text=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else ""


def _run_dir(data: dict[str, Any], run_date: str) -> Path:
    output = data.get("output", {})
    template = output.get("run_dir", "runs/{run_yyyymmdd}")
    return Path(str(template).format(run_date=run_date, run_yyyymmdd=run_date.replace("-", "")))


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate
