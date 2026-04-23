from __future__ import annotations

from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path

import pandas as pd

from .config import ProjectConfig
from .data_quality import check_signal_quality
from .orders import OrderConfig
from .paper_batch import PaperBatchConfig, run_paper_batch, write_paper_batch_outputs
from .paper_broker import PaperFillConfig
from .portfolio import PortfolioConfig, build_target_portfolio, write_portfolio_summary, write_target_portfolio
from .reconcile import ReconcileConfig
from .risk import RiskConfig, check_portfolio_risk, write_risk_report
from .signal import (
    SignalConfig,
    SignalFactor,
    build_daily_signal,
    fetch_daily_factor_exposures,
    write_daily_signal,
)
from .tradability import TradabilityConfig, apply_tradability_filter


@dataclass(frozen=True)
class BatchPaths:
    signal_paths: list[Path]
    target_paths: list[Path]
    batch_summary_csv: Path
    batch_summary_md: Path


def recent_trading_dates(project_config: ProjectConfig, days: int, end_date: str | None = None) -> list[str]:
    from qlib.data import D

    end = end_date or project_config.end_time
    calendar = D.calendar(start_time=project_config.start_time, end_time=end, freq=project_config.freq)
    dates = [pd.Timestamp(value).strftime("%Y-%m-%d") for value in calendar]
    return dates[-days:]


def build_historical_targets(
    project_config: ProjectConfig,
    factors: list[SignalFactor],
    signal_config: SignalConfig,
    trading_config: TradabilityConfig,
    portfolio_config: PortfolioConfig,
    risk_config: RiskConfig,
    run_dates: list[str],
    signal_dir: Path,
    target_dir: Path,
    current_positions: pd.DataFrame | None = None,
) -> BatchPaths:
    signal_paths: list[Path] = []
    target_paths: list[Path] = []
    signal_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    for run_date in run_dates:
        dated_signal_config = replace(signal_config, run_date=run_date)
        exposures = fetch_daily_factor_exposures(project_config, factors, run_date)
        signal = build_daily_signal(exposures, factors, dated_signal_config)
        signal_path = signal_dir / f"signals_{run_date.replace('-', '')}.csv"
        write_daily_signal(signal, signal_path)
        signal_paths.append(signal_path)

        quality = check_signal_quality(signal)
        if not quality.passed:
            raise ValueError(f"data quality failed for {run_date}: {quality.to_frame().to_dict('records')}")
        tradable = apply_tradability_filter(signal, trading_config)
        portfolio = build_target_portfolio(tradable, portfolio_config, current_positions=current_positions)
        risk = check_portfolio_risk(portfolio, tradable, risk_config, current_positions=current_positions)
        if not risk.passed:
            raise ValueError(f"risk check failed for {run_date}: {risk.to_frame().to_dict('records')}")
        target_path = target_dir / f"target_portfolio_{run_date.replace('-', '')}.csv"
        write_target_portfolio(portfolio, target_path)
        write_portfolio_summary(portfolio, target_dir / f"target_portfolio_summary_{run_date.replace('-', '')}.md")
        write_risk_report(risk, target_dir / f"portfolio_risk_{run_date.replace('-', '')}.md")
        target_paths.append(target_path)
        current_positions = portfolio.rename(columns={"target_weight": "current_weight"})[["instrument", "current_weight"]]
    return BatchPaths(signal_paths=signal_paths, target_paths=target_paths, batch_summary_csv=Path(), batch_summary_md=Path())


def run_historical_paper_batch(
    target_paths: list[Path],
    initial_positions: pd.DataFrame,
    order_config: OrderConfig,
    fill_config: PaperFillConfig,
    reconcile_config: ReconcileConfig,
    batch_config: PaperBatchConfig,
) -> BatchPaths:
    result = run_paper_batch(target_paths, initial_positions, order_config, fill_config, reconcile_config, batch_config)
    summary_csv, summary_md = write_paper_batch_outputs(result, batch_config)
    return BatchPaths(signal_paths=[], target_paths=target_paths, batch_summary_csv=summary_csv, batch_summary_md=summary_md)


def write_historical_batch_summary(paths: BatchPaths, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Historical Paper Batch",
        "",
        f"- signals: {len(paths.signal_paths)}",
        f"- targets: {len(paths.target_paths)}",
        f"- batch_summary_csv: {paths.batch_summary_csv}",
        f"- batch_summary_md: {paths.batch_summary_md}",
        "",
        "## Signals",
        "",
    ]
    lines.extend(f"- {path}" for path in paths.signal_paths[:50])
    lines.extend(["", "## Targets", ""])
    lines.extend(f"- {path}" for path in paths.target_paths[:50])
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output
