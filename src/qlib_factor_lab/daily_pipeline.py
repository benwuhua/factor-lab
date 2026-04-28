from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .akshare_data import read_latest_qlib_calendar_date
from .company_events import (
    EVENT_RISK_SNAPSHOT_COLUMNS,
    build_event_risk_snapshot,
    load_company_events,
    load_event_risk_config,
)
from .config import load_project_config, load_yaml
from .data_governance import build_data_governance_report, load_data_governance_config, write_data_governance_report
from .data_quality import check_signal_quality, load_data_quality_config, write_quality_report
from .expert_review import (
    apply_expert_review_portfolio_gate,
    build_expert_review_packet,
    load_expert_review_run_config,
    run_expert_review_command,
    write_expert_review_result,
)
from .orders import build_order_suggestions, load_order_config, write_orders
from .paper_broker import load_paper_fill_config, simulate_paper_fills, write_fills
from .portfolio import build_target_portfolio, load_portfolio_config, write_portfolio_summary, write_target_portfolio
from .reconcile import load_reconcile_config, reconcile_positions, write_reconciliation_report
from .risk import check_portfolio_risk, load_configured_factor_family_map, load_risk_config, write_risk_report
from .security_master import enrich_with_security_master, load_security_master
from .signal import (
    build_daily_signal,
    fetch_daily_factor_exposures,
    load_approved_signal_factors,
    load_signal_config,
    write_daily_signal,
    write_signal_summary,
)
from .state import apply_fills_to_positions, write_positions_state
from .stock_cards import build_stock_cards, write_stock_cards
from .tradability import apply_tradability_filter, load_trading_config


@dataclass(frozen=True)
class DailyPipelineInputs:
    signal_config_path: Path
    trading_config_path: Path
    portfolio_config_path: Path
    risk_config_path: Path
    execution_config_path: Path
    event_risk_config_path: Path | None = None
    data_governance_config_path: Path | None = None
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

    execution_config = load_yaml(_resolve(root_path, inputs.execution_config_path))
    artifacts: dict[str, str] = {}
    run_dir: Path | None = None
    freshness_config = execution_config.get("data_freshness", {}) or {}
    if _should_check_provider_freshness(signal_config.run_date, inputs.exposures_csv, freshness_config):
        freshness_report = check_provider_data_freshness(
            root_path,
            signal_config.provider_config,
            max_age_days=int(freshness_config.get("max_age_days", 3)),
        )
        freshness_run_date = str(freshness_report.get("provider_end_time") or datetime.now().date())
        run_dir = _resolve(root_path, _run_dir(execution_config, freshness_run_date))
        run_dir.mkdir(parents=True, exist_ok=True)
        freshness_path = write_provider_data_freshness_report(freshness_report, run_dir / "data_freshness.md")
        artifacts["data_freshness"] = str(freshness_path)
        if not freshness_report["passed"]:
            _copy_configs(root_path, run_dir, inputs, artifacts)
            run_summary_path = _write_run_summary(
                run_dir / "run_summary.md",
                run_date=freshness_run_date,
                status="data_freshness_failed",
                risk_passed=False,
                artifacts=artifacts,
            )
            artifacts["run_summary"] = str(run_summary_path)
            manifest_path = _write_manifest(root_path, run_dir, freshness_run_date, "data_freshness_failed", False, artifacts, inputs)
            return DailyPipelineResult(freshness_run_date, run_dir, "data_freshness_failed", False, manifest_path, artifacts)
    governance_checked = False
    preflight_run_date = _preflight_run_date(signal_config.run_date)
    if preflight_run_date is not None:
        run_dir = _resolve(root_path, _run_dir(execution_config, preflight_run_date))
        run_dir.mkdir(parents=True, exist_ok=True)
        governance_report = _write_data_governance_artifacts(root_path, run_dir, preflight_run_date, inputs, artifacts)
        governance_checked = True
        if governance_report is not None and not governance_report.passed:
            _copy_configs(root_path, run_dir, inputs, artifacts)
            run_summary_path = _write_run_summary(
                run_dir / "run_summary.md",
                run_date=preflight_run_date,
                status="data_governance_failed",
                risk_passed=False,
                artifacts=artifacts,
            )
            artifacts["run_summary"] = str(run_summary_path)
            manifest_path = _write_manifest(root_path, run_dir, preflight_run_date, "data_governance_failed", False, artifacts, inputs)
            return DailyPipelineResult(preflight_run_date, run_dir, "data_governance_failed", False, manifest_path, artifacts)

    factors = load_approved_signal_factors(_resolve(root_path, signal_config.approved_factors_path))
    exposures = _load_exposures(root_path, inputs.exposures_csv, signal_config, factors)
    if signal_config.run_date == "latest":
        signal_config = signal_config.__class__(**{**signal_config.__dict__, "run_date": str(exposures["date"].max())})

    signal = build_daily_signal(exposures, factors, signal_config)
    run_date = str(signal["date"].max()) if not signal.empty else signal_config.run_date
    run_dir = run_dir or _resolve(root_path, _run_dir(execution_config, run_date))
    run_dir.mkdir(parents=True, exist_ok=True)

    if not governance_checked:
        governance_report = _write_data_governance_artifacts(root_path, run_dir, run_date, inputs, artifacts)
    else:
        governance_report = None
    if not governance_checked and governance_report is not None and not governance_report.passed:
        run_summary_path = _write_run_summary(
            run_dir / "run_summary.md",
            run_date=run_date,
            status="data_governance_failed",
            risk_passed=False,
            artifacts=artifacts,
        )
        artifacts["run_summary"] = str(run_summary_path)
        manifest_path = _write_manifest(root_path, run_dir, run_date, "data_governance_failed", False, artifacts, inputs)
        return DailyPipelineResult(run_date, run_dir, "data_governance_failed", False, manifest_path, artifacts)

    signal = _enrich_signal_with_event_risk(root_path, run_dir, signal, inputs, artifacts)
    signal_path = write_daily_signal(signal, run_dir / "signals.csv")
    signal_summary_path = write_signal_summary(signal, factors, signal_config, run_dir / "signal_summary.md")
    artifacts.update({"signals": str(signal_path), "signal_summary": str(signal_summary_path)})
    _copy_if_exists(_resolve(root_path, signal_config.approved_factors_path), run_dir / "approved_factors.yaml", artifacts, "approved_factors")
    _copy_configs(root_path, run_dir, inputs, artifacts)

    quality_report = check_signal_quality(signal, load_data_quality_config(_resolve(root_path, inputs.trading_config_path)))
    quality_path = write_quality_report(quality_report, run_dir / "data_quality.md")
    artifacts["data_quality"] = str(quality_path)
    if not quality_report.passed:
        run_summary_path = _write_run_summary(
            run_dir / "run_summary.md",
            run_date=run_date,
            status="quality_failed",
            risk_passed=False,
            artifacts=artifacts,
        )
        artifacts["run_summary"] = str(run_summary_path)
        manifest_path = _write_manifest(root_path, run_dir, run_date, "quality_failed", False, artifacts, inputs)
        return DailyPipelineResult(run_date, run_dir, "quality_failed", False, manifest_path, artifacts)

    current_positions = _load_current_positions(root_path, inputs.current_positions_csv)
    tradable_signal = apply_tradability_filter(signal, load_trading_config(_resolve(root_path, inputs.trading_config_path)))
    portfolio = build_target_portfolio(
        tradable_signal,
        load_portfolio_config(_resolve(root_path, inputs.portfolio_config_path)),
        current_positions=current_positions,
    )

    diagnostics = _load_factor_diagnostics(root_path, run_date)
    pre_review_stock_cards = build_stock_cards(
        portfolio,
        run_id=f"daily_{run_date.replace('-', '')}",
        as_of_date=run_date,
        gate_decision="pending",
    )
    expert_review_path = run_dir / "expert_review_packet.md"
    expert_review_packet = build_expert_review_packet(portfolio, diagnostics, run_date=run_date, stock_cards=pre_review_stock_cards)
    expert_review_path.write_text(expert_review_packet, encoding="utf-8")
    artifacts["expert_review_packet"] = str(expert_review_path)
    expert_review_config = load_expert_review_run_config(execution_config)
    expert_review = run_expert_review_command(
        expert_review_packet,
        expert_review_config,
        cwd=root_path,
    )
    expert_review_result_path = write_expert_review_result(expert_review, run_dir / "expert_review_result.md")
    artifacts["expert_review_result"] = str(expert_review_result_path)
    portfolio, expert_review_gate = apply_expert_review_portfolio_gate(
        portfolio,
        decision=expert_review.decision,
        review_status=expert_review.status,
        review_required=expert_review_config.required,
        caution_action=expert_review_config.caution_action,
        caution_weight_multiplier=expert_review_config.caution_weight_multiplier,
        review_output=expert_review.output,
        manual_confirmation=expert_review_config.manual_confirmation,
    )
    portfolio_path = write_target_portfolio(portfolio, run_dir / "target_portfolio.csv")
    portfolio_summary_path = write_portfolio_summary(portfolio, run_dir / "target_portfolio_summary.md")
    artifacts.update({"target_portfolio": str(portfolio_path), "target_portfolio_summary": str(portfolio_summary_path)})
    stock_cards = build_stock_cards(
        portfolio,
        run_id=f"daily_{run_date.replace('-', '')}",
        as_of_date=run_date,
        gate_decision=expert_review_gate["status"],
    )
    stock_cards_path = write_stock_cards(stock_cards, run_dir / "stock_cards.jsonl")
    artifacts["stock_cards"] = str(stock_cards_path)
    if expert_review_gate["status"] in {"blocked", "manual_confirmation_required"}:
        block_report_path = _write_block_report(
            run_dir / "block_report.md",
            status="expert_review_blocked",
            expert_review_gate=expert_review_gate,
        )
        artifacts["block_report"] = str(block_report_path)
        run_summary_path = _write_run_summary(
            run_dir / "run_summary.md",
            run_date=run_date,
            status="expert_review_blocked",
            risk_passed=False,
            artifacts=artifacts,
            expert_review=expert_review.to_manifest(),
            expert_review_gate=expert_review_gate,
            block_report_path=block_report_path,
        )
        artifacts["run_summary"] = str(run_summary_path)
        manifest_path = _write_manifest(
            root_path,
            run_dir,
            run_date,
            "expert_review_blocked",
            False,
            artifacts,
            inputs,
            expert_review=expert_review.to_manifest(),
            expert_review_gate=expert_review_gate,
        )
        return DailyPipelineResult(run_date, run_dir, "expert_review_blocked", False, manifest_path, artifacts)

    risk_config = load_risk_config(_resolve(root_path, inputs.risk_config_path))
    risk_report = check_portfolio_risk(
        portfolio,
        tradable_signal,
        risk_config,
        current_positions=current_positions,
        factor_family_map=load_configured_factor_family_map(risk_config, root=root_path),
    )
    risk_path = write_risk_report(risk_report, run_dir / "risk_report.md")
    artifacts["risk_report"] = str(risk_path)
    if not risk_report.passed:
        block_report_path = _write_block_report(
            run_dir / "block_report.md",
            status="risk_failed",
            risk_report=risk_report,
            expert_review_gate=expert_review_gate,
        )
        artifacts["block_report"] = str(block_report_path)
        run_summary_path = _write_run_summary(
            run_dir / "run_summary.md",
            run_date=run_date,
            status="risk_failed",
            risk_passed=False,
            artifacts=artifacts,
            expert_review=expert_review.to_manifest(),
            expert_review_gate=expert_review_gate,
            block_report_path=block_report_path,
        )
        artifacts["run_summary"] = str(run_summary_path)
        manifest_path = _write_manifest(
            root_path,
            run_dir,
            run_date,
            "risk_failed",
            False,
            artifacts,
            inputs,
            expert_review=expert_review.to_manifest(),
            expert_review_gate=expert_review_gate,
        )
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
    run_summary_path = _write_run_summary(
        run_dir / "run_summary.md",
        run_date=run_date,
        status="pass",
        risk_passed=True,
        artifacts=artifacts,
        expert_review=expert_review.to_manifest(),
        expert_review_gate=expert_review_gate,
    )
    artifacts["run_summary"] = str(run_summary_path)
    manifest_path = _write_manifest(
        root_path,
        run_dir,
        run_date,
        "pass",
        True,
        artifacts,
        inputs,
        expert_review=expert_review.to_manifest(),
        expert_review_gate=expert_review_gate,
    )
    return DailyPipelineResult(run_date, run_dir, "pass", True, manifest_path, artifacts)


def check_provider_data_freshness(
    root: str | Path,
    provider_config_path: str | Path,
    *,
    now: pd.Timestamp | datetime | None = None,
    max_age_days: int = 3,
) -> dict[str, Any]:
    root_path = Path(root).expanduser().resolve()
    config_path = _resolve(root_path, provider_config_path)
    project_config = load_project_config(config_path)
    latest_calendar_date = read_latest_qlib_calendar_date(project_config.provider_uri, freq=project_config.freq)
    current = pd.Timestamp(now or datetime.now()).normalize()
    provider_end = _normalize_date_text(project_config.end_time)
    calendar_end = _normalize_date_text(latest_calendar_date)
    failures: list[str] = []
    age_days: int | None = None

    if calendar_end is None:
        failures.append("missing_calendar")
    else:
        age_days = int((current - pd.Timestamp(calendar_end)).days)
        if age_days < 0:
            failures.append("calendar_after_current_date")
        if age_days > max_age_days:
            failures.append(f"stale_calendar_age_{age_days}d_gt_{max_age_days}d")

    if provider_end is None:
        failures.append("missing_provider_end_time")
    elif calendar_end is not None and pd.Timestamp(provider_end) != pd.Timestamp(calendar_end):
        failures.append(f"provider_end_time_mismatch_calendar:{provider_end}!={calendar_end}")

    return {
        "passed": not failures,
        "provider_config": str(config_path),
        "provider_uri": str(project_config.provider_uri),
        "provider_end_time": provider_end or "",
        "latest_calendar_date": calendar_end or "",
        "current_date": str(current.date()),
        "max_age_days": max_age_days,
        "age_days": "" if age_days is None else age_days,
        "detail": "; ".join(failures),
    }


def write_provider_data_freshness_report(report: dict[str, Any], output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Data Freshness Report",
        "",
        f"- status: {'pass' if report.get('passed') else 'fail'}",
        f"- provider_config: {report.get('provider_config', '')}",
        f"- provider_uri: {report.get('provider_uri', '')}",
        f"- provider_end_time: {report.get('provider_end_time', '')}",
        f"- latest_calendar_date: {report.get('latest_calendar_date', '')}",
        f"- current_date: {report.get('current_date', '')}",
        f"- max_age_days: {report.get('max_age_days', '')}",
        f"- age_days: {report.get('age_days', '')}",
        f"- detail: {report.get('detail', '')}",
    ]
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output


def _should_check_provider_freshness(run_date: str, exposures_csv: Path | None, data: dict[str, Any]) -> bool:
    if data.get("enabled", True) is False:
        return False
    if exposures_csv is not None:
        return False
    return str(run_date).lower() == "latest"


def _normalize_date_text(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        return None
    return str(timestamp.date())


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


def _preflight_run_date(run_date: str) -> str | None:
    value = str(run_date or "").strip()
    if not value or value.lower() == "latest":
        return None
    return value


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


def _enrich_signal_with_event_risk(
    root: Path,
    run_dir: Path,
    signal: pd.DataFrame,
    inputs: DailyPipelineInputs,
    artifacts: dict[str, str],
) -> pd.DataFrame:
    if inputs.event_risk_config_path is None:
        return signal

    config_path = _resolve(root, inputs.event_risk_config_path)
    if not config_path.exists():
        return signal

    event_risk_config = load_event_risk_config(config_path)
    security_master = load_security_master(_resolve_optional(root, event_risk_config.security_master_path))
    enriched_signal = enrich_with_security_master(signal, security_master)
    company_events = load_company_events(_resolve_optional(root, event_risk_config.events_path))
    snapshot = build_event_risk_snapshot(enriched_signal, company_events, event_risk_config)
    snapshot_path = run_dir / "event_risk_snapshot.csv"
    snapshot.to_csv(snapshot_path, index=False)
    artifacts["event_risk_snapshot"] = str(snapshot_path)

    snapshot_value_columns = [column for column in EVENT_RISK_SNAPSHOT_COLUMNS if column not in {"date", "instrument"}]
    enriched_signal = enriched_signal.reset_index(drop=True)
    snapshot = snapshot.reset_index(drop=True)
    if len(enriched_signal) != len(snapshot):
        raise ValueError("event risk snapshot row count must match signal row count")
    for column in snapshot_value_columns:
        enriched_signal[column] = snapshot[column]
    return enriched_signal


def _write_data_governance_artifacts(
    root: Path,
    run_dir: Path,
    run_date: str,
    inputs: DailyPipelineInputs,
    artifacts: dict[str, str],
):
    if inputs.data_governance_config_path is None:
        return None
    config_path = _resolve(root, inputs.data_governance_config_path)
    if not config_path.exists():
        return None
    report = build_data_governance_report(
        load_data_governance_config(config_path),
        project_root=root,
        as_of_date=run_date,
    )
    report_path = write_data_governance_report(report, run_dir / "data_governance.md")
    artifacts["data_governance"] = str(report_path)
    artifacts["data_governance_csv"] = str(report_path.with_suffix(".csv"))
    return report


def _copy_configs(root: Path, run_dir: Path, inputs: DailyPipelineInputs, artifacts: dict[str, str]) -> None:
    config_dir = run_dir / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_paths = [
        ("signal_config", inputs.signal_config_path),
        ("trading_config", inputs.trading_config_path),
        ("portfolio_config", inputs.portfolio_config_path),
        ("risk_config", inputs.risk_config_path),
        ("execution_config", inputs.execution_config_path),
    ]
    if inputs.event_risk_config_path is not None:
        config_paths.append(("event_risk_config", inputs.event_risk_config_path))
    if inputs.data_governance_config_path is not None:
        config_paths.append(("data_governance_config", inputs.data_governance_config_path))
    for name, path in config_paths:
        _copy_if_exists(_resolve(root, path), config_dir / Path(path).name, artifacts, name)


def _copy_if_exists(source: Path, dest: Path, artifacts: dict[str, str], key: str) -> None:
    if not source.exists():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)
    artifacts[key] = str(dest)


def _write_block_report(
    path: str | Path,
    *,
    status: str,
    risk_report: Any | None = None,
    expert_review_gate: dict[str, str] | None = None,
) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Block Report",
        "",
        "## Status",
        "",
        f"- status: {status}",
        "",
        "## Expert Review Gate",
        "",
    ]
    if expert_review_gate:
        for key in ["status", "action", "decision", "detail"]:
            lines.append(f"- {key}: {expert_review_gate.get(key, '')}")
    else:
        lines.append("- No expert review gate was available.")

    lines.extend(["", "## Failed Risk Checks", ""])
    if risk_report is None:
        lines.append("- No risk report was generated before this block.")
    else:
        failed = [row for row in risk_report.rows if row["status"] != "pass"]
        if not failed:
            lines.append("- No failed risk checks.")
        else:
            lines.extend(["| check | value | threshold | detail |", "|---|---:|---:|---|"])
            for row in failed:
                lines.append(f"| {row['check']} | {row['value']} | {row['threshold']} | {row['detail']} |")

    lines.extend(
        [
            "",
            "## Next Actions",
            "",
            "- Review the expert gate and failed risk checks before releasing orders.",
            "- Update event, risk, or review inputs if the block is cleared by a human reviewer.",
            "- Re-run the daily pipeline after the blocking condition is resolved.",
        ]
    )
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output


def _write_run_summary(
    path: str | Path,
    *,
    run_date: str,
    status: str,
    risk_passed: bool,
    artifacts: dict[str, str],
    expert_review: dict[str, str] | None = None,
    expert_review_gate: dict[str, str] | None = None,
    block_report_path: str | Path | None = None,
) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Daily Run Summary",
        "",
        f"- run_date: {run_date}",
        f"- status: {status}",
        f"- risk_passed: {risk_passed}",
        "",
        "## Expert Review",
        "",
    ]
    expert_review = expert_review or {"status": "not_run", "decision": "not_run", "error": ""}
    for key in ["status", "decision", "error"]:
        lines.append(f"- {key}: {expert_review.get(key, '')}")
    lines.extend(["", "## Expert Review Gate", ""])
    expert_review_gate = expert_review_gate or {"status": "not_run", "action": "none", "decision": "not_run", "detail": ""}
    for key in ["status", "action", "decision", "detail"]:
        lines.append(f"- {key}: {expert_review_gate.get(key, '')}")

    if block_report_path is not None and Path(block_report_path).exists():
        lines.extend(["", "## Block Report Summary", ""])
        lines.extend(_block_report_excerpt(Path(block_report_path)))

    lines.extend(["", "## Artifacts", ""])
    for key in sorted(artifacts):
        lines.append(f"- {key}: {artifacts[key]}")
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output


def _block_report_excerpt(path: Path, max_lines: int = 40) -> list[str]:
    lines = [line.rstrip() for line in path.read_text(encoding="utf-8").splitlines()]
    kept = [line for line in lines if line.strip()]
    if len(kept) <= max_lines:
        return kept
    return kept[:max_lines] + ["...", f"- See full block report: {path}"]


def _write_manifest(
    root: Path,
    run_dir: Path,
    run_date: str,
    status: str,
    risk_passed: bool,
    artifacts: dict[str, str],
    inputs: DailyPipelineInputs,
    expert_review: dict[str, str] | None = None,
    expert_review_gate: dict[str, str] | None = None,
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
            "event_risk_config": str(inputs.event_risk_config_path) if inputs.event_risk_config_path else None,
            "data_governance_config": str(inputs.data_governance_config_path) if inputs.data_governance_config_path else None,
            "exposures_csv": str(inputs.exposures_csv) if inputs.exposures_csv else None,
            "current_positions_csv": str(inputs.current_positions_csv) if inputs.current_positions_csv else None,
        },
        "artifacts": artifacts,
        "expert_review": expert_review or {"status": "not_run", "decision": "not_run", "error": ""},
        "expert_review_gate": expert_review_gate or {"status": "not_run", "action": "none", "decision": "not_run", "detail": ""},
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


def _resolve_optional(root: Path, path: str | Path | None) -> Path | None:
    if path is None:
        return None
    return _resolve(root, path)
