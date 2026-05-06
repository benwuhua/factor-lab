from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from .akshare_data import read_latest_qlib_calendar_date
from .company_events import (
    EVENT_RISK_SNAPSHOT_COLUMNS,
    build_event_risk_snapshot,
    load_company_events,
    load_event_risk_config,
)
from .config import load_project_config, load_yaml
from .combo_spec import (
    approved_factors_payload_from_combo_spec,
    build_combo_exposures,
    factor_diagnostics_from_combo_spec,
    load_combo_spec,
    market_signal_factors_from_combo_spec,
    signal_config_for_combo_spec,
    signal_factors_from_combo_spec,
)
from .data_governance import build_data_governance_report, load_data_governance_config, write_data_governance_report
from .data_quality import check_signal_quality, load_data_quality_config, write_quality_report
from .data_update import load_env_file
from .expert_review import (
    apply_expert_review_portfolio_gate,
    build_expert_review_packet,
    load_expert_review_run_config,
    run_expert_review_command,
    write_expert_review_result,
)
from .broker_adapter import load_broker_adapter
from .orders import build_order_suggestions, load_order_config, write_orders
from .paper_broker import write_fills
from .portfolio import build_target_portfolio, load_portfolio_config, write_portfolio_summary, write_target_portfolio
from .reconcile import write_reconciliation_report
from .risk import (
    check_portfolio_risk,
    load_configured_factor_family_map,
    load_configured_factor_logic_map,
    load_risk_config,
    write_risk_report,
)
from .security_master import enrich_with_security_master, load_security_master
from .signal import (
    build_daily_signal,
    fetch_daily_factor_exposures,
    load_approved_signal_factors,
    load_signal_config,
    write_daily_signal,
    write_signal_summary,
)
from .state import write_positions_state
from .stock_cards import build_stock_cards, write_stock_cards
from .tradability import apply_tradability_filter, load_trading_config
from .tushare_data import get_tushare_token, resolve_latest_tushare_daily_date


@dataclass(frozen=True)
class DailyPipelineInputs:
    signal_config_path: Path
    trading_config_path: Path
    portfolio_config_path: Path
    risk_config_path: Path
    execution_config_path: Path
    event_risk_config_path: Path | None = None
    data_governance_config_path: Path | None = None
    combo_spec_path: Path | None = None
    exposures_csv: Path | None = None
    current_positions_csv: Path | None = None
    expert_manual_confirm: bool = False
    expert_reviewer: str = ""
    expert_confirm_reason: str = ""
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


_GENERATED_RUN_ARTIFACTS = {
    "approved_factors.yaml",
    "block_report.md",
    "combo_factors.yaml",
    "combo_spec.yaml",
    "data_freshness.md",
    "data_governance.csv",
    "data_governance.md",
    "data_quality.md",
    "event_risk_snapshot.csv",
    "execution_portfolio.csv",
    "execution_portfolio_summary.md",
    "expert_review_packet.md",
    "expert_review_result.md",
    "fills.csv",
    "manifest.json",
    "orders.csv",
    "portfolio_gate_explanation.md",
    "positions_expected.csv",
    "reconciliation.md",
    "research_portfolio.csv",
    "research_portfolio_summary.md",
    "research_stock_cards.jsonl",
    "research_stock_cards.md",
    "risk_report.md",
    "run_summary.md",
    "signal_summary.md",
    "signals.csv",
    "stock_cards.jsonl",
    "target_portfolio.csv",
    "target_portfolio_summary.md",
    "configs",
}


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
    execution_config = _apply_expert_manual_confirmation_override(execution_config, inputs)
    artifacts: dict[str, str] = {}
    run_dir: Path | None = None
    freshness_config = execution_config.get("data_freshness", {}) or {}
    if _should_check_provider_freshness(signal_config.run_date, inputs.exposures_csv, freshness_config):
        freshness_report = check_provider_data_freshness(
            root_path,
            signal_config.provider_config,
            max_age_days=int(freshness_config.get("max_age_days", 3)),
            latest_available_data_date=_latest_available_data_date(root_path, freshness_config),
        )
        freshness_run_date = str(freshness_report.get("provider_end_time") or datetime.now().date())
        run_dir = _resolve(root_path, _run_dir(execution_config, freshness_run_date))
        _prepare_run_dir(run_dir)
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
        _prepare_run_dir(run_dir)
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

    approved_factors = load_approved_signal_factors(_resolve(root_path, signal_config.approved_factors_path))
    combo_spec = load_combo_spec(_resolve(root_path, inputs.combo_spec_path)) if inputs.combo_spec_path is not None else None
    if combo_spec is None:
        factors = approved_factors
        exposures = _load_exposures(root_path, inputs.exposures_csv, signal_config, factors)
    else:
        market_factors = market_signal_factors_from_combo_spec(combo_spec, approved_factors)
        base_exposures = _load_exposures(root_path, inputs.exposures_csv, signal_config, market_factors)
        signal_config = signal_config_for_combo_spec(signal_config, combo_spec)
        factors = signal_factors_from_combo_spec(combo_spec, approved_factors)
        exposures = build_combo_exposures(root_path, combo_spec, base_exposures, signal_config)
    if signal_config.run_date == "latest":
        signal_config = signal_config.__class__(**{**signal_config.__dict__, "run_date": str(exposures["date"].max())})

    signal = build_daily_signal(exposures, factors, signal_config)
    run_date = str(signal["date"].max()) if not signal.empty else signal_config.run_date
    if run_dir is None:
        run_dir = _resolve(root_path, _run_dir(execution_config, run_date))
        _prepare_run_dir(run_dir)
    else:
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
    if combo_spec is not None and inputs.combo_spec_path is not None:
        _copy_if_exists(_resolve(root_path, inputs.combo_spec_path), run_dir / "combo_spec.yaml", artifacts, "combo_spec")
        combo_factors_path = run_dir / "combo_factors.yaml"
        combo_factors_path.write_text(
            yaml.safe_dump(approved_factors_payload_from_combo_spec(combo_spec, approved_factors), allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        artifacts["combo_factors"] = str(combo_factors_path)
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
    research_portfolio = build_target_portfolio(
        tradable_signal,
        load_portfolio_config(_resolve(root_path, inputs.portfolio_config_path)),
        current_positions=current_positions,
    )
    _write_pipeline_portfolio_outputs(
        research_portfolio,
        run_dir,
        artifacts,
        artifact_prefix="research",
        summary_title="Research Portfolio Summary",
    )

    diagnostics = (
        factor_diagnostics_from_combo_spec(combo_spec, _load_factor_diagnostics(root_path, run_date, combo_spec.name))
        if combo_spec is not None
        else _load_factor_diagnostics(root_path, run_date)
    )
    announcement_evidence = _load_announcement_evidence(root_path)
    pre_review_stock_cards = build_stock_cards(
        research_portfolio,
        run_id=f"daily_{run_date.replace('-', '')}",
        as_of_date=run_date,
        gate_decision="pending",
        announcement_evidence=announcement_evidence,
    )
    expert_review_path = run_dir / "expert_review_packet.md"
    expert_review_packet = build_expert_review_packet(
        research_portfolio,
        diagnostics,
        run_date=run_date,
        stock_cards=pre_review_stock_cards,
    )
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
    execution_portfolio, expert_review_gate = apply_expert_review_portfolio_gate(
        research_portfolio,
        decision=expert_review.decision,
        review_status=expert_review.status,
        review_required=expert_review_config.required,
        caution_action=expert_review_config.caution_action,
        caution_weight_multiplier=expert_review_config.caution_weight_multiplier,
        review_output=expert_review.output,
        manual_confirmation=expert_review_config.manual_confirmation,
    )
    _write_pipeline_portfolio_outputs(
        execution_portfolio,
        run_dir,
        artifacts,
        artifact_prefix="execution",
        summary_title="Execution Portfolio Summary",
    )
    portfolio_path = write_target_portfolio(execution_portfolio, run_dir / "target_portfolio.csv")
    portfolio_summary_path = write_portfolio_summary(
        execution_portfolio,
        run_dir / "target_portfolio_summary.md",
        title="Execution Portfolio Summary",
    )
    artifacts.update({"target_portfolio": str(portfolio_path), "target_portfolio_summary": str(portfolio_summary_path)})
    stock_cards = build_stock_cards(
        execution_portfolio,
        run_id=f"daily_{run_date.replace('-', '')}",
        as_of_date=run_date,
        gate_decision=expert_review_gate["status"],
        announcement_evidence=announcement_evidence,
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
        execution_portfolio,
        tradable_signal,
        risk_config,
        current_positions=current_positions,
        factor_family_map=load_configured_factor_family_map(risk_config, root=root_path),
        factor_logic_map=load_configured_factor_logic_map(risk_config, root=root_path),
    )
    risk_report = _apply_vendor_data_gate(root_path, risk_report, risk_config)
    risk_path = write_risk_report(risk_report, run_dir / "risk_report.md")
    artifacts["risk_report"] = str(risk_path)
    portfolio_gate_path = _write_portfolio_gate_explanation(
        run_dir / "portfolio_gate_explanation.md",
        risk_report=risk_report,
        risk_report_path=risk_path,
        expert_review_gate=expert_review_gate,
    )
    artifacts["portfolio_gate_explanation"] = str(portfolio_gate_path)
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
    broker = load_broker_adapter(load_yaml(execution_path), run_id=f"{run_date.replace('-', '')}-paper")
    orders = broker.submit_orders(
        broker.validate_orders(build_order_suggestions(execution_portfolio, current_positions, load_order_config(execution_path)))
    )
    fills = broker.fetch_fills(orders)
    expected = broker.fetch_positions(current_positions, fills)
    reconcile_report = broker.reconcile(expected, expected.copy())
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
    latest_available_data_date: str | None = None,
) -> dict[str, Any]:
    root_path = Path(root).expanduser().resolve()
    config_path = _resolve(root_path, provider_config_path)
    project_config = load_project_config(config_path)
    latest_calendar_date = read_latest_qlib_calendar_date(project_config.provider_uri, freq=project_config.freq)
    current = pd.Timestamp(now or datetime.now()).normalize()
    provider_end = _normalize_date_text(project_config.end_time)
    calendar_end = _normalize_date_text(latest_calendar_date)
    available_end = _normalize_date_text(latest_available_data_date)
    failures: list[str] = []
    age_days: int | None = None

    if calendar_end is None:
        failures.append("missing_calendar")
    else:
        age_days = int((current - pd.Timestamp(calendar_end)).days)
        if age_days < 0:
            failures.append("calendar_after_current_date")
        if available_end is not None:
            calendar_ts = pd.Timestamp(calendar_end)
            available_ts = pd.Timestamp(available_end)
            if calendar_ts < available_ts:
                failures.append(f"calendar_behind_available_data:{calendar_end}!={available_end}")
            if calendar_ts > available_ts:
                failures.append(f"calendar_after_available_data:{calendar_end}!={available_end}")
        elif age_days > max_age_days:
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
        "latest_available_data_date": available_end or "",
        "freshness_basis": "latest_available_data_date" if available_end else "calendar_age_days",
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
        f"- latest_available_data_date: {report.get('latest_available_data_date', '')}",
        f"- freshness_basis: {report.get('freshness_basis', '')}",
        f"- current_date: {report.get('current_date', '')}",
        f"- max_age_days: {report.get('max_age_days', '')}",
        f"- age_days: {report.get('age_days', '')}",
        f"- detail: {report.get('detail', '')}",
    ]
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output


def _latest_available_data_date(root: Path, freshness_config: dict[str, Any]) -> str | None:
    provider = str(freshness_config.get("latest_available_provider", "") or "").strip().lower()
    if not provider:
        return None
    if provider != "tushare":
        raise ValueError(f"unsupported data freshness latest_available_provider: {provider}")
    env_file = freshness_config.get("env_file", ".env")
    env_path = Path(str(env_file))
    env = load_env_file(env_path if env_path.is_absolute() else root / env_path)
    token = get_tushare_token(env=env)
    as_of_date = pd.Timestamp(datetime.now()).strftime("%Y%m%d")
    return resolve_latest_tushare_daily_date(as_of_date, token=token)


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
        return pd.read_csv(_resolve(root, exposures_csv), low_memory=False)
    project_config = load_project_config(_resolve(root, signal_config.provider_config))
    return fetch_daily_factor_exposures(project_config, factors, signal_config.run_date)


def _load_current_positions(root: Path, current_positions_csv: Path | None) -> pd.DataFrame:
    columns = ["instrument", "current_weight"]
    if current_positions_csv is None:
        return pd.DataFrame(columns=columns)
    path = _resolve(root, current_positions_csv)
    if not path.exists():
        return pd.DataFrame(columns=columns)
    return pd.read_csv(path, low_memory=False)


def _preflight_run_date(run_date: str) -> str | None:
    value = str(run_date or "").strip()
    if not value or value.lower() == "latest":
        return None
    return value


def _load_factor_diagnostics(root: Path, run_date: str, combo_name: str | None = None) -> pd.DataFrame | None:
    yyyymmdd = run_date.replace("-", "")
    candidates = [
        root / "reports" / f"single_factor_diagnostics_{yyyymmdd}.csv",
        root / "reports" / f"single_factor_diagnostics_{run_date}.csv",
        root / "reports" / "single_factor_diagnostics.csv",
    ]
    if combo_name:
        candidates = [
            root / "reports" / f"combo_member_diagnostics_{combo_name}_{yyyymmdd}.csv",
            root / "reports" / f"combo_member_diagnostics_{combo_name}_{run_date}.csv",
            root / "reports" / f"combo_member_diagnostics_{combo_name}.csv",
            *candidates,
        ]
    for path in candidates:
        if path.exists():
            return pd.read_csv(path, low_memory=False)
    return None


def _load_announcement_evidence(root: Path) -> pd.DataFrame | None:
    path = root / "data" / "announcement_evidence.csv"
    if not path.exists():
        return None
    return pd.read_csv(path, low_memory=False)


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


def _prepare_run_dir(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    for name in _GENERATED_RUN_ARTIFACTS:
        path = run_dir / name
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)


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
    if inputs.combo_spec_path is not None:
        config_paths.append(("combo_spec_config", inputs.combo_spec_path))
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


def _write_portfolio_gate_explanation(
    path: str | Path,
    *,
    risk_report: Any,
    risk_report_path: str | Path,
    expert_review_gate: dict[str, str] | None = None,
) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    failed = [row for row in risk_report.rows if row["status"] != "pass"]
    expert_review_gate = expert_review_gate or {}
    decision = _portfolio_gate_decision(failed, expert_review_gate)
    lines = [
        "# Portfolio Gate Explanation",
        "",
        f"- decision: {decision}",
        f"- risk_report: {risk_report_path}",
        f"- expert_gate_status: {expert_review_gate.get('status', '')}",
        f"- expert_gate_action: {expert_review_gate.get('action', '')}",
        f"- expert_gate_detail: {expert_review_gate.get('detail', '')}",
        "",
        "## Failed Checks",
        "",
    ]
    if not failed:
        lines.append("- No failed portfolio gate checks.")
    else:
        lines.extend(["| check | measured_value | threshold | affected_instruments | next_action | artifact |", "|---|---:|---:|---|---|---|"])
        for row in failed:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("check", "")),
                        _format_gate_value(row.get("value")),
                        _format_gate_value(row.get("threshold")),
                        _affected_instruments(row.get("detail", "")),
                        _next_action_for_gate(str(row.get("check", ""))),
                        str(risk_report_path),
                    ]
                )
                + " |"
            )
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output


def _write_pipeline_portfolio_outputs(
    portfolio: pd.DataFrame,
    run_dir: Path,
    artifacts: dict[str, str],
    *,
    artifact_prefix: str,
    summary_title: str,
) -> None:
    portfolio_path = write_target_portfolio(portfolio, run_dir / f"{artifact_prefix}_portfolio.csv")
    summary_path = write_portfolio_summary(
        portfolio,
        run_dir / f"{artifact_prefix}_portfolio_summary.md",
        title=summary_title,
    )
    artifacts[f"{artifact_prefix}_portfolio"] = str(portfolio_path)
    artifacts[f"{artifact_prefix}_portfolio_summary"] = str(summary_path)


def _portfolio_gate_decision(failed_rows: list[dict[str, Any]], expert_review_gate: dict[str, str]) -> str:
    if expert_review_gate.get("status") in {"blocked", "manual_confirmation_required"}:
        return "reject"
    failed = {str(row.get("check", "")) for row in failed_rows}
    if not failed:
        return "pass"
    caution_checks = {
        "max_industry_weight",
        "min_factor_family_count",
        "max_factor_family_concentration",
        "min_factor_logic_count",
        "max_factor_logic_concentration",
    }
    return "caution" if failed <= caution_checks else "reject"


def _format_gate_value(value: Any) -> str:
    try:
        return f"{float(value):.6g}"
    except (TypeError, ValueError):
        return str(value)


def _affected_instruments(detail: Any) -> str:
    text = str(detail or "").strip()
    if not text:
        return ""
    return "; ".join(part.split(":", 1)[0].strip() for part in text.split(";") if part.strip())


def _next_action_for_gate(check_name: str) -> str:
    if check_name == "event_blocked_positions":
        return "remove affected instruments or wait for event risk to clear"
    if check_name in {"max_single_weight", "min_positions", "no_negative_weights"}:
        return "rebalance portfolio construction inputs"
    if check_name.startswith("max_") or check_name.startswith("min_factor"):
        return "reduce concentration or add independent factor families"
    return "review gate inputs and rerun daily pipeline"


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
            "combo_spec": str(inputs.combo_spec_path) if inputs.combo_spec_path else None,
            "exposures_csv": str(inputs.exposures_csv) if inputs.exposures_csv else None,
            "current_positions_csv": str(inputs.current_positions_csv) if inputs.current_positions_csv else None,
            "expert_manual_confirm": inputs.expert_manual_confirm,
            "expert_reviewer": inputs.expert_reviewer,
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


def _apply_vendor_data_gate(root: Path, risk_report, risk_config):
    if not getattr(risk_config, "enable_vendor_data_gate", False):
        return risk_report
    from .risk import RiskReport
    from .workbench import build_tushare_data_coverage, build_tushare_data_gate_checks

    checks = build_tushare_data_gate_checks(
        build_tushare_data_coverage(root),
        min_instruments=int(getattr(risk_config, "min_tushare_domain_instruments", 1)),
    )
    if checks.empty:
        return risk_report
    return RiskReport(tuple(list(risk_report.rows) + checks.to_dict("records")))


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


def _apply_expert_manual_confirmation_override(data: dict[str, Any], inputs: DailyPipelineInputs) -> dict[str, Any]:
    if not inputs.expert_manual_confirm:
        return data
    output = dict(data)
    expert_review = dict(output.get("expert_review", {}) or {})
    expert_review["caution_action"] = "manual_confirmation"
    expert_review["manual_confirmation"] = {
        "enabled": True,
        "reviewer": inputs.expert_reviewer or "manual",
        "reason": inputs.expert_confirm_reason or "manual confirmation override from CLI",
    }
    output["expert_review"] = expert_review
    return output
