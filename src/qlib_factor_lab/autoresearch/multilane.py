from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qlib_factor_lab.autoresearch.cross_sectional_oracle import run_cross_sectional_lane_oracle
from qlib_factor_lab.autoresearch.event_oracle import run_event_lane_oracle
from qlib_factor_lab.autoresearch.fundamental_oracle import run_fundamental_lane_oracle
from qlib_factor_lab.autoresearch.oracle import run_expression_oracle
from qlib_factor_lab.autoresearch.regime_oracle import run_regime_lane_oracle
from qlib_factor_lab.config import load_yaml
from qlib_factor_lab.factor_mining import generate_candidate_factors, load_mining_config


_QLIB_ORACLE_LOCK = threading.RLock()


@dataclass(frozen=True)
class MultiLaneReport:
    rows: tuple[dict[str, Any], ...]
    output_path: Path | None = None

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            self.rows,
            columns=[
                "lane",
                "activation_status",
                "run_status",
                "candidate",
                "primary_metric",
                "artifact_dir",
                "detail",
            ],
        )


def run_multilane_autoresearch(
    *,
    lane_space_path: str | Path,
    project_root: str | Path = ".",
    contract_path: str | Path = "configs/autoresearch/contracts/csi500_current_v1.yaml",
    expression_space_path: str | Path = "configs/autoresearch/expression_space.yaml",
    expression_candidate_path: str | Path = "configs/autoresearch/candidates/example_expression.yaml",
    mining_config_path: str | Path = "configs/factor_mining.yaml",
    provider_config_path: str | Path = "configs/provider_current.yaml",
    output_path: str | Path = "reports/autoresearch/multilane_summary.md",
    data_governance_report_path: str | Path | None = None,
    include_shadow: bool = False,
    max_workers: int = 4,
    start_time: str | None = None,
    end_time: str | None = None,
    lane_factor_name_overrides: dict[str, list[str]] | None = None,
) -> MultiLaneReport:
    root = Path(project_root)
    lane_space = load_yaml(_resolve(root, lane_space_path))
    lanes = lane_space.get("lanes") or {}
    lane_overrides = _load_data_governance_activation_overrides(root, data_governance_report_path)
    rows: list[dict[str, Any]] = []
    futures = {}
    with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as executor:
        for lane_name, lane in lanes.items():
            activation = lane_overrides.get(lane_name, str((lane or {}).get("activation_status", "active")))
            if activation == "shadow" and not include_shadow:
                rows.append(_row(lane_name, activation, "shadow_skipped", "", float("nan"), "", "lane is shadow"))
                continue
            if activation == "disabled":
                rows.append(_row(lane_name, activation, "disabled_skipped", "", float("nan"), "", "lane is disabled"))
                continue
            if lane_name in {"pattern_event", "emotion_atmosphere"}:
                future = executor.submit(
                    _run_qlib_oracle,
                    run_event_lane_oracle,
                    {
                        "lane_name": lane_name,
                        "factor_specs": _event_factor_specs(root, mining_config_path, lane_name, lane_factor_name_overrides),
                        "provider_config": provider_config_path,
                        "project_root": root,
                        "start_time": start_time,
                        "end_time": end_time,
                    },
                )
                futures[future] = (lane_name, activation)
                continue
            if lane_name in {"liquidity_microstructure", "risk_structure"}:
                future = executor.submit(
                    _run_qlib_oracle,
                    run_cross_sectional_lane_oracle,
                    {
                        "lane_name": lane_name,
                        "factor_specs": _lane_factor_specs(root, mining_config_path, lane_name, lane_factor_name_overrides),
                        "contract_path": contract_path,
                        "project_root": root,
                        "start_time": start_time,
                        "end_time": end_time,
                    },
                )
                futures[future] = (lane_name, activation)
                continue
            if lane_name == "regime":
                future = executor.submit(
                    _run_qlib_oracle,
                    run_regime_lane_oracle,
                    {
                        "lane_name": lane_name,
                        "provider_config": provider_config_path,
                        "project_root": root,
                        "start_time": start_time,
                        "end_time": end_time,
                    },
                )
                futures[future] = (lane_name, activation)
                continue
            if lane_name == "fundamental_quality":
                future = executor.submit(
                    _run_qlib_oracle,
                    run_fundamental_lane_oracle,
                    {
                        "lane_name": lane_name,
                        "contract_path": contract_path,
                        "project_root": root,
                        "start_time": start_time,
                        "end_time": end_time,
                    },
                )
                futures[future] = (lane_name, activation)
                continue
            if lane_name != "expression_price_volume":
                rows.append(_row(lane_name, activation, "unsupported", "", float("nan"), "", "no runner implemented"))
                continue
            future = executor.submit(
                _run_qlib_oracle,
                run_expression_oracle,
                {
                    "contract_path": contract_path,
                    "space_path": expression_space_path,
                    "candidate_path": expression_candidate_path,
                    "project_root": root,
                    "start_time": start_time,
                    "end_time": end_time,
                },
            )
            futures[future] = (lane_name, activation)
        for future in as_completed(futures):
            lane_name, activation = futures[future]
            try:
                payload, _ = future.result()
                rows.append(
                    _row(
                        lane_name,
                        activation,
                        "completed",
                        str(payload.get("candidate", "")),
                        payload.get("primary_metric", float("nan")),
                        str(payload.get("artifact_dir", "")),
                        str(payload.get("status", "")),
                    )
                )
            except Exception as exc:
                rows.append(_row(lane_name, activation, "crash", "", float("nan"), "", str(exc)))
    rows = sorted(rows, key=lambda item: item["lane"])
    report = MultiLaneReport(tuple(rows), output_path=_resolve(root, output_path))
    write_multilane_report(report, report.output_path)
    return report


def _run_qlib_oracle(oracle_func, kwargs: dict[str, Any]) -> tuple[dict[str, Any], str]:
    # Qlib keeps process-global config/cache state; serialize oracle calls so
    # lane-level parallelism does not corrupt dataset/provider state.
    with _QLIB_ORACLE_LOCK:
        return oracle_func(**kwargs)


def _load_data_governance_activation_overrides(root: Path, report_path: str | Path | None) -> dict[str, str]:
    if report_path is None:
        return {}
    path = _resolve(root, report_path)
    if path.suffix.lower() == ".md":
        path = path.with_suffix(".csv")
    if not path.exists():
        return {}
    frame = pd.read_csv(path)
    if "activation_status" not in frame.columns:
        return {}
    lane_col = "activation_lane" if "activation_lane" in frame.columns else "domain"
    if lane_col not in frame.columns:
        return {}
    output: dict[str, str] = {}
    for _, row in frame.iterrows():
        lane = str(row.get(lane_col, "") or "").strip()
        status = _normalize_activation_status(row.get("activation_status"))
        if lane and status:
            output[lane] = status
    return output


def _normalize_activation_status(value: Any) -> str:
    status = str(value or "").strip().lower()
    if status == "block":
        return "disabled"
    if status in {"active", "shadow", "disabled"}:
        return status
    return status


def write_multilane_report(report: MultiLaneReport, output_path: str | Path | None) -> Path:
    if output_path is None:
        raise ValueError("output_path is required")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame = report.to_frame()
    lines = [
        "# Multilane Autoresearch Summary",
        "",
        f"- generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"- lanes: {len(frame)}",
        "",
        "| lane | activation | run_status | candidate | primary_metric | detail |",
        "|---|---|---|---|---:|---|",
    ]
    for _, row in frame.iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["lane"]),
                    str(row["activation_status"]),
                    str(row["run_status"]),
                    str(row["candidate"]),
                    _format_float(row["primary_metric"]),
                    str(row["detail"]),
                ]
            )
            + " |"
        )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    output.with_suffix(".json").write_text(
        json.dumps(frame.to_dict(orient="records"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output


def _row(
    lane: str,
    activation_status: str,
    run_status: str,
    candidate: str,
    primary_metric: Any,
    artifact_dir: str,
    detail: str,
) -> dict[str, Any]:
    return {
        "lane": lane,
        "activation_status": activation_status,
        "run_status": run_status,
        "candidate": candidate,
        "primary_metric": primary_metric,
        "artifact_dir": artifact_dir,
        "detail": detail,
    }


def _resolve(root: Path, path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else root / value


def _event_factor_specs(
    root: Path,
    mining_config_path: str | Path,
    lane_name: str,
    lane_factor_name_overrides: dict[str, list[str]] | None = None,
) -> list[dict[str, Any]]:
    return _lane_factor_specs(root, mining_config_path, lane_name, lane_factor_name_overrides)


def _lane_factor_specs(
    root: Path,
    mining_config_path: str | Path,
    lane_name: str,
    lane_factor_name_overrides: dict[str, list[str]] | None = None,
) -> list[dict[str, Any]]:
    factors = generate_candidate_factors(load_mining_config(_resolve(root, mining_config_path)))
    names = _lane_factor_names(lane_name, lane_factor_name_overrides)
    specs = [
        {
            "name": factor.name,
            "expression": factor.expression,
            "direction": factor.direction,
            "category": factor.category,
            "description": factor.description,
        }
        for factor in factors
        if factor.name in names
    ]
    if not specs:
        raise ValueError(f"no factor specs configured for lane: {lane_name}")
    return specs


def _event_factor_names(lane_name: str) -> set[str]:
    return _lane_factor_names(lane_name)


def _lane_factor_names(lane_name: str, lane_factor_name_overrides: dict[str, list[str]] | None = None) -> set[str]:
    if lane_factor_name_overrides and lane_name in lane_factor_name_overrides:
        return {str(name) for name in lane_factor_name_overrides[lane_name] if str(name).strip()}
    if lane_name == "pattern_event":
        return {"wangji-factor1", "wangji-reversal20-combo", "quiet_breakout_20", "quiet_breakout_60"}
    if lane_name == "emotion_atmosphere":
        return {
            "arbr_26",
            "breadth_proxy_20",
            "davol_5",
            "davol_10",
            "davol_20",
            "heat_cooling_5_20",
            "limit_pressure_5",
            "turnover_mean_5",
            "turnover_mean_20",
        }
    if lane_name == "liquidity_microstructure":
        return {
            "amount_mean_5",
            "amount_mean_20",
            "amount_mean_60",
            "amihud_illiq_10",
            "amihud_illiq_20",
            "amihud_illiq_60",
            "turnover_mean_5",
            "turnover_mean_20",
            "turnover_mean_60",
            "turnover_volatility_20",
            "vosc_12_26",
        }
    if lane_name == "risk_structure":
        return {
            "max_drawdown_20",
            "max_drawdown_60",
            "downside_vol_20",
            "downside_vol_60",
            "gap_risk_20",
            "intraday_excursion_20",
        }
    return set()


def _format_float(value: Any) -> str:
    try:
        return f"{float(value):.6g}"
    except (TypeError, ValueError):
        return ""
