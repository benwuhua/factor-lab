from __future__ import annotations

import json
import math
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qlib_factor_lab.autoresearch.contracts import load_expression_contract
from qlib_factor_lab.autoresearch.oracle import (
    build_expression_summary_payload,
    compute_complexity_score,
    determine_expression_status,
    render_summary_block,
)
from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_eval import EvalConfig, evaluate_factor, write_eval_report
from qlib_factor_lab.factor_registry import FactorDef


def run_cross_sectional_lane_oracle(
    *,
    lane_name: str,
    factor_specs: list[dict[str, Any]],
    contract_path: str | Path,
    project_root: str | Path = ".",
    start_time: str | None = None,
    end_time: str | None = None,
) -> tuple[dict[str, Any], str]:
    started = time.time()
    root = Path(project_root)
    contract = load_expression_contract(_resolve(root, contract_path))
    run_id = _make_run_id(lane_name)
    artifact_dir = _resolve(root, contract.artifact_root) / f"{lane_name}_{run_id}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    project_config = load_project_config(_resolve(root, contract.provider_config))
    project_config = replace(
        project_config,
        start_time=start_time or contract.start_time,
        end_time=end_time or contract.end_time,
    )

    rows: list[dict[str, Any]] = []
    initialized = False
    for spec in factor_specs:
        factor = _factor_from_spec(spec, lane_name)
        factor_dir = artifact_dir / factor.name
        factor_dir.mkdir(parents=True, exist_ok=True)
        raw_eval = pd.DataFrame()
        neutralized_eval = pd.DataFrame()
        if contract.write_raw:
            raw_eval = evaluate_factor(
                project_config,
                factor,
                EvalConfig(
                    horizons=contract.horizons,
                    neutralize_size=False,
                    purification_steps=contract.purification_steps,
                    purification_mad_n=contract.purification_mad_n,
                ),
                initialize=not initialized,
            )
            initialized = True
            write_eval_report(raw_eval, factor_dir / "raw_eval.csv")
        if contract.neutralize_size_proxy:
            neutralized_eval = evaluate_factor(
                project_config,
                factor,
                EvalConfig(
                    horizons=contract.horizons,
                    neutralize_size=True,
                    purification_steps=contract.purification_steps,
                    purification_mad_n=contract.purification_mad_n,
                ),
                initialize=not initialized,
            )
            initialized = True
            write_eval_report(neutralized_eval, factor_dir / "neutralized_eval.csv")
        payload = build_expression_summary_payload(
            run_id=run_id,
            candidate_name=factor.name,
            commit="",
            contract_name=contract.name,
            universe=contract.universe,
            horizons=contract.horizons,
            raw_eval=raw_eval,
            neutralized_eval=neutralized_eval,
            complexity_score=compute_complexity_score(factor.expression),
            artifact_dir=factor_dir,
        )
        status, reason = determine_expression_status(payload, contract)
        payload["loop"] = lane_name
        payload["status"] = status
        payload["decision_reason"] = reason
        payload["purification"] = "+".join(contract.purification_steps)
        rows.append(payload)

    factors_frame = pd.DataFrame(rows)
    write_eval_report(factors_frame, artifact_dir / "factor_summaries.csv")
    payload = _lane_payload(
        lane_name=lane_name,
        run_id=run_id,
        factor_specs=factor_specs,
        factors_frame=factors_frame,
        artifact_dir=artifact_dir,
        elapsed_sec=round(time.time() - started, 3),
    )
    block = render_summary_block(payload)
    (artifact_dir / "summary.txt").write_text(block, encoding="utf-8")
    (artifact_dir / "summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload, block


def _lane_payload(
    *,
    lane_name: str,
    run_id: str,
    factor_specs: list[dict[str, Any]],
    factors_frame: pd.DataFrame,
    artifact_dir: Path,
    elapsed_sec: float,
) -> dict[str, Any]:
    best = _best_factor_row(factors_frame)
    primary = _float(best.get("primary_metric", float("nan"))) if best is not None else float("nan")
    candidate = str(best.get("candidate", "")) if best is not None else ""
    status = str(best.get("status", "")) if best is not None else "discard_candidate"
    if status != "review":
        status = "discard_candidate"
    return {
        "loop": lane_name,
        "run_id": run_id,
        "candidate": candidate,
        "factor_count": len(factor_specs),
        "status": status,
        "decision_reason": "" if status == "review" else f"no reviewed {lane_name} candidate",
        "primary_metric": primary,
        "artifact_dir": str(artifact_dir),
        "elapsed_sec": elapsed_sec,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def _best_factor_row(frame: pd.DataFrame) -> pd.Series | None:
    if frame.empty or "primary_metric" not in frame.columns:
        return None
    metrics = pd.to_numeric(frame["primary_metric"], errors="coerce")
    if metrics.dropna().empty:
        return None
    return frame.loc[metrics.idxmax()]


def _factor_from_spec(spec: dict[str, Any], lane_name: str) -> FactorDef:
    return FactorDef(
        name=str(spec["name"]),
        expression=str(spec["expression"]),
        direction=int(spec.get("direction", 1)),
        category=str(spec.get("category", lane_name)),
        description=str(spec.get("description", "")),
    )


def _float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return result if math.isfinite(result) else float("nan")


def _make_run_id(lane_name: str) -> str:
    return f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{lane_name}"


def _resolve(root: Path, path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else root / value
