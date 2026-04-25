from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qlib_factor_lab.autoresearch.contracts import ExpressionContract, load_expression_contract
from qlib_factor_lab.autoresearch.expressions import (
    ExpressionCandidate,
    load_expression_candidate,
    load_expression_space,
)
from qlib_factor_lab.autoresearch.ledger import append_expression_ledger_row
from qlib_factor_lab.config import load_project_config, load_yaml
from qlib_factor_lab.factor_eval import EvalConfig, evaluate_factor, write_eval_report


DISCARD_PRIMARY_METRIC_THRESHOLD = 0.01
DISCARD_COMPLEXITY_THRESHOLD = 0.70


def build_expression_summary_payload(
    run_id: str,
    candidate_name: str,
    commit: str,
    contract_name: str,
    universe: str,
    horizons: tuple[int, ...],
    raw_eval: pd.DataFrame,
    neutralized_eval: pd.DataFrame,
    complexity_score: float,
    artifact_dir: str | Path,
    status: str = "review",
    decision_reason: str = "",
) -> dict[str, Any]:
    focus_horizon = 20 if 20 in horizons else max(horizons)
    payload: dict[str, Any] = {
        "loop": "expression",
        "run_id": run_id,
        "candidate": candidate_name,
        "commit": commit,
        "contract": contract_name,
        "universe": universe,
        "horizons": ",".join(str(horizon) for horizon in horizons),
        "complexity_score": round(float(complexity_score), 6),
        "purification": "",
        "status": status,
        "decision_reason": decision_reason,
        "artifact_dir": str(artifact_dir),
    }
    for horizon in horizons:
        payload[f"rank_ic_mean_h{horizon}"] = _metric_for_horizon(raw_eval, horizon, "rank_ic_mean")
        payload[f"neutral_rank_ic_mean_h{horizon}"] = _metric_for_horizon(neutralized_eval, horizon, "rank_ic_mean")
    focus_raw_rank_ic = _metric_for_horizon(raw_eval, focus_horizon, "rank_ic_mean")
    focus_neutral_rank_ic = _metric_for_horizon(neutralized_eval, focus_horizon, "rank_ic_mean")
    payload[f"long_short_mean_return_h{focus_horizon}"] = _metric_for_horizon(
        raw_eval, focus_horizon, "long_short_mean_return"
    )
    payload[f"top_quantile_turnover_h{focus_horizon}"] = _metric_for_horizon(
        raw_eval, focus_horizon, "top_quantile_turnover"
    )
    observations = _first_finite(
        _metric_for_horizon(raw_eval, focus_horizon, "observations"),
        _metric_for_horizon(neutralized_eval, focus_horizon, "observations"),
    )
    payload[f"observations_h{focus_horizon}"] = int(observations) if math.isfinite(observations) else 0
    payload["primary_metric"] = _first_finite(focus_neutral_rank_ic, focus_raw_rank_ic)
    payload["secondary_metric"] = focus_raw_rank_ic
    payload["guard_metric"] = _metric_for_horizon(raw_eval, focus_horizon, "top_quantile_turnover")
    return payload


def render_summary_block(payload: dict[str, Any]) -> str:
    lines = ["---"]
    for key, value in payload.items():
        lines.append(f"{key}: {_format_summary_value(value)}")
    lines.append("---")
    return "\n".join(lines) + "\n"


def compute_complexity_score(expression: str) -> float:
    operator_count = len(re.findall(r"\b[A-Z][A-Za-z0-9_]*\s*\(", expression))
    field_count = len(set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", expression)))
    window_count = len(set(int(value) for value in re.findall(r",\s*(\d+)\s*\)", expression)))
    raw = (len(expression) / 500) + (operator_count / 20) + (field_count / 20) + (window_count / 20)
    return round(min(1.0, max(0.0, raw)), 6)


def determine_expression_status(payload: dict[str, Any], contract: ExpressionContract) -> tuple[str, str]:
    focus_horizon = 20 if 20 in contract.horizons else max(contract.horizons)
    observations = int(payload.get(f"observations_h{focus_horizon}", 0) or 0)
    if observations < contract.minimum_observations:
        return "discard_candidate", "observations_h20 below minimum_observations"
    primary_metric = float(payload.get("primary_metric", float("nan")))
    if math.isnan(primary_metric) or primary_metric < DISCARD_PRIMARY_METRIC_THRESHOLD:
        return "discard_candidate", "primary_metric below discard threshold"
    complexity = float(payload.get("complexity_score", 0.0) or 0.0)
    if complexity > DISCARD_COMPLEXITY_THRESHOLD:
        return "discard_candidate", "complexity_score above discard threshold"
    return "review", ""


def run_expression_oracle(
    contract_path: str | Path,
    space_path: str | Path,
    candidate_path: str | Path,
    project_root: str | Path = ".",
) -> tuple[dict[str, Any], str]:
    started = time.time()
    root = Path(project_root)
    contract = load_expression_contract(_resolve(root, contract_path))
    candidate_name = _candidate_name_from_file(_resolve(root, candidate_path))
    run_id = _make_run_id(candidate_name)
    artifact_dir = _resolve(root, contract.artifact_root) / f"expression_{run_id}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    try:
        space = load_expression_space(_resolve(root, space_path))
        candidate = load_expression_candidate(_resolve(root, candidate_path), space)
        candidate_name = candidate.name
        factor = candidate.to_factor_def()
        project_config = load_project_config(_resolve(root, contract.provider_config))
        project_config = replace(project_config, start_time=contract.start_time, end_time=contract.end_time)
        initialized = False
        raw_eval = pd.DataFrame()
        neutralized_eval = pd.DataFrame()
        purification_steps = contract.purification_steps
        purification_mad_n = contract.purification_mad_n
        if contract.write_raw:
            raw_eval = evaluate_factor(
                project_config,
                factor,
                EvalConfig(
                    horizons=contract.horizons,
                    neutralize_size=False,
                    purification_steps=purification_steps,
                    purification_mad_n=purification_mad_n,
                ),
                initialize=True,
            )
            initialized = True
            write_eval_report(raw_eval, artifact_dir / "raw_eval.csv")
        if contract.neutralize_size_proxy:
            neutralized_eval = evaluate_factor(
                project_config,
                factor,
                EvalConfig(
                    horizons=contract.horizons,
                    neutralize_size=True,
                    purification_steps=purification_steps,
                    purification_mad_n=purification_mad_n,
                ),
                initialize=not initialized,
            )
            write_eval_report(neutralized_eval, artifact_dir / "neutralized_eval.csv")
        candidate_copy = artifact_dir / "candidate.yaml"
        candidate_copy.write_text(_resolve(root, candidate_path).read_text(encoding="utf-8"), encoding="utf-8")

        payload = build_expression_summary_payload(
            run_id=run_id,
            candidate_name=candidate.name,
            commit=_git_commit(root),
            contract_name=contract.name,
            universe=contract.universe,
            horizons=contract.horizons,
            raw_eval=raw_eval,
            neutralized_eval=neutralized_eval,
            complexity_score=compute_complexity_score(candidate.expression),
            artifact_dir=artifact_dir,
        )
        payload["purification"] = "+".join(contract.purification_steps)
        status, reason = determine_expression_status(payload, contract)
        payload["status"] = status
        payload["decision_reason"] = reason
    except Exception as exc:
        payload = _build_crash_payload(
            run_id=run_id,
            candidate_name=candidate_name,
            commit=_git_commit(root),
            contract=contract,
            candidate_path=candidate_path,
            candidate_hash=_safe_sha256(_resolve(root, candidate_path)),
            artifact_dir=artifact_dir,
            started=started,
            exc=exc,
        )
    payload["elapsed_sec"] = round(time.time() - started, 3)
    payload["timestamp"] = datetime.now().isoformat(timespec="seconds")
    payload["candidate_file"] = str(candidate_path)
    payload["candidate_hash"] = payload.get("candidate_hash") or _safe_sha256(_resolve(root, candidate_path))
    block = render_summary_block(payload)
    (artifact_dir / "summary.txt").write_text(block, encoding="utf-8")
    (artifact_dir / "summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    append_expression_ledger_row(_resolve(root, contract.ledger_path), _ledger_row(payload, contract, candidate_name))
    return payload, block


def _row_for_horizon(frame: pd.DataFrame, horizon: int) -> pd.Series:
    matched = frame[frame["horizon"] == horizon]
    if matched.empty:
        raise ValueError(f"evaluation frame is missing horizon: {horizon}")
    return matched.iloc[0]


def _metric_for_horizon(frame: pd.DataFrame, horizon: int, column: str) -> float:
    if frame.empty or "horizon" not in frame.columns or column not in frame.columns:
        return float("nan")
    matched = frame[frame["horizon"] == horizon]
    if matched.empty:
        return float("nan")
    return _float_value(matched.iloc[0], column)


def _float_value(row: pd.Series, column: str) -> float:
    value = float(row[column])
    return value if math.isfinite(value) else float("nan")


def _first_finite(*values: float) -> float:
    for value in values:
        if math.isfinite(value):
            return value
    return float("nan")


def _format_summary_value(value: Any) -> str:
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        return f"{value:.6g}"
    return str(value)


def _make_run_id(candidate_name: str) -> str:
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    return f"{stamp}_{candidate_name}"


def _git_commit(root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
    return result.stdout.strip()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _safe_sha256(path: Path) -> str:
    try:
        return _sha256_file(path)
    except OSError:
        return ""


def _candidate_name_from_file(path: Path) -> str:
    try:
        name = load_yaml(path).get("name", path.stem)
    except Exception:
        name = path.stem
    return str(name)


def _resolve(root: Path, path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else root / value


def _build_crash_payload(
    run_id: str,
    candidate_name: str,
    commit: str,
    contract: ExpressionContract,
    candidate_path: str | Path,
    candidate_hash: str,
    artifact_dir: Path,
    started: float,
    exc: Exception,
) -> dict[str, Any]:
    focus_horizon = 20 if 20 in contract.horizons else max(contract.horizons)
    payload: dict[str, Any] = {
        "loop": "expression",
        "run_id": run_id,
        "candidate": candidate_name,
        "commit": commit,
        "contract": contract.name,
        "universe": contract.universe,
        "horizons": ",".join(str(horizon) for horizon in contract.horizons),
        "complexity_score": float("nan"),
        "purification": "+".join(contract.purification_steps),
        "status": "crash",
        "decision_reason": str(exc),
        "artifact_dir": str(artifact_dir),
        "primary_metric": float("nan"),
        "secondary_metric": float("nan"),
        "guard_metric": float("nan"),
        f"long_short_mean_return_h{focus_horizon}": float("nan"),
        f"top_quantile_turnover_h{focus_horizon}": float("nan"),
        f"observations_h{focus_horizon}": 0,
        "candidate_file": str(candidate_path),
        "candidate_hash": candidate_hash,
        "elapsed_sec": round(time.time() - started, 3),
    }
    for horizon in contract.horizons:
        payload[f"rank_ic_mean_h{horizon}"] = float("nan")
        payload[f"neutral_rank_ic_mean_h{horizon}"] = float("nan")
    return payload


def _ledger_row(payload: dict[str, Any], contract: ExpressionContract, candidate: str) -> dict[str, Any]:
    row = dict(payload)
    row["candidate_name"] = candidate
    row["contract"] = contract.name
    return row
