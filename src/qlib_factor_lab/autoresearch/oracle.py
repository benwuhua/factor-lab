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
from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_eval import EvalConfig, evaluate_factor, write_eval_report


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
        "status": status,
        "artifact_dir": str(artifact_dir),
    }
    for horizon in horizons:
        raw_row = _row_for_horizon(raw_eval, horizon)
        neutral_row = _row_for_horizon(neutralized_eval, horizon)
        payload[f"rank_ic_mean_h{horizon}"] = _float_value(raw_row, "rank_ic_mean")
        payload[f"neutral_rank_ic_mean_h{horizon}"] = _float_value(neutral_row, "rank_ic_mean")
    focus_raw = _row_for_horizon(raw_eval, focus_horizon)
    focus_neutral = _row_for_horizon(neutralized_eval, focus_horizon)
    payload[f"long_short_mean_return_h{focus_horizon}"] = _float_value(focus_raw, "long_short_mean_return")
    payload[f"top_quantile_turnover_h{focus_horizon}"] = _float_value(focus_raw, "top_quantile_turnover")
    payload[f"observations_h{focus_horizon}"] = int(_float_value(focus_raw, "observations"))
    payload["primary_metric"] = _float_value(focus_neutral, "rank_ic_mean")
    payload["secondary_metric"] = _float_value(focus_raw, "rank_ic_mean")
    payload["guard_metric"] = _float_value(focus_raw, "top_quantile_turnover")
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


def run_expression_oracle(
    contract_path: str | Path,
    space_path: str | Path,
    candidate_path: str | Path,
    project_root: str | Path = ".",
) -> tuple[dict[str, Any], str]:
    started = time.time()
    root = Path(project_root)
    contract = load_expression_contract(_resolve(root, contract_path))
    space = load_expression_space(_resolve(root, space_path))
    candidate = load_expression_candidate(_resolve(root, candidate_path), space)
    run_id = _make_run_id(candidate)
    artifact_dir = _resolve(root, contract.artifact_root) / f"expression_{run_id}"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    factor = candidate.to_factor_def()
    project_config = load_project_config(_resolve(root, contract.provider_config))
    project_config = replace(project_config, start_time=contract.start_time, end_time=contract.end_time)
    raw_eval = evaluate_factor(
        project_config,
        factor,
        EvalConfig(horizons=contract.horizons, neutralize_size=False),
        initialize=True,
    )
    neutralized_eval = evaluate_factor(
        project_config,
        factor,
        EvalConfig(horizons=contract.horizons, neutralize_size=True),
        initialize=False,
    )
    write_eval_report(raw_eval, artifact_dir / "raw_eval.csv")
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
    payload["elapsed_sec"] = round(time.time() - started, 3)
    payload["timestamp"] = datetime.now().isoformat(timespec="seconds")
    payload["candidate_file"] = str(candidate_path)
    payload["candidate_hash"] = _sha256_file(_resolve(root, candidate_path))
    payload["decision_reason"] = ""
    block = render_summary_block(payload)
    (artifact_dir / "summary.txt").write_text(block, encoding="utf-8")
    (artifact_dir / "summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    append_expression_ledger_row(_resolve(root, contract.ledger_path), _ledger_row(payload, contract, candidate))
    return payload, block


def _row_for_horizon(frame: pd.DataFrame, horizon: int) -> pd.Series:
    matched = frame[frame["horizon"] == horizon]
    if matched.empty:
        raise ValueError(f"evaluation frame is missing horizon: {horizon}")
    return matched.iloc[0]


def _float_value(row: pd.Series, column: str) -> float:
    value = float(row[column])
    return value if math.isfinite(value) else float("nan")


def _format_summary_value(value: Any) -> str:
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        return f"{value:.6g}"
    return str(value)


def _make_run_id(candidate: ExpressionCandidate) -> str:
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    return f"{stamp}_{candidate.name}"


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


def _resolve(root: Path, path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else root / value


def _ledger_row(payload: dict[str, Any], contract: ExpressionContract, candidate: ExpressionCandidate) -> dict[str, Any]:
    row = dict(payload)
    row["candidate_name"] = candidate.name
    row["contract"] = contract.name
    return row
