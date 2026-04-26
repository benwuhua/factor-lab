from __future__ import annotations

import json
import math
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.event_backtest import EventBacktestConfig, build_event_trades, summarize_trades
from qlib_factor_lab.factor_eval import write_eval_report
from qlib_factor_lab.factor_registry import FactorDef
from qlib_factor_lab.qlib_bootstrap import init_qlib


def run_event_lane_oracle(
    *,
    lane_name: str,
    factor_specs: list[dict[str, Any]],
    provider_config: str | Path,
    project_root: str | Path = ".",
    artifact_root: str | Path = "reports/autoresearch/runs",
    horizons: tuple[int, ...] = (5, 20),
    buckets: tuple[tuple[float, float], ...] = ((0.85, 0.95), (0.95, 1.0)),
    start_time: str | None = None,
    end_time: str | None = None,
) -> tuple[dict[str, Any], str]:
    started = time.time()
    root = Path(project_root)
    run_id = _make_run_id(lane_name)
    artifact_dir = _resolve(root, artifact_root) / f"{lane_name}_{run_id}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    project_config = load_project_config(_resolve(root, provider_config))
    if start_time or end_time:
        project_config = replace(
            project_config,
            start_time=start_time or project_config.start_time,
            end_time=end_time or project_config.end_time,
        )
    init_qlib(project_config)

    backtest_config = EventBacktestConfig(horizons=horizons, buckets=buckets)
    factors = [_factor_from_spec(spec, lane_name) for spec in factor_specs]
    frame = fetch_event_backtest_frame(project_config, factors)
    all_trades = []
    summaries = []
    for factor in factors:
        factor_frame = frame.dropna(subset=[factor.name, "open", "high", "low", "close", "volume"])
        trades = build_event_trades(factor_frame, factor.name, backtest_config, signal_direction=factor.direction)
        if not trades.empty:
            trades.insert(0, "factor", factor.name)
        summary = summarize_trades(trades, group_cols=("factor", "bucket", "horizon"))
        all_trades.append(trades)
        summaries.append(summary)

    trades_frame = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    summary_frame = pd.concat(summaries, ignore_index=True) if summaries else pd.DataFrame()
    write_eval_report(trades_frame, artifact_dir / "trades.csv")
    write_eval_report(summary_frame, artifact_dir / "summary.csv")

    payload = _build_payload(
        lane_name=lane_name,
        run_id=run_id,
        factor_specs=factor_specs,
        summary=summary_frame,
        horizons=horizons,
        artifact_dir=artifact_dir,
        elapsed_sec=round(time.time() - started, 3),
    )
    block = _render_summary_block(payload)
    (artifact_dir / "summary.txt").write_text(block, encoding="utf-8")
    (artifact_dir / "summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload, block


def fetch_event_backtest_frame(config, factors: FactorDef | list[FactorDef]) -> pd.DataFrame:
    from qlib.data import D

    factor_list = factors if isinstance(factors, list) else [factors]
    factor_expressions = [factor.expression for factor in factor_list]
    fields = factor_expressions + ["$open", "$high", "$low", "$close", "$volume"]
    frame = D.features(
        D.instruments(config.market),
        fields,
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    frame.columns = [factor.name for factor in factor_list] + ["open", "high", "low", "close", "volume"]
    return frame


def _build_payload(
    *,
    lane_name: str,
    run_id: str,
    factor_specs: list[dict[str, Any]],
    summary: pd.DataFrame,
    horizons: tuple[int, ...],
    artifact_dir: Path,
    elapsed_sec: float,
) -> dict[str, Any]:
    focus_horizon = 20 if 20 in horizons else max(horizons)
    focus = _focus_summary(summary, focus_horizon)
    mean_return = _metric(focus, "mean_return")
    trade_count = int(_metric(focus, "trade_count", default=0.0) or 0)
    payload: dict[str, Any] = {
        "loop": lane_name,
        "run_id": run_id,
        "candidate": ",".join(str(spec.get("name", "")) for spec in factor_specs),
        "factor_count": len(factor_specs),
        "status": "review" if trade_count > 0 else "discard_candidate",
        "decision_reason": "" if trade_count > 0 else "no event trades",
        "primary_metric": mean_return,
        "event_mean_return_h%s" % focus_horizon: mean_return,
        "event_trade_count_h%s" % focus_horizon: trade_count,
        "artifact_dir": str(artifact_dir),
        "elapsed_sec": elapsed_sec,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    return payload


def _focus_summary(summary: pd.DataFrame, horizon: int) -> pd.DataFrame:
    if summary.empty or "horizon" not in summary.columns:
        return pd.DataFrame()
    focused = summary[summary["horizon"] == horizon]
    if "bucket" in focused.columns:
        p95 = focused[focused["bucket"] == "p95_p100"]
        if not p95.empty:
            return p95
    return focused


def _metric(frame: pd.DataFrame, column: str, default: float = float("nan")) -> float:
    if frame.empty or column not in frame.columns:
        return default
    value = pd.to_numeric(frame[column], errors="coerce").dropna()
    if value.empty:
        return default
    result = float(value.mean())
    return result if math.isfinite(result) else default


def _factor_from_spec(spec: dict[str, Any], lane_name: str) -> FactorDef:
    return FactorDef(
        name=str(spec["name"]),
        expression=str(spec["expression"]),
        direction=int(spec.get("direction", 1)),
        category=str(spec.get("category", lane_name)),
        description=str(spec.get("description", "")),
    )


def _render_summary_block(payload: dict[str, Any]) -> str:
    lines = ["---"]
    for key, value in payload.items():
        lines.append(f"{key}: {_format_value(value)}")
    lines.append("---")
    return "\n".join(lines) + "\n"


def _format_value(value: Any) -> str:
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        return f"{value:.6g}"
    return str(value)


def _make_run_id(lane_name: str) -> str:
    return f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{lane_name}"


def _resolve(root: Path, path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else root / value
