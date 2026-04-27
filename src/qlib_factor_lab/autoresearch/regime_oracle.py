from __future__ import annotations

import json
import math
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qlib_factor_lab.autoresearch.oracle import render_summary_block
from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_eval import write_eval_report
from qlib_factor_lab.market_regime import compute_equal_weight_market_regime
from qlib_factor_lab.qlib_bootstrap import init_qlib


def run_regime_lane_oracle(
    *,
    lane_name: str = "regime",
    provider_config: str | Path = "configs/provider_current.yaml",
    project_root: str | Path = ".",
    artifact_root: str | Path = "reports/autoresearch/runs",
    start_time: str | None = None,
    end_time: str | None = None,
    fast_window: int = 20,
    slow_window: int = 60,
    trend_window: int = 20,
    trend_threshold: float = 0.02,
) -> tuple[dict[str, Any], str]:
    started = time.time()
    root = Path(project_root)
    run_id = _make_run_id(lane_name)
    artifact_dir = _resolve(root, artifact_root) / f"{lane_name}_{run_id}"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    project_config = load_project_config(_resolve(root, provider_config))
    project_config = replace(
        project_config,
        start_time=start_time or project_config.start_time,
        end_time=end_time or project_config.end_time,
    )
    init_qlib(project_config)
    market_frame = fetch_market_regime_frame(project_config)

    if market_frame.empty:
        regime = pd.DataFrame(columns=["market_ret", "market_proxy", "fast_ma", "slow_ma", "trend_return", "market_regime"])
        regime.index.name = "datetime"
    else:
        regime = compute_equal_weight_market_regime(
            market_frame,
            fast_window=fast_window,
            slow_window=slow_window,
            trend_window=trend_window,
            trend_threshold=trend_threshold,
        )

    write_eval_report(regime.reset_index(), artifact_dir / "market_regime.csv")
    payload = build_regime_summary_payload(
        lane_name=lane_name,
        run_id=run_id,
        regime=regime,
        artifact_dir=artifact_dir,
        elapsed_sec=round(time.time() - started, 3),
    )
    block = render_summary_block(payload)
    (artifact_dir / "summary.txt").write_text(block, encoding="utf-8")
    (artifact_dir / "summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload, block


def fetch_market_regime_frame(project_config) -> pd.DataFrame:
    from qlib.data import D

    instruments = D.instruments(project_config.market)
    frame = D.features(
        instruments,
        ["$close"],
        start_time=project_config.start_time,
        end_time=project_config.end_time,
        freq=project_config.freq,
    )
    frame.columns = ["close"]
    return frame.dropna(subset=["close"])


def build_regime_summary_payload(
    *,
    lane_name: str,
    run_id: str,
    regime: pd.DataFrame,
    artifact_dir: str | Path,
    elapsed_sec: float,
) -> dict[str, Any]:
    if regime.empty or "market_regime" not in regime.columns:
        active_regime = "unknown"
        counts: dict[str, int] = {}
        switch_count = 0
        status = "discard_candidate"
        decision_reason = "no market regime observations"
    else:
        states = regime["market_regime"].fillna("unknown").astype(str)
        active_regime = str(states.iloc[-1])
        counts = {str(key): int(value) for key, value in states.value_counts().to_dict().items()}
        switch_count = int((states != states.shift(1)).sum() - 1)
        switch_count = max(0, switch_count)
        status = "review"
        decision_reason = "allocator only; inspect regime switch stability"

    return {
        "loop": lane_name,
        "run_id": run_id,
        "candidate": "",
        "active_regime": active_regime,
        "regime_counts": counts,
        "switch_count": switch_count,
        "status": status,
        "decision_reason": decision_reason,
        "primary_metric": _float(switch_count),
        "artifact_dir": str(artifact_dir),
        "elapsed_sec": elapsed_sec,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def _make_run_id(lane_name: str) -> str:
    return f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{lane_name}"


def _resolve(root: Path, path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else root / value


def _float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return result if math.isfinite(result) else float("nan")
