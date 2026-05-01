from __future__ import annotations

import json
import math
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from qlib_factor_lab.config import ProjectConfig, load_project_config
from qlib_factor_lab.factor_eval import write_eval_report
from qlib_factor_lab.qlib_bootstrap import init_qlib


DEFAULT_EMOTION_DATA_FACTOR_SPECS: tuple[dict[str, Any], ...] = (
    {
        "name": "instrument_emotion_score",
        "source_column": "instrument_emotion_score",
        "direction": 1,
        "category": "candidate_emotion_data",
        "description": "Cross-sectional per-stock atmosphere score built from the persisted emotion_atmosphere data domain.",
    },
    {
        "name": "crowding_cooling_score",
        "source_column": "crowding_cooling_score",
        "direction": 1,
        "category": "candidate_emotion_data",
        "description": "Inverse of instrument emotion heat; higher values represent cooling after crowding.",
    },
    {
        "name": "emotion_pct_change",
        "source_column": "pct_change",
        "direction": 1,
        "category": "candidate_emotion_data",
        "description": "Current-day stock return as captured in the emotion_atmosphere data domain.",
    },
)


def load_emotion_data_factor_specs(names: set[str] | None = None) -> list[dict[str, Any]]:
    specs = [dict(spec) for spec in DEFAULT_EMOTION_DATA_FACTOR_SPECS]
    if names is None:
        return specs
    return [spec for spec in specs if str(spec["name"]) in names]


def run_emotion_data_lane_oracle(
    *,
    lane_name: str,
    factor_specs: list[dict[str, Any]],
    provider_config: str | Path,
    project_root: str | Path = ".",
    data_path: str | Path = "data/emotion_atmosphere.csv",
    artifact_root: str | Path = "reports/autoresearch/runs",
    horizons: tuple[int, ...] = (5, 20),
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
    emotion = pd.read_csv(_resolve(root, data_path)) if _resolve(root, data_path).exists() else pd.DataFrame()
    close = fetch_close_frame(project_config)
    summary = evaluate_emotion_data_factors(emotion, close, factor_specs, horizons=horizons)
    write_eval_report(summary, artifact_dir / "factor_summaries.csv")
    payload = _build_payload(
        lane_name=lane_name,
        run_id=run_id,
        factor_specs=factor_specs,
        summary=summary,
        artifact_dir=artifact_dir,
        elapsed_sec=round(time.time() - started, 3),
    )
    block = _render_summary_block(payload)
    (artifact_dir / "summary.txt").write_text(block, encoding="utf-8")
    (artifact_dir / "summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload, block


def fetch_close_frame(project_config: ProjectConfig) -> pd.DataFrame:
    init_qlib(project_config)
    from qlib.data import D

    frame = D.features(
        D.instruments(project_config.market),
        ["$close"],
        start_time=project_config.start_time,
        end_time=project_config.end_time,
        freq=project_config.freq,
    )
    frame.columns = ["close"]
    frame = frame.reset_index()
    if "datetime" in frame.columns:
        frame = frame.rename(columns={"datetime": "trade_date"})
    elif "date" in frame.columns:
        frame = frame.rename(columns={"date": "trade_date"})
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return frame.loc[:, ["trade_date", "instrument", "close"]]


def evaluate_emotion_data_factors(
    emotion: pd.DataFrame,
    close: pd.DataFrame,
    factor_specs: list[dict[str, Any]],
    *,
    horizons: tuple[int, ...] = (5, 20),
    quantiles: int = 5,
) -> pd.DataFrame:
    base = _prepare_base_frame(emotion, close)
    rows: list[dict[str, Any]] = []
    for spec in factor_specs:
        source_column = str(spec.get("source_column") or spec.get("name") or "")
        factor_name = str(spec.get("name") or source_column)
        direction = int(spec.get("direction", 1))
        category = str(spec.get("category", "candidate_emotion_data"))
        if base.empty or source_column not in base.columns:
            rows.extend(_empty_rows(factor_name, category, direction, horizons))
            continue
        factor_base = base.copy()
        factor_base["signal"] = pd.to_numeric(factor_base[source_column], errors="coerce") * direction
        for horizon in horizons:
            scored = factor_base.copy()
            scored["future_ret"] = scored.groupby("instrument")["close"].shift(-int(horizon)) / scored["close"] - 1.0
            scored = scored.dropna(subset=["signal", "future_ret"])
            daily_pairs = scored.groupby("trade_date")[["signal", "future_ret"]]
            daily_ic = daily_pairs.apply(lambda x: x["signal"].corr(x["future_ret"], method="pearson"))
            daily_rank_ic = daily_pairs.apply(lambda x: x["signal"].corr(x["future_ret"], method="spearman"))
            rows.append(
                {
                    "factor": factor_name,
                    "category": category,
                    "direction": direction,
                    "horizon": int(horizon),
                    "ic_mean": _finite_mean(daily_ic),
                    "rank_ic_mean": _finite_mean(daily_rank_ic),
                    "top_quantile_turnover": _estimate_top_quantile_turnover(scored, "signal", quantiles),
                    "observations": int(len(scored)),
                    "primary_metric": _finite_mean(daily_rank_ic),
                }
            )
    return pd.DataFrame(rows)


def _prepare_base_frame(emotion: pd.DataFrame, close: pd.DataFrame) -> pd.DataFrame:
    if emotion is None or emotion.empty or close is None or close.empty:
        return pd.DataFrame()
    left = emotion.copy()
    right = close.copy()
    for frame in [left, right]:
        if "trade_date" not in frame.columns and "date" in frame.columns:
            frame["trade_date"] = frame["date"]
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        frame["instrument"] = frame["instrument"].astype(str)
    right["close"] = pd.to_numeric(right["close"], errors="coerce")
    merged = left.merge(right.loc[:, ["trade_date", "instrument", "close"]], on=["trade_date", "instrument"], how="inner")
    return merged.sort_values(["instrument", "trade_date"]).reset_index(drop=True)


def _empty_rows(factor_name: str, category: str, direction: int, horizons: tuple[int, ...]) -> list[dict[str, Any]]:
    return [
        {
            "factor": factor_name,
            "category": category,
            "direction": direction,
            "horizon": int(horizon),
            "ic_mean": float("nan"),
            "rank_ic_mean": float("nan"),
            "top_quantile_turnover": float("nan"),
            "observations": 0,
            "primary_metric": float("nan"),
        }
        for horizon in horizons
    ]


def _build_payload(
    *,
    lane_name: str,
    run_id: str,
    factor_specs: list[dict[str, Any]],
    summary: pd.DataFrame,
    artifact_dir: Path,
    elapsed_sec: float,
) -> dict[str, Any]:
    best = _best_row(summary)
    candidate = str(best.get("factor", "")) if best is not None else ""
    primary = _float(best.get("primary_metric", float("nan"))) if best is not None else float("nan")
    observations = int(best.get("observations", 0)) if best is not None else 0
    status = "review" if observations > 0 and math.isfinite(primary) else "discard_candidate"
    return {
        "loop": lane_name,
        "run_id": run_id,
        "candidate": candidate,
        "factor_count": len(factor_specs),
        "status": status,
        "decision_reason": "" if status == "review" else "no usable emotion data-domain signal",
        "primary_metric": primary,
        "observations": observations,
        "artifact_dir": str(artifact_dir),
        "elapsed_sec": elapsed_sec,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def _best_row(summary: pd.DataFrame) -> pd.Series | None:
    if summary.empty or "primary_metric" not in summary.columns:
        return None
    metrics = pd.to_numeric(summary["primary_metric"], errors="coerce")
    if metrics.dropna().empty:
        return None
    return summary.loc[metrics.idxmax()]


def _estimate_top_quantile_turnover(frame: pd.DataFrame, signal_col: str, quantiles: int) -> float:
    memberships: list[set[str]] = []
    for _, daily in frame.groupby("trade_date"):
        ranks = daily[signal_col].rank(method="first", pct=True)
        top = daily.loc[ranks >= 1 - 1 / quantiles, "instrument"]
        memberships.append(set(top.astype(str)))
    if len(memberships) < 2:
        return float("nan")
    changes = [1 - len(prev & cur) / len(prev) for prev, cur in zip(memberships, memberships[1:]) if prev]
    return float(pd.Series(changes).mean()) if changes else float("nan")


def _finite_mean(values: Any) -> float:
    if isinstance(values, pd.DataFrame):
        raw = values.to_numpy().ravel()
    elif isinstance(values, pd.Series):
        raw = values.to_numpy()
    elif isinstance(values, (list, tuple)):
        raw = values
    else:
        raw = [values]
    numeric = pd.to_numeric(pd.Series(raw), errors="coerce").dropna()
    if numeric.empty:
        return float("nan")
    result = float(numeric.mean())
    return result if math.isfinite(result) else float("nan")


def _float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return result if math.isfinite(result) else float("nan")


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
