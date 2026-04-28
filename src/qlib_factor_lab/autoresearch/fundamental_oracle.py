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
from qlib_factor_lab.config import load_project_config, load_yaml
from qlib_factor_lab.factor_eval import compute_quantile_return_summary, write_eval_report
from qlib_factor_lab.neutralization import add_size_proxy, neutralize_signal
from qlib_factor_lab.qlib_bootstrap import init_qlib


DEFAULT_FUNDAMENTAL_FACTOR_SPECS: tuple[dict[str, Any], ...] = (
    {
        "name": "roe",
        "direction": 1,
        "category": "candidate_quality",
        "description": "Point-in-time return on equity; higher quality is ranked higher.",
    },
    {
        "name": "gross_margin",
        "direction": 1,
        "category": "candidate_quality",
        "description": "Point-in-time gross margin; higher margin is ranked higher.",
    },
    {
        "name": "debt_ratio",
        "direction": -1,
        "category": "candidate_quality",
        "description": "Point-in-time debt ratio; lower leverage is ranked higher.",
    },
    {
        "name": "revenue_growth_yoy",
        "direction": 1,
        "category": "candidate_growth",
        "description": "Point-in-time revenue growth year over year.",
    },
    {
        "name": "net_profit_growth_yoy",
        "direction": 1,
        "category": "candidate_growth",
        "description": "Point-in-time net profit growth year over year.",
    },
    {
        "name": "operating_cashflow_to_net_profit",
        "direction": 1,
        "category": "candidate_cashflow",
        "description": "Point-in-time operating cashflow to net profit quality proxy.",
    },
)


def run_fundamental_lane_oracle(
    *,
    lane_name: str,
    contract_path: str | Path,
    project_root: str | Path = ".",
    fundamental_path: str | Path = "data/fundamental_quality.csv",
    space_path: str | Path = "configs/autoresearch/fundamental_space.yaml",
    security_master_path: str | Path = "data/security_master.csv",
    close_frame: pd.DataFrame | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> tuple[dict[str, Any], str]:
    started = time.time()
    root = Path(project_root)
    contract = load_expression_contract(_resolve(root, contract_path))
    run_id = _make_run_id(lane_name)
    artifact_dir = _resolve(root, contract.artifact_root) / f"{lane_name}_{run_id}"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    fundamentals = _load_fundamentals(_resolve(root, fundamental_path))
    factor_specs = load_fundamental_factor_specs(_resolve(root, space_path))
    industry_map = _load_security_master_industry_map(_resolve(root, security_master_path))
    if close_frame is None:
        project_config = load_project_config(_resolve(root, contract.provider_config))
        project_config = replace(
            project_config,
            start_time=start_time or contract.start_time,
            end_time=end_time or contract.end_time,
        )
        close_frame = _fetch_close_frame(project_config)

    rows: list[dict[str, Any]] = []
    for spec in factor_specs:
        name = str(spec["name"])
        if "components" in spec:
            factor_frame = build_fundamental_combo_frame(fundamentals, close_frame, spec)
        elif name in fundamentals.columns:
            factor_frame = build_fundamental_factor_frame(fundamentals, close_frame, name)
        else:
            continue
        factor_dir = artifact_dir / name
        factor_dir.mkdir(parents=True, exist_ok=True)
        write_eval_report(factor_frame.reset_index(), factor_dir / "factor_frame.csv")
        raw_eval = evaluate_fundamental_factor_frame(
            factor_frame,
            factor_name=name,
            direction=int(spec.get("direction", 1)),
            horizons=contract.horizons,
        )
        eval_frames = [raw_eval]
        write_eval_report(raw_eval, factor_dir / "raw_eval.csv")
        if contract.neutralize_size_proxy:
            neutralized_eval = evaluate_fundamental_factor_frame(
                factor_frame,
                factor_name=name,
                direction=int(spec.get("direction", 1)),
                horizons=contract.horizons,
                neutralize_size=True,
                industry_map=industry_map,
            )
            eval_frames.append(neutralized_eval)
            write_eval_report(neutralized_eval, factor_dir / "neutralized_eval.csv")
        eval_frame = pd.concat(eval_frames, ignore_index=True) if eval_frames else pd.DataFrame(columns=_eval_columns())
        eval_frame["category"] = str(spec.get("category", lane_name))
        eval_frame["description"] = str(spec.get("description", ""))
        write_eval_report(eval_frame, factor_dir / "eval.csv")
        rows.extend(eval_frame.to_dict(orient="records"))

    factors_frame = pd.DataFrame(rows)
    write_eval_report(factors_frame, artifact_dir / "factor_summaries.csv")
    payload = _lane_payload(
        lane_name=lane_name,
        run_id=run_id,
        factors_frame=factors_frame,
        artifact_dir=artifact_dir,
        elapsed_sec=round(time.time() - started, 3),
        minimum_observations=contract.minimum_observations,
        no_data=fundamentals.empty,
        factor_count=len(factor_specs),
    )
    block = _render_summary_block(payload)
    (artifact_dir / "summary.txt").write_text(block, encoding="utf-8")
    (artifact_dir / "summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload, block


def load_fundamental_factor_specs(path: str | Path) -> list[dict[str, Any]]:
    config_path = Path(path)
    if not config_path.exists():
        return [dict(spec) for spec in DEFAULT_FUNDAMENTAL_FACTOR_SPECS]
    raw = load_yaml(config_path)
    candidates = raw.get("candidate_factors")
    if not candidates:
        return [dict(spec) for spec in DEFAULT_FUNDAMENTAL_FACTOR_SPECS]
    specs: list[dict[str, Any]] = []
    for item in candidates:
        if not bool(item.get("active", True)):
            continue
        name = str(item["name"])
        base = next((dict(spec) for spec in DEFAULT_FUNDAMENTAL_FACTOR_SPECS if spec["name"] == name), {})
        base.update(item)
        base.pop("active", None)
        specs.append(base)
    return specs


def build_fundamental_factor_frame(fundamentals: pd.DataFrame, close_frame: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    if fundamentals.empty or close_frame.empty or factor_name not in fundamentals.columns:
        return _empty_factor_frame(factor_name)

    close = _normalize_close_frame(close_frame)
    data = fundamentals.copy()
    if "available_at" not in data.columns:
        data["available_at"] = data.get("announce_date", "")
    data["available_at"] = pd.to_datetime(data["available_at"], errors="coerce")
    data[factor_name] = pd.to_numeric(data[factor_name], errors="coerce")
    data = data.dropna(subset=["instrument", "available_at", factor_name])
    if data.empty:
        return _empty_factor_frame(factor_name)

    pieces: list[pd.DataFrame] = []
    for instrument, daily in close.groupby("instrument"):
        facts = data[data["instrument"].astype(str) == str(instrument)].sort_values("available_at")
        if facts.empty:
            continue
        merged = pd.merge_asof(
            daily.sort_values("datetime"),
            facts[["available_at", factor_name]].sort_values("available_at"),
            left_on="datetime",
            right_on="available_at",
            direction="backward",
        )
        merged = merged.dropna(subset=[factor_name])
        if not merged.empty:
            columns = ["datetime", "instrument", "close", factor_name]
            if "volume" in merged.columns:
                columns.append("volume")
            pieces.append(merged[columns])
    if not pieces:
        return _empty_factor_frame(factor_name)
    return pd.concat(pieces, ignore_index=True).set_index(["datetime", "instrument"]).sort_index()


def build_fundamental_combo_frame(fundamentals: pd.DataFrame, close_frame: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    name = str(spec.get("name", "") or "fundamental_combo")
    components = [item for item in spec.get("components", []) if str(item.get("field", "")).strip()]
    if not components:
        return _empty_factor_frame(name)

    combined: pd.DataFrame | None = None
    for index, component in enumerate(components):
        field = str(component["field"])
        frame = build_fundamental_factor_frame(fundamentals, close_frame, field)
        if frame.empty:
            return _empty_factor_frame(name)
        signal = pd.to_numeric(frame[field], errors="coerce") * int(component.get("direction", 1))
        zscore = signal.groupby(level="datetime").transform(_cross_sectional_zscore)
        column = f"__component_{index}"
        component_frame = zscore.rename(column).to_frame()
        if combined is None:
            base_columns = ["close"]
            if "volume" in frame.columns:
                base_columns.append("volume")
            combined = frame[base_columns].join(component_frame, how="inner")
        else:
            combined = combined.join(component_frame, how="inner")

    if combined is None or combined.empty:
        return _empty_factor_frame(name)
    total = pd.Series(0.0, index=combined.index)
    for index, component in enumerate(components):
        total = total.add(combined[f"__component_{index}"] * float(component.get("weight", 1.0)), fill_value=0.0)
    output_columns = ["close"]
    if "volume" in combined.columns:
        output_columns.append("volume")
    result = combined[output_columns].copy()
    result[name] = total
    return result.dropna(subset=[name]).sort_index()


def evaluate_fundamental_factor_frame(
    frame: pd.DataFrame,
    *,
    factor_name: str,
    direction: int,
    horizons: tuple[int, ...],
    quantiles: int = 5,
    neutralize_size: bool = False,
    industry_map: pd.Series | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if frame.empty:
        return pd.DataFrame(columns=_eval_columns())
    for horizon in horizons:
        scored = frame.copy()
        scored["signal"] = pd.to_numeric(scored[factor_name], errors="coerce") * int(direction)
        signal_col = "signal"
        exposure_cols: list[str] = []
        group_col = None
        neutralization_parts: list[str] = []
        if neutralize_size:
            scored = _add_size_proxy(scored)
            exposure_cols.append("size_proxy")
            neutralization_parts.append("size_proxy")
        if industry_map is not None and not industry_map.empty:
            scored = _attach_industry(scored, industry_map)
            group_col = "industry"
            neutralization_parts.append("industry")
        if exposure_cols or group_col is not None:
            scored = neutralize_signal(scored, exposure_cols=exposure_cols, group_col=group_col)
            signal_col = "signal_neutral"
        close = scored["close"].groupby(level="instrument")
        scored["future_ret"] = close.shift(-int(horizon)) / scored["close"] - 1.0
        scored = scored.dropna(subset=[signal_col, "future_ret"])
        daily_ic = scored.groupby(level="datetime").apply(lambda x: x[signal_col].corr(x["future_ret"], method="pearson"))
        daily_rank_ic = scored.groupby(level="datetime").apply(lambda x: x[signal_col].corr(x["future_ret"], method="spearman"))
        row = {
            "factor": factor_name,
            "direction": int(direction),
            "horizon": int(horizon),
            "neutralization": "+".join(neutralization_parts) if neutralization_parts else "none",
            "ic_mean": _series_mean(daily_ic),
            "ic_std": _series_std(daily_ic),
            "icir": _safe_div(_series_mean(daily_ic), _series_std(daily_ic)),
            "rank_ic_mean": _series_mean(daily_rank_ic),
            "rank_ic_std": _series_std(daily_rank_ic),
            "rank_icir": _safe_div(_series_mean(daily_rank_ic), _series_std(daily_rank_ic)),
            "top_quantile_turnover": _estimate_top_quantile_turnover(scored, signal_col, quantiles),
            "observations": int(len(scored)),
        }
        row.update(compute_quantile_return_summary(scored, signal_col, "future_ret", quantiles))
        rows.append(row)
    return pd.DataFrame(rows, columns=_eval_columns() + [f"q{i}_mean_return" for i in range(1, quantiles + 1)] + ["long_short_mean_return"])


def _lane_payload(
    *,
    lane_name: str,
    run_id: str,
    factors_frame: pd.DataFrame,
    artifact_dir: Path,
    elapsed_sec: float,
    minimum_observations: int,
    no_data: bool,
    factor_count: int = len(DEFAULT_FUNDAMENTAL_FACTOR_SPECS),
) -> dict[str, Any]:
    if no_data:
        status = "discard_candidate"
        reason = "no fundamental rows"
        best = None
    else:
        best = _best_factor_row(factors_frame)
        observations = int(best.get("observations", 0)) if best is not None else 0
        status = "review" if best is not None and observations >= minimum_observations else "discard_candidate"
        reason = "" if status == "review" else f"observations_below_{minimum_observations}"
    return {
        "loop": lane_name,
        "run_id": run_id,
        "candidate": str(best.get("factor", "")) if best is not None else "",
        "factor_count": factor_count,
        "status": status,
        "decision_reason": reason,
        "primary_metric": _float(best.get("rank_ic_mean", float("nan"))) if best is not None else float("nan"),
        "artifact_dir": str(artifact_dir),
        "elapsed_sec": elapsed_sec,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }


def _render_summary_block(payload: dict[str, Any]) -> str:
    ordered = [
        "loop",
        "run_id",
        "candidate",
        "factor_count",
        "status",
        "decision_reason",
        "primary_metric",
        "artifact_dir",
        "elapsed_sec",
        "timestamp",
    ]
    lines = ["---"]
    lines.extend(f"{key}: {payload.get(key, '')}" for key in ordered)
    lines.append("---")
    return "\n".join(lines) + "\n"


def _fetch_close_frame(project_config: Any) -> pd.DataFrame:
    from qlib.data import D

    init_qlib(project_config)
    fields = ["$close", "$volume"]
    frame = D.features(
        D.instruments(project_config.market),
        fields,
        start_time=project_config.start_time,
        end_time=project_config.end_time,
        freq=project_config.freq,
    )
    frame.columns = ["close", "volume"]
    return frame.dropna(subset=["close"])


def _load_fundamentals(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _normalize_close_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame.index, pd.MultiIndex):
        raise ValueError("close_frame must use a datetime/instrument MultiIndex")
    reset = frame.reset_index()
    rename = {}
    if "datetime" not in reset.columns:
        rename[reset.columns[0]] = "datetime"
    if "instrument" not in reset.columns:
        rename[reset.columns[1]] = "instrument"
    reset = reset.rename(columns=rename)
    reset["datetime"] = pd.to_datetime(reset["datetime"], errors="coerce")
    reset["instrument"] = reset["instrument"].astype(str)
    reset["close"] = pd.to_numeric(reset["close"], errors="coerce")
    if "volume" in reset.columns:
        reset["volume"] = pd.to_numeric(reset["volume"], errors="coerce")
    return reset.dropna(subset=["datetime", "instrument", "close"]).sort_values(["instrument", "datetime"])


def _empty_factor_frame(factor_name: str) -> pd.DataFrame:
    return pd.DataFrame(columns=["datetime", "instrument", "close", factor_name]).set_index(["datetime", "instrument"])


def _add_size_proxy(frame: pd.DataFrame) -> pd.DataFrame:
    if "volume" in frame.columns:
        return add_size_proxy(frame)
    result = frame.copy()
    result["size_proxy"] = (pd.to_numeric(result["close"], errors="coerce").abs() + 1.0).map(math.log)
    return result


def _attach_industry(frame: pd.DataFrame, industry_map: pd.Series) -> pd.DataFrame:
    result = frame.copy()
    instruments = result.index.get_level_values("instrument")
    result["industry"] = instruments.map(industry_map)
    return result


def _load_security_master_industry_map(path: Path) -> pd.Series:
    if not path.exists():
        return pd.Series(dtype=str)
    data = pd.read_csv(path)
    if "instrument" not in data.columns:
        return pd.Series(dtype=str)
    for column in ("industry_sw", "industry", "industry_csrc"):
        if column in data.columns:
            frame = data[["instrument", column]].copy()
            frame[column] = frame[column].fillna("").astype(str).str.strip()
            frame = frame[frame[column] != ""]
            if not frame.empty:
                return frame.drop_duplicates("instrument", keep="last").set_index("instrument")[column]
    return pd.Series(dtype=str)


def _best_factor_row(frame: pd.DataFrame) -> pd.Series | None:
    if frame.empty or "rank_ic_mean" not in frame.columns:
        return None
    metrics = pd.to_numeric(frame["rank_ic_mean"], errors="coerce")
    if metrics.dropna().empty:
        return None
    return frame.loc[metrics.idxmax()]


def _estimate_top_quantile_turnover(frame: pd.DataFrame, signal_col: str, quantiles: int) -> float:
    memberships: list[set[str]] = []
    for _, daily in frame.groupby(level="datetime"):
        ranks = daily[signal_col].rank(method="first", pct=True)
        top = daily.index.get_level_values("instrument")[ranks >= 1 - 1 / quantiles]
        memberships.append(set(top))
    if len(memberships) < 2:
        return float("nan")
    changes = [1 - len(prev & cur) / len(prev) for prev, cur in zip(memberships, memberships[1:]) if prev]
    return float(pd.Series(changes).mean()) if changes else float("nan")


def _cross_sectional_zscore(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    mean = values.mean()
    std = values.std(ddof=0)
    if not math.isfinite(float(std)) or float(std) == 0:
        return pd.Series(float("nan"), index=series.index)
    return (values - mean) / std


def _series_mean(series: pd.Series) -> float:
    value = series.dropna().mean()
    return _float(value)


def _series_std(series: pd.Series) -> float:
    value = series.dropna().std()
    return _float(value)


def _safe_div(numerator: float, denominator: float) -> float:
    if not math.isfinite(numerator) or not math.isfinite(denominator) or denominator == 0:
        return float("nan")
    return float(numerator / denominator)


def _float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return result if math.isfinite(result) else float("nan")


def _eval_columns() -> list[str]:
    return [
        "factor",
        "direction",
        "horizon",
        "neutralization",
        "ic_mean",
        "ic_std",
        "icir",
        "rank_ic_mean",
        "rank_ic_std",
        "rank_icir",
        "top_quantile_turnover",
        "observations",
    ]


def _make_run_id(lane_name: str) -> str:
    return f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{lane_name}"


def _resolve(root: Path, path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else root / value
