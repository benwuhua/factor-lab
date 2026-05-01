from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .config import ProjectConfig
from .factor_purification import purify_factor_frame
from .factor_registry import FactorDef
from .neutralization import add_size_proxy, attach_industry, load_industry_map, neutralize_signal
from .qlib_bootstrap import init_qlib


@dataclass(frozen=True)
class EvalConfig:
    quantiles: int = 5
    horizons: tuple[int, ...] = (1, 5, 10, 20)
    neutralize_size: bool = False
    industry_map_path: Path | None = None
    purification_steps: tuple[str, ...] = ()
    purification_mad_n: float = 3.0


def load_instruments(config: ProjectConfig):
    from qlib.data import D

    return D.instruments(config.market)


def fetch_factor_frame(config: ProjectConfig, factor: FactorDef, include_volume: bool = False) -> pd.DataFrame:
    from qlib.data import D

    instruments = load_instruments(config)
    fields = [factor.expression, "$close"]
    names = [factor.name, "close"]
    if include_volume:
        fields.append("$volume")
        names.append("volume")
    frame = D.features(
        instruments,
        fields,
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    frame.columns = names
    return frame.dropna(subset=[factor.name, "close"])


def evaluate_factor(
    config: ProjectConfig,
    factor: FactorDef,
    eval_config: EvalConfig = EvalConfig(),
    initialize: bool = True,
) -> pd.DataFrame:
    if initialize:
        init_qlib(config)
    frame = fetch_factor_frame(config, factor, include_volume=eval_config.neutralize_size)
    if frame.empty:
        return _empty_factor_eval_result(factor, eval_config)
    results: list[dict[str, float | int | str]] = []
    for horizon in eval_config.horizons:
        scored = prepare_factor_signal(frame, factor, eval_config)
        signal_col = "signal"
        exposure_cols = []
        group_col = None
        if eval_config.neutralize_size:
            scored = add_size_proxy(scored)
            exposure_cols.append("size_proxy")
        if eval_config.industry_map_path is not None:
            scored = attach_industry(scored, load_industry_map(eval_config.industry_map_path))
            group_col = "industry"
        if exposure_cols or group_col is not None:
            scored = neutralize_signal(scored, exposure_cols=exposure_cols, group_col=group_col)
            signal_col = "signal_neutral"
        close = scored["close"].groupby(level="instrument")
        scored["future_ret"] = close.shift(-horizon) / scored["close"] - 1.0
        scored = scored.dropna(subset=[signal_col, "future_ret"])
        daily_ic = scored.groupby(level="datetime").apply(
            lambda x: x[signal_col].corr(x["future_ret"], method="pearson")
        )
        daily_rank_ic = scored.groupby(level="datetime").apply(
            lambda x: x[signal_col].corr(x["future_ret"], method="spearman")
        )
        turnover = _estimate_top_quantile_turnover(scored, signal_col, eval_config.quantiles)
        quantile_summary = compute_quantile_return_summary(scored, signal_col, "future_ret", eval_config.quantiles)
        ic_mean, ic_std, icir = _summarize_numeric_series(daily_ic)
        rank_ic_mean, rank_ic_std, rank_icir = _summarize_numeric_series(daily_rank_ic)
        neutralization = []
        if eval_config.purification_steps:
            neutralization.append(f"purify:{'+'.join(eval_config.purification_steps)}")
        if eval_config.neutralize_size:
            neutralization.append("size_proxy")
        if eval_config.industry_map_path is not None:
            neutralization.append("industry")
        row = {
                "factor": factor.name,
                "category": factor.category,
                "direction": factor.direction,
                "horizon": horizon,
                "neutralization": "+".join(neutralization) if neutralization else "none",
                "ic_mean": ic_mean,
                "ic_std": ic_std,
                "icir": icir,
                "rank_ic_mean": rank_ic_mean,
                "rank_ic_std": rank_ic_std,
                "rank_icir": rank_icir,
                "top_quantile_turnover": turnover,
                "observations": int(len(scored)),
        }
        row.update(quantile_summary)
        results.append(row)
    return pd.DataFrame(results)


def _summarize_numeric_series(values: pd.Series | pd.DataFrame | float) -> tuple[float, float, float]:
    series = _coerce_numeric_series(values)
    if series.empty:
        return float("nan"), float("nan"), float("nan")
    mean = _finite_float(series.mean())
    std = _finite_float(series.std())
    if np.isnan(mean) or np.isnan(std) or std == 0:
        return mean, std, float("nan")
    return mean, std, float(mean / std)


def _coerce_numeric_series(values: pd.Series | pd.DataFrame | float) -> pd.Series:
    if isinstance(values, pd.DataFrame):
        values = pd.Series(values.to_numpy().reshape(-1))
    elif not isinstance(values, pd.Series):
        values = pd.Series([values])
    return pd.to_numeric(values, errors="coerce").dropna()


def _finite_float(value: object) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return numeric if np.isfinite(numeric) else float("nan")


def _empty_factor_eval_result(factor: FactorDef, eval_config: EvalConfig) -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []
    empty_scored = pd.DataFrame(columns=["signal", "future_ret"])
    quantile_summary = compute_quantile_return_summary(empty_scored, "signal", "future_ret", eval_config.quantiles)
    for horizon in eval_config.horizons:
        row = {
            "factor": factor.name,
            "category": factor.category,
            "direction": factor.direction,
            "horizon": horizon,
            "neutralization": "none",
            "ic_mean": float("nan"),
            "ic_std": float("nan"),
            "icir": float("nan"),
            "rank_ic_mean": float("nan"),
            "rank_ic_std": float("nan"),
            "rank_icir": float("nan"),
            "top_quantile_turnover": float("nan"),
            "observations": 0,
        }
        row.update(quantile_summary)
        rows.append(row)
    return pd.DataFrame(rows)


def with_directional_signal(frame: pd.DataFrame, factor: FactorDef) -> pd.DataFrame:
    scored = frame.copy()
    scored["signal"] = scored[factor.name] * factor.direction
    return scored


def prepare_factor_signal(frame: pd.DataFrame, factor: FactorDef, eval_config: EvalConfig = EvalConfig()) -> pd.DataFrame:
    scored = with_directional_signal(frame, factor)
    scored["purification"] = "none"
    if not eval_config.purification_steps:
        return scored
    scored = purify_factor_frame(
        scored,
        "signal",
        steps=eval_config.purification_steps,
        output_col="signal",
        mad_n=eval_config.purification_mad_n,
    )
    scored["purification"] = "+".join(eval_config.purification_steps)
    return scored


def _estimate_top_quantile_turnover(frame: pd.DataFrame, factor_col: str, quantiles: int) -> float:
    memberships: list[set[str]] = []
    for _, daily in frame.groupby(level="datetime"):
        ranks = daily[factor_col].rank(method="first", pct=True)
        top = daily.index.get_level_values("instrument")[ranks >= 1 - 1 / quantiles]
        memberships.append(set(top))
    if len(memberships) < 2:
        return float("nan")
    changes = []
    for prev, cur in zip(memberships, memberships[1:]):
        if not prev:
            continue
        changes.append(1 - len(prev & cur) / len(prev))
    return float(pd.Series(changes).mean()) if changes else float("nan")


def compute_quantile_return_summary(
    frame: pd.DataFrame,
    signal_col: str,
    ret_col: str,
    quantiles: int,
) -> dict[str, float]:
    valid = frame.dropna(subset=[signal_col, ret_col])
    result = {f"q{i}_mean_return": float("nan") for i in range(1, quantiles + 1)}
    if valid.empty:
        result["long_short_mean_return"] = float("nan")
        return result

    pieces = []
    for _, daily in valid.groupby(level="datetime") if isinstance(valid.index, pd.MultiIndex) else [(None, valid)]:
        if len(daily) < quantiles:
            continue
        ranks = daily[signal_col].rank(method="first")
        buckets = pd.qcut(ranks, quantiles, labels=False, duplicates="drop")
        tmp = daily[[ret_col]].copy()
        tmp["quantile"] = buckets.astype(float) + 1
        pieces.append(tmp.dropna(subset=["quantile"]))
    if not pieces:
        result["long_short_mean_return"] = float("nan")
        return result

    assigned = pd.concat(pieces)
    means = assigned.groupby("quantile")[ret_col].mean()
    for quantile, value in means.items():
        result[f"q{int(quantile)}_mean_return"] = float(value)
    top = result[f"q{quantiles}_mean_return"]
    bottom = result["q1_mean_return"]
    result["long_short_mean_return"] = float(top - bottom) if not np.isnan(top) and not np.isnan(bottom) else float("nan")
    return result


def write_eval_report(frame: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return output
