from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from .neutralization import neutralize_signal


def mad_winsorize_by_date(
    frame: pd.DataFrame,
    factor_col: str,
    *,
    n_mad: float = 3.0,
    output_col: str | None = None,
    date_level: str = "datetime",
) -> pd.DataFrame:
    """Cap cross-sectional outliers by median absolute deviation for each date."""
    if n_mad <= 0:
        raise ValueError("n_mad must be positive")
    output = output_col or factor_col
    return _transform_by_date(
        frame,
        factor_col,
        output,
        date_level,
        lambda values: _mad_cap(values, n_mad),
    )


def zscore_standardize_by_date(
    frame: pd.DataFrame,
    factor_col: str,
    *,
    output_col: str | None = None,
    date_level: str = "datetime",
) -> pd.DataFrame:
    """Standardize a factor to zero mean and unit population std per date."""
    output = output_col or factor_col
    return _transform_by_date(frame, factor_col, output, date_level, _zscore)


def rank_standardize_by_date(
    frame: pd.DataFrame,
    factor_col: str,
    *,
    output_col: str | None = None,
    date_level: str = "datetime",
) -> pd.DataFrame:
    """Map each daily cross-section to centered percentile ranks in (-0.5, 0.5)."""
    output = output_col or factor_col
    return _transform_by_date(frame, factor_col, output, date_level, _centered_rank)


def neutralize_by_date(
    frame: pd.DataFrame,
    factor_col: str,
    *,
    exposure_cols: Iterable[str] | None = None,
    group_col: str | None = None,
    output_col: str | None = None,
) -> pd.DataFrame:
    """Residualize a factor against numeric exposures and optional groups by date."""
    output = output_col or factor_col
    renamed = frame.copy()
    signal_col = "__purify_signal__"
    renamed[signal_col] = renamed[factor_col]
    neutral = neutralize_signal(
        renamed,
        signal_col=signal_col,
        exposure_cols=list(exposure_cols or []),
        group_col=group_col,
        output_col=output,
    )
    return neutral.drop(columns=[signal_col])


def purify_factor_frame(
    frame: pd.DataFrame,
    factor_col: str,
    *,
    steps: Iterable[str] = ("mad", "zscore"),
    output_col: str | None = None,
    exposure_cols: Iterable[str] | None = None,
    group_col: str | None = None,
    mad_n: float = 3.0,
    date_level: str = "datetime",
) -> pd.DataFrame:
    """Apply an ordered, deterministic purification pipeline to a factor column."""
    result = frame.copy()
    current_col = factor_col
    final_col = output_col or factor_col
    for step in steps:
        normalized = step.strip().lower()
        target_col = final_col
        if normalized == "mad":
            result = mad_winsorize_by_date(result, current_col, n_mad=mad_n, output_col=target_col, date_level=date_level)
        elif normalized == "zscore":
            result = zscore_standardize_by_date(result, current_col, output_col=target_col, date_level=date_level)
        elif normalized == "rank":
            result = rank_standardize_by_date(result, current_col, output_col=target_col, date_level=date_level)
        elif normalized == "neutralize":
            result = neutralize_by_date(
                result,
                current_col,
                exposure_cols=exposure_cols,
                group_col=group_col,
                output_col=target_col,
            )
        else:
            raise ValueError(f"unknown purification step: {step}")
        current_col = target_col
    return result


def _transform_by_date(
    frame: pd.DataFrame,
    factor_col: str,
    output_col: str,
    date_level: str,
    transform,
) -> pd.DataFrame:
    if factor_col not in frame.columns:
        raise ValueError(f"frame is missing factor column: {factor_col}")
    result = frame.copy()
    source = result[factor_col].copy()
    result[output_col] = np.nan
    for _, daily in _group_by_date(result, date_level):
        result.loc[daily.index, output_col] = transform(source.loc[daily.index].astype(float))
    return result


def _group_by_date(frame: pd.DataFrame, date_level: str):
    if isinstance(frame.index, pd.MultiIndex) and date_level in frame.index.names:
        return frame.groupby(level=date_level, sort=False)
    if date_level in frame.columns:
        return frame.groupby(date_level, sort=False)
    raise ValueError(f"frame must have a '{date_level}' index level or column")


def _mad_cap(values: pd.Series, n_mad: float) -> pd.Series:
    median = values.median(skipna=True)
    mad = (values - median).abs().median(skipna=True)
    if pd.isna(mad) or mad == 0:
        return values.copy()
    scaled_mad = 1.4826 * mad
    lower = median - n_mad * scaled_mad
    upper = median + n_mad * scaled_mad
    return values.clip(lower=lower, upper=upper)


def _zscore(values: pd.Series) -> pd.Series:
    mean = values.mean(skipna=True)
    std = values.std(skipna=True, ddof=0)
    if pd.isna(std) or std == 0:
        return values * 0.0
    return (values - mean) / std


def _centered_rank(values: pd.Series) -> pd.Series:
    valid = values.dropna()
    if valid.empty:
        return values.copy()
    ranks = valid.rank(method="first")
    centered = (ranks - 0.5) / len(valid) - 0.5
    result = pd.Series(np.nan, index=values.index, dtype=float)
    result.loc[valid.index] = centered
    return result
