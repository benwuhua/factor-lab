from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


def load_industry_map(path: str | Path) -> pd.Series:
    data = pd.read_csv(path)
    required = {"instrument", "industry"}
    missing = required - set(data.columns)
    if missing:
        raise ValueError(f"industry map is missing columns: {sorted(missing)}")
    return data.drop_duplicates("instrument").set_index("instrument")["industry"]


def attach_industry(frame: pd.DataFrame, industry_map: pd.Series, column: str = "industry") -> pd.DataFrame:
    result = frame.copy()
    instruments = result.index.get_level_values("instrument")
    result[column] = instruments.map(industry_map)
    return result


def add_size_proxy(
    frame: pd.DataFrame,
    close_col: str = "close",
    volume_col: str = "volume",
    output_col: str = "size_proxy",
) -> pd.DataFrame:
    result = frame.copy()
    traded_value = result[close_col].abs() * result[volume_col].abs()
    result[output_col] = np.log1p(traded_value.replace([np.inf, -np.inf], np.nan))
    return result


def neutralize_signal(
    frame: pd.DataFrame,
    signal_col: str = "signal",
    exposure_cols: Iterable[str] | None = None,
    group_col: str | None = None,
    output_col: str = "signal_neutral",
) -> pd.DataFrame:
    exposure_cols = list(exposure_cols or [])
    if not exposure_cols and group_col is None:
        result = frame.copy()
        result[output_col] = result[signal_col]
        return result

    result = frame.copy()
    result[output_col] = np.nan
    for _, daily in result.groupby(level="datetime", sort=False):
        usable_cols = [signal_col, *exposure_cols]
        if group_col is not None:
            usable_cols.append(group_col)
        usable = daily.dropna(subset=usable_cols)
        if len(usable) < 2:
            continue

        design_parts = []
        if exposure_cols:
            design_parts.append(usable[exposure_cols].astype(float))
        if group_col is not None:
            dummies = pd.get_dummies(usable[group_col].astype(str), prefix=group_col, drop_first=True, dtype=float)
            if not dummies.empty:
                design_parts.append(dummies)

        if design_parts:
            design = pd.concat(design_parts, axis=1)
            design.insert(0, "intercept", 1.0)
        else:
            design = pd.DataFrame({"intercept": 1.0}, index=usable.index)

        y = usable[signal_col].astype(float).to_numpy()
        x = design.astype(float).to_numpy()
        beta, *_ = np.linalg.lstsq(x, y, rcond=None)
        residual = y - x @ beta
        result.loc[usable.index, output_col] = residual
    return result
