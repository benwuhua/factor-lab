from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import ProjectConfig, load_project_config
from .qlib_bootstrap import init_qlib


LIQUIDITY_MICROSTRUCTURE_COLUMNS = [
    "date",
    "instrument",
    "available_at",
    "tradable",
    "suspended",
    "limit_up",
    "limit_down",
    "buy_blocked",
    "sell_blocked",
    "amount",
    "volume",
    "amount_20d",
    "turnover_20d",
    "pct_change",
    "intraday_range",
    "gap_pct",
    "limit_pressure",
]

FEATURE_COLUMNS = [
    "date",
    "instrument",
    "close",
    "prev_close",
    "open",
    "high",
    "low",
    "amount",
    "volume",
    "turnover",
    "amount_20d",
    "turnover_20d",
]

QLIB_EXPRESSIONS = [
    "$close",
    "Ref($close,1)",
    "$open",
    "$high",
    "$low",
    "$amount",
    "$volume",
    "$turnover",
    "Mean($amount,20)",
    "Mean($turnover,20)",
]


def build_liquidity_microstructure(
    features: pd.DataFrame,
    *,
    limit_up_pct: float = 0.098,
    limit_down_pct: float = -0.098,
) -> pd.DataFrame:
    if features is None or features.empty:
        return _empty_liquidity_frame()
    missing = {"date", "instrument"} - set(features.columns)
    if missing:
        raise ValueError(f"features are missing columns: {sorted(missing)}")

    frame = features.copy()
    for column in FEATURE_COLUMNS:
        if column not in frame.columns:
            frame[column] = np.nan
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame["available_at"] = frame["date"]
    for column in ["close", "prev_close", "open", "high", "low", "amount", "volume", "turnover", "amount_20d", "turnover_20d"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["suspended"] = _suspended_flags(frame)
    valid_prev_close = frame["prev_close"].notna() & frame["prev_close"].gt(0)
    frame["pct_change"] = np.where(valid_prev_close, frame["close"] / frame["prev_close"] - 1.0, np.nan)
    frame["intraday_range"] = np.where(valid_prev_close, (frame["high"] - frame["low"]) / frame["prev_close"], np.nan)
    frame["gap_pct"] = np.where(valid_prev_close, frame["open"] / frame["prev_close"] - 1.0, np.nan)

    tradable_for_limits = ~frame["suspended"] & pd.Series(frame["pct_change"], index=frame.index).notna()
    frame["limit_up"] = tradable_for_limits & pd.Series(frame["pct_change"], index=frame.index).ge(limit_up_pct)
    frame["limit_down"] = tradable_for_limits & pd.Series(frame["pct_change"], index=frame.index).le(limit_down_pct)
    frame["tradable"] = ~frame["suspended"]
    frame["buy_blocked"] = frame["suspended"] | frame["limit_up"]
    frame["sell_blocked"] = frame["suspended"] | frame["limit_down"]
    frame["limit_pressure"] = _limit_pressure(frame["pct_change"], limit_up_pct=limit_up_pct, limit_down_pct=limit_down_pct)

    return _normalize_liquidity_columns(frame)


def fetch_liquidity_microstructure_features(
    provider_config: ProjectConfig | str | Path,
    *,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    project_config = _load_project_config(provider_config)
    init_qlib(project_config)
    from qlib.data import D

    frame = D.features(
        D.instruments(project_config.market),
        QLIB_EXPRESSIONS,
        start_time=start_date,
        end_time=end_date,
        freq=project_config.freq,
    )
    frame.columns = ["close", "prev_close", "open", "high", "low", "amount", "volume", "turnover", "amount_20d", "turnover_20d"]
    frame = frame.reset_index()
    if "datetime" in frame.columns:
        frame = frame.rename(columns={"datetime": "date"})
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return frame.loc[:, FEATURE_COLUMNS]


def merge_liquidity_microstructure(new: pd.DataFrame, existing: pd.DataFrame | None = None) -> pd.DataFrame:
    pieces = []
    if existing is not None and not existing.empty:
        pieces.append(existing)
    if new is not None and not new.empty:
        pieces.append(new)
    if not pieces:
        return _empty_liquidity_frame()

    merged = pd.concat(pieces, ignore_index=True, sort=False)
    if "date" not in merged.columns or "instrument" not in merged.columns:
        raise ValueError("liquidity microstructure data must include date and instrument")
    for column in LIQUIDITY_MICROSTRUCTURE_COLUMNS:
        if column not in merged.columns:
            merged[column] = np.nan
    merged["date"] = pd.to_datetime(merged["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    merged["available_at"] = pd.to_datetime(merged["available_at"], errors="coerce").dt.strftime("%Y-%m-%d")
    merged = merged.drop_duplicates(["date", "instrument"], keep="last")
    merged = merged.sort_values(["date", "instrument"]).reset_index(drop=True)
    return merged.loc[:, LIQUIDITY_MICROSTRUCTURE_COLUMNS]


def write_liquidity_microstructure(
    liquidity: pd.DataFrame,
    output_path: str | Path = "data/liquidity_microstructure.csv",
    *,
    merge_existing: bool = False,
    existing_path: str | Path | None = None,
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    existing = None
    source_path = Path(existing_path) if existing_path is not None else output
    if merge_existing and source_path.exists():
        existing = pd.read_csv(source_path)
    merged = merge_liquidity_microstructure(liquidity, existing) if merge_existing else _normalize_liquidity_columns(liquidity)
    merged.to_csv(output, index=False)
    return output


def _load_project_config(provider_config: ProjectConfig | str | Path) -> ProjectConfig:
    if isinstance(provider_config, ProjectConfig):
        return provider_config
    return load_project_config(provider_config)


def _suspended_flags(frame: pd.DataFrame) -> pd.Series:
    close_bad = frame["close"].isna() | frame["close"].le(0)
    amount_bad = frame["amount"].notna() & frame["amount"].le(0)
    volume_bad = frame["volume"].notna() & frame["volume"].le(0)
    return close_bad | amount_bad | volume_bad


def _limit_pressure(values: Any, *, limit_up_pct: float, limit_down_pct: float) -> pd.Series:
    pct = pd.to_numeric(values, errors="coerce")
    pressure = pd.Series(np.nan, index=pct.index, dtype="float64")
    up = pct.ge(0) & pd.notna(pct)
    down = pct.lt(0) & pd.notna(pct)
    if limit_up_pct != 0:
        pressure.loc[up] = pct.loc[up] / abs(limit_up_pct)
    if limit_down_pct != 0:
        pressure.loc[down] = pct.loc[down] / abs(limit_down_pct)
    return pressure.clip(lower=-1.0, upper=1.0)


def _normalize_liquidity_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return _empty_liquidity_frame()
    result = frame.copy()
    for column in LIQUIDITY_MICROSTRUCTURE_COLUMNS:
        if column not in result.columns:
            result[column] = np.nan
    return result.loc[:, LIQUIDITY_MICROSTRUCTURE_COLUMNS].reset_index(drop=True)


def _empty_liquidity_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=LIQUIDITY_MICROSTRUCTURE_COLUMNS)
