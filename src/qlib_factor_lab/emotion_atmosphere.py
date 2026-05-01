from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


OUTPUT_COLUMNS = [
    "trade_date",
    "instrument",
    "available_at",
    "pct_change",
    "amount_20d",
    "turnover_20d",
    "limit_up",
    "limit_down",
    "suspended",
    "tradable_count",
    "suspended_count",
    "up_ratio",
    "down_ratio",
    "limit_up_count",
    "limit_down_count",
    "avg_pct_change",
    "median_pct_change",
    "hot_turnover_ratio",
    "panic_down_ratio",
    "emotion_score",
    "instrument_emotion_score",
    "crowding_cooling_score",
]


def build_emotion_atmosphere(
    liquidity: pd.DataFrame,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Build per-instrument daily market emotion features from liquidity data."""
    data = _normalize_liquidity_frame(liquidity)
    if start_date is not None:
        data = data[data["trade_date"] >= _date_string(start_date)]
    if end_date is not None:
        data = data[data["trade_date"] <= _date_string(end_date)]
    if data.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    tradable = data[~data["suspended"]].copy()
    market = _build_market_summary(tradable, data)
    result = data.merge(market, on="trade_date", how="left")
    result["available_at"] = result["trade_date"]
    result["emotion_score"] = _score_emotion(result)
    result["instrument_emotion_score"] = _instrument_emotion_score(result)
    result["crowding_cooling_score"] = (100.0 - result["instrument_emotion_score"]).clip(0.0, 100.0).round(4)
    result = result[OUTPUT_COLUMNS].copy()
    return result.sort_values(["trade_date", "instrument"]).reset_index(drop=True)


def merge_emotion_atmosphere(existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    """Merge snapshots, keeping the latest row for each trade_date/instrument key."""
    frames = []
    if existing is not None and not existing.empty:
        frames.append(existing.copy())
    if new is not None and not new.empty:
        frames.append(new.copy())
    if not frames:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    merged = pd.concat(frames, ignore_index=True, sort=False)
    for column in ["trade_date", "available_at"]:
        if column in merged.columns:
            merged[column] = pd.to_datetime(merged[column], errors="coerce").dt.strftime("%Y-%m-%d")
    merged["instrument"] = merged["instrument"].astype(str)
    merged = merged.drop_duplicates(["trade_date", "instrument"], keep="last")
    ordered = [column for column in OUTPUT_COLUMNS if column in merged.columns]
    extras = [column for column in merged.columns if column not in ordered]
    return merged[ordered + extras].sort_values(["trade_date", "instrument"]).reset_index(drop=True)


def write_emotion_atmosphere(
    frame: pd.DataFrame,
    output_path: str | Path = "data/emotion_atmosphere.csv",
    *,
    merge_existing: bool = False,
) -> pd.DataFrame:
    path = Path(output_path)
    output = frame.copy()
    if merge_existing and path.exists():
        existing = pd.read_csv(path)
        output = merge_emotion_atmosphere(existing, output)
    else:
        output = merge_emotion_atmosphere(None, output)
    path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(path, index=False)
    return output


def _normalize_liquidity_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["trade_date", "instrument", "pct_change", "amount_20d", "turnover_20d", "limit_up", "limit_down", "suspended"])
    data = frame.copy()
    if "trade_date" not in data.columns:
        if "date" in data.columns:
            data["trade_date"] = data["date"]
        elif "datetime" in data.columns:
            data["trade_date"] = data["datetime"]
        else:
            raise ValueError("liquidity data must include trade_date")
    if "instrument" not in data.columns:
        raise ValueError("liquidity data must include instrument")

    data["trade_date"] = pd.to_datetime(data["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    data["instrument"] = data["instrument"].astype(str)
    data = data.dropna(subset=["trade_date", "instrument"]).copy()
    data["pct_change"] = _numeric(data, "pct_change").fillna(0.0)
    data["amount_20d"] = _numeric(data, "amount_20d")
    data["turnover_20d"] = _numeric(data, "turnover_20d")
    data["limit_up"] = _boolean(data, "limit_up", data["pct_change"] >= 0.095)
    data["limit_down"] = _boolean(data, "limit_down", data["pct_change"] <= -0.095)
    data["suspended"] = _boolean(data, "suspended", False)
    return data


def _build_market_summary(tradable: pd.DataFrame, all_rows: pd.DataFrame) -> pd.DataFrame:
    grouped_all = all_rows.groupby("trade_date", sort=True)
    suspended_count = grouped_all["suspended"].sum().rename("suspended_count")
    if tradable.empty:
        summary = pd.DataFrame(index=suspended_count.index)
        summary["tradable_count"] = 0
        summary["up_ratio"] = 0.0
        summary["down_ratio"] = 0.0
        summary["limit_up_count"] = 0
        summary["limit_down_count"] = 0
        summary["avg_pct_change"] = 0.0
        summary["median_pct_change"] = 0.0
        summary["hot_turnover_ratio"] = 0.0
        summary["panic_down_ratio"] = 0.0
        summary["suspended_count"] = suspended_count
        return summary.reset_index()

    grouped = tradable.groupby("trade_date", sort=True)
    summary = grouped["instrument"].count().rename("tradable_count").to_frame()
    summary["up_ratio"] = grouped["pct_change"].apply(lambda values: float((values > 0).mean()))
    summary["down_ratio"] = grouped["pct_change"].apply(lambda values: float((values < 0).mean()))
    summary["limit_up_count"] = grouped["limit_up"].sum().astype(int)
    summary["limit_down_count"] = grouped["limit_down"].sum().astype(int)
    summary["avg_pct_change"] = grouped["pct_change"].mean()
    summary["median_pct_change"] = grouped["pct_change"].median()
    summary["hot_turnover_ratio"] = grouped["turnover_20d"].apply(_hot_turnover_ratio)
    summary["panic_down_ratio"] = grouped["pct_change"].apply(lambda values: float((values <= -0.05).mean()))
    summary["suspended_count"] = suspended_count
    return summary.fillna(0.0).reset_index()


def _score_emotion(frame: pd.DataFrame) -> pd.Series:
    tradable_count = _safe_denominator(frame["tradable_count"])
    limit_up_ratio = frame["limit_up_count"] / tradable_count
    limit_down_ratio = frame["limit_down_count"] / tradable_count
    breadth_component = frame["up_ratio"].clip(0.0, 1.0)
    limit_component = (limit_up_ratio * 5.0).clip(0.0, 1.0)
    heat_component = frame["hot_turnover_ratio"].clip(0.0, 1.0)
    panic_penalty = frame["panic_down_ratio"].clip(0.0, 1.0)
    limit_down_penalty = (limit_down_ratio * 5.0).clip(0.0, 1.0)
    avg_return_component = ((frame["avg_pct_change"].clip(-0.05, 0.05) + 0.05) / 0.10).clip(0.0, 1.0)

    score = (
        40.0 * breadth_component
        + 20.0 * limit_component
        + 15.0 * heat_component
        + 15.0 * avg_return_component
        + 10.0 * (1.0 - panic_penalty)
        - 10.0 * limit_down_penalty
    )
    return score.fillna(50.0).clip(0.0, 100.0).round(4)


def _instrument_emotion_score(frame: pd.DataFrame) -> pd.Series:
    pieces = []
    for _, daily in frame.groupby("trade_date", sort=False):
        pct_rank = _daily_rank(daily["pct_change"], neutral=0.5)
        turnover_rank = _daily_rank(daily["turnover_20d"].fillna(daily["amount_20d"]), neutral=0.5)
        limit_bonus = daily["limit_up"].astype(float)
        limit_down_penalty = daily["limit_down"].astype(float)
        suspended_penalty = daily["suspended"].astype(float)
        raw = (
            100.0 * (0.55 * pct_rank + 0.25 * turnover_rank + 0.20 * limit_bonus)
            - 35.0 * limit_down_penalty
            - 50.0 * suspended_penalty
        )
        pieces.append(raw.clip(0.0, 100.0))
    if not pieces:
        return pd.Series(dtype="float64")
    return pd.concat(pieces).sort_index().round(4)


def _daily_rank(values: pd.Series, *, neutral: float) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    valid = numeric.notna()
    if int(valid.sum()) <= 1:
        return pd.Series(neutral, index=values.index, dtype="float64")
    ranked = numeric.rank(method="average", pct=True)
    return ranked.fillna(neutral).astype(float)


def _hot_turnover_ratio(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return 0.0
    threshold = max(float(numeric.median()) * 1.5, 0.0)
    if threshold <= 0.0:
        return 0.0
    return float((numeric >= threshold).mean())


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _boolean(frame: pd.DataFrame, column: str, default: bool | pd.Series) -> pd.Series:
    if column not in frame.columns:
        if isinstance(default, pd.Series):
            return default.fillna(False).astype(bool)
        return pd.Series(bool(default), index=frame.index, dtype="bool")
    if frame[column].dtype == bool:
        return frame[column].fillna(False).astype(bool)
    return frame[column].map(_coerce_bool).fillna(False).astype(bool)


def _coerce_bool(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y"}
    return bool(value)


def _safe_denominator(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").replace(0, np.nan)
    return numeric.fillna(1.0)


def _date_string(value: str) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")
