from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class EventEvalConfig:
    horizons: tuple[int, ...] = (5, 20)
    buckets: tuple[tuple[float, float], ...] = ((0.70, 0.85), (0.85, 0.95), (0.95, 1.0))
    by_year: bool = False


def evaluate_event_buckets(frame: pd.DataFrame, signal_col: str, config: EventEvalConfig = EventEvalConfig()) -> pd.DataFrame:
    prepared = _prepare_event_frame(frame, signal_col)
    rows: list[dict[str, float | int | str]] = []
    for horizon in config.horizons:
        outcomes = _add_forward_outcomes(prepared, horizon)
        outcomes = outcomes.dropna(subset=["future_return", "mfe", "mae", "score_pct"])
        for bucket_low, bucket_high in config.buckets:
            bucketed = _select_bucket(outcomes, bucket_low, bucket_high)
            if config.by_year:
                for year, yearly in bucketed.groupby(bucketed["datetime"].dt.year):
                    rows.append(_summarize_events(yearly, horizon, bucket_low, bucket_high, year=int(year)))
            else:
                rows.append(_summarize_events(bucketed, horizon, bucket_low, bucket_high))
    return pd.DataFrame(rows)


def _prepare_event_frame(frame: pd.DataFrame, signal_col: str) -> pd.DataFrame:
    required = {signal_col, "close", "high", "low"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"missing event evaluation columns: {sorted(missing)}")
    if not isinstance(frame.index, pd.MultiIndex):
        raise ValueError("event evaluation frame must use a MultiIndex of datetime and instrument")
    data = frame[[signal_col, "close", "high", "low"]].copy()
    data = data.rename(columns={signal_col: "signal"})
    data = data.dropna(subset=["signal", "close", "high", "low"])
    data["score_pct"] = data.groupby(level="datetime")["signal"].rank(method="first", pct=True)
    return data.reset_index().sort_values(["instrument", "datetime"]).reset_index(drop=True)


def _add_forward_outcomes(frame: pd.DataFrame, horizon: int) -> pd.DataFrame:
    pieces = []
    for _, instrument_frame in frame.groupby("instrument", sort=False):
        data = instrument_frame.copy().reset_index(drop=True)
        data["future_close"] = data["close"].shift(-horizon)
        data["future_return"] = data["future_close"] / data["close"] - 1.0
        data["mfe"] = _forward_rolling_extreme(data["high"], horizon, "max") / data["close"] - 1.0
        data["mae"] = _forward_rolling_extreme(data["low"], horizon, "min") / data["close"] - 1.0
        pieces.append(data)
    return pd.concat(pieces, ignore_index=True) if pieces else frame.iloc[0:0].copy()


def _forward_rolling_extreme(series: pd.Series, horizon: int, method: str) -> pd.Series:
    shifted = series.shift(-1)
    rolling = shifted.iloc[::-1].rolling(window=horizon, min_periods=horizon)
    result = rolling.max() if method == "max" else rolling.min()
    return result.iloc[::-1]


def _select_bucket(frame: pd.DataFrame, low: float, high: float) -> pd.DataFrame:
    lower = frame["score_pct"] >= low
    upper = frame["score_pct"] <= high if high >= 1.0 else frame["score_pct"] < high
    return frame[lower & upper]


def _summarize_events(
    frame: pd.DataFrame,
    horizon: int,
    bucket_low: float,
    bucket_high: float,
    year: int | None = None,
) -> dict[str, float | int | str]:
    returns = frame["future_return"] if not frame.empty else pd.Series(dtype=float)
    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    avg_win = float(wins.mean()) if not wins.empty else (float("nan") if frame.empty else 0.0)
    avg_loss = float(losses.mean()) if not losses.empty else (float("nan") if frame.empty else 0.0)
    payoff_ratio = _payoff_ratio(avg_win, avg_loss)
    row: dict[str, float | int | str] = {
        "bucket": _format_bucket(bucket_low, bucket_high),
        "bucket_low": bucket_low,
        "bucket_high": bucket_high,
        "horizon": horizon,
        "event_count": int(len(frame)),
        "mean_return": float(returns.mean()) if not returns.empty else float("nan"),
        "median_return": float(returns.median()) if not returns.empty else float("nan"),
        "win_rate": float((returns > 0).mean()) if not returns.empty else float("nan"),
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "payoff_ratio": payoff_ratio,
        "expectancy": float(returns.mean()) if not returns.empty else float("nan"),
        "mfe_mean": float(frame["mfe"].mean()) if not frame.empty else float("nan"),
        "mfe_median": float(frame["mfe"].median()) if not frame.empty else float("nan"),
        "mae_mean": float(frame["mae"].mean()) if not frame.empty else float("nan"),
        "mae_median": float(frame["mae"].median()) if not frame.empty else float("nan"),
    }
    if year is not None:
        row["year"] = year
    return row


def _payoff_ratio(avg_win: float, avg_loss: float) -> float:
    if np.isnan(avg_win):
        return float("nan")
    if avg_loss == 0:
        return float("inf")
    return avg_win / abs(avg_loss)


def _format_bucket(low: float, high: float) -> str:
    return f"p{int(low * 100):02d}_p{int(high * 100):02d}"
