from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class EventBacktestConfig:
    horizons: tuple[int, ...] = (5, 20)
    buckets: tuple[tuple[float, float], ...] = (
        (0.70, 0.85),
        (0.85, 0.95),
        (0.70, 0.95),
        (0.95, 1.0),
    )
    confirmation_window: int = 0
    confirmation_breakout_lookback: int = 14
    confirmation_volume_lookback: int = 5
    confirmation_volume_ratio: float = 1.2


def build_event_trades(
    frame: pd.DataFrame,
    signal_col: str,
    config: EventBacktestConfig = EventBacktestConfig(),
    signal_direction: int = 1,
) -> pd.DataFrame:
    prepared = _prepare_backtest_frame(frame, signal_col, signal_direction)
    trades: list[dict[str, float | int | str | pd.Timestamp]] = []
    for horizon in config.horizons:
        for bucket_low, bucket_high in config.buckets:
            trades.extend(_build_bucket_trades(prepared, horizon, bucket_low, bucket_high, config))
    return pd.DataFrame(trades, columns=_trade_columns())


def summarize_trades(
    trades: pd.DataFrame,
    by_year: bool = False,
    group_cols: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    resolved_group_cols = list(group_cols or ("bucket", "horizon"))
    if trades.empty:
        columns = _summary_columns(by_year, tuple(resolved_group_cols))
        return pd.DataFrame(columns=columns)

    data = trades.copy()
    if by_year:
        data["year"] = pd.to_datetime(data["entry_date"]).dt.year
        resolved_group_cols = ["year", *resolved_group_cols]

    rows = []
    for keys, group in data.groupby(resolved_group_cols, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(resolved_group_cols, keys))
        row.update(_summarize_trade_group(group))
        rows.append(row)
    return pd.DataFrame(rows, columns=_summary_columns(by_year, tuple(group_cols or ("bucket", "horizon"))))


def _prepare_backtest_frame(frame: pd.DataFrame, signal_col: str, signal_direction: int = 1) -> pd.DataFrame:
    required = {signal_col, "open", "high", "low", "close"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"missing event backtest columns: {sorted(missing)}")
    if not isinstance(frame.index, pd.MultiIndex):
        raise ValueError("event backtest frame must use a MultiIndex of datetime and instrument")

    columns = [signal_col, "open", "high", "low", "close"]
    if "volume" in frame.columns:
        columns.append("volume")
    data = frame[columns].copy()
    data = data.rename(columns={signal_col: "signal"})
    dropna_columns = ["signal", "open", "high", "low", "close"]
    if "volume" in data.columns:
        dropna_columns.append("volume")
    data = data.dropna(subset=dropna_columns)
    data["rank_signal"] = data["signal"] * signal_direction
    data["score_pct"] = data.groupby(level="datetime")["rank_signal"].rank(method="first", pct=True)
    return data.reset_index().sort_values(["instrument", "datetime"]).reset_index(drop=True)


def _build_bucket_trades(
    frame: pd.DataFrame,
    horizon: int,
    bucket_low: float,
    bucket_high: float,
    config: EventBacktestConfig,
) -> list[dict[str, float | int | str | pd.Timestamp]]:
    trades: list[dict[str, float | int | str | pd.Timestamp]] = []
    bucket_name = _format_bucket(bucket_low, bucket_high)
    for instrument, instrument_frame in frame.groupby("instrument", sort=False):
        data = instrument_frame.reset_index(drop=True)
        next_available_signal_idx = 0
        for signal_idx, row in data.iterrows():
            if signal_idx < next_available_signal_idx:
                continue
            if not _in_bucket(float(row["score_pct"]), bucket_low, bucket_high):
                continue
            confirmation = _find_confirmation(data, signal_idx, config)
            if confirmation is None:
                continue
            confirmation_idx, breakout_level, volume_ratio = confirmation
            entry_idx = confirmation_idx + 1
            exit_idx = confirmation_idx + horizon
            if exit_idx >= len(data):
                break

            path = data.iloc[entry_idx : exit_idx + 1]
            entry_open = float(data.loc[entry_idx, "open"])
            exit_close = float(data.loc[exit_idx, "close"])
            trades.append(
                {
                    "bucket": bucket_name,
                    "bucket_low": bucket_low,
                    "bucket_high": bucket_high,
                    "horizon": horizon,
                    "instrument": instrument,
                    "signal_date": row["datetime"],
                    "confirmation_date": data.loc[confirmation_idx, "datetime"],
                    "entry_date": data.loc[entry_idx, "datetime"],
                    "exit_date": data.loc[exit_idx, "datetime"],
                    "score": float(row["signal"]),
                    "score_pct": float(row["score_pct"]),
                    "breakout_level": breakout_level,
                    "confirmation_close": float(data.loc[confirmation_idx, "close"]),
                    "confirmation_volume_ratio": volume_ratio,
                    "entry_open": entry_open,
                    "exit_close": exit_close,
                    "return": exit_close / entry_open - 1.0,
                    "mfe": float(path["high"].max()) / entry_open - 1.0,
                    "mae": float(path["low"].min()) / entry_open - 1.0,
                }
            )
            next_available_signal_idx = exit_idx + 1
    return trades


def _find_confirmation(
    data: pd.DataFrame,
    signal_idx: int,
    config: EventBacktestConfig,
) -> tuple[int, float, float] | None:
    if config.confirmation_window <= 0:
        return signal_idx, float("nan"), float("nan")
    if "volume" not in data.columns:
        raise ValueError("volume is required when confirmation_window is greater than 0")

    breakout_start = max(0, signal_idx - config.confirmation_breakout_lookback + 1)
    breakout_level = float(data.loc[breakout_start:signal_idx, "high"].max())
    search_end = min(len(data) - 1, signal_idx + config.confirmation_window)
    for confirmation_idx in range(signal_idx + 1, search_end + 1):
        volume_start = max(0, confirmation_idx - config.confirmation_volume_lookback)
        volume_base = data.loc[volume_start : confirmation_idx - 1, "volume"].mean()
        volume_ratio = float(data.loc[confirmation_idx, "volume"] / volume_base) if volume_base else float("nan")
        if float(data.loc[confirmation_idx, "close"]) > breakout_level and volume_ratio >= config.confirmation_volume_ratio:
            return confirmation_idx, breakout_level, volume_ratio
    return None


def _summarize_trade_group(group: pd.DataFrame) -> dict[str, float | int]:
    returns = group["return"]
    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    avg_win = float(wins.mean()) if not wins.empty else 0.0
    avg_loss = float(losses.mean()) if not losses.empty else 0.0
    return {
        "trade_count": int(len(group)),
        "mean_return": float(returns.mean()),
        "median_return": float(returns.median()),
        "win_rate": float((returns > 0).mean()),
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "payoff_ratio": _payoff_ratio(avg_win, avg_loss),
        "expectancy": float(returns.mean()),
        "mfe_mean": float(group["mfe"].mean()),
        "mfe_median": float(group["mfe"].median()),
        "mae_mean": float(group["mae"].mean()),
        "mae_median": float(group["mae"].median()),
    }


def _in_bucket(score_pct: float, low: float, high: float) -> bool:
    lower = score_pct >= low
    upper = score_pct <= high if high >= 1.0 else score_pct < high
    return bool(lower and upper)


def _payoff_ratio(avg_win: float, avg_loss: float) -> float:
    if avg_loss == 0:
        return float("inf") if avg_win > 0 else 0.0
    return avg_win / abs(avg_loss)


def _format_bucket(low: float, high: float) -> str:
    return f"p{int(low * 100):02d}_p{int(high * 100):02d}"


def _trade_columns() -> list[str]:
    return [
        "bucket",
        "bucket_low",
        "bucket_high",
        "horizon",
        "instrument",
        "signal_date",
        "confirmation_date",
        "entry_date",
        "exit_date",
        "score",
        "score_pct",
        "breakout_level",
        "confirmation_close",
        "confirmation_volume_ratio",
        "entry_open",
        "exit_close",
        "return",
        "mfe",
        "mae",
    ]


def _summary_columns(by_year: bool, group_cols: tuple[str, ...] = ("bucket", "horizon")) -> list[str]:
    columns = [
        *group_cols,
        "trade_count",
        "mean_return",
        "median_return",
        "win_rate",
        "avg_win",
        "avg_loss",
        "payoff_ratio",
        "expectancy",
        "mfe_mean",
        "mfe_median",
        "mae_mean",
        "mae_median",
    ]
    return ["year", *columns] if by_year else columns
