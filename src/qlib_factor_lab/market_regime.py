from __future__ import annotations

import pandas as pd


def compute_equal_weight_market_regime(
    frame: pd.DataFrame,
    close_col: str = "close",
    fast_window: int = 20,
    slow_window: int = 60,
    trend_window: int = 20,
    trend_threshold: float = 0.02,
) -> pd.DataFrame:
    if close_col not in frame.columns:
        raise ValueError(f"missing market close column: {close_col}")
    if not isinstance(frame.index, pd.MultiIndex):
        raise ValueError("market regime frame must use a MultiIndex of datetime and instrument")

    data = frame[[close_col]].dropna().copy()
    close = data[close_col].groupby(level="instrument")
    data["stock_ret"] = close.pct_change()
    market_ret = data.groupby(level="datetime")["stock_ret"].mean().fillna(0.0)
    market_proxy = (1.0 + market_ret).cumprod()
    result = pd.DataFrame({"market_ret": market_ret, "market_proxy": market_proxy})
    result["fast_ma"] = result["market_proxy"].rolling(fast_window, min_periods=1).mean()
    result["slow_ma"] = result["market_proxy"].rolling(slow_window, min_periods=1).mean()
    result["trend_return"] = result["market_proxy"].pct_change(trend_window).fillna(0.0)
    result["market_regime"] = "sideways"
    result.loc[
        (result["fast_ma"] >= result["slow_ma"])
        & (result["market_proxy"] >= result["slow_ma"])
        & (result["trend_return"] > trend_threshold),
        "market_regime",
    ] = "up"
    result.loc[
        (result["fast_ma"] <= result["slow_ma"])
        & (result["market_proxy"] <= result["slow_ma"])
        & (result["trend_return"] < -trend_threshold),
        "market_regime",
    ] = "down"
    result.index.name = "datetime"
    return result


def annotate_trades_with_market_regime(
    trades: pd.DataFrame,
    regime: pd.DataFrame,
    date_col: str = "signal_date",
) -> pd.DataFrame:
    if trades.empty:
        annotated = trades.copy()
        annotated["market_regime"] = pd.Series(dtype="object")
        return annotated
    if date_col not in trades.columns:
        raise ValueError(f"missing trade date column: {date_col}")
    if "market_regime" not in regime.columns:
        raise ValueError("market regime data must include market_regime")

    lookup = regime[["market_regime"]].copy()
    lookup.index = pd.to_datetime(lookup.index)
    annotated = trades.copy()
    annotated[date_col] = pd.to_datetime(annotated[date_col])
    annotated = annotated.merge(
        lookup,
        left_on=date_col,
        right_index=True,
        how="left",
    )
    annotated["market_regime"] = annotated["market_regime"].fillna("unknown")
    return annotated
