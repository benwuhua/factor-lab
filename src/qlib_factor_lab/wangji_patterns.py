from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class WangjiFactor2EventConfig:
    golden_min_age: int = 5
    golden_max_age: int = 60
    pressure_lookback: int = 180
    pressure_exclusion: int = 21
    min_pressure_history: int = 40
    breakout_min_age: int = 3
    breakout_max_age: int = 25
    pullback_support_tolerance: float = 0.04
    pullback_break_tolerance: float = 0.94
    pullback_min_depth: float = 0.03
    pullback_pressure_undercut_max: float = 0.07
    pullback_confirm_max_age: int = 8
    pullback_volume_ratio_max: float = 1.35
    confirmation_return_min: float = 0.02
    confirmation_body_min: float = 0.02
    confirmation_volume_ratio_min: float = 1.10
    confirmation_close_position_min: float = 0.45
    attack_close_tolerance: float = 0.005
    attack_return_min: float = 0.0
    attack_return_max: float = 0.05
    attack_body_min: float = 0.0
    pre_breakout_lookback: int = 20
    pre_breakout_return_max: float = 0.20
    platform_reset_min_age: int = 8
    platform_reset_ma_tolerance: float = 0.04
    platform_breakout_lookback: int = 12
    local_break_lookback: int = 10


FACTOR2_SIGNAL_COLUMN = "wangji-factor2"
FACTOR2_BREAKOUT_COLUMN = "wangji-factor2-breakout"


def compute_wangji_factor2_events(
    frame: pd.DataFrame,
    config: WangjiFactor2EventConfig = WangjiFactor2EventConfig(),
) -> pd.DataFrame:
    """Detect Wangji diamond-buy 2B pullback confirmations from OHLCV bars.

    The detector is intentionally event-oriented: it emits both the binary
    signal and the evidence chain that produced the decision.
    """

    _validate_price_frame(frame)
    prepared = frame[["open", "high", "low", "close", "volume"]].copy()
    pieces = []
    for instrument, group in prepared.groupby(level="instrument", sort=False):
        pieces.append(_compute_instrument_events(group.droplevel("instrument"), str(instrument), config))
    result = pd.concat(pieces).sort_index()
    return result


def _compute_instrument_events(
    bars: pd.DataFrame,
    instrument: str,
    config: WangjiFactor2EventConfig,
) -> pd.DataFrame:
    data = bars.sort_index().copy()
    for window in (5, 13, 21, 60):
        data[f"ma{window}"] = data["close"].rolling(window, min_periods=window).mean()

    data["weekly_trend_ok"] = _weekly_trend(data["close"])
    data["golden_buy_signal"] = _golden_buy_signal(data)
    data["pressure_high"] = data["high"].shift(config.pressure_exclusion).rolling(
        config.pressure_lookback,
        min_periods=config.min_pressure_history,
    ).max()
    data["breakout_signal"] = data["close"] > data["pressure_high"]
    data["first_close_breakout"] = data["breakout_signal"] & (
        data["close"].shift(1) <= data["pressure_high"]
    )

    rows = []
    dates = list(data.index)
    active_diamond_support: float | None = None
    active_diamond_idx: int | None = None
    for idx, date in enumerate(dates):
        row = data.iloc[idx]
        platform_base_idx: int | None = None
        evidence = _empty_evidence(row)
        evidence["weekly_trend_ok"] = int(bool(row["weekly_trend_ok"]))
        evidence["prior_golden_buy_ok"] = 0
        evidence["breakout_ok"] = 0
        evidence["pullback_hold_ok"] = 0
        evidence["confirmation_ok"] = 0
        evidence["failure_reason"] = "insufficient_history"

        if idx < max(60, config.pressure_exclusion + config.min_pressure_history):
            rows.append(evidence)
            continue

        if active_diamond_support is not None:
            reset_reason = _active_structure_reset_reason(data, idx, active_diamond_support, active_diamond_idx, config)
            if reset_reason == "broken":
                active_diamond_support = None
                active_diamond_idx = None
            else:
                evidence["failure_reason"] = "diamond_structure_already_active"
                rows.append(evidence)
                continue

        prior_golden_idx = _last_true_index(
            data["golden_buy_signal"],
            idx - config.golden_max_age,
            idx - config.golden_min_age,
        )
        if not bool(row["weekly_trend_ok"]):
            evidence["failure_reason"] = "weekly_trend_not_ready"
            rows.append(evidence)
            continue
        if prior_golden_idx is None:
            evidence["failure_reason"] = "missing_prior_golden_buy"
            rows.append(evidence)
            continue

        evidence["prior_golden_buy_ok"] = 1
        evidence["golden_buy_date"] = dates[prior_golden_idx]

        breakout_watch = _breakout_attack_evidence(data, idx, config)
        if bool(breakout_watch["breakout_attack_ok"]):
            evidence[FACTOR2_BREAKOUT_COLUMN] = 1.0
            evidence["breakout_watch_date"] = date
            evidence["breakout_watch_close"] = float(row["close"])
            evidence["breakout_watch_pressure_high"] = float(breakout_watch["pressure_high"])
            evidence["breakout_watch_pressure_high_date"] = breakout_watch["pressure_high_date"]
            evidence["breakout_watch_return"] = float(breakout_watch["confirmation_return"])
            evidence["breakout_watch_body"] = float(breakout_watch["confirmation_body"])
            evidence["breakout_watch_volume_ratio"] = float(breakout_watch["confirmation_volume_ratio"])
            evidence["breakout_watch_pre_breakout_return"] = float(breakout_watch["pre_breakout_return"])

        breakout_idx = idx - 1
        if breakout_idx < 0 or not bool(data.iloc[breakout_idx]["first_close_breakout"]):
            evidence["failure_reason"] = "missing_previous_day_close_breakout"
            rows.append(evidence)
            continue

        pressure_high = float(data.iloc[breakout_idx]["pressure_high"])
        if not np.isfinite(pressure_high) or pressure_high <= 0:
            evidence["failure_reason"] = "invalid_pressure_high"
            rows.append(evidence)
            continue

        pressure_date = _pressure_high_date(data, breakout_idx, config)
        evidence["breakout_ok"] = 1
        evidence["breakout_date"] = dates[breakout_idx]
        evidence["breakout_close"] = float(data.iloc[breakout_idx]["close"])
        evidence["pressure_high"] = pressure_high
        evidence["pressure_high_date"] = pressure_date

        breakout = _breakout_attack_evidence(data, breakout_idx, config)
        evidence.update(breakout)
        if not bool(breakout["breakout_attack_ok"]):
            evidence["failure_reason"] = str(breakout["confirmation_failure_reason"])
            rows.append(evidence)
            continue

        confirmation = _next_day_shrink_bull_confirmation_evidence(data, breakout_idx, idx, pressure_high, config)
        evidence.update(confirmation)
        if not bool(confirmation["confirmation_ok"]):
            evidence["failure_reason"] = str(confirmation["confirmation_failure_reason"])
            rows.append(evidence)
            continue

        evidence[FACTOR2_SIGNAL_COLUMN] = 1.0
        evidence["failure_reason"] = "matched"
        evidence["diamond_type"] = "breakout_attack"
        active_diamond_support = pressure_high
        active_diamond_idx = idx
        rows.append(evidence)

    events = pd.DataFrame(rows, index=data.index)
    events["instrument"] = instrument
    events = events.set_index("instrument", append=True)
    events.index = events.index.set_names(["datetime", "instrument"])
    return events


def _validate_price_frame(frame: pd.DataFrame) -> None:
    required = {"open", "high", "low", "close", "volume"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"wangji factor2 detector missing columns: {sorted(missing)}")
    if not isinstance(frame.index, pd.MultiIndex):
        raise ValueError("wangji factor2 detector frame must use a MultiIndex of datetime and instrument")
    names = set(frame.index.names)
    if {"datetime", "instrument"} - names:
        raise ValueError("wangji factor2 detector index names must include datetime and instrument")


def _weekly_trend(close: pd.Series) -> pd.Series:
    short_cost = close.rolling(5, min_periods=5).mean()
    medium_cost = close.rolling(13, min_periods=13).mean()
    month_cost = close.rolling(21, min_periods=21).mean()
    return (close > month_cost) & (month_cost > month_cost.shift(5)) & (short_cost > medium_cost)


def _golden_buy_signal(data: pd.DataFrame) -> pd.Series:
    low20 = data["low"].shift(1).rolling(20, min_periods=10).min()
    return (
        (data["close"] > data["ma5"])
        & (data["close"] > data["ma13"])
        & (data["close"] > data["ma21"])
        & (data["ma5"] > data["ma13"])
        & (data["ma5"] > data["ma21"])
        & (data["ma21"] > data["ma21"].shift(3))
        & ((data["close"] / (low20 + 0.000001) - 1.0) >= 0.08)
    ).fillna(False)


def _empty_evidence(row: pd.Series) -> dict[str, object]:
    return {
        FACTOR2_SIGNAL_COLUMN: 0.0,
        FACTOR2_BREAKOUT_COLUMN: 0.0,
        "weekly_trend_ok": 0,
        "prior_golden_buy_ok": 0,
        "golden_buy_date": pd.NaT,
        "breakout_ok": 0,
        "breakout_date": pd.NaT,
        "breakout_close": float("nan"),
        "pressure_high": float("nan"),
        "pressure_high_date": pd.NaT,
        "pullback_hold_ok": 0,
        "pullback_low": float("nan"),
        "pullback_low_date": pd.NaT,
        "pullback_min_close": float("nan"),
        "pullback_depth": float("nan"),
        "pullback_pressure_undercut": float("nan"),
        "pullback_confirm_age": float("nan"),
        "pullback_volume_ratio": float("nan"),
        "pullback_failure_reason": "",
        "diamond_type": "",
        "confirmation_ok": 0,
        "confirmation_return": float("nan"),
        "confirmation_body": float("nan"),
        "confirmation_close_position": float("nan"),
        "confirmation_volume_ratio": float("nan"),
        "confirmation_failure_reason": "",
        "breakout_watch_date": pd.NaT,
        "breakout_watch_close": float("nan"),
        "breakout_watch_pressure_high": float("nan"),
        "breakout_watch_pressure_high_date": pd.NaT,
        "breakout_watch_return": float("nan"),
        "breakout_watch_body": float("nan"),
        "breakout_watch_volume_ratio": float("nan"),
        "breakout_watch_pre_breakout_return": float("nan"),
        "pre_breakout_return": float("nan"),
        "failure_reason": "",
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
    }


def _last_true_index(series: pd.Series, start: int, end: int) -> int | None:
    left = max(0, start)
    right = min(len(series) - 1, end)
    if right < left:
        return None
    values = series.iloc[left : right + 1].fillna(False).to_numpy(dtype=bool)
    true_positions = np.flatnonzero(values)
    if len(true_positions) == 0:
        return None
    return int(left + true_positions[-1])


def _pressure_high_date(data: pd.DataFrame, breakout_idx: int, config: WangjiFactor2EventConfig) -> pd.Timestamp | pd.NaT:
    end = breakout_idx - config.pressure_exclusion
    start = max(0, end - config.pressure_lookback + 1)
    if end < start:
        return pd.NaT
    window = data["high"].iloc[start : end + 1].dropna()
    if window.empty:
        return pd.NaT
    return window.idxmax()


def _active_structure_reset_reason(
    data: pd.DataFrame,
    idx: int,
    active_support: float,
    active_idx: int | None,
    config: WangjiFactor2EventConfig,
) -> str:
    row = data.iloc[idx]
    current_ma21 = float(row["ma21"]) if pd.notna(row["ma21"]) else float("nan")
    broke_prior_structure = row["close"] < active_support * config.pullback_break_tolerance
    broke_month_cost = np.isfinite(current_ma21) and row["close"] < current_ma21 * 0.97
    if broke_prior_structure or broke_month_cost:
        return "broken"

    return "active"


def _platform_pullback_confirm_evidence(
    data: pd.DataFrame,
    idx: int,
    active_idx: int,
    config: WangjiFactor2EventConfig,
) -> dict[str, object]:
    platform_window = data.iloc[active_idx + 1 : idx]
    if platform_window.empty:
        return {"platform_confirm_ok": 0}

    pressure_date = platform_window["high"].idxmax()
    pressure_idx = int(data.index.get_loc(pressure_date))
    pressure_high = float(data.loc[pressure_date, "high"])
    if pressure_idx >= idx - 1 or not np.isfinite(pressure_high) or pressure_high <= 0:
        return {"platform_confirm_ok": 0}

    pullback = data.iloc[pressure_idx + 1 : idx]
    if pullback.empty:
        return {"platform_confirm_ok": 0}

    row = data.iloc[idx]
    prev = data.iloc[idx - 1]
    prior_volume_base = data["volume"].iloc[max(0, idx - 10) : idx].mean()
    ret = float(row["close"] / prev["close"] - 1.0) if prev["close"] else float("nan")
    body = float(row["close"] / row["open"] - 1.0) if row["open"] else float("nan")
    close_position = float((row["close"] - row["low"]) / (row["high"] - row["low"] + 0.000001))
    volume_ratio = float(row["volume"] / prior_volume_base) if prior_volume_base else float("nan")
    pullback_low_date = pullback["low"].idxmin()
    pullback_low_idx = int(data.index.get_loc(pullback_low_date))
    pullback_low = float(pullback["low"].min())
    pullback_min_close = float(pullback["close"].min())
    pullback_depth = 1.0 - pullback_low / pressure_high if pressure_high else float("nan")
    pullback_confirm_age = int(idx - pullback_low_idx)
    pullback_volume_ratio = float(pullback["volume"].tail(5).mean() / prior_volume_base) if prior_volume_base else float("nan")
    current_ma21 = float(row["ma21"]) if pd.notna(row["ma21"]) else float("nan")

    support_test_ok = pullback_low <= max(pressure_high * 0.96, current_ma21 * (1.0 + config.platform_reset_ma_tolerance))
    checks = [
        (row["high"] > pressure_high, "platform_did_not_take_high"),
        (row["close"] >= pressure_high * (1.0 - config.attack_close_tolerance), "platform_close_below_high"),
        (support_test_ok, "platform_pullback_did_not_reset"),
        (pullback_depth >= config.pullback_min_depth, "platform_pullback_too_shallow"),
        (pullback_confirm_age <= config.pullback_confirm_max_age, "platform_confirmation_too_late"),
        (ret >= config.confirmation_return_min, "platform_return_too_weak"),
        (body >= config.confirmation_body_min, "platform_body_too_weak"),
        (row["close"] > row["ma5"], "platform_below_ma5"),
        (row["close"] > row["ma13"], "platform_below_ma13"),
        (close_position >= config.confirmation_close_position_min, "platform_close_not_near_high"),
        (volume_ratio >= config.confirmation_volume_ratio_min, "platform_volume_too_low"),
    ]
    failure = next((reason for ok, reason in checks if not ok), "")
    return {
        "platform_confirm_ok": int(failure == ""),
        "breakout_date": pressure_date,
        "pressure_high": pressure_high,
        "pressure_high_date": pressure_date,
        "pullback_low": pullback_low,
        "pullback_low_date": pullback_low_date,
        "pullback_min_close": pullback_min_close,
        "pullback_depth": float(pullback_depth),
        "pullback_pressure_undercut": float("nan"),
        "pullback_confirm_age": float(pullback_confirm_age),
        "pullback_volume_ratio": pullback_volume_ratio,
        "pullback_failure_reason": "",
        "confirmation_return": ret,
        "confirmation_body": body,
        "confirmation_close_position": close_position,
        "confirmation_volume_ratio": volume_ratio,
        "confirmation_failure_reason": failure,
    }


def _pullback_evidence(
    data: pd.DataFrame,
    breakout_idx: int,
    idx: int,
    pressure_high: float,
    config: WangjiFactor2EventConfig,
) -> dict[str, object]:
    if idx <= breakout_idx + 1:
        return {
            "pullback_hold_ok": 0,
            "pullback_failure_reason": "pullback_window_too_short",
        }

    pullback = data.iloc[breakout_idx + 1 : idx]
    pullback_low_date = pullback["low"].idxmin()
    pullback_low = float(pullback["low"].min())
    pullback_min_close = float(pullback["close"].min())
    pullback_high_reference = float(max(pullback["close"].max(), pullback["high"].max()))
    current_ma21 = float(data.iloc[idx]["ma21"]) if pd.notna(data.iloc[idx]["ma21"]) else float("nan")
    pullback_low_idx = int(data.index.get_loc(pullback_low_date))
    pullback_confirm_age = int(idx - pullback_low_idx)
    pullback_pressure_undercut = 1.0 - pullback_low / pressure_high if pressure_high else float("nan")

    support_ceiling = pressure_high * (1.0 + config.pullback_support_tolerance)
    if np.isfinite(current_ma21):
        support_ceiling = max(support_ceiling, current_ma21 * (1.0 + config.pullback_support_tolerance))
    support_floor = pressure_high * config.pullback_break_tolerance
    if np.isfinite(current_ma21):
        support_floor = min(support_floor, current_ma21 * 0.93)

    breakout_volume_base = data["volume"].iloc[max(0, breakout_idx - 10) : breakout_idx].mean()
    pullback_volume = pullback["volume"].tail(5).mean()
    pullback_volume_ratio = float(pullback_volume / breakout_volume_base) if breakout_volume_base else float("nan")
    pullback_depth = 1.0 - pullback_low / pullback_high_reference if pullback_high_reference else float("nan")

    checks = [
        (pullback_low <= support_ceiling, "pullback_did_not_test_support"),
        (
            pullback_pressure_undercut <= config.pullback_pressure_undercut_max,
            "pullback_too_far_below_pressure",
        ),
        (pullback_min_close >= support_floor, "pullback_broke_structure"),
        (pullback_depth >= config.pullback_min_depth, "pullback_too_shallow"),
        (pullback_confirm_age <= config.pullback_confirm_max_age, "pullback_confirmation_too_late"),
        (pullback_volume_ratio <= config.pullback_volume_ratio_max, "pullback_volume_too_hot"),
    ]
    failure = next((reason for ok, reason in checks if not ok), "")
    return {
        "pullback_hold_ok": int(failure == ""),
        "pullback_low": pullback_low,
        "pullback_low_date": pullback_low_date,
        "pullback_min_close": pullback_min_close,
        "pullback_depth": float(pullback_depth),
        "pullback_pressure_undercut": float(pullback_pressure_undercut),
        "pullback_confirm_age": float(pullback_confirm_age),
        "pullback_volume_ratio": pullback_volume_ratio,
        "pullback_failure_reason": failure,
    }


def _breakout_attack_evidence(
    data: pd.DataFrame,
    idx: int,
    config: WangjiFactor2EventConfig,
) -> dict[str, object]:
    row = data.iloc[idx]
    prev = data.iloc[idx - 1]
    pressure_high = float(row["pressure_high"])
    if not np.isfinite(pressure_high) or pressure_high <= 0:
        return {
            "breakout_attack_ok": 0,
            "pressure_high": float("nan"),
            "pressure_high_date": pd.NaT,
        }

    prior_volume_base = data["volume"].iloc[max(0, idx - 10) : idx].mean()
    ret = float(row["close"] / prev["close"] - 1.0) if prev["close"] else float("nan")
    body = float(row["close"] / row["open"] - 1.0) if row["open"] else float("nan")
    close_position = float((row["close"] - row["low"]) / (row["high"] - row["low"] + 0.000001))
    volume_ratio = float(row["volume"] / prior_volume_base) if prior_volume_base else float("nan")
    pre_breakout_return = _pre_breakout_return(data, idx, config)

    checks = [
        (bool(row["first_close_breakout"]), "attack_not_first_close_breakout"),
        (
            (not np.isfinite(pre_breakout_return)) or pre_breakout_return <= config.pre_breakout_return_max,
            "attack_prior_four_weeks_already_extended",
        ),
        (row["close"] > pressure_high, "attack_close_below_pressure"),
        (ret >= config.attack_return_min, "attack_return_too_weak"),
        (ret <= config.attack_return_max, "attack_return_too_hot"),
        (body >= config.attack_body_min, "attack_body_too_weak"),
        (row["close"] > row["ma5"], "attack_below_ma5"),
        (row["close"] > row["ma13"], "attack_below_ma13"),
        (close_position >= config.confirmation_close_position_min, "attack_close_not_near_high"),
        (volume_ratio >= config.confirmation_volume_ratio_min, "attack_volume_too_low"),
    ]
    failure = next((reason for ok, reason in checks if not ok), "")
    return {
        "breakout_attack_ok": int(failure == ""),
        "pressure_high": pressure_high,
        "pressure_high_date": _pressure_high_date(data, idx, config),
        "confirmation_return": ret,
        "confirmation_body": body,
        "confirmation_close_position": close_position,
        "confirmation_volume_ratio": volume_ratio,
        "pre_breakout_return": pre_breakout_return,
        "confirmation_failure_reason": failure,
    }


def _pre_breakout_return(data: pd.DataFrame, idx: int, config: WangjiFactor2EventConfig) -> float:
    if idx <= 0:
        return float("nan")
    start = idx - config.pre_breakout_lookback
    if start < 0:
        return float("nan")
    base = float(data.iloc[start]["close"])
    prev_close = float(data.iloc[idx - 1]["close"])
    if not np.isfinite(base) or base <= 0 or not np.isfinite(prev_close):
        return float("nan")
    return prev_close / base - 1.0


def _next_day_shrink_bull_confirmation_evidence(
    data: pd.DataFrame,
    breakout_idx: int,
    idx: int,
    pressure_high: float,
    config: WangjiFactor2EventConfig,
) -> dict[str, object]:
    breakout = data.iloc[breakout_idx]
    row = data.iloc[idx]
    prev = data.iloc[idx - 1]

    ret = float(row["close"] / prev["close"] - 1.0) if prev["close"] else float("nan")
    body = float(row["close"] / row["open"] - 1.0) if row["open"] else float("nan")
    close_position = float((row["close"] - row["low"]) / (row["high"] - row["low"] + 0.000001))
    volume_ratio = float(row["volume"] / breakout["volume"]) if breakout["volume"] else float("nan")

    checks = [
        (idx == breakout_idx + 1, "confirmation_not_next_trading_day"),
        (row["close"] > row["open"], "confirmation_not_bullish_candle"),
        (row["volume"] < breakout["volume"], "confirmation_volume_not_shrinking"),
        (row["close"] >= pressure_high * (1.0 - config.attack_close_tolerance), "confirmation_lost_pressure"),
        (row["close"] >= breakout["close"], "confirmation_close_below_breakout_close"),
        (row["close"] > row["ma5"], "confirmation_below_ma5"),
        (row["close"] > row["ma13"], "confirmation_below_ma13"),
    ]
    failure = next((reason for ok, reason in checks if not ok), "")
    return {
        "confirmation_ok": int(failure == ""),
        "confirmation_return": ret,
        "confirmation_body": body,
        "confirmation_close_position": close_position,
        "confirmation_volume_ratio": volume_ratio,
        "confirmation_failure_reason": failure,
    }


def _confirmation_evidence(
    data: pd.DataFrame,
    idx: int,
    config: WangjiFactor2EventConfig,
) -> dict[str, object]:
    row = data.iloc[idx]
    prev = data.iloc[idx - 1]
    local_start = max(0, idx - config.local_break_lookback)
    local_close_high = float(data["close"].iloc[local_start:idx].max())
    prior_volume_base = data["volume"].iloc[max(0, idx - 10) : idx].mean()

    ret = float(row["close"] / prev["close"] - 1.0) if prev["close"] else float("nan")
    body = float(row["close"] / row["open"] - 1.0) if row["open"] else float("nan")
    close_position = float((row["close"] - row["low"]) / (row["high"] - row["low"] + 0.000001))
    volume_ratio = float(row["volume"] / prior_volume_base) if prior_volume_base else float("nan")

    checks = [
        (ret >= config.confirmation_return_min, "confirmation_return_too_weak"),
        (body >= config.confirmation_body_min, "confirmation_body_too_weak"),
        (row["close"] > row["ma5"], "confirmation_below_ma5"),
        (row["close"] > row["ma13"], "confirmation_below_ma13"),
        (close_position >= config.confirmation_close_position_min, "confirmation_close_not_near_high"),
        (volume_ratio >= config.confirmation_volume_ratio_min, "confirmation_volume_too_low"),
        (row["close"] > local_close_high, "confirmation_no_local_close_break"),
    ]
    failure = next((reason for ok, reason in checks if not ok), "")
    return {
        "confirmation_ok": int(failure == ""),
        "confirmation_return": ret,
        "confirmation_body": body,
        "confirmation_close_position": close_position,
        "confirmation_volume_ratio": volume_ratio,
        "confirmation_failure_reason": failure,
    }
