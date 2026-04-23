from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_yaml


@dataclass(frozen=True)
class TradabilityConfig:
    require_tradable: bool = True
    min_amount_20d: float = 0.0
    liquidity_column: str = "amount_20d"
    blocked_risk_flags: tuple[str, ...] = ("not_tradable", "limit_locked")


def load_trading_config(path: str | Path) -> TradabilityConfig:
    data = load_yaml(path)
    raw = data.get("tradability", data)
    return TradabilityConfig(
        require_tradable=bool(raw.get("require_tradable", True)),
        min_amount_20d=float(raw.get("min_amount_20d", 0.0)),
        liquidity_column=str(raw.get("liquidity_column", "amount_20d")),
        blocked_risk_flags=tuple(str(flag) for flag in raw.get("blocked_risk_flags", ["not_tradable", "limit_locked"])),
    )


def apply_tradability_filter(signal: pd.DataFrame, config: TradabilityConfig = TradabilityConfig()) -> pd.DataFrame:
    frame = signal.copy()
    if "risk_flags" in frame.columns:
        frame["risk_flags"] = frame["risk_flags"].fillna("")
    reasons = []
    eligible = []
    for _, row in frame.iterrows():
        row_reasons = _rejection_reasons(row, config)
        reasons.append(";".join(row_reasons))
        eligible.append(not row_reasons)
    frame["eligible"] = eligible
    frame["rejection_reason"] = reasons
    if "ensemble_score" in frame.columns:
        frame = frame.sort_values("ensemble_score", ascending=False)
    return frame.reset_index(drop=True)


def _rejection_reasons(row: pd.Series, config: TradabilityConfig) -> list[str]:
    reasons: list[str] = []
    if config.require_tradable and not _bool_value(row.get("tradable", False)):
        reasons.append("not_tradable")

    flags = _split_flags(row.get("risk_flags", ""))
    for flag in config.blocked_risk_flags:
        if flag in flags:
            reasons.append(f"risk_flag:{flag}")

    if config.min_amount_20d > 0:
        if config.liquidity_column not in row:
            reasons.append("missing_liquidity")
        else:
            amount = pd.to_numeric(pd.Series([row.get(config.liquidity_column)]), errors="coerce").iloc[0]
            if pd.isna(amount) or float(amount) < config.min_amount_20d:
                reasons.append("low_liquidity")
    return _dedupe(reasons)


def _split_flags(value: Any) -> set[str]:
    if pd.isna(value):
        return set()
    return {part.strip() for part in str(value).split(";") if part.strip()}


def _bool_value(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "nan"}
    return bool(value)


def _dedupe(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
