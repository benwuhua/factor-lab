from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .config import ProjectConfig
from .qlib_bootstrap import init_qlib


EXECUTION_CALENDAR_COLUMNS = [
    "date",
    "instrument",
    "tradable",
    "suspended",
    "limit_up",
    "limit_down",
    "buy_blocked",
    "sell_blocked",
]


def fetch_execution_calendar_features(project_config: ProjectConfig, run_date: str) -> pd.DataFrame:
    init_qlib(project_config)
    from qlib.data import D

    frame = D.features(
        D.instruments(project_config.market),
        ["$close", "Ref($close,1)", "$amount", "$volume"],
        start_time=run_date,
        end_time=run_date,
        freq=project_config.freq,
    )
    frame.columns = ["close", "prev_close", "amount", "volume"]
    frame = frame.reset_index()
    if "datetime" in frame.columns:
        frame = frame.rename(columns={"datetime": "date"})
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    return frame.loc[:, ["date", "instrument", "close", "prev_close", "amount", "volume"]]


def build_execution_calendar(
    features: pd.DataFrame,
    *,
    limit_up_pct: float = 0.098,
    limit_down_pct: float = -0.098,
) -> pd.DataFrame:
    required = {"date", "instrument", "close", "prev_close", "amount", "volume"}
    missing = required - set(features.columns)
    if missing:
        raise ValueError(f"features are missing columns: {sorted(missing)}")

    frame = features.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    for column in ["close", "prev_close", "amount", "volume"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["suspended"] = frame.apply(_suspended, axis=1)
    pct_change = frame["close"] / frame["prev_close"] - 1.0
    frame["limit_up"] = (~frame["suspended"]) & pct_change.ge(limit_up_pct)
    frame["limit_down"] = (~frame["suspended"]) & pct_change.le(limit_down_pct)
    frame["tradable"] = ~frame["suspended"]
    frame["buy_blocked"] = frame["suspended"] | frame["limit_up"]
    frame["sell_blocked"] = frame["suspended"] | frame["limit_down"]
    return frame.loc[:, EXECUTION_CALENDAR_COLUMNS].reset_index(drop=True)


def write_execution_calendar(calendar: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    calendar.loc[:, EXECUTION_CALENDAR_COLUMNS].to_csv(output, index=False)
    return output


def _suspended(row: pd.Series) -> bool:
    close = row.get("close")
    amount = row.get("amount")
    volume = row.get("volume")
    return _missing_or_nonpositive(close) or _missing_or_nonpositive(amount) or _missing_or_nonpositive(volume)


def _missing_or_nonpositive(value: Any) -> bool:
    return pd.isna(value) or float(value) <= 0.0
