from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


SECURITY_MASTER_COLUMNS = [
    "instrument",
    "name",
    "exchange",
    "board",
    "industry_sw",
    "industry_csrc",
    "is_st",
    "listing_date",
    "delisting_date",
    "valid_from",
    "valid_to",
]


def load_security_master(path: str | Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame(columns=SECURITY_MASTER_COLUMNS)

    security_master_path = Path(path)
    if not security_master_path.exists():
        return pd.DataFrame(columns=SECURITY_MASTER_COLUMNS)

    return pd.read_csv(security_master_path)


def enrich_with_security_master(signal: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
    frame = signal.copy()
    for column in SECURITY_MASTER_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA

    missing = []
    for index, row in frame.iterrows():
        master_row = _select_security_master_row(row, master)
        if master_row is None:
            missing.append(True)
            for column in SECURITY_MASTER_COLUMNS:
                if column == "instrument":
                    continue
                frame.at[index, column] = pd.NA
            continue

        missing.append(False)
        for column in SECURITY_MASTER_COLUMNS:
            if column == "instrument":
                continue
            frame.at[index, column] = master_row.get(column, pd.NA)

    frame["security_master_missing"] = missing
    return frame


def _select_security_master_row(signal_row: pd.Series, master: pd.DataFrame) -> pd.Series | None:
    if master.empty:
        return None

    signal_date = pd.to_datetime(signal_row["date"])
    valid_rows = []
    for _, master_row in master.iterrows():
        if master_row.get("instrument") != signal_row["instrument"]:
            continue
        if not _valid_on_date(master_row, signal_date):
            continue
        valid_rows.append(master_row)

    if not valid_rows:
        return None

    return max(valid_rows, key=lambda row: pd.to_datetime(row.get("valid_from")))


def _valid_on_date(master_row: pd.Series, signal_date: pd.Timestamp) -> bool:
    valid_from = pd.to_datetime(master_row.get("valid_from"))
    if pd.isna(valid_from) or valid_from > signal_date:
        return False

    valid_to = master_row.get("valid_to")
    if _is_blank(valid_to):
        return True

    return signal_date <= pd.to_datetime(valid_to)


def _is_blank(value: Any) -> bool:
    return pd.isna(value) or str(value).strip() == ""
