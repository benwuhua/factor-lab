from __future__ import annotations

import pandas as pd


def signal_universe(signal: pd.DataFrame, eligible_only: bool = False) -> list[str]:
    frame = signal
    if eligible_only and "eligible" in frame.columns:
        frame = frame[frame["eligible"]]
    if "instrument" not in frame.columns:
        return []
    return sorted(frame["instrument"].astype(str).unique())
