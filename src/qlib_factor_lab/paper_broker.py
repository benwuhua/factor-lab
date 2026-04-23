from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import load_yaml


@dataclass(frozen=True)
class PaperFillConfig:
    fill_ratio: float = 1.0


def load_paper_fill_config(path: str | Path) -> PaperFillConfig:
    data = load_yaml(path)
    raw = data.get("paper_broker", data)
    return PaperFillConfig(fill_ratio=float(raw.get("fill_ratio", 1.0)))


def simulate_paper_fills(orders: pd.DataFrame, config: PaperFillConfig = PaperFillConfig()) -> pd.DataFrame:
    fill_ratio = max(0.0, min(1.0, config.fill_ratio))
    rows = []
    for index, row in orders.iterrows():
        fill_delta_weight = float(row["delta_weight"]) * fill_ratio
        status = "filled" if fill_ratio >= 1.0 else "partial" if fill_ratio > 0 else "rejected"
        rows.append(
            {
                "fill_id": index + 1,
                "date": row.get("date", ""),
                "instrument": row["instrument"],
                "side": row["side"],
                "order_delta_weight": float(row["delta_weight"]),
                "fill_delta_weight": fill_delta_weight,
                "order_value": float(row["order_value"]),
                "fill_value": float(row["order_value"]) * fill_ratio,
                "status": status,
                "reject_reason": "" if fill_ratio > 0 else "zero_fill_ratio",
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "fill_id",
            "date",
            "instrument",
            "side",
            "order_delta_weight",
            "fill_delta_weight",
            "order_value",
            "fill_value",
            "status",
            "reject_reason",
        ],
    )


def write_fills(fills: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fills.to_csv(output, index=False)
    return output
