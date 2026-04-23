from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import load_yaml


@dataclass(frozen=True)
class OrderConfig:
    total_equity: float = 1_000_000.0
    min_order_value: float = 1_000.0
    weight_tolerance: float = 1e-6


def load_order_config(path: str | Path) -> OrderConfig:
    data = load_yaml(path)
    raw = data.get("orders", data)
    return OrderConfig(
        total_equity=float(raw.get("total_equity", 1_000_000.0)),
        min_order_value=float(raw.get("min_order_value", 1_000.0)),
        weight_tolerance=float(raw.get("weight_tolerance", 1e-6)),
    )


def build_order_suggestions(
    target_portfolio: pd.DataFrame,
    current_positions: pd.DataFrame | None,
    config: OrderConfig = OrderConfig(),
) -> pd.DataFrame:
    target_weights = _weights(target_portfolio, "target_weight")
    current_weights = _weights(current_positions, "current_weight")
    instruments = sorted(set(target_weights) | set(current_weights))
    run_date = _run_date(target_portfolio, current_positions)
    rows = []
    for instrument in instruments:
        current_weight = current_weights.get(instrument, 0.0)
        target_weight = target_weights.get(instrument, 0.0)
        delta_weight = target_weight - current_weight
        order_value = abs(delta_weight) * config.total_equity
        if abs(delta_weight) <= config.weight_tolerance:
            continue
        if order_value < config.min_order_value:
            continue
        rows.append(
            {
                "date": run_date,
                "instrument": instrument,
                "side": "BUY" if delta_weight > 0 else "SELL",
                "current_weight": current_weight,
                "target_weight": target_weight,
                "delta_weight": delta_weight,
                "order_value": order_value,
                "status": "suggested",
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "date",
            "instrument",
            "side",
            "current_weight",
            "target_weight",
            "delta_weight",
            "order_value",
            "status",
        ],
    )


def write_orders(orders: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    orders.to_csv(output, index=False)
    return output


def _weights(frame: pd.DataFrame | None, column: str) -> dict[str, float]:
    if frame is None or frame.empty or column not in frame.columns:
        return {}
    return {str(row["instrument"]): float(row[column]) for _, row in frame.iterrows()}


def _run_date(target_portfolio: pd.DataFrame, current_positions: pd.DataFrame | None) -> str:
    if "date" in target_portfolio.columns and not target_portfolio.empty:
        return str(target_portfolio["date"].max())
    if current_positions is not None and "date" in current_positions.columns and not current_positions.empty:
        return str(current_positions["date"].max())
    return ""
