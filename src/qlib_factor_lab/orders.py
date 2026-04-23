from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_yaml


@dataclass(frozen=True)
class OrderConfig:
    total_equity: float = 1_000_000.0
    min_order_value: float = 1_000.0
    weight_tolerance: float = 1e-6
    lot_size: int = 0
    price_column: str = "last_price"
    metadata_columns: tuple[str, ...] = (
        "tradable",
        "suspended",
        "limit_up",
        "limit_down",
        "buy_blocked",
        "sell_blocked",
    )


def load_order_config(path: str | Path) -> OrderConfig:
    data = load_yaml(path)
    raw = data.get("orders", data)
    return OrderConfig(
        total_equity=float(raw.get("total_equity", 1_000_000.0)),
        min_order_value=float(raw.get("min_order_value", 1_000.0)),
        weight_tolerance=float(raw.get("weight_tolerance", 1e-6)),
        lot_size=int(raw.get("lot_size", 0)),
        price_column=str(raw.get("price_column", "last_price")),
        metadata_columns=tuple(
            str(column)
            for column in raw.get(
                "metadata_columns",
                ["tradable", "suspended", "limit_up", "limit_down", "buy_blocked", "sell_blocked"],
            )
        ),
    )


def build_order_suggestions(
    target_portfolio: pd.DataFrame,
    current_positions: pd.DataFrame | None,
    config: OrderConfig = OrderConfig(),
) -> pd.DataFrame:
    target_weights = _weights(target_portfolio, "target_weight")
    current_weights = _weights(current_positions, "current_weight")
    target_prices = _values(target_portfolio, config.price_column)
    current_prices = _values(current_positions, config.price_column)
    target_metadata = _metadata_by_instrument(target_portfolio, config.metadata_columns)
    current_metadata = _metadata_by_instrument(current_positions, config.metadata_columns)
    instruments = sorted(set(target_weights) | set(current_weights))
    run_date = _run_date(target_portfolio, current_positions)
    rows = []
    for instrument in instruments:
        current_weight = current_weights.get(instrument, 0.0)
        target_weight = target_weights.get(instrument, 0.0)
        requested_delta_weight = target_weight - current_weight
        requested_order_value = abs(requested_delta_weight) * config.total_equity
        if abs(requested_delta_weight) <= config.weight_tolerance:
            continue
        side = "BUY" if requested_delta_weight > 0 else "SELL"
        price = _order_price(instrument, side, target_prices, current_prices)
        order_value, order_shares = _rounded_order_value(requested_order_value, price, config)
        if order_value < config.min_order_value:
            continue
        delta_weight = (1 if requested_delta_weight > 0 else -1) * order_value / config.total_equity
        order = {
                "date": run_date,
                "instrument": instrument,
                "side": side,
                "current_weight": current_weight,
                "target_weight": target_weight,
                "requested_delta_weight": requested_delta_weight,
                "delta_weight": delta_weight,
                "requested_order_value": requested_order_value,
                "order_value": order_value,
                "price": price,
                "order_shares": order_shares,
                "total_equity": config.total_equity,
                "status": "suggested",
            }
        order.update(_order_metadata(instrument, side, target_metadata, current_metadata, config.metadata_columns))
        rows.append(order)
    columns = [
        "date",
        "instrument",
        "side",
        "current_weight",
        "target_weight",
        "requested_delta_weight",
        "delta_weight",
        "requested_order_value",
        "order_value",
        "price",
        "order_shares",
        "total_equity",
        *config.metadata_columns,
        "status",
    ]
    return pd.DataFrame(
        rows,
        columns=columns,
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


def _values(frame: pd.DataFrame | None, column: str) -> dict[str, float]:
    if frame is None or frame.empty or column not in frame.columns:
        return {}
    values = {}
    for _, row in frame.iterrows():
        value = _float_or_nan(row[column])
        if math.isfinite(value) and value > 0:
            values[str(row["instrument"])] = value
    return values


def _order_price(
    instrument: str,
    side: str,
    target_prices: dict[str, float],
    current_prices: dict[str, float],
) -> float:
    primary = target_prices if side == "BUY" else current_prices
    fallback = current_prices if side == "BUY" else target_prices
    return primary.get(instrument, fallback.get(instrument, float("nan")))


def _metadata_by_instrument(frame: pd.DataFrame | None, columns: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    present = [column for column in columns if column in frame.columns]
    if not present:
        return {}
    return {
        str(row["instrument"]): {column: row[column] for column in present}
        for _, row in frame.iterrows()
    }


def _order_metadata(
    instrument: str,
    side: str,
    target_metadata: dict[str, dict[str, Any]],
    current_metadata: dict[str, dict[str, Any]],
    columns: tuple[str, ...],
) -> dict[str, Any]:
    primary = target_metadata if side == "BUY" else current_metadata
    fallback = current_metadata if side == "BUY" else target_metadata
    values = dict(fallback.get(instrument, {}))
    values.update(primary.get(instrument, {}))
    defaults = {
        "tradable": True,
        "suspended": False,
        "limit_up": False,
        "limit_down": False,
        "buy_blocked": False,
        "sell_blocked": False,
    }
    return {column: values.get(column, defaults.get(column, "")) for column in columns}


def _rounded_order_value(requested_value: float, price: float, config: OrderConfig) -> tuple[float, float]:
    if config.lot_size <= 0 or not math.isfinite(price) or price <= 0:
        return requested_value, float("nan")
    shares = math.floor(requested_value / price / config.lot_size) * config.lot_size
    if shares <= 0:
        return 0.0, 0.0
    return shares * price, float(shares)


def _float_or_nan(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _run_date(target_portfolio: pd.DataFrame, current_positions: pd.DataFrame | None) -> str:
    if "date" in target_portfolio.columns and not target_portfolio.empty:
        return str(target_portfolio["date"].max())
    if current_positions is not None and "date" in current_positions.columns and not current_positions.empty:
        return str(current_positions["date"].max())
    return ""
