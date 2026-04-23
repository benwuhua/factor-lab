from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .config import load_yaml


@dataclass(frozen=True)
class PaperFillConfig:
    fill_ratio: float = 1.0
    slippage_bps: float = 0.0
    commission_bps: float = 0.0
    stamp_tax_bps: float = 0.0


def load_paper_fill_config(path: str | Path) -> PaperFillConfig:
    data = load_yaml(path)
    raw = data.get("paper_broker", data)
    return PaperFillConfig(
        fill_ratio=float(raw.get("fill_ratio", 1.0)),
        slippage_bps=float(raw.get("slippage_bps", 0.0)),
        commission_bps=float(raw.get("commission_bps", 0.0)),
        stamp_tax_bps=float(raw.get("stamp_tax_bps", 0.0)),
    )


def simulate_paper_fills(orders: pd.DataFrame, config: PaperFillConfig = PaperFillConfig()) -> pd.DataFrame:
    fill_ratio = max(0.0, min(1.0, config.fill_ratio))
    rows = []
    for index, row in orders.iterrows():
        side = str(row["side"])
        total_equity = float(row.get("total_equity", 1.0))
        reference_value = float(row["order_value"]) * fill_ratio
        reference_price = _float_or_nan(row.get("price"))
        order_shares = _float_or_nan(row.get("order_shares"))
        fill_shares = order_shares * fill_ratio if math.isfinite(order_shares) else float("nan")
        execution_price = _execution_price(reference_price, side, config)
        fill_value = _fill_value(reference_value, fill_shares, execution_price)
        transaction_cost = _transaction_cost(fill_value, side, config)
        fill_delta_weight = (1 if side == "BUY" else -1) * fill_value / total_equity if total_equity else 0.0
        net_cash_effect = -fill_value - transaction_cost if side == "BUY" else fill_value - transaction_cost
        status = "filled" if fill_ratio >= 1.0 else "partial" if fill_ratio > 0 else "rejected"
        rows.append(
            {
                "fill_id": index + 1,
                "date": row.get("date", ""),
                "instrument": row["instrument"],
                "side": side,
                "order_delta_weight": float(row["delta_weight"]),
                "fill_delta_weight": fill_delta_weight,
                "order_value": float(row["order_value"]),
                "fill_value": fill_value,
                "price": reference_price,
                "execution_price": execution_price,
                "fill_shares": fill_shares,
                "transaction_cost": transaction_cost,
                "net_cash_effect": net_cash_effect,
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
            "price",
            "execution_price",
            "fill_shares",
            "transaction_cost",
            "net_cash_effect",
            "status",
            "reject_reason",
        ],
    )


def write_fills(fills: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fills.to_csv(output, index=False)
    return output


def _execution_price(price: float, side: str, config: PaperFillConfig) -> float:
    if not math.isfinite(price) or price <= 0:
        return float("nan")
    slippage = config.slippage_bps / 10000.0
    return price * (1 + slippage if side == "BUY" else 1 - slippage)


def _fill_value(reference_value: float, fill_shares: float, execution_price: float) -> float:
    if math.isfinite(fill_shares) and math.isfinite(execution_price):
        return fill_shares * execution_price
    return reference_value


def _transaction_cost(fill_value: float, side: str, config: PaperFillConfig) -> float:
    bps = config.commission_bps
    if side == "SELL":
        bps += config.stamp_tax_bps
    return fill_value * bps / 10000.0


def _float_or_nan(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")
