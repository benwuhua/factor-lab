from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd

from .paper_broker import PaperFillConfig, simulate_paper_fills
from .reconcile import ReconcileConfig, ReconcileReport, reconcile_positions
from .state import apply_fills_to_positions


class BrokerDisabledError(RuntimeError):
    """Raised when a disabled execution adapter is asked to submit orders."""


class BrokerAdapter(Protocol):
    run_id: str

    def validate_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        ...

    def submit_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        ...

    def cancel_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        ...

    def fetch_fills(self, orders: pd.DataFrame) -> pd.DataFrame:
        ...

    def fetch_positions(self, current_positions: pd.DataFrame, fills: pd.DataFrame) -> pd.DataFrame:
        ...

    def fetch_cash(self, fills: pd.DataFrame) -> dict[str, float]:
        ...

    def reconcile(self, expected_positions: pd.DataFrame, actual_positions: pd.DataFrame) -> ReconcileReport:
        ...


@dataclass(frozen=True)
class PaperBrokerAdapter:
    run_id: str
    fill_config: PaperFillConfig = PaperFillConfig()
    reconcile_config: ReconcileConfig = ReconcileConfig()

    def validate_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        validated = _with_identity(orders, self.run_id)
        if "status" not in validated.columns:
            validated["status"] = "validated"
        else:
            validated["status"] = "validated"
        if "validation_reason" not in validated.columns:
            validated["validation_reason"] = ""
        return validated

    def submit_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        submitted = _with_identity(orders, self.run_id)
        submitted["status"] = "submitted"
        return submitted

    def cancel_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        cancelled = _with_identity(orders, self.run_id)
        cancelled["status"] = "cancelled"
        return cancelled

    def fetch_fills(self, orders: pd.DataFrame) -> pd.DataFrame:
        submitted = _with_identity(orders, self.run_id)
        fills = simulate_paper_fills(submitted, self.fill_config)
        if fills.empty:
            return _empty_fills()
        fills.insert(0, "order_id", list(submitted["order_id"]))
        fills.insert(1, "audit_id", list(submitted["audit_id"]))
        return fills

    def fetch_positions(self, current_positions: pd.DataFrame, fills: pd.DataFrame) -> pd.DataFrame:
        return apply_fills_to_positions(current_positions, fills)

    def fetch_cash(self, fills: pd.DataFrame) -> dict[str, float]:
        if fills.empty or "net_cash_effect" not in fills.columns:
            return {"net_cash_effect": 0.0}
        return {"net_cash_effect": float(fills["net_cash_effect"].sum())}

    def reconcile(self, expected_positions: pd.DataFrame, actual_positions: pd.DataFrame) -> ReconcileReport:
        return reconcile_positions(expected_positions, actual_positions, self.reconcile_config)


@dataclass(frozen=True)
class DryRunBrokerAdapter(PaperBrokerAdapter):
    def fetch_fills(self, orders: pd.DataFrame) -> pd.DataFrame:
        return _empty_fills()


@dataclass(frozen=True)
class ManualTicketBrokerAdapter(PaperBrokerAdapter):
    def submit_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        submitted = _with_identity(orders, self.run_id)
        submitted["status"] = "manual_ticket"
        return submitted


@dataclass(frozen=True)
class RealBrokerAdapter(PaperBrokerAdapter):
    enabled: bool = False

    def submit_orders(self, orders: pd.DataFrame) -> pd.DataFrame:
        if not self.enabled:
            raise BrokerDisabledError("real broker adapter is disabled by default")
        return super().submit_orders(orders)


def load_broker_adapter(data: dict, run_id: str) -> BrokerAdapter:
    raw = data.get("broker_adapter", data)
    mode = str(raw.get("mode", "paper")).strip().lower()
    fill_config = _paper_fill_config(data)
    reconcile_config = _reconcile_config(data)
    if mode == "paper":
        return PaperBrokerAdapter(run_id=run_id, fill_config=fill_config, reconcile_config=reconcile_config)
    if mode == "dry_run":
        return DryRunBrokerAdapter(run_id=run_id, fill_config=fill_config, reconcile_config=reconcile_config)
    if mode == "manual_ticket":
        return ManualTicketBrokerAdapter(run_id=run_id, fill_config=fill_config, reconcile_config=reconcile_config)
    if mode == "real":
        return RealBrokerAdapter(
            run_id=run_id,
            fill_config=fill_config,
            reconcile_config=reconcile_config,
            enabled=bool(raw.get("enabled", raw.get("real_enabled", False))),
        )
    raise ValueError(f"unsupported broker adapter mode: {mode}")


def _with_identity(orders: pd.DataFrame, run_id: str) -> pd.DataFrame:
    frame = orders.copy()
    if "order_id" not in frame.columns:
        frame.insert(0, "order_id", [_order_id(run_id, index) for index in range(len(frame))])
    if "audit_id" not in frame.columns:
        frame.insert(1, "audit_id", [f"{run_id}:{order_id}" for order_id in frame["order_id"]])
    return frame


def _order_id(run_id: str, index: int) -> str:
    return f"{run_id}-{index + 1:06d}"


def _paper_fill_config(data: dict) -> PaperFillConfig:
    raw = data.get("paper_broker", {})
    return PaperFillConfig(
        fill_ratio=float(raw.get("fill_ratio", 1.0)),
        slippage_bps=float(raw.get("slippage_bps", 0.0)),
        commission_bps=float(raw.get("commission_bps", 0.0)),
        stamp_tax_bps=float(raw.get("stamp_tax_bps", 0.0)),
        min_trade_value=float(raw.get("min_trade_value", 0.0)),
    )


def _reconcile_config(data: dict) -> ReconcileConfig:
    raw = data.get("reconcile", {})
    return ReconcileConfig(weight_tolerance=float(raw.get("weight_tolerance", 1e-4)))


def _empty_fills() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "order_id",
            "audit_id",
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
        ]
    )
