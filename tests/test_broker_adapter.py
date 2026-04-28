from __future__ import annotations

import unittest

import pandas as pd

from qlib_factor_lab.broker_adapter import (
    BrokerDisabledError,
    DryRunBrokerAdapter,
    ManualTicketBrokerAdapter,
    PaperBrokerAdapter,
    RealBrokerAdapter,
    load_broker_adapter,
)
from qlib_factor_lab.paper_broker import PaperFillConfig
from qlib_factor_lab.reconcile import ReconcileConfig


class BrokerAdapterTest(unittest.TestCase):
    def test_paper_adapter_validates_orders_with_audit_ids(self) -> None:
        adapter = PaperBrokerAdapter(run_id="20260423-paper")

        validated = adapter.validate_orders(self._orders())

        self.assertEqual(list(validated["status"]), ["validated", "validated"])
        self.assertEqual(list(validated["order_id"]), ["20260423-paper-000001", "20260423-paper-000002"])
        self.assertTrue(validated["audit_id"].str.startswith("20260423-paper:").all())

    def test_paper_adapter_submits_and_reconciles_positions(self) -> None:
        adapter = PaperBrokerAdapter(
            run_id="20260423-paper",
            fill_config=PaperFillConfig(fill_ratio=1.0, commission_bps=1.0),
            reconcile_config=ReconcileConfig(weight_tolerance=0.0001),
        )
        current = pd.DataFrame(
            {
                "date": ["2026-04-23"],
                "instrument": ["BBB"],
                "current_weight": [0.10],
            }
        )

        submitted = adapter.submit_orders(adapter.validate_orders(self._orders()))
        fills = adapter.fetch_fills(submitted)
        positions = adapter.fetch_positions(current, fills)
        reconciliation = adapter.reconcile(positions, positions.copy())

        self.assertEqual(list(submitted["status"]), ["submitted", "submitted"])
        self.assertEqual(list(fills["order_id"]), ["20260423-paper-000001", "20260423-paper-000002"])
        self.assertIn("current_weight", positions.columns)
        self.assertTrue(reconciliation.passed)

    def test_dry_run_adapter_never_creates_fills(self) -> None:
        adapter = DryRunBrokerAdapter(run_id="20260423-dry")

        fills = adapter.fetch_fills(adapter.submit_orders(adapter.validate_orders(self._orders())))

        self.assertTrue(fills.empty)
        self.assertIn("status", fills.columns)

    def test_real_adapter_is_disabled_by_default(self) -> None:
        adapter = RealBrokerAdapter(run_id="20260423-real")

        with self.assertRaises(BrokerDisabledError):
            adapter.submit_orders(adapter.validate_orders(self._orders()))

    def test_load_broker_adapter_uses_execution_config_mode(self) -> None:
        self.assertIsInstance(load_broker_adapter({"broker_adapter": {"mode": "paper"}}, "run"), PaperBrokerAdapter)
        self.assertIsInstance(load_broker_adapter({"broker_adapter": {"mode": "dry_run"}}, "run"), DryRunBrokerAdapter)
        self.assertIsInstance(load_broker_adapter({"broker_adapter": {"mode": "manual_ticket"}}, "run"), ManualTicketBrokerAdapter)

        real = load_broker_adapter({"broker_adapter": {"mode": "real"}}, "run")
        self.assertIsInstance(real, RealBrokerAdapter)
        with self.assertRaises(BrokerDisabledError):
            real.submit_orders(real.validate_orders(self._orders()))

    def _orders(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "BBB"],
                "side": ["BUY", "SELL"],
                "delta_weight": [0.05, -0.03],
                "order_value": [5000.0, 3000.0],
                "price": [10.0, 20.0],
                "order_shares": [500.0, 150.0],
                "total_equity": [100000.0, 100000.0],
                "tradable": [True, True],
                "suspended": [False, False],
                "limit_up": [False, False],
                "limit_down": [False, False],
                "buy_blocked": [False, False],
                "sell_blocked": [False, False],
                "status": ["suggested", "suggested"],
            }
        )


if __name__ == "__main__":
    unittest.main()
