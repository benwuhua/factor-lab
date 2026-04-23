import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.manual_live import ManualTicketConfig, build_manual_order_ticket, write_manual_order_ticket


class ManualLiveTests(unittest.TestCase):
    def test_build_manual_order_ticket_marks_rejected_and_review_required_orders(self):
        orders = pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "BBB"],
                "side": ["BUY", "SELL"],
                "order_value": [10000.0, 5000.0],
                "order_shares": [1000, 500],
                "price": [10.0, 10.0],
            }
        )
        fills = pd.DataFrame(
            {
                "instrument": ["AAA", "BBB"],
                "status": ["filled", "rejected"],
                "reject_reason": [pd.NA, "limit_down_sell_blocked"],
            }
        )

        ticket = build_manual_order_ticket(orders, fills)

        self.assertEqual(list(ticket["action"]), ["BUY", "REVIEW"])
        self.assertEqual(ticket.loc[1, "review_reason"], "limit_down_sell_blocked")

    def test_build_manual_order_ticket_applies_pretrade_checks(self):
        orders = pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "BBB", "CCC"],
                "side": ["BUY", "BUY", "SELL"],
                "order_value": [12000.0, 6000.0, 3000.0],
                "order_shares": [1200, 600, 300],
                "price": [10.0, 10.0, 10.0],
            }
        )

        ticket = build_manual_order_ticket(
            orders,
            pretrade_config=ManualTicketConfig(
                available_cash=10_000.0,
                banned_instruments=("BBB",),
                max_order_value=8_000.0,
                allow_sells=False,
            ),
        )

        by_instrument = ticket.set_index("instrument")
        self.assertEqual(by_instrument.loc["AAA", "action"], "REVIEW")
        self.assertIn("insufficient_cash", by_instrument.loc["AAA", "pretrade_reason"])
        self.assertIn("above_max_order_value", by_instrument.loc["AAA", "pretrade_reason"])
        self.assertIn("banned_instrument", by_instrument.loc["BBB", "pretrade_reason"])
        self.assertIn("sell_not_allowed", by_instrument.loc["CCC", "pretrade_reason"])

    def test_write_manual_order_ticket_outputs_csv_and_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ticket = build_manual_order_ticket(
                pd.DataFrame(
                    {
                        "date": ["2026-04-23"],
                        "instrument": ["AAA"],
                        "side": ["BUY"],
                        "order_value": [10000.0],
                        "order_shares": [1000],
                        "price": [10.0],
                    }
                ),
                pd.DataFrame({"instrument": ["AAA"], "status": ["filled"], "reject_reason": [""]}),
            )

            csv_path, md_path = write_manual_order_ticket(
                ticket,
                root / "reports/order_ticket_20260423.csv",
                root / "reports/order_ticket_20260423.md",
            )

            self.assertTrue(csv_path.exists())
            self.assertTrue(md_path.exists())
            self.assertIn("Manual Order Ticket", md_path.read_text(encoding="utf-8"))

    def test_generate_manual_ticket_cli_writes_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            orders = root / "runs/20260423/orders.csv"
            fills = root / "runs/20260423/fills.csv"
            orders.parent.mkdir(parents=True)
            pd.DataFrame(
                {
                    "date": ["2026-04-23"],
                    "instrument": ["AAA"],
                    "side": ["BUY"],
                    "order_value": [10000.0],
                    "order_shares": [1000],
                    "price": [10.0],
                }
            ).to_csv(orders, index=False)
            pd.DataFrame({"instrument": ["AAA"], "status": ["filled"], "reject_reason": [""]}).to_csv(fills, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/generate_manual_ticket.py"),
                    "--orders-csv",
                    str(orders.relative_to(root)),
                    "--fills-csv",
                    str(fills.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((root / "reports/order_ticket_20260423.csv").exists())
            self.assertTrue((root / "reports/order_ticket_20260423.md").exists())
            self.assertIn("wrote:", result.stdout)

    def test_generate_manual_ticket_cli_applies_pretrade_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            orders = root / "runs/20260423/orders.csv"
            banned = root / "configs/banned.csv"
            orders.parent.mkdir(parents=True)
            banned.parent.mkdir(parents=True)
            pd.DataFrame(
                {
                    "date": ["2026-04-23", "2026-04-23"],
                    "instrument": ["AAA", "BBB"],
                    "side": ["BUY", "SELL"],
                    "order_value": [12000.0, 5000.0],
                    "order_shares": [1200, 500],
                    "price": [10.0, 10.0],
                }
            ).to_csv(orders, index=False)
            pd.DataFrame({"instrument": ["AAA"]}).to_csv(banned, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/generate_manual_ticket.py"),
                    "--orders-csv",
                    str(orders.relative_to(root)),
                    "--project-root",
                    str(root),
                    "--available-cash",
                    "10000",
                    "--max-order-value",
                    "8000",
                    "--banned-instruments-csv",
                    str(banned.relative_to(root)),
                    "--no-sells",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            output = pd.read_csv(root / "reports/order_ticket_20260423.csv")
            by_instrument = output.set_index("instrument")
            self.assertIn("banned_instrument", by_instrument.loc["AAA", "pretrade_reason"])
            self.assertIn("sell_not_allowed", by_instrument.loc["BBB", "pretrade_reason"])


if __name__ == "__main__":
    unittest.main()
