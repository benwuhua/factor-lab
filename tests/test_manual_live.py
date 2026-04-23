import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.manual_live import build_manual_order_ticket, write_manual_order_ticket


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


if __name__ == "__main__":
    unittest.main()
