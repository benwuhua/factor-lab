import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.orders import OrderConfig, build_order_suggestions
from qlib_factor_lab.paper_broker import PaperFillConfig, simulate_paper_fills
from qlib_factor_lab.reconcile import ReconcileConfig, reconcile_positions
from qlib_factor_lab.state import apply_fills_to_positions


class StageDTests(unittest.TestCase):
    def test_build_order_suggestions_diffs_target_against_current_positions(self):
        orders = build_order_suggestions(
            self._target_portfolio(),
            self._current_positions(),
            OrderConfig(total_equity=1_000_000, min_order_value=1_000),
        )

        by_instrument = {row["instrument"]: row for _, row in orders.iterrows()}
        self.assertEqual(by_instrument["AAA"]["side"], "BUY")
        self.assertAlmostEqual(by_instrument["AAA"]["delta_weight"], 0.05)
        self.assertEqual(by_instrument["CCC"]["side"], "BUY")
        self.assertEqual(by_instrument["OLD"]["side"], "SELL")
        self.assertAlmostEqual(by_instrument["OLD"]["target_weight"], 0.0)

    def test_paper_fills_update_expected_positions(self):
        orders = build_order_suggestions(
            self._target_portfolio(),
            self._current_positions(),
            OrderConfig(total_equity=1_000_000, min_order_value=1_000),
        )

        fills = simulate_paper_fills(orders, PaperFillConfig(fill_ratio=1.0))
        expected = apply_fills_to_positions(self._current_positions(), fills)

        weights = dict(zip(expected["instrument"], expected["current_weight"]))
        self.assertAlmostEqual(weights["AAA"], 0.15)
        self.assertAlmostEqual(weights["CCC"], 0.20)
        self.assertNotIn("OLD", weights)

    def test_reconcile_positions_flags_weight_mismatch(self):
        expected = pd.DataFrame({"instrument": ["AAA", "CCC"], "current_weight": [0.15, 0.20]})
        actual = pd.DataFrame({"instrument": ["AAA", "CCC"], "current_weight": [0.15, 0.18]})

        report = reconcile_positions(expected, actual, ReconcileConfig(weight_tolerance=0.005))

        self.assertFalse(report.passed)
        failed = report.to_frame().query("status == 'fail'")
        self.assertIn("weight_mismatch", set(failed["check"]))

    def test_generate_orders_cli_writes_run_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_path, current_path, execution_path = self._write_paper_fixture(root)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/generate_orders.py"),
                    "--target-portfolio",
                    str(target_path.relative_to(root)),
                    "--current-positions",
                    str(current_path.relative_to(root)),
                    "--execution-config",
                    str(execution_path.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            run_dir = root / "runs/20260423"
            self.assertTrue((run_dir / "orders.csv").exists())
            self.assertTrue((run_dir / "fills.csv").exists())
            self.assertTrue((run_dir / "positions_expected.csv").exists())
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["run_date"], "2026-04-23")
            self.assertIn("wrote:", result.stdout)

    def test_reconcile_account_cli_writes_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = root / "runs/20260423/positions_expected.csv"
            actual = root / "runs/20260423/positions_actual.csv"
            config = root / "configs/execution.yaml"
            expected.parent.mkdir(parents=True)
            config.parent.mkdir(parents=True)
            pd.DataFrame({"instrument": ["AAA"], "current_weight": [0.15]}).to_csv(expected, index=False)
            pd.DataFrame({"instrument": ["AAA"], "current_weight": [0.15]}).to_csv(actual, index=False)
            config.write_text(yaml.safe_dump({"reconcile": {"weight_tolerance": 0.001}}, sort_keys=False), encoding="utf-8")
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/reconcile_account.py"),
                    "--expected-positions",
                    str(expected.relative_to(root)),
                    "--actual-positions",
                    str(actual.relative_to(root)),
                    "--execution-config",
                    str(config.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((root / "runs/20260423/reconciliation.md").exists())
            self.assertIn("wrote:", result.stdout)

    def _target_portfolio(self):
        return pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "CCC"],
                "target_weight": [0.15, 0.20],
                "ensemble_score": [5.0, 4.0],
            }
        )

    def _current_positions(self):
        return pd.DataFrame(
            {
                "instrument": ["AAA", "OLD"],
                "current_weight": [0.10, 0.15],
            }
        )

    def _write_paper_fixture(self, root: Path):
        target_path = root / "reports/target_portfolio_20260423.csv"
        current_path = root / "state/current_positions.csv"
        execution_path = root / "configs/execution.yaml"
        target_path.parent.mkdir(parents=True)
        current_path.parent.mkdir(parents=True)
        execution_path.parent.mkdir(parents=True)
        self._target_portfolio().to_csv(target_path, index=False)
        self._current_positions().to_csv(current_path, index=False)
        execution_path.write_text(
            yaml.safe_dump(
                {
                    "orders": {"total_equity": 1_000_000, "min_order_value": 1_000},
                    "paper_broker": {"fill_ratio": 1.0},
                    "reconcile": {"weight_tolerance": 0.001},
                    "output": {"run_dir": "runs/{run_yyyymmdd}"},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return target_path, current_path, execution_path


if __name__ == "__main__":
    unittest.main()
