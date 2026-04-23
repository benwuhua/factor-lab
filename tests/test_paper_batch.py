from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.orders import OrderConfig
from qlib_factor_lab.paper_batch import PaperBatchConfig, run_paper_batch
from qlib_factor_lab.paper_broker import PaperFillConfig
from qlib_factor_lab.reconcile import ReconcileConfig


class PaperBatchTests(unittest.TestCase):
    def test_run_paper_batch_rolls_positions_and_reports_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_paths = self._write_30_targets(root)

            result = run_paper_batch(
                target_paths,
                pd.DataFrame(columns=["instrument", "current_weight"]),
                OrderConfig(total_equity=100_000, min_order_value=100, lot_size=100),
                PaperFillConfig(fill_ratio=1.0, commission_bps=2),
                ReconcileConfig(weight_tolerance=0.001),
                PaperBatchConfig(run_root=root / "runs/paper_batch", max_days=30),
            )

            self.assertEqual(len(result.metrics), 30)
            self.assertEqual(int(result.summary["reconciliation_failures"]), 0)
            self.assertGreater(float(result.summary["average_turnover"]), 0)
            self.assertGreater(float(result.summary["total_transaction_cost"]), 0)
            self.assertTrue((root / "runs/paper_batch/20260430/orders.csv").exists())

    def test_run_paper_batch_cli_writes_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_dir = root / "targets"
            self._write_30_targets(root, target_dir=target_dir)
            config = root / "configs/execution.yaml"
            config.parent.mkdir(parents=True)
            config.write_text(
                yaml.safe_dump(
                    {
                        "orders": {"total_equity": 100_000, "min_order_value": 100, "lot_size": 100},
                        "paper_broker": {"fill_ratio": 1.0, "commission_bps": 2},
                        "reconcile": {"weight_tolerance": 0.001},
                        "batch": {
                            "max_days": 30,
                            "run_root": "runs/paper_batch",
                            "summary_csv": "runs/paper_batch_summary.csv",
                            "summary_md": "runs/paper_batch_summary.md",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/run_paper_batch.py"),
                    "--target-glob",
                    "targets/target_portfolio_*.csv",
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
            self.assertTrue((root / "runs/paper_batch_summary.csv").exists())
            self.assertTrue((root / "runs/paper_batch_summary.md").exists())
            summary = pd.read_csv(root / "runs/paper_batch_summary.csv")
            self.assertEqual(int(summary.loc[0, "days"]), 30)
            self.assertIn("wrote:", result.stdout)

    def _write_30_targets(self, root: Path, target_dir: Path | None = None):
        target_dir = target_dir or root / "targets"
        target_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for offset in range(30):
            date = pd.Timestamp("2026-04-01") + pd.Timedelta(days=offset)
            frame = pd.DataFrame(
                {
                    "date": [date.strftime("%Y-%m-%d"), date.strftime("%Y-%m-%d")],
                    "instrument": ["AAA", "BBB" if offset % 2 == 0 else "CCC"],
                    "target_weight": [0.1, 0.1],
                    "last_price": [10.0, 20.0],
                    "ensemble_score": [5.0, 4.0],
                }
            )
            path = target_dir / f"target_portfolio_{date.strftime('%Y%m%d')}.csv"
            frame.to_csv(path, index=False)
            paths.append(path)
        return paths


if __name__ == "__main__":
    unittest.main()
