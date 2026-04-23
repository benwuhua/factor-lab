import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.historical_paper_batch import BatchPaths, write_historical_batch_summary


class HistoricalPaperBatchTests(unittest.TestCase):
    def test_write_historical_batch_summary_records_pipeline_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = BatchPaths(
                signal_paths=[root / "reports/signals_20260401.csv"],
                target_paths=[root / "reports/target_portfolio_20260401.csv"],
                batch_summary_csv=root / "runs/paper_batch_summary.csv",
                batch_summary_md=root / "runs/paper_batch_summary.md",
            )
            paths.batch_summary_csv.parent.mkdir(parents=True)
            pd.DataFrame([{"days": 1, "average_turnover": 0.1, "reconciliation_failures": 0}]).to_csv(
                paths.batch_summary_csv,
                index=False,
            )

            output = write_historical_batch_summary(paths, root / "reports/historical_paper_batch.md")

            self.assertTrue(output.exists())
            text = output.read_text(encoding="utf-8")
            self.assertIn("Historical Paper Batch", text)
            self.assertIn("signals_20260401.csv", text)

    def test_historical_paper_batch_cli_accepts_fixture_targets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_dir = root / "reports/historical_targets"
            target_dir.mkdir(parents=True)
            for day in range(2):
                date = pd.Timestamp("2026-04-01") + pd.Timedelta(days=day)
                pd.DataFrame(
                    {
                        "date": [date.strftime("%Y-%m-%d")],
                        "instrument": ["AAA"],
                        "target_weight": [0.1],
                        "last_price": [10.0],
                    }
                ).to_csv(target_dir / f"target_portfolio_{date.strftime('%Y%m%d')}.csv", index=False)
            config = root / "configs/execution.yaml"
            config.parent.mkdir(parents=True)
            config.write_text(
                yaml.safe_dump(
                    {
                        "orders": {"total_equity": 100000, "min_order_value": 100, "lot_size": 100},
                        "paper_broker": {"fill_ratio": 1.0},
                        "reconcile": {"weight_tolerance": 0.001},
                        "batch": {
                            "max_days": 2,
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
                    str(repo / "scripts/run_historical_paper_batch.py"),
                    "--target-glob",
                    "reports/historical_targets/target_portfolio_*.csv",
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
            self.assertTrue((root / "reports/historical_paper_batch.md").exists())
            self.assertIn("wrote:", result.stdout)


if __name__ == "__main__":
    unittest.main()
