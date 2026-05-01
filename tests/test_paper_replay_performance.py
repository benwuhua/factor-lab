import tempfile
import subprocess
import sys
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.paper_replay_performance import (
    compute_paper_replay_returns,
    summarize_paper_replay_returns,
    summarize_paper_replay_monthly_returns,
    write_paper_replay_report,
)


class PaperReplayPerformanceTests(unittest.TestCase):
    def test_compute_paper_replay_returns_uses_next_close_and_transaction_costs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_dir = root / "targets"
            target_dir.mkdir()
            pd.DataFrame(
                {
                    "date": ["2026-01-01", "2026-01-01"],
                    "instrument": ["AAA", "BBB"],
                    "target_weight": [0.5, 0.25],
                }
            ).to_csv(target_dir / "target_portfolio_20260101.csv", index=False)
            pd.DataFrame(
                {
                    "date": ["2026-01-02"],
                    "instrument": ["AAA"],
                    "target_weight": [0.75],
                }
            ).to_csv(target_dir / "target_portfolio_20260102.csv", index=False)
            run_dir = root / "runs/paper_batch/20260101"
            run_dir.mkdir(parents=True)
            pd.DataFrame({"transaction_cost": [100.0, 50.0]}).to_csv(run_dir / "fills.csv", index=False)

            close = pd.DataFrame(
                {
                    "date": [
                        "2026-01-01",
                        "2026-01-01",
                        "2026-01-02",
                        "2026-01-02",
                        "2026-01-05",
                        "2026-01-05",
                    ],
                    "instrument": ["AAA", "BBB", "AAA", "BBB", "AAA", "BBB"],
                    "close": [10.0, 20.0, 11.0, 18.0, 12.1, 18.9],
                }
            )

            daily = compute_paper_replay_returns(
                sorted(target_dir.glob("target_portfolio_*.csv")),
                close,
                paper_run_root=root / "runs/paper_batch",
                total_equity=100_000,
            )

            self.assertEqual(list(daily["date"]), ["2026-01-01", "2026-01-02"])
            self.assertAlmostEqual(float(daily.loc[0, "gross_return"]), 0.025)
            self.assertAlmostEqual(float(daily.loc[0, "transaction_cost_return"]), 0.0015)
            self.assertAlmostEqual(float(daily.loc[0, "net_return"]), 0.0235)
            self.assertAlmostEqual(float(daily.loc[0, "market_equal_weight_return"]), 0.0)
            self.assertAlmostEqual(float(daily.loc[1, "gross_return"]), 0.075)
            self.assertEqual(int(daily.loc[1, "position_count"]), 1)

    def test_summaries_and_report_are_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            daily = pd.DataFrame(
                {
                    "date": ["2026-01-30", "2026-02-02"],
                    "next_date": ["2026-02-02", "2026-02-03"],
                    "net_return": [0.02, -0.01],
                    "gross_return": [0.021, -0.009],
                    "market_equal_weight_return": [0.01, -0.02],
                    "transaction_cost_return": [0.001, 0.001],
                    "turnover": [0.2, 0.1],
                    "position_count": [2, 2],
                    "gross_exposure": [0.8, 0.8],
                }
            )

            summary = summarize_paper_replay_returns(daily)
            monthly = summarize_paper_replay_monthly_returns(daily)
            output = write_paper_replay_report(
                daily,
                summary,
                monthly,
                root / "reports/offensive_paper_replay_report.md",
                title="Offensive Replay",
            )

            self.assertGreater(summary["total_return"], 0.0)
            self.assertEqual(list(monthly["month"]), ["2026-01", "2026-02"])
            text = output.read_text(encoding="utf-8")
            self.assertIn("Offensive Replay", text)
            self.assertIn("total_return", text)

    def test_build_paper_replay_performance_cli_accepts_close_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            targets = root / "targets"
            targets.mkdir()
            pd.DataFrame(
                {
                    "date": ["2026-01-01"],
                    "instrument": ["AAA"],
                    "target_weight": [1.0],
                }
            ).to_csv(targets / "target_portfolio_20260101.csv", index=False)
            close_csv = root / "close.csv"
            pd.DataFrame(
                {
                    "date": ["2026-01-01", "2026-01-02"],
                    "instrument": ["AAA", "AAA"],
                    "close": [10.0, 10.5],
                }
            ).to_csv(close_csv, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_paper_replay_performance.py"),
                    "--project-root",
                    str(root),
                    "--target-glob",
                    "targets/target_portfolio_*.csv",
                    "--close-csv",
                    str(close_csv),
                    "--output-dir",
                    "reports/replay",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((root / "reports/replay/paper_replay_daily_returns.csv").exists())
            self.assertTrue((root / "reports/replay/paper_replay_report.md").exists())


if __name__ == "__main__":
    unittest.main()
