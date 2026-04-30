import tempfile
import unittest
from pathlib import Path
import subprocess
import sys

import pandas as pd

from qlib_factor_lab.portfolio_performance import (
    build_intraday_performance,
    summarize_intraday_performance,
    write_intraday_performance_report,
)


class PortfolioPerformanceTests(unittest.TestCase):
    def test_build_intraday_performance_joins_quotes_and_calculates_weighted_return(self):
        portfolio = pd.DataFrame(
            {
                "instrument": ["SH600000", "SZ000001"],
                "target_weight": [0.25, 0.15],
                "rank": [1, 2],
                "industry_sw": ["bank", "bank"],
                "top_factor_1": ["value", "momentum"],
                "event_count": [0, 1],
                "event_risk_summary": ["", "earnings watch"],
            }
        )
        quotes = pd.DataFrame(
            {
                "instrument": ["SH600000", "SZ000001"],
                "display_name": ["浦发银行", "平安银行"],
                "prev_close": [10.0, 20.0],
                "current": [10.5, 19.0],
                "quote_time": ["2026-04-30 14:30:00", "2026-04-30 14:30:00"],
            }
        )

        frame = build_intraday_performance(portfolio, quotes)

        self.assertEqual(list(frame["instrument"]), ["SH600000", "SZ000001"])
        self.assertAlmostEqual(float(frame.loc[0, "pct_today"]), 5.0)
        self.assertAlmostEqual(float(frame.loc[0, "weighted_return_pct"]), 1.25)
        self.assertAlmostEqual(float(frame.loc[1, "pct_today"]), -5.0)
        self.assertEqual(frame.loc[0, "direction"], "up")
        self.assertEqual(frame.loc[1, "event_bucket"], "event_watch")

    def test_summarize_intraday_performance_groups_by_industry_factor_and_event(self):
        frame = pd.DataFrame(
            {
                "instrument": ["AAA", "BBB", "CCC"],
                "target_weight": [0.2, 0.3, 0.5],
                "pct_today": [2.0, -1.0, -3.0],
                "weighted_return_pct": [0.4, -0.3, -1.5],
                "industry": ["tech", "tech", "energy"],
                "factor": ["momentum", "value", "value"],
                "event_bucket": ["event_watch", "no_event", "event_block"],
                "direction": ["up", "down", "down"],
            }
        )

        summary = summarize_intraday_performance(frame)

        self.assertAlmostEqual(summary["summary"]["weighted_return_pct"], -1.4)
        self.assertEqual(summary["summary"]["up_count"], 1)
        self.assertEqual(summary["summary"]["down_count"], 2)
        self.assertAlmostEqual(summary["industry"].set_index("industry").loc["tech", "weighted_return_pct"], 0.1)
        self.assertAlmostEqual(summary["factor"].set_index("factor").loc["value", "weighted_return_pct"], -1.8)
        self.assertEqual(summary["contributors"].iloc[0]["instrument"], "CCC")

    def test_write_intraday_performance_report_writes_csv_and_markdown(self):
        frame = pd.DataFrame(
            {
                "instrument": ["AAA"],
                "display_name": ["Alpha"],
                "target_weight": [0.2],
                "pct_today": [1.5],
                "weighted_return_pct": [0.3],
                "industry": ["tech"],
                "factor": ["value"],
                "event_bucket": ["no_event"],
                "direction": ["up"],
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            output_csv = Path(tmp) / "portfolio_intraday_performance_20260430.csv"
            output_md = Path(tmp) / "portfolio_intraday_performance_20260430.md"

            write_intraday_performance_report(frame, output_csv, output_md)

            self.assertTrue(output_csv.exists())
            self.assertTrue(output_md.exists())
            self.assertIn("Portfolio Intraday Performance", output_md.read_text(encoding="utf-8"))
            self.assertIn("weighted_return_pct", output_md.read_text(encoding="utf-8"))

    def test_build_portfolio_intraday_performance_cli_writes_formal_artifacts(self):
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs/20260430"
            run.mkdir(parents=True)
            quotes = root / "quotes.csv"
            output_csv = run / "portfolio_intraday_performance.csv"
            output_md = run / "portfolio_intraday_performance.md"
            pd.DataFrame(
                {
                    "instrument": ["SH600000"],
                    "target_weight": [0.2],
                    "industry_sw": ["bank"],
                    "top_factor_1": ["value"],
                }
            ).to_csv(run / "execution_portfolio.csv", index=False)
            pd.DataFrame(
                {
                    "instrument": ["SH600000"],
                    "display_name": ["浦发银行"],
                    "prev_close": [10.0],
                    "current": [10.2],
                    "quote_time": ["2026-04-30 14:55:00"],
                }
            ).to_csv(quotes, index=False)

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_portfolio_intraday_performance.py"),
                    "--project-root",
                    str(root),
                    "--portfolio",
                    "runs/20260430/execution_portfolio.csv",
                    "--quote-csv",
                    str(quotes),
                    "--run-date",
                    "20260430",
                    "--output-csv",
                    str(output_csv),
                    "--output-md",
                    str(output_md),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(output_csv.exists())
            self.assertTrue(output_md.exists())
            frame = pd.read_csv(output_csv)
            self.assertAlmostEqual(float(frame.loc[0, "weighted_return_pct"]), 0.4)
            self.assertIn("wrote:", result.stdout)


if __name__ == "__main__":
    unittest.main()
