import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.factor_eval import compute_quantile_return_summary
from qlib_factor_lab.reports import plot_quantile_returns, render_event_summary_markdown


class ReportsTests(unittest.TestCase):
    def test_compute_quantile_return_summary_returns_long_short_spread(self):
        frame = pd.DataFrame(
            {
                "signal": [1.0, 2.0, 3.0, 4.0],
                "future_ret": [0.01, 0.02, 0.05, 0.08],
            }
        )

        summary = compute_quantile_return_summary(frame, "signal", "future_ret", quantiles=2)

        self.assertAlmostEqual(summary["q1_mean_return"], 0.015)
        self.assertAlmostEqual(summary["q2_mean_return"], 0.065)
        self.assertAlmostEqual(summary["long_short_mean_return"], 0.05)

    def test_plot_quantile_returns_writes_png(self):
        frame = pd.DataFrame(
            [
                {
                    "factor": "ret_20",
                    "horizon": 5,
                    "q1_mean_return": -0.01,
                    "q2_mean_return": 0.02,
                    "long_short_mean_return": 0.03,
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "quantile.png"

            path = plot_quantile_returns(frame, output)

            self.assertEqual(path, output)
            self.assertTrue(output.exists())
            self.assertGreater(output.stat().st_size, 0)

    def test_render_event_summary_markdown_uses_p95_twenty_day_focus(self):
        frame = pd.DataFrame(
            [
                {
                    "bucket": "p70_p85",
                    "horizon": 20,
                    "trade_count": 10,
                    "mean_return": 0.01,
                    "median_return": 0.005,
                    "win_rate": 0.50,
                    "payoff_ratio": 1.20,
                    "mfe_mean": 0.04,
                    "mae_mean": -0.03,
                },
                {
                    "bucket": "p95_p100",
                    "horizon": 20,
                    "trade_count": 42,
                    "mean_return": 0.0345,
                    "median_return": 0.0123,
                    "win_rate": 0.5238,
                    "payoff_ratio": 2.10,
                    "mfe_mean": 0.09,
                    "mae_mean": -0.04,
                },
            ]
        )

        markdown = render_event_summary_markdown(
            frame,
            name="arbr_26 CSI300 event backtest",
            factor="arbr_26",
            universe="csi300_current",
            provider_config="configs/provider_csi300_current.yaml",
            command="make event-csi300 FACTOR=arbr_26",
        )

        self.assertIn("# arbr_26 CSI300 event backtest", markdown)
        self.assertIn("- Related factor(s): arbr_26", markdown)
        self.assertIn("- Universe: csi300_current", markdown)
        self.assertIn("make event-csi300 FACTOR=arbr_26", markdown)
        self.assertIn("| p95-p100 mean return | 3.45% | horizon=20, trades=42 |", markdown)
        self.assertIn("| p95-p100 win rate | 52.38% |  |", markdown)


if __name__ == "__main__":
    unittest.main()
