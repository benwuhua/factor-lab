import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.factor_eval import compute_quantile_return_summary
from qlib_factor_lab.reports import plot_quantile_returns


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


if __name__ == "__main__":
    unittest.main()
