import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.wangji_diagnostics import write_mfe_mae_distribution_plot


class WangjiDiagnosticsTests(unittest.TestCase):
    def test_write_mfe_mae_distribution_plot_creates_png(self):
        trades = pd.DataFrame(
            {
                "factor": ["wangji-factor1", "wangji-factor1", "wangji-factor2", "wangji-factor2"],
                "trade_side": ["left", "left", "right", "right"],
                "horizon": [5, 20, 5, 20],
                "mfe": [0.06, 0.12, 0.08, 0.20],
                "mae": [-0.03, -0.05, -0.04, -0.07],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "mfe_mae.png"

            write_mfe_mae_distribution_plot(trades, output)

            self.assertTrue(output.exists())
            self.assertGreater(output.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()
