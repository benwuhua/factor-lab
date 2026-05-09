import subprocess
import sys
import unittest
from pathlib import Path


class WangjiIndependentBacktestCliTests(unittest.TestCase):
    def test_script_exposes_independent_price_volume_backtest_help(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts/backtest_wangji_independent_events.py"

        result = subprocess.run(
            [sys.executable, str(script), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("independent Wangji price-volume event factors", result.stdout)
        self.assertIn("--factor", result.stdout)
        self.assertIn("--provider-config", result.stdout)
        self.assertIn("--start-time", result.stdout)
        self.assertIn("--end-time", result.stdout)
        self.assertIn("--mfe-mae-plot-output", result.stdout)

    def test_explanation_script_exposes_factor2_evidence_help(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts/explain_wangji_factor2_events.py"

        result = subprocess.run(
            [sys.executable, str(script), "--help"],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("Explain Wangji factor2 2B pullback event evidence", result.stdout)
        self.assertIn("--sample", result.stdout)
        self.assertIn("--provider-config", result.stdout)
        self.assertIn("--sample-csv-dir", result.stdout)
        self.assertIn("--output", result.stdout)


if __name__ == "__main__":
    unittest.main()
