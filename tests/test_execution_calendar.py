import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.execution_calendar import build_execution_calendar


class ExecutionCalendarTests(unittest.TestCase):
    def test_build_execution_calendar_marks_suspended_and_limit_locked(self):
        features = pd.DataFrame(
            {
                "date": ["2026-04-23"] * 4,
                "instrument": ["UP", "DOWN", "SUSP", "OK"],
                "close": [11.0, 9.0, float("nan"), 10.2],
                "prev_close": [10.0, 10.0, 10.0, 10.0],
                "amount": [1000.0, 1000.0, 0.0, 1000.0],
                "volume": [100.0, 100.0, 0.0, 100.0],
            }
        )

        calendar = build_execution_calendar(features, limit_up_pct=0.098, limit_down_pct=-0.098)

        by_instrument = calendar.set_index("instrument")
        self.assertTrue(bool(by_instrument.loc["UP", "limit_up"]))
        self.assertTrue(bool(by_instrument.loc["UP", "buy_blocked"]))
        self.assertTrue(bool(by_instrument.loc["DOWN", "limit_down"]))
        self.assertTrue(bool(by_instrument.loc["DOWN", "sell_blocked"]))
        self.assertTrue(bool(by_instrument.loc["SUSP", "suspended"]))
        self.assertFalse(bool(by_instrument.loc["SUSP", "tradable"]))
        self.assertFalse(bool(by_instrument.loc["OK", "buy_blocked"]))

    def test_build_execution_calendar_cli_accepts_feature_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            features = root / "features.csv"
            output = root / "reports/execution_calendar_20260423.csv"
            pd.DataFrame(
                {
                    "date": ["2026-04-23"],
                    "instrument": ["AAA"],
                    "close": [11.0],
                    "prev_close": [10.0],
                    "amount": [1000.0],
                    "volume": [100.0],
                }
            ).to_csv(features, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_execution_calendar.py"),
                    "--features-csv",
                    str(features),
                    "--output",
                    str(output),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(output.exists())
            self.assertTrue(bool(pd.read_csv(output).loc[0, "limit_up"]))
            self.assertIn("wrote:", result.stdout)


if __name__ == "__main__":
    unittest.main()
