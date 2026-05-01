import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.liquidity_microstructure import (
    build_liquidity_microstructure,
    merge_liquidity_microstructure,
    write_liquidity_microstructure,
)


class LiquidityMicrostructureTests(unittest.TestCase):
    def test_build_liquidity_microstructure_marks_limit_flags_and_pressure(self) -> None:
        features = pd.DataFrame(
            {
                "date": ["2026-04-23"] * 3,
                "instrument": ["UP", "DOWN", "OK"],
                "close": [11.0, 9.0, 10.2],
                "prev_close": [10.0, 10.0, 10.0],
                "open": [10.5, 9.5, 10.1],
                "high": [11.0, 9.8, 10.4],
                "low": [10.4, 9.0, 10.0],
                "amount": [1000.0, 1000.0, 1000.0],
                "volume": [100.0, 100.0, 100.0],
                "turnover": [0.03, 0.04, 0.02],
                "amount_20d": [900.0, 950.0, 980.0],
                "turnover_20d": [0.02, 0.03, 0.02],
            }
        )

        result = build_liquidity_microstructure(features, limit_up_pct=0.098, limit_down_pct=-0.098)

        by_instrument = result.set_index("instrument")
        self.assertTrue(bool(by_instrument.loc["UP", "limit_up"]))
        self.assertTrue(bool(by_instrument.loc["UP", "buy_blocked"]))
        self.assertFalse(bool(by_instrument.loc["UP", "sell_blocked"]))
        self.assertTrue(bool(by_instrument.loc["DOWN", "limit_down"]))
        self.assertTrue(bool(by_instrument.loc["DOWN", "sell_blocked"]))
        self.assertAlmostEqual(0.10, float(by_instrument.loc["UP", "pct_change"]))
        self.assertAlmostEqual(0.06, float(by_instrument.loc["UP", "intraday_range"]))
        self.assertAlmostEqual(0.05, float(by_instrument.loc["UP", "gap_pct"]))
        self.assertEqual("2026-04-23", by_instrument.loc["UP", "available_at"])
        self.assertGreater(float(by_instrument.loc["UP", "limit_pressure"]), 0.99)

    def test_build_liquidity_microstructure_marks_suspended_but_tolerates_missing_optional_fields(self) -> None:
        features = pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23"],
                "instrument": ["SUSP", "PARTIAL"],
                "close": [float("nan"), 10.0],
                "prev_close": [10.0, 10.0],
                "amount": [0.0, 1000.0],
                "volume": [0.0, 100.0],
            }
        )

        result = build_liquidity_microstructure(features)

        by_instrument = result.set_index("instrument")
        self.assertTrue(bool(by_instrument.loc["SUSP", "suspended"]))
        self.assertFalse(bool(by_instrument.loc["SUSP", "tradable"]))
        self.assertTrue(bool(by_instrument.loc["SUSP", "buy_blocked"]))
        self.assertTrue(bool(by_instrument.loc["SUSP", "sell_blocked"]))
        self.assertFalse(bool(by_instrument.loc["PARTIAL", "suspended"]))
        self.assertIn("turnover_20d", result.columns)
        self.assertTrue(pd.isna(by_instrument.loc["PARTIAL", "turnover_20d"]))

    def test_merge_liquidity_microstructure_deduplicates_by_date_and_instrument(self) -> None:
        existing = pd.DataFrame(
            {
                "date": ["2026-04-22", "2026-04-23"],
                "instrument": ["AAA", "AAA"],
                "available_at": ["2026-04-22", "2026-04-23"],
                "amount": [100.0, 200.0],
            }
        )
        new = pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-24"],
                "instrument": ["AAA", "BBB"],
                "available_at": ["2026-04-23", "2026-04-24"],
                "amount": [250.0, 300.0],
            }
        )

        merged = merge_liquidity_microstructure(new, existing)

        self.assertEqual([("2026-04-22", "AAA"), ("2026-04-23", "AAA"), ("2026-04-24", "BBB")], list(zip(merged["date"], merged["instrument"])))
        self.assertEqual(250.0, float(merged[(merged["date"] == "2026-04-23") & (merged["instrument"] == "AAA")].iloc[0]["amount"]))

    def test_write_liquidity_microstructure_appends_existing_csv_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "data/liquidity_microstructure.csv"
            output.parent.mkdir(parents=True)
            pd.DataFrame(
                {
                    "date": ["2026-04-23"],
                    "instrument": ["AAA"],
                    "available_at": ["2026-04-23"],
                    "amount": [100.0],
                }
            ).to_csv(output.parent / "seed.csv", index=False)
            existing_path = output.parent / "seed.csv"
            new = pd.DataFrame(
                {
                    "date": ["2026-04-23", "2026-04-24"],
                    "instrument": ["AAA", "BBB"],
                    "available_at": ["2026-04-23", "2026-04-24"],
                    "amount": [150.0, 200.0],
                }
            )

            path = write_liquidity_microstructure(new, output, merge_existing=True, existing_path=existing_path)

            written = pd.read_csv(path)
            self.assertEqual(2, len(written))
            self.assertEqual(150.0, float(written[written["instrument"] == "AAA"].iloc[0]["amount"]))

    def test_build_liquidity_microstructure_cli_accepts_feature_csv_and_merge_existing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            features = root / "features.csv"
            output = root / "data/liquidity_microstructure.csv"
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
            output.parent.mkdir(parents=True)
            pd.DataFrame(
                {
                    "date": ["2026-04-22"],
                    "instrument": ["OLD"],
                    "available_at": ["2026-04-22"],
                    "amount": [50.0],
                }
            ).to_csv(output, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_liquidity_microstructure.py"),
                    "--features-csv",
                    str(features),
                    "--output",
                    str(output),
                    "--merge-existing",
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            written = pd.read_csv(output)
            self.assertEqual({"OLD", "AAA"}, set(written["instrument"]))
            self.assertTrue(bool(written[written["instrument"] == "AAA"].iloc[0]["limit_up"]))
            self.assertIn("wrote:", result.stdout)


if __name__ == "__main__":
    unittest.main()
