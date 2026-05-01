import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.emotion_atmosphere import (
    build_emotion_atmosphere,
    merge_emotion_atmosphere,
    write_emotion_atmosphere,
)


class EmotionAtmosphereTests(unittest.TestCase):
    def test_build_emotion_atmosphere_replicates_market_breadth_per_instrument(self):
        liquidity = pd.DataFrame(
            {
                "trade_date": ["2026-04-20", "2026-04-20", "2026-04-20", "2026-04-21"],
                "instrument": ["AAA", "BBB", "CCC", "AAA"],
                "pct_change": [0.10, -0.03, None, 0.02],
                "amount_20d": [200.0, 80.0, 50.0, 300.0],
                "turnover_20d": [0.04, 0.01, 0.02, 0.05],
                "limit_up": [True, False, False, False],
                "limit_down": [False, False, False, False],
                "suspended": [False, False, True, False],
            }
        )

        atmosphere = build_emotion_atmosphere(liquidity)
        day = atmosphere[atmosphere["trade_date"] == "2026-04-20"].sort_values("instrument")

        self.assertEqual(day["tradable_count"].tolist(), [2, 2, 2])
        self.assertEqual(day["suspended_count"].tolist(), [1, 1, 1])
        self.assertEqual(day["limit_up_count"].tolist(), [1, 1, 1])
        self.assertAlmostEqual(day["up_ratio"].iloc[0], 0.5)
        self.assertAlmostEqual(day["down_ratio"].iloc[0], 0.5)
        self.assertAlmostEqual(day["avg_pct_change"].iloc[0], 0.035)
        self.assertTrue(day.loc[day["instrument"] == "AAA", "limit_up"].iloc[0])
        self.assertTrue(day.loc[day["instrument"] == "CCC", "suspended"].iloc[0])
        self.assertEqual(day["available_at"].tolist(), ["2026-04-20", "2026-04-20", "2026-04-20"])

    def test_emotion_score_is_higher_for_stronger_breadth_and_heat(self):
        liquidity = pd.DataFrame(
            {
                "trade_date": ["2026-04-20"] * 4 + ["2026-04-21"] * 4,
                "instrument": ["A", "B", "C", "D"] * 2,
                "pct_change": [-0.07, -0.05, -0.02, 0.01, 0.10, 0.06, 0.04, -0.01],
                "amount_20d": [10.0, 12.0, 11.0, 9.0, 200.0, 210.0, 190.0, 180.0],
                "turnover_20d": [0.01, 0.01, 0.01, 0.01, 0.08, 0.07, 0.06, 0.05],
                "limit_up": [False, False, False, False, True, False, False, False],
                "limit_down": [False, False, False, False, False, False, False, False],
                "suspended": [False] * 8,
            }
        )

        atmosphere = build_emotion_atmosphere(liquidity)
        scores = atmosphere.groupby("trade_date")["emotion_score"].first()

        self.assertGreater(scores.loc["2026-04-21"], scores.loc["2026-04-20"])
        self.assertTrue(atmosphere["emotion_score"].between(0.0, 100.0).all())

    def test_merge_emotion_atmosphere_deduplicates_by_date_and_instrument(self):
        existing = pd.DataFrame(
            {
                "trade_date": ["2026-04-20"],
                "instrument": ["AAA"],
                "available_at": ["2026-04-20"],
                "emotion_score": [10.0],
            }
        )
        new = pd.DataFrame(
            {
                "trade_date": ["2026-04-20", "2026-04-21"],
                "instrument": ["AAA", "AAA"],
                "available_at": ["2026-04-20", "2026-04-21"],
                "emotion_score": [80.0, 70.0],
            }
        )

        merged = merge_emotion_atmosphere(existing, new)

        self.assertEqual(len(merged), 2)
        latest = merged.set_index(["trade_date", "instrument"])
        self.assertEqual(latest.loc[("2026-04-20", "AAA"), "emotion_score"], 80.0)
        self.assertEqual(latest.loc[("2026-04-21", "AAA"), "emotion_score"], 70.0)

    def test_build_and_write_handle_missing_optional_fields(self):
        liquidity = pd.DataFrame(
            {
                "trade_date": ["2026-04-20", "2026-04-20"],
                "instrument": ["AAA", "BBB"],
                "pct_change": [0.096, -0.101],
            }
        )

        atmosphere = build_emotion_atmosphere(liquidity)

        self.assertEqual(atmosphere["limit_up_count"].iloc[0], 1)
        self.assertEqual(atmosphere["limit_down_count"].iloc[0], 1)
        self.assertEqual(atmosphere["suspended_count"].iloc[0], 0)
        self.assertIn("amount_20d", atmosphere.columns)
        self.assertIn("turnover_20d", atmosphere.columns)

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "emotion_atmosphere.csv"
            write_emotion_atmosphere(atmosphere, output, merge_existing=True)
            write_emotion_atmosphere(atmosphere.assign(emotion_score=42.0), output, merge_existing=True)
            saved = pd.read_csv(output)

        self.assertEqual(len(saved), 2)
        self.assertTrue((saved["emotion_score"] == 42.0).all())


if __name__ == "__main__":
    unittest.main()
