from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.autoresearch.emotion_data_oracle import evaluate_emotion_data_factors, load_emotion_data_factor_specs


class EmotionDataOracleTests(unittest.TestCase):
    def test_load_emotion_data_factor_specs_prefers_data_domain_columns(self) -> None:
        specs = load_emotion_data_factor_specs()

        by_name = {spec["name"]: spec for spec in specs}
        self.assertIn("instrument_emotion_score", by_name)
        self.assertIn("crowding_cooling_score", by_name)
        self.assertEqual(by_name["instrument_emotion_score"]["source_column"], "instrument_emotion_score")
        self.assertEqual(by_name["crowding_cooling_score"]["direction"], 1)

    def test_evaluate_emotion_data_factors_uses_tabular_signal_not_qlib_expression(self) -> None:
        dates = pd.date_range("2026-01-01", periods=5, freq="D").strftime("%Y-%m-%d").tolist()
        emotion_rows = []
        close_rows = []
        for i, date in enumerate(dates):
            emotion_rows.extend(
                [
                    {"trade_date": date, "instrument": "AAA", "instrument_emotion_score": 90 - i},
                    {"trade_date": date, "instrument": "BBB", "instrument_emotion_score": 10 + i},
                ]
            )
            close_rows.extend(
                [
                    {"trade_date": date, "instrument": "AAA", "close": 10 + i},
                    {"trade_date": date, "instrument": "BBB", "close": 10 - i * 0.1},
                ]
            )

        summary = evaluate_emotion_data_factors(
            pd.DataFrame(emotion_rows),
            pd.DataFrame(close_rows),
            [{"name": "instrument_emotion_score", "source_column": "instrument_emotion_score", "direction": 1}],
            horizons=(1,),
        )

        row = summary.iloc[0]
        self.assertEqual(row["factor"], "instrument_emotion_score")
        self.assertGreater(float(row["rank_ic_mean"]), 0.9)
        self.assertEqual(int(row["observations"]), 8)

    def test_evaluate_emotion_data_factors_discards_missing_source_columns(self) -> None:
        summary = evaluate_emotion_data_factors(
            pd.DataFrame({"trade_date": ["2026-01-01"], "instrument": ["AAA"]}),
            pd.DataFrame({"trade_date": ["2026-01-01"], "instrument": ["AAA"], "close": [10.0]}),
            [{"name": "missing", "source_column": "missing", "direction": 1}],
            horizons=(1,),
        )

        self.assertEqual(int(summary.iloc[0]["observations"]), 0)
        self.assertTrue(pd.isna(summary.iloc[0]["rank_ic_mean"]))

    def test_evaluate_emotion_data_factors_handles_missing_forward_returns(self) -> None:
        summary = evaluate_emotion_data_factors(
            pd.DataFrame(
                {
                    "trade_date": ["2026-04-30"],
                    "instrument": ["AAA"],
                    "instrument_emotion_score": [75.0],
                }
            ),
            pd.DataFrame({"trade_date": ["2026-04-30"], "instrument": ["AAA"], "close": [10.0]}),
            [{"name": "instrument_emotion_score", "source_column": "instrument_emotion_score", "direction": 1}],
            horizons=(5,),
        )

        self.assertEqual(int(summary.iloc[0]["observations"]), 0)
        self.assertTrue(pd.isna(summary.iloc[0]["ic_mean"]))
        self.assertTrue(pd.isna(summary.iloc[0]["rank_ic_mean"]))


if __name__ == "__main__":
    unittest.main()
