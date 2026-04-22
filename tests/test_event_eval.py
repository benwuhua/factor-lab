import unittest

import pandas as pd

from qlib_factor_lab.event_eval import EventEvalConfig, evaluate_event_buckets


class EventEvalTests(unittest.TestCase):
    def test_evaluate_event_buckets_computes_returns_mfe_mae_and_payoff(self):
        dates = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"])
        instruments = ["A", "B", "C", "D"]
        index = pd.MultiIndex.from_product([dates, instruments], names=["datetime", "instrument"])
        frame = pd.DataFrame(index=index)
        frame["signal"] = [1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4]
        frame["close"] = [
            10,
            20,
            30,
            40,
            11,
            18,
            33,
            36,
            12,
            22,
            36,
            32,
        ]
        frame["high"] = [
            10,
            20,
            30,
            40,
            12,
            21,
            34,
            41,
            13,
            23,
            37,
            37,
        ]
        frame["low"] = [
            10,
            20,
            30,
            40,
            9,
            17,
            29,
            35,
            11,
            18,
            32,
            31,
        ]

        result = evaluate_event_buckets(
            frame,
            "signal",
            EventEvalConfig(horizons=(2,), buckets=((0.70, 0.85), (0.85, 1.0))),
        )

        rows = {row["bucket"]: row for row in result.to_dict("records")}
        self.assertAlmostEqual(rows["p70_p85"]["mean_return"], 0.2)
        self.assertAlmostEqual(rows["p70_p85"]["mfe_mean"], (37 / 30) - 1)
        self.assertAlmostEqual(rows["p70_p85"]["mae_mean"], (29 / 30) - 1)
        self.assertEqual(rows["p70_p85"]["win_rate"], 1.0)
        self.assertAlmostEqual(rows["p85_p100"]["mean_return"], -0.2)
        self.assertAlmostEqual(rows["p85_p100"]["avg_loss"], -0.2)
        self.assertEqual(rows["p85_p100"]["payoff_ratio"], 0.0)

    def test_evaluate_event_buckets_reports_yearly_stability_rows(self):
        dates = pd.to_datetime(["2025-12-30", "2025-12-31", "2026-01-01", "2026-01-02"])
        instruments = ["A", "B", "C", "D"]
        index = pd.MultiIndex.from_product([dates, instruments], names=["datetime", "instrument"])
        frame = pd.DataFrame(index=index)
        frame["signal"] = [1, 2, 3, 4] * len(dates)
        frame["close"] = [10, 20, 30, 40, 11, 21, 31, 41, 12, 22, 32, 42, 13, 23, 33, 43]
        frame["high"] = frame["close"] + 1
        frame["low"] = frame["close"] - 1

        result = evaluate_event_buckets(
            frame,
            "signal",
            EventEvalConfig(horizons=(1,), buckets=((0.85, 1.0),), by_year=True),
        )

        self.assertEqual(result["year"].dropna().astype(int).tolist(), [2025, 2026])
        self.assertEqual(result["event_count"].tolist(), [2, 1])


if __name__ == "__main__":
    unittest.main()
