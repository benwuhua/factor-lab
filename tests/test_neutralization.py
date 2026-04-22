import unittest

import pandas as pd

from qlib_factor_lab.neutralization import neutralize_signal


class NeutralizationTests(unittest.TestCase):
    def test_neutralize_signal_removes_linear_size_exposure_by_date(self):
        index = pd.MultiIndex.from_tuples(
            [
                ("2020-01-01", "a"),
                ("2020-01-01", "b"),
                ("2020-01-01", "c"),
                ("2020-01-01", "d"),
                ("2020-01-02", "a"),
                ("2020-01-02", "b"),
                ("2020-01-02", "c"),
                ("2020-01-02", "d"),
            ],
            names=["datetime", "instrument"],
        )
        frame = pd.DataFrame(
            {
                "signal": [2.0, 5.0, 6.0, 9.0, 1.0, 4.0, 5.0, 8.0],
                "size": [1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0],
            },
            index=index,
        )

        result = neutralize_signal(frame, exposure_cols=["size"])

        for _, daily in result.groupby(level="datetime"):
            self.assertAlmostEqual(daily["signal_neutral"].corr(daily["size"]), 0.0, places=6)
            self.assertAlmostEqual(daily["signal_neutral"].mean(), 0.0, places=6)

    def test_neutralize_signal_handles_industry_groups(self):
        index = pd.MultiIndex.from_tuples(
            [
                ("2020-01-01", "a"),
                ("2020-01-01", "b"),
                ("2020-01-01", "c"),
                ("2020-01-01", "d"),
            ],
            names=["datetime", "instrument"],
        )
        frame = pd.DataFrame(
            {
                "signal": [10.0, 12.0, 20.0, 22.0],
                "industry": ["bank", "bank", "tech", "tech"],
            },
            index=index,
        )

        result = neutralize_signal(frame, group_col="industry")

        means = result.groupby("industry")["signal_neutral"].mean()
        self.assertAlmostEqual(means["bank"], 0.0, places=6)
        self.assertAlmostEqual(means["tech"], 0.0, places=6)


if __name__ == "__main__":
    unittest.main()
