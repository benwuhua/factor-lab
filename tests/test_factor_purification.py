import unittest

import pandas as pd

from qlib_factor_lab.factor_purification import (
    mad_winsorize_by_date,
    neutralize_by_date,
    purify_factor_frame,
    rank_standardize_by_date,
    zscore_standardize_by_date,
)


class FactorPurificationTests(unittest.TestCase):
    def test_mad_winsorize_caps_cross_sectional_outlier_by_date(self):
        frame = _factor_frame([1.0, 2.0, 3.0, 100.0])

        result = mad_winsorize_by_date(frame, "factor", n_mad=2.0, output_col="factor_mad")

        self.assertLess(result.loc[("2020-01-01", "d"), "factor_mad"], 100.0)
        self.assertEqual(result.loc[("2020-01-01", "a"), "factor_mad"], 1.0)

    def test_zscore_standardize_is_cross_sectional_by_date(self):
        frame = _factor_frame([1.0, 2.0, 3.0, 4.0])

        result = zscore_standardize_by_date(frame, "factor", output_col="factor_z")

        daily = result.xs("2020-01-01", level="datetime")
        self.assertAlmostEqual(float(daily["factor_z"].mean()), 0.0, places=7)
        self.assertAlmostEqual(float(daily["factor_z"].std(ddof=0)), 1.0, places=7)

    def test_rank_standardize_maps_to_centered_percentile(self):
        frame = _factor_frame([10.0, 20.0, 30.0, 40.0])

        result = rank_standardize_by_date(frame, "factor", output_col="factor_rank")

        self.assertEqual(result["factor_rank"].round(6).tolist(), [-0.375, -0.125, 0.125, 0.375])

    def test_neutralize_by_date_removes_linear_exposure(self):
        frame = _factor_frame([2.0, 5.0, 6.0, 9.0])
        frame["size"] = [1.0, 2.0, 3.0, 4.0]

        result = neutralize_by_date(frame, "factor", exposure_cols=["size"], output_col="factor_neutral")

        daily = result.xs("2020-01-01", level="datetime")
        self.assertAlmostEqual(float(daily["factor_neutral"].corr(daily["size"])), 0.0, places=6)
        self.assertAlmostEqual(float(daily["factor_neutral"].mean()), 0.0, places=6)

    def test_purify_factor_frame_applies_ordered_steps(self):
        frame = _factor_frame([1.0, 2.0, 3.0, 100.0])

        result = purify_factor_frame(
            frame,
            "factor",
            steps=("mad", "zscore", "rank"),
            output_col="factor_purified",
            mad_n=1.0,
        )

        self.assertIn("factor_purified", result.columns)
        self.assertAlmostEqual(float(result["factor_purified"].mean()), 0.0, places=7)
        self.assertEqual(result["factor_purified"].round(6).tolist(), [-0.375, -0.125, 0.125, 0.375])


def _factor_frame(values):
    index = pd.MultiIndex.from_tuples(
        [("2020-01-01", "a"), ("2020-01-01", "b"), ("2020-01-01", "c"), ("2020-01-01", "d")],
        names=["datetime", "instrument"],
    )
    return pd.DataFrame({"factor": values}, index=index)


if __name__ == "__main__":
    unittest.main()
