import unittest
from unittest.mock import patch

import pandas as pd
from pandas.core.groupby.generic import DataFrameGroupBy

from qlib_factor_lab.factor_eval import EvalConfig, evaluate_factor, prepare_factor_signal, with_directional_signal
from qlib_factor_lab.factor_registry import FactorDef


class FactorEvalTests(unittest.TestCase):
    def test_with_directional_signal_flips_negative_direction_factors(self):
        frame = pd.DataFrame({"vol_20": [0.1, 0.2, 0.3]})
        factor = FactorDef(name="vol_20", expression="Std($close, 20)", direction=-1)

        result = with_directional_signal(frame, factor)

        self.assertEqual(result["signal"].tolist(), [-0.1, -0.2, -0.3])

    def test_prepare_factor_signal_applies_cross_sectional_purification_steps(self):
        index = pd.MultiIndex.from_tuples(
            [
                ("2020-01-01", "a"),
                ("2020-01-01", "b"),
                ("2020-01-01", "c"),
                ("2020-01-01", "d"),
            ],
            names=["datetime", "instrument"],
        )
        frame = pd.DataFrame({"alpha": [1.0, 2.0, 3.0, 100.0], "close": [1.0] * 4}, index=index)
        factor = FactorDef(name="alpha", expression="$close", direction=1)

        result = prepare_factor_signal(
            frame,
            factor,
            EvalConfig(purification_steps=("mad", "rank"), purification_mad_n=2.0),
        )

        self.assertEqual(result["signal"].round(6).tolist(), [-0.375, -0.125, 0.125, 0.375])
        self.assertEqual(set(result["purification"]), {"mad+rank"})

    def test_evaluate_factor_reports_zero_observations_for_empty_factor_frame(self):
        factor = FactorDef(name="missing_value_factor", expression="$missing", direction=1)
        empty = pd.DataFrame(columns=["missing_value_factor", "close"])

        with patch("qlib_factor_lab.factor_eval.fetch_factor_frame", return_value=empty):
            result = evaluate_factor(object(), factor, initialize=False)

        self.assertEqual([1, 5, 10, 20], result["horizon"].tolist())
        self.assertEqual([0, 0, 0, 0], result["observations"].tolist())
        self.assertTrue(result["ic_mean"].isna().all())
        self.assertTrue(result["long_short_mean_return"].isna().all())

    def test_evaluate_factor_collapses_dataframe_ic_outputs_to_numeric_metrics(self):
        index = pd.MultiIndex.from_product(
            [
                pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
                ["a", "b", "c"],
            ],
            names=["datetime", "instrument"],
        )
        frame = pd.DataFrame(
            {
                "alpha": [1.0, 2.0, 3.0, 1.5, 2.5, 3.5, 2.0, 3.0, 4.0],
                "close": [10.0, 11.0, 12.0, 10.5, 11.5, 12.5, 11.0, 12.0, 13.0],
            },
            index=index,
        )
        factor = FactorDef(name="alpha", expression="$close", direction=1)

        with (
            patch("qlib_factor_lab.factor_eval.fetch_factor_frame", return_value=frame),
            patch.object(
                DataFrameGroupBy,
                "apply",
                side_effect=[
                    pd.DataFrame({"corr": [0.10, 0.20]}),
                    pd.DataFrame({"corr": [0.20, 0.40]}),
                ],
            ),
        ):
            result = evaluate_factor(
                object(),
                factor,
                EvalConfig(horizons=(1,), quantiles=2),
                initialize=False,
            )

        self.assertEqual([0.15], result["ic_mean"].round(6).tolist())
        self.assertEqual([0.30], result["rank_ic_mean"].round(6).tolist())
        self.assertTrue(pd.api.types.is_numeric_dtype(result["icir"]))
        self.assertTrue(pd.api.types.is_numeric_dtype(result["rank_icir"]))


if __name__ == "__main__":
    unittest.main()
