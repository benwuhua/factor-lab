import unittest

import pandas as pd

from qlib_factor_lab.factor_eval import EvalConfig, prepare_factor_signal, with_directional_signal
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


if __name__ == "__main__":
    unittest.main()
