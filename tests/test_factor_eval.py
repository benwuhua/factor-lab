import unittest

import pandas as pd

from qlib_factor_lab.factor_eval import with_directional_signal
from qlib_factor_lab.factor_registry import FactorDef


class FactorEvalTests(unittest.TestCase):
    def test_with_directional_signal_flips_negative_direction_factors(self):
        frame = pd.DataFrame({"vol_20": [0.1, 0.2, 0.3]})
        factor = FactorDef(name="vol_20", expression="Std($close, 20)", direction=-1)

        result = with_directional_signal(frame, factor)

        self.assertEqual(result["signal"].tolist(), [-0.1, -0.2, -0.3])


if __name__ == "__main__":
    unittest.main()
