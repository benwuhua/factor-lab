import unittest

import pandas as pd

from qlib_factor_lab.combo_diagnostics import evaluate_combo_member_diagnostics
from qlib_factor_lab.combo_spec import load_combo_spec


class ComboDiagnosticsTests(unittest.TestCase):
    def test_evaluate_combo_member_diagnostics_outputs_recent_ic_and_long_short(self):
        dates = pd.date_range("2026-01-01", periods=24, freq="D")
        instruments = ["AAA", "BBB", "CCC", "DDD", "EEE"]
        rows = []
        for date_index, date in enumerate(dates):
            for score, instrument in enumerate(instruments, start=1):
                rows.append(
                    {
                        "datetime": date,
                        "instrument": instrument,
                        "close": 10 + date_index * (1 + score / 10),
                        "quality_low_leverage": float(score),
                    }
                )
        frame = pd.DataFrame(rows).set_index(["datetime", "instrument"])
        spec = load_combo_spec(
            {
                "name": "balanced_multifactor_v1",
                "members": [
                    {
                        "name": "quality_low_leverage",
                        "source": "fundamental_quality",
                        "family": "fundamental_quality",
                        "direction": 1,
                        "weight": 0.3,
                    }
                ],
            }
        )

        diagnostics = evaluate_combo_member_diagnostics(frame, spec, horizons=(5,), quantiles=5)

        self.assertEqual(["quality_low_leverage"], diagnostics["factor"].tolist())
        row = diagnostics.iloc[0]
        self.assertGreater(float(row["neutral_rank_ic_h5"]), 0)
        self.assertGreater(float(row["neutral_long_short_h5"]), 0)
        self.assertEqual("combo_recent_formal", row["suggested_role"])
        self.assertIn("recent_formal_raw_ic_ls", row["concerns"])


if __name__ == "__main__":
    unittest.main()
