import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.combo_spec import build_combo_exposures, load_combo_spec, signal_factors_from_combo_spec
from qlib_factor_lab.signal import SignalConfig


class ComboSpecTests(unittest.TestCase):
    def test_load_combo_spec_returns_signal_factors_with_family_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            spec_path = root / "combo.yaml"
            spec_path.write_text(
                yaml.safe_dump(
                    {
                        "name": "quality_gap_breakout_v1",
                        "members": [
                            {
                                "name": "quality_low_leverage",
                                "source": "fundamental_quality",
                                "family": "fundamental_quality",
                                "logic_bucket": "quality_low_leverage",
                                "direction": 1,
                                "weight": 0.6,
                            },
                            {
                                "name": "gap_risk_20",
                                "source": "approved_factor",
                                "family": "gap_risk",
                                "logic_bucket": "risk_structure",
                                "direction": 1,
                                "weight": 0.25,
                            },
                        ],
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            spec = load_combo_spec(spec_path)
            factors = signal_factors_from_combo_spec(spec)

            self.assertEqual("quality_gap_breakout_v1", spec.name)
            self.assertEqual(["quality_low_leverage", "gap_risk_20"], [factor.name for factor in factors])
            self.assertEqual("fundamental_quality", factors[0].family)
            self.assertEqual("quality_low_leverage", factors[0].logic_bucket)

    def test_build_combo_exposures_merges_point_in_time_fundamentals(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame(
                {
                    "instrument": ["AAA", "BBB", "AAA"],
                    "available_at": ["2026-04-01", "2026-04-01", "2026-05-01"],
                    "roe": [12.0, 6.0, 30.0],
                    "debt_ratio": [20.0, 80.0, 5.0],
                }
            ).to_csv(root / "data/fundamental_quality.csv", index=False)
            base = pd.DataFrame(
                {
                    "date": ["2026-04-23", "2026-04-23"],
                    "instrument": ["AAA", "BBB"],
                    "tradable": [True, True],
                    "gap_risk_20": [0.2, -0.1],
                    "last_price": [10.0, 20.0],
                    "amount_20d": [1_000_000, 1_000_000],
                }
            )
            spec = load_combo_spec(
                {
                    "name": "quality_gap_breakout_v1",
                    "members": [
                        {
                            "name": "quality_low_leverage",
                            "source": "fundamental_quality",
                            "family": "fundamental_quality",
                            "logic_bucket": "quality_low_leverage",
                            "direction": 1,
                            "weight": 0.6,
                            "components": [
                                {"field": "roe", "direction": 1, "weight": 1.0},
                                {"field": "debt_ratio", "direction": -1, "weight": 0.25},
                            ],
                        },
                        {
                            "name": "gap_risk_20",
                            "source": "approved_factor",
                            "family": "gap_risk",
                            "logic_bucket": "risk_structure",
                            "direction": 1,
                            "weight": 0.25,
                        },
                    ],
                }
            )
            config = SignalConfig(
                approved_factors_path=Path("reports/approved_factors.yaml"),
                provider_config=Path("configs/provider_current.yaml"),
                run_date="2026-04-23",
                active_regime="sideways",
                status_weights={"core": 1.0},
                regime_weights={"all_weather": {"sideways": 1.0}},
                rule_weight=1.0,
                model_weight=0.0,
                signals_output_path=Path("reports/signals.csv"),
                summary_output_path=Path("reports/summary.md"),
                combination_mode="family_first",
            )

            output = build_combo_exposures(root, spec, base, config)

            self.assertIn("quality_low_leverage", output.columns)
            aaa = output[output["instrument"] == "AAA"].iloc[0]
            bbb = output[output["instrument"] == "BBB"].iloc[0]
            self.assertGreater(float(aaa["quality_low_leverage"]), float(bbb["quality_low_leverage"]))
            self.assertEqual(12.0, float(aaa["roe"]))
            self.assertEqual(20.0, float(aaa["debt_ratio"]))


if __name__ == "__main__":
    unittest.main()
