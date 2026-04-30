import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.combo_spec import (
    build_combo_exposures,
    factor_diagnostics_from_combo_spec,
    load_combo_spec,
    signal_factors_from_combo_spec,
)
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

    def test_inactive_combo_members_are_kept_in_spec_but_not_scored(self):
        spec = load_combo_spec(
            {
                "name": "balanced_multifactor_v1",
                "members": [
                    {"name": "quality_low_leverage", "source": "fundamental_quality", "family": "fundamental_quality"},
                    {"name": "dividend_yield", "source": "fundamental_quality", "family": "dividend", "active": False},
                ],
            }
        )

        factors = signal_factors_from_combo_spec(spec)

        self.assertEqual(["quality_low_leverage"], [factor.name for factor in factors])
        self.assertEqual(["quality_low_leverage", "dividend_yield"], [member.name for member in spec.members])

    def test_combo_diagnostics_merge_existing_ic_and_keep_member_context(self):
        spec = load_combo_spec(
            {
                "name": "quality_gap_breakout_v1",
                "members": [
                    {
                        "name": "gap_risk_20",
                        "source": "qlib_expression",
                        "family": "gap_risk",
                        "logic_bucket": "risk_structure",
                        "direction": -1,
                        "weight": 0.25,
                    },
                    {
                        "name": "quality_low_leverage",
                        "source": "fundamental_quality",
                        "family": "fundamental_quality",
                        "direction": 1,
                        "weight": 0.6,
                    },
                ],
            }
        )
        existing = pd.DataFrame(
            {
                "factor": ["gap_risk_20"],
                "family": ["risk_structure_old"],
                "suggested_role": ["watch"],
                "neutral_rank_ic_h20": [0.0123],
                "neutral_long_short_h20": [0.0045],
                "concerns": ["low_sample"],
            }
        )

        diagnostics = factor_diagnostics_from_combo_spec(spec, existing)
        by_factor = diagnostics.set_index("factor")

        self.assertEqual(0.0123, float(by_factor.loc["gap_risk_20", "neutral_rank_ic_h20"]))
        self.assertEqual(0.0045, float(by_factor.loc["gap_risk_20", "neutral_long_short_h20"]))
        self.assertIn("source=qlib_expression; direction=-1; weight=0.25", by_factor.loc["gap_risk_20", "concerns"])
        self.assertIn("low_sample", by_factor.loc["gap_risk_20", "concerns"])
        self.assertEqual("", by_factor.loc["quality_low_leverage", "neutral_rank_ic_h20"])

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

    def test_balanced_multifactor_disables_unstable_quality_member(self):
        spec = load_combo_spec(Path(__file__).resolve().parents[1] / "configs/combo_specs/balanced_multifactor_v1.yaml")
        by_name = {member.name: member for member in spec.members}
        factors = signal_factors_from_combo_spec(spec)

        self.assertIn("quality_low_leverage", by_name)
        self.assertFalse(by_name["quality_low_leverage"].active)
        self.assertNotIn("quality_low_leverage", [factor.name for factor in factors])

    def test_offensive_multifactor_spec_is_not_defensive_value_or_dividend_dominated(self):
        spec = load_combo_spec(Path(__file__).resolve().parents[1] / "configs/combo_specs/offensive_multifactor_v1.yaml")
        active = [member for member in spec.members if member.active]
        by_family = {}
        for member in active:
            by_family[member.family] = by_family.get(member.family, 0.0) + member.weight

        self.assertGreaterEqual(by_family.get("momentum", 0.0), 0.35)
        self.assertGreaterEqual(by_family.get("volume_confirm", 0.0) + by_family.get("quiet_breakout", 0.0), 0.25)
        self.assertLessEqual(by_family.get("value", 0.0), 0.05)
        self.assertLessEqual(by_family.get("dividend", 0.0), 0.05)
        self.assertIn("growth_improvement", [member.name for member in active])

    def test_tushare_market_enriched_spec_uses_daily_basic_value_dividend_liquidity_fields(self):
        spec = load_combo_spec(Path(__file__).resolve().parents[1] / "configs/combo_specs/tushare_market_enriched_v1.yaml")
        by_name = {member.name: member for member in spec.members}

        self.assertIn("$pe_ttm", by_name["tushare_ep_ttm"].expression)
        self.assertIn("$pb", by_name["tushare_bp"].expression)
        self.assertIn("$dividend_yield", by_name["tushare_dividend_yield"].expression)
        self.assertIn("$turnover_rate_f", by_name["tushare_free_float_turnover_20"].expression)
        self.assertIn("$total_mv", by_name["tushare_total_mv"].expression)
        self.assertEqual("qlib_expression", by_name["tushare_amount_20"].source)
        self.assertFalse(by_name["tushare_amount_20"].active)
        self.assertEqual(0.0, by_name["tushare_amount_20"].weight)
        self.assertEqual("guardrail", by_name["tushare_amount_20"].approval_status)


if __name__ == "__main__":
    unittest.main()
