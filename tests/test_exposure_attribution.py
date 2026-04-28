import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.exposure_attribution import (
    build_exposure_attribution,
    load_factor_family_map,
    load_factor_logic_map,
    write_exposure_attribution_markdown,
)


class ExposureAttributionTests(unittest.TestCase):
    def test_build_exposure_attribution_sums_weighted_factor_family_contribution(self):
        portfolio = _portfolio_frame()
        family_map = {"mom_20": "momentum", "rev_20": "reversal", "liq_20": "liquidity"}

        result = build_exposure_attribution(portfolio, family_map=family_map)

        family = result.family.set_index("family")["weighted_contribution"]
        self.assertAlmostEqual(float(family["momentum"]), 0.03, places=7)
        self.assertAlmostEqual(float(family["reversal"]), 0.006, places=7)
        self.assertAlmostEqual(float(family["liquidity"]), -0.003, places=7)

    def test_build_exposure_attribution_reports_industry_and_style_exposure(self):
        portfolio = _portfolio_frame()

        result = build_exposure_attribution(
            portfolio,
            style_cols=["amount_20d", "turnover_20d"],
        )

        industry = result.industry.set_index("industry")["weight"]
        style = result.style.set_index("style")["weighted_average"]
        self.assertAlmostEqual(float(industry["tech"]), 0.3, places=7)
        self.assertAlmostEqual(float(industry["bank"]), 0.2, places=7)
        self.assertAlmostEqual(float(style["amount_20d"]), 140.0, places=7)
        self.assertAlmostEqual(float(style["turnover_20d"]), 0.16, places=7)

    def test_build_exposure_attribution_uses_equal_weights_when_weight_column_is_missing(self):
        signal = _portfolio_frame().drop(columns=["target_weight"])

        result = build_exposure_attribution(signal)

        summary = result.summary.iloc[0]
        industry = result.industry.set_index("industry")["weight"]
        self.assertAlmostEqual(float(summary["gross_weight"]), 1.0, places=7)
        self.assertAlmostEqual(float(industry["tech"]), 0.5, places=7)
        self.assertAlmostEqual(float(industry["bank"]), 0.5, places=7)

    def test_build_exposure_attribution_prefers_family_score_columns(self):
        portfolio = pd.DataFrame(
            {
                "instrument": ["AAA", "BBB"],
                "target_weight": [0.5, 0.5],
                "top_factor_1": ["factor_a", "factor_a"],
                "top_factor_1_contribution": [5.0, 5.0],
                "family_divergence_weak_reversal_score": [0.25, 0.25],
                "family_quiet_range_divergence_score": [0.25, -0.25],
            }
        )

        result = build_exposure_attribution(portfolio, family_map={"factor_a": "divergence_weak_reversal"})

        family = result.family.set_index("family")
        self.assertAlmostEqual(float(family.loc["divergence_weak_reversal", "weighted_contribution"]), 0.25)
        self.assertAlmostEqual(float(family.loc["quiet_range_divergence", "abs_weighted_contribution"]), 0.25)

    def test_build_exposure_attribution_reports_logic_bucket_exposure(self):
        portfolio = pd.DataFrame(
            {
                "instrument": ["AAA", "BBB"],
                "target_weight": [0.5, 0.5],
                "logic_reversal_repair_score": [0.4, 0.2],
                "logic_liquidity_quality_score": [0.1, -0.1],
            }
        )

        result = build_exposure_attribution(portfolio)

        logic = result.logic.set_index("logic_bucket")
        self.assertAlmostEqual(float(logic.loc["reversal_repair", "weighted_contribution"]), 0.3)
        self.assertAlmostEqual(float(logic.loc["liquidity_quality", "abs_weighted_contribution"]), 0.1)

    def test_load_factor_family_map_reads_approved_factors_yaml(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "approved.yaml"
            path.write_text(
                "approved_factors:\n"
                "  - name: mom_20\n"
                "    family: momentum\n"
                "  - name: rev_20\n"
                "    family: reversal\n",
                encoding="utf-8",
            )

            result = load_factor_family_map(path)

        self.assertEqual(result, {"mom_20": "momentum", "rev_20": "reversal"})

    def test_load_factor_logic_map_reads_approved_factors_yaml(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "approved.yaml"
            path.write_text(
                "approved_factors:\n"
                "  - name: mom_20\n"
                "    family: momentum\n"
                "    logic_bucket: trend_following\n"
                "  - name: rev_20\n"
                "    family: divergence_weak_reversal\n",
                encoding="utf-8",
            )

            result = load_factor_logic_map(path)

        self.assertEqual(result, {"mom_20": "trend_following", "rev_20": "reversal_repair"})

    def test_write_exposure_attribution_markdown(self):
        result = build_exposure_attribution(_portfolio_frame(), family_map={"mom_20": "momentum"})
        with tempfile.TemporaryDirectory() as tmp:
            output = write_exposure_attribution_markdown(result, Path(tmp) / "exposure.md")
            text = output.read_text(encoding="utf-8")

        self.assertIn("# Exposure Attribution", text)
        self.assertIn("## Factor Families", text)
        self.assertIn("momentum", text)


def _portfolio_frame():
    return pd.DataFrame(
        {
            "instrument": ["a", "b"],
            "target_weight": [0.3, 0.2],
            "industry": ["tech", "bank"],
            "amount_20d": [100.0, 200.0],
            "turnover_20d": [0.1, 0.25],
            "top_factor_1": ["mom_20", "mom_20"],
            "top_factor_1_contribution": [0.10, 0.00],
            "top_factor_2": ["rev_20", "liq_20"],
            "top_factor_2_contribution": [0.02, -0.015],
        }
    )


if __name__ == "__main__":
    unittest.main()
