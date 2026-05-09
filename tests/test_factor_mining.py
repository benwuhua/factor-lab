import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.factor_mining import generate_candidate_factors, load_mining_config, rank_factor_results


class FactorMiningTests(unittest.TestCase):
    def test_generate_candidate_factors_expands_windows(self):
        config = {
            "templates": [
                {
                    "name": "mom_{window}",
                    "expression": "$close / Ref($close, {window}) - 1",
                    "windows": [5, 20],
                    "category": "candidate_momentum",
                    "direction": 1,
                }
            ]
        }

        factors = generate_candidate_factors(config)

        self.assertEqual([f.name for f in factors], ["mom_5", "mom_20"])
        self.assertEqual(factors[1].expression, "$close / Ref($close, 20) - 1")
        self.assertEqual(factors[1].category, "candidate_momentum")

    def test_generate_candidate_factors_expands_parameter_grid(self):
        config = {
            "templates": [
                {
                    "name": "mom_skip_{lookback}_{skip}",
                    "expression": "Ref($close, {skip}) / Ref($close, {lookback}) - 1",
                    "params": {"lookback": [20, 60], "skip": [5]},
                    "category": "candidate_momentum",
                    "direction": 1,
                }
            ]
        }

        factors = generate_candidate_factors(config)

        self.assertEqual([f.name for f in factors], ["mom_skip_20_5", "mom_skip_60_5"])
        self.assertEqual(factors[0].expression, "Ref($close, 5) / Ref($close, 20) - 1")

    def test_rank_factor_results_sorts_by_absolute_rank_ic(self):
        results = pd.DataFrame(
            [
                {"factor": "a", "horizon": 5, "rank_ic_mean": 0.01},
                {"factor": "b", "horizon": 5, "rank_ic_mean": -0.08},
                {"factor": "c", "horizon": 5, "rank_ic_mean": 0.03},
            ]
        )

        ranked = rank_factor_results(results, metric="rank_ic_mean")

        self.assertEqual(ranked["factor"].tolist(), ["b", "c", "a"])
        self.assertEqual(ranked.iloc[0]["abs_rank_ic_mean"], 0.08)

    def test_default_mining_config_covers_core_candidate_families(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))

        categories = {factor.category for factor in factors}
        self.assertGreaterEqual(len(factors), 30)
        self.assertTrue(
            {
                "candidate_momentum",
                "candidate_reversal",
                "candidate_volume_price",
                "candidate_volatility",
                "candidate_liquidity",
                "candidate_divergence",
                "candidate_pattern",
            }.issubset(categories)
        )

    def test_default_mining_config_includes_wangji_factor1_binary_14_day_match(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))
        factor_by_name = {factor.name: factor for factor in factors}

        factor = factor_by_name["wangji-factor1"]
        self.assertEqual(factor.category, "candidate_pattern")
        self.assertTrue(factor.expression.startswith("If("))
        self.assertTrue(factor.expression.endswith(", 1, 0)"))
        self.assertIn("Le(Max(Ref($close, 4), 10) / Min(Ref($close, 4), 10) - 1, 0.06)", factor.expression)
        self.assertIn("Ge(Ref($close, 3) / Ref($close, 4) - 1, 0.07)", factor.expression)
        self.assertIn("Gt(Ref($close, 3), Ref($open, 3))", factor.expression)
        self.assertIn("Ge(Ref($volume, 3) / (Mean(Ref($volume, 4), 10) + 1), 2.0)", factor.expression)
        self.assertIn("Gt(Ref($close, 2) / Ref($close, 3) - 1, -0.03)", factor.expression)
        self.assertIn("Lt(Ref($close, 2) / Ref($close, 3) - 1, 0)", factor.expression)
        self.assertIn("Lt(Ref($volume, 2), Ref($volume, 3))", factor.expression)
        self.assertIn("Lt($volume, Ref($volume, 3))", factor.expression)
        self.assertNotIn("4 - Abs(", factor.expression)
        self.assertNotIn("/ 0.06", factor.expression)
        self.assertNotIn("2.2 *", factor.expression)
        self.assertNotIn("Ref(Max($high, 120), 30)", factor.expression)
        self.assertIn("binary", factor.description)
        self.assertIn("full match", factor.description)

    def test_default_mining_config_includes_wangji_ignition_setup(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))
        factor_by_name = {factor.name: factor for factor in factors}

        factor = factor_by_name["wangji-ignition-setup"]
        self.assertEqual(factor.category, "candidate_pattern")
        self.assertIn("Max(Ref($close, 1), 20) / Min(Ref($close, 1), 20)", factor.expression)
        self.assertIn("Ref(Max($close, 60), 1) / Ref($close, 1) - 1", factor.expression)
        self.assertIn("Mean(Ref($close, 1), 5) / Mean(Ref($close, 1), 30)", factor.expression)
        self.assertIn("Less($close / Ref($close, 1) - 1, 0.12) / 0.06", factor.expression)
        self.assertIn("Less($close / $open - 1, 0.12) / 0.06", factor.expression)
        self.assertIn("($close - $low) / ($high - $low + 0.000001)", factor.expression)
        self.assertIn("Less($volume / (Mean(Ref($volume, 1), 20) + 1), 3.0) / 1.5", factor.expression)
        self.assertIn("ignition day", factor.description)
        self.assertIn("pre-confirmation", factor.description)

    def test_default_mining_config_includes_wangji_reversal20_combo_gate(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))
        factor_by_name = {factor.name: factor for factor in factors}

        factor = factor_by_name["wangji-reversal20-combo"]
        self.assertEqual(factor.category, "candidate_pattern")
        self.assertIn("If(And(", factor.expression)
        self.assertTrue(factor.expression.endswith(", 1, 0)"))
        self.assertIn("Ref($close, 20) / $close - 1", factor.expression)
        self.assertIn("Ge(Ref($close, 20) / $close - 1, 0.03)", factor.expression)
        self.assertIn("Le(Max(Ref($close, 4), 10) / Min(Ref($close, 4), 10) - 1, 0.06)", factor.expression)
        self.assertNotIn("4 - Abs(", factor.expression)
        self.assertNotIn("- 10", factor.expression)
        self.assertEqual(factor.direction, 1)

    def test_default_mining_config_includes_wangji_factor2_2b_pullback_confirmation(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))
        factor_by_name = {factor.name: factor for factor in factors}
        wangji_factor2_names = [factor.name for factor in factors if factor.name.startswith("wangji-factor2")]

        self.assertEqual(wangji_factor2_names, ["wangji-factor2"])
        factor = factor_by_name["wangji-factor2"]
        self.assertEqual(factor.category, "candidate_pattern")
        self.assertEqual(factor.direction, 1)
        self.assertTrue(factor.expression.startswith("If("))
        self.assertTrue(factor.expression.endswith(", 1, 0)"))
        self.assertIn("Gt(Mean($close, 5), Mean(Ref($close, 5), 5))", factor.expression)
        self.assertIn("Gt(Mean(Ref($close, 5), 5), Mean(Ref($close, 10), 5))", factor.expression)
        self.assertIn("Gt(Mean(Ref($close, 10), 5), Mean(Ref($close, 15), 5))", factor.expression)
        self.assertIn("Gt(Mean(Ref($close, 15), 5), Mean(Ref($close, 20), 5))", factor.expression)
        self.assertIn("Gt($close, Mean($close, 21))", factor.expression)
        self.assertIn("Max(Ref(And(Gt($close, Mean($close, 21)), And(Gt(Mean($close, 5), Mean($close, 13))", factor.expression)
        self.assertIn("Ge($close / (Min(Ref($low, 1), 20) + 0.000001) - 1, 0.08)", factor.expression)
        self.assertIn("Gt(Mean($close, 13), Ref(Mean($close, 13), 3)", factor.expression)
        self.assertIn("), 10), 51), 1)", factor.expression)
        self.assertIn("Gt($close, Mean($close, 60))", factor.expression)
        self.assertIn("Gt(Mean($close, 5), Mean($close, 13))", factor.expression)
        self.assertIn("Gt(Mean($close, 13), Mean($close, 21))", factor.expression)
        self.assertIn("Gt(Max(Ref($close, 3), 20), Max(Ref($high, 23), 60))", factor.expression)
        self.assertIn("Le(Min(Ref($low, 1), 10) / (Mean($close, 21) + 0.000001), 1.04)", factor.expression)
        self.assertIn("Ge(Min(Ref($close, 1), 10) / (Mean($close, 21) + 0.000001), 0.93)", factor.expression)
        self.assertIn("Le(Mean(Ref($volume, 1), 5) / (Mean(Ref($volume, 10), 10) + 1), 1.35)", factor.expression)
        self.assertIn("Ge($close / Ref($close, 1) - 1, 0.02)", factor.expression)
        self.assertIn("Ge($close / $open - 1, 0.02)", factor.expression)
        self.assertIn("Gt($close, Mean($close, 5))", factor.expression)
        self.assertIn("Gt($close, Mean($close, 13))", factor.expression)
        self.assertIn("Ge($volume / (Mean(Ref($volume, 1), 10) + 1), 1.1)", factor.expression)
        self.assertIn("Gt($close, Max(Ref($close, 1), 10))", factor.expression)
        self.assertNotIn("Le($close / (Mean($close, 5) + 0.000001), 1.08)", factor.expression)
        self.assertNotIn("Gt($close, Max(Ref($high, 1), 120))", factor.expression)
        self.assertNotIn("Ref($close, 0)", factor.expression)
        self.assertNotIn("Ref($open, 0)", factor.expression)
        self.assertNotIn("Ref(Mean($close, 5), 0)", factor.expression)
        self.assertIn("2B pullback confirmation", factor.description)
        self.assertIn("prior golden buy", factor.description)
        self.assertIn("weekly bullish", factor.description)
        self.assertIn("space-open breakout", factor.description)
        self.assertIn("10-day close high", factor.description)

    def test_default_mining_config_includes_joinquant_factorlib_migrations(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))
        factor_by_name = {factor.name: factor for factor in factors}

        expected = {
            "davol_20": ("candidate_turnover", "Mean($turnover, 20) / (Mean($turnover, 120) + 0.000001)", -1),
            "turnover_volatility_20": ("candidate_turnover", "Std($turnover, 20)", -1),
            "arbr_26": ("candidate_emotion", "Sum($high - $open, 26)", -1),
            "vosc_12_26": ("candidate_volume_price", "Mean($volume, 12) - Mean($volume, 26)", -1),
            "boll_position_20": ("candidate_technical", "($close - Mean($close, 20)) / (2 * Std($close, 20) + 0.000001)", -1),
            "return_variance_20": ("candidate_volatility", "Std($close / Ref($close, 1) - 1, 20) * Std($close / Ref($close, 1) - 1, 20)", -1),
        }
        for name, (category, expression_fragment, direction) in expected.items():
            with self.subTest(name=name):
                factor = factor_by_name[name]
                self.assertEqual(factor.category, category)
                self.assertIn(expression_fragment, factor.expression)
                self.assertEqual(factor.direction, direction)

    def test_default_mining_config_includes_tushare_market_enrichment_alpha_factors(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))
        factor_by_name = {factor.name: factor for factor in factors}

        expected = {
            "tushare_ep_ttm": ("candidate_value", "$pe_ttm", 1),
            "tushare_bp": ("candidate_value", "$pb", 1),
            "tushare_dividend_yield": ("candidate_dividend", "$dividend_yield", 1),
            "tushare_free_float_turnover_20": ("candidate_liquidity", "$turnover_rate_f", 1),
            "tushare_total_mv": ("candidate_size_capacity", "$total_mv", 1),
        }
        for name, (category, expression_fragment, direction) in expected.items():
            with self.subTest(name=name):
                factor = factor_by_name[name]
                self.assertEqual(category, factor.category)
                self.assertIn(expression_fragment, factor.expression)
                self.assertEqual(direction, factor.direction)

        self.assertNotIn("tushare_amount_20", factor_by_name)
        self.assertNotIn("amount_mean_20", factor_by_name)

    def test_default_mining_config_avoids_unary_minus_expressions(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))

        self.assertFalse([factor.name for factor in factors if factor.expression.lstrip().startswith("-")])


if __name__ == "__main__":
    unittest.main()
