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

    def test_default_mining_config_includes_wangji_factor1_soft_14_day_pattern(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))
        factor_by_name = {factor.name: factor for factor in factors}

        factor = factor_by_name["wangji-factor1"]
        self.assertEqual(factor.category, "candidate_pattern")
        self.assertIn("4 - Abs((Max(Ref($close, 4), 10) / Min(Ref($close, 4), 10) - 1) - 0.08) / 0.04", factor.expression)
        self.assertIn("Less(Ref($close, 3) / Ref($close, 4) - 1, 0.12) / 0.06", factor.expression)
        self.assertIn("Less(Ref($close, 3) / Ref($open, 3) - 1, 0.12) / 0.06", factor.expression)
        self.assertIn("Less(Ref($volume, 3) / (Mean(Ref($volume, 4), 10) + 1), 2.5) / 1.5", factor.expression)
        self.assertIn("Abs(Ref($close, 2) / Ref($close, 3) - 1)", factor.expression)
        self.assertIn("2.2 * ((Ref($volume, 2) + Ref($volume, 1) + $volume)", factor.expression)
        self.assertNotIn("If(", factor.expression)
        self.assertNotIn("Ref(Max($high, 120), 30)", factor.expression)

    def test_default_mining_config_includes_wangji_reversal20_combo_gate(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))
        factor_by_name = {factor.name: factor for factor in factors}

        factor = factor_by_name["wangji-reversal20-combo"]
        self.assertEqual(factor.category, "candidate_pattern")
        self.assertIn("If(And(Gt(", factor.expression)
        self.assertIn("Ref($close, 20) / $close - 1", factor.expression)
        self.assertIn("Greater(Less(Ref($close, 20) / $close - 1, 0.30), 0)", factor.expression)
        self.assertIn("4 - Abs((Max(Ref($close, 4), 10) / Min(Ref($close, 4), 10) - 1) - 0.08) / 0.04", factor.expression)
        self.assertIn("- 10", factor.expression)
        self.assertEqual(factor.direction, 1)

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

    def test_default_mining_config_includes_tushare_market_enrichment_factors(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))
        factor_by_name = {factor.name: factor for factor in factors}

        expected = {
            "tushare_ep_ttm": ("candidate_value", "$pe_ttm", 1),
            "tushare_bp": ("candidate_value", "$pb", 1),
            "tushare_dividend_yield": ("candidate_dividend", "$dividend_yield", 1),
            "tushare_amount_20": ("candidate_liquidity", "Mean($amount, 20)", 1),
            "tushare_free_float_turnover_20": ("candidate_liquidity", "$turnover_rate_f", 1),
            "tushare_total_mv": ("candidate_size_capacity", "$total_mv", 1),
        }
        for name, (category, expression_fragment, direction) in expected.items():
            with self.subTest(name=name):
                factor = factor_by_name[name]
                self.assertEqual(category, factor.category)
                self.assertIn(expression_fragment, factor.expression)
                self.assertEqual(direction, factor.direction)

    def test_default_mining_config_avoids_unary_minus_expressions(self):
        root = Path(__file__).resolve().parents[1]

        factors = generate_candidate_factors(load_mining_config(root / "configs/factor_mining.yaml"))

        self.assertFalse([factor.name for factor in factors if factor.expression.lstrip().startswith("-")])


if __name__ == "__main__":
    unittest.main()
