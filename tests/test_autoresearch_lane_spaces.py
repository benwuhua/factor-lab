import unittest
from pathlib import Path

import yaml


class AutoresearchLaneSpaceTests(unittest.TestCase):
    def test_lane_editable_spaces_exist_and_define_logic_buckets(self):
        repo_root = Path(__file__).resolve().parents[1]
        lane_space = yaml.safe_load((repo_root / "configs/autoresearch/lane_space.yaml").read_text(encoding="utf-8"))

        missing = []
        missing_logic = []
        for lane_name, lane in lane_space.get("lanes", {}).items():
            editable = repo_root / lane["editable_space"]
            if not editable.exists():
                missing.append(lane["editable_space"])
                continue
            data = yaml.safe_load(editable.read_text(encoding="utf-8")) or {}
            if lane_name != "expression_price_volume" and not data.get("logic_buckets"):
                missing_logic.append(lane_name)

        self.assertEqual(missing, [])
        self.assertEqual(missing_logic, [])

    def test_fundamental_space_exposes_p1_families_without_expressions(self):
        repo_root = Path(__file__).resolve().parents[1]
        data = yaml.safe_load((repo_root / "configs/autoresearch/fundamental_space.yaml").read_text(encoding="utf-8"))
        by_name = {item["name"]: item for item in data.get("candidate_factors", [])}
        expected = {
            "fundamental_ep": ("value", "ep"),
            "fundamental_cfp": ("value", "cfp"),
            "fundamental_dividend_yield": ("value", "dividend_yield"),
            "fundamental_roe": ("quality", "roe"),
            "fundamental_roic": ("quality", "roic"),
            "fundamental_cfo_to_ni": ("quality", "operating_cashflow_to_net_profit"),
            "fundamental_low_debt": ("quality", "debt_ratio"),
            "fundamental_low_accrual": ("quality", "accrual_ratio"),
            "fundamental_revenue_growth_change": ("growth_improvement", "revenue_growth_change_yoy"),
            "fundamental_profit_growth_change": ("growth_improvement", "net_profit_growth_change_yoy"),
            "fundamental_margin_change": ("growth_improvement", "gross_margin_change_yoy"),
            "fundamental_dividend_stability": ("dividend", "dividend_stability"),
            "fundamental_dividend_cashflow_coverage": ("dividend", "dividend_cashflow_coverage"),
        }

        for name, (family, field) in expected.items():
            self.assertIn(name, by_name)
            spec = by_name[name]
            self.assertEqual(family, spec["family"])
            self.assertNotIn("expression", spec)
            fields = [component.get("field") for component in spec.get("components", [])] or [spec.get("field")]
            self.assertIn(field, fields)

        self.assertEqual("fundamental_quality", data["data_domain"])
        self.assertEqual("data/fundamental_quality.csv", data["source_path"])


if __name__ == "__main__":
    unittest.main()
