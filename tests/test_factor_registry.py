import tempfile
import unittest
from pathlib import Path

import yaml

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_registry import load_factor_registry, select_factors


class FactorRegistryTests(unittest.TestCase):
    def test_load_project_config_expands_relative_provider_uri(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "provider.yaml"
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "provider_uri": "data/qlib/cn_data",
                        "region": "cn",
                        "market": "csi500",
                    }
                ),
                encoding="utf-8",
            )

            config = load_project_config(config_path)

            self.assertEqual(config.region, "cn")
            self.assertEqual(config.market, "csi500")
            self.assertEqual(config.provider_uri, (root / "data/qlib/cn_data").resolve())

    def test_load_factor_registry_preserves_factor_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "registry.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "factors": [
                            {
                                "name": "ret_20",
                                "expression": "$close / Ref($close, 20) - 1",
                                "direction": 1,
                                "category": "momentum",
                                "description": "20 day return",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            factors = load_factor_registry(path)

            self.assertEqual(len(factors), 1)
            self.assertEqual(factors[0].name, "ret_20")
            self.assertEqual(factors[0].direction, 1)
            self.assertEqual(factors[0].category, "momentum")

    def test_select_factors_filters_by_category_and_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "registry.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "factors": [
                            {
                                "name": "ret_20",
                                "expression": "$close / Ref($close, 20) - 1",
                                "direction": 1,
                                "category": "momentum",
                            },
                            {
                                "name": "vol_20",
                                "expression": "Std($close / Ref($close, 1) - 1, 20)",
                                "direction": -1,
                                "category": "volatility",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            factors = load_factor_registry(path)

            by_category = select_factors(factors, categories=["momentum"])
            by_name = select_factors(factors, names=["vol_20"])

            self.assertEqual([f.name for f in by_category], ["ret_20"])
            self.assertEqual([f.name for f in by_name], ["vol_20"])

    def test_project_registry_contains_promoted_autoresearch_factors(self):
        root = Path(__file__).resolve().parents[1]
        factors = load_factor_registry(root / "factors/registry.yaml")
        promoted = {
            factor.name
            for factor in select_factors(
                factors,
                categories=["autoresearch_divergence"],
            )
        }

        self.assertLessEqual(
            {
                "high_mean60_discount_volume_divergence_reversal_20_60_v1",
                "fast_high_60d_discount_volume_divergence_reversal_10_60_v1",
                "high_norm_price_volume_divergence_20_v1",
                "normalized_price_volume_divergence_20_v1",
                "high_norm_price_amount_divergence_20_v1",
            },
            promoted,
        )


if __name__ == "__main__":
    unittest.main()
