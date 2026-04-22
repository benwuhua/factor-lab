import tempfile
import unittest
from pathlib import Path

import yaml

from qlib_factor_lab.autoresearch.contracts import load_expression_contract


class AutoresearchContractTests(unittest.TestCase):
    def test_load_expression_contract_resolves_core_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "contract.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "name": "csi500_current_v1",
                        "provider_config": "configs/provider_current.yaml",
                        "universe": "csi500_current",
                        "benchmark": "SH000905",
                        "start_time": "2015-01-01",
                        "end_time": "2026-04-20",
                        "horizons": [5, 20],
                        "metric": "rank_ic_mean",
                        "neutralization": {"raw": True, "size_proxy": True},
                        "minimum_observations": 10000,
                        "artifact_root": "reports/autoresearch/runs",
                        "ledger_path": "reports/autoresearch/expression_results.tsv",
                    }
                ),
                encoding="utf-8",
            )

            contract = load_expression_contract(path)

            self.assertEqual(contract.name, "csi500_current_v1")
            self.assertEqual(contract.horizons, (5, 20))
            self.assertEqual(contract.provider_config, Path("configs/provider_current.yaml"))
            self.assertTrue(contract.neutralize_size_proxy)
            self.assertTrue(contract.write_raw)

    def test_load_expression_contract_rejects_missing_required_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "contract.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "name": "broken",
                        "provider_config": "configs/provider_current.yaml",
                        "horizons": [5, 20],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "missing required contract field: universe"):
                load_expression_contract(path)

    def test_load_expression_contract_rejects_non_integer_horizon(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "contract.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "name": "broken",
                        "provider_config": "configs/provider_current.yaml",
                        "universe": "csi500_current",
                        "benchmark": "SH000905",
                        "start_time": "2015-01-01",
                        "end_time": "2026-04-20",
                        "horizons": [5, "20d"],
                        "metric": "rank_ic_mean",
                        "neutralization": {"raw": True, "size_proxy": True},
                        "minimum_observations": 10000,
                        "artifact_root": "reports/autoresearch/runs",
                        "ledger_path": "reports/autoresearch/expression_results.tsv",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "horizons must be positive integers"):
                load_expression_contract(path)


if __name__ == "__main__":
    unittest.main()
