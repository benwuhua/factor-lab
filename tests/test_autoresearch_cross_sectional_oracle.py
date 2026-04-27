import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from qlib_factor_lab.autoresearch.cross_sectional_oracle import run_cross_sectional_lane_oracle


class CrossSectionalLaneOracleTests(unittest.TestCase):
    def test_cross_sectional_lane_oracle_selects_best_candidate_and_writes_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "configs/autoresearch/contracts").mkdir(parents=True)
            (root / "configs").mkdir(exist_ok=True)
            (root / "configs/provider_current.yaml").write_text(
                yaml.safe_dump(
                    {
                        "provider_uri": "data/qlib/cn_data_current",
                        "market": "csi500",
                        "benchmark": "SH000905",
                        "start_time": "2026-01-01",
                        "end_time": "2026-04-20",
                    }
                ),
                encoding="utf-8",
            )
            contract = root / "configs/autoresearch/contracts/test.yaml"
            contract.write_text(
                yaml.safe_dump(
                    {
                        "name": "test_contract",
                        "provider_config": "configs/provider_current.yaml",
                        "universe": "csi500",
                        "benchmark": "SH000905",
                        "start_time": "2026-01-01",
                        "end_time": "2026-04-20",
                        "horizons": [5, 20],
                        "metric": "rank_ic_mean",
                        "neutralization": {"raw": True, "size_proxy": True},
                        "purification": {"steps": ["mad", "zscore"], "mad_n": 3.0},
                        "minimum_observations": 10,
                        "artifact_root": "reports/autoresearch/runs",
                        "ledger_path": "reports/autoresearch/expression_results.tsv",
                    }
                ),
                encoding="utf-8",
            )
            factor_specs = [
                {"name": "amount_mean_20", "expression": "$amount", "direction": 1, "category": "candidate_liquidity"},
                {"name": "amihud_illiq_20", "expression": "$close", "direction": -1, "category": "candidate_liquidity"},
            ]

            def fake_evaluate(_project_config, factor, eval_config, initialize=True):
                score = 0.03 if factor.name == "amount_mean_20" and eval_config.neutralize_size else 0.01
                return pd.DataFrame(
                    {
                        "factor": [factor.name, factor.name],
                        "category": [factor.category, factor.category],
                        "direction": [factor.direction, factor.direction],
                        "horizon": [5, 20],
                        "rank_ic_mean": [score, score],
                        "long_short_mean_return": [0.001, 0.002],
                        "top_quantile_turnover": [0.1, 0.1],
                        "observations": [100, 100],
                    }
                )

            with patch("qlib_factor_lab.autoresearch.cross_sectional_oracle.evaluate_factor", side_effect=fake_evaluate):
                payload, block = run_cross_sectional_lane_oracle(
                    lane_name="liquidity_microstructure",
                    factor_specs=factor_specs,
                    contract_path=contract,
                    project_root=root,
                )

            self.assertEqual(payload["candidate"], "amount_mean_20")
            self.assertEqual(payload["status"], "review")
            self.assertAlmostEqual(float(payload["primary_metric"]), 0.03)
            self.assertIn("loop: liquidity_microstructure", block)
            artifact_dir = Path(payload["artifact_dir"])
            self.assertTrue((artifact_dir / "summary.json").exists())
            self.assertTrue((artifact_dir / "factor_summaries.csv").exists())

    def test_cross_sectional_lane_oracle_uses_lane_name_in_discard_reason(self):
        frame = pd.DataFrame(
            {
                "candidate": ["downside_vol_20"],
                "status": ["discard_candidate"],
                "primary_metric": [0.01],
            }
        )

        from qlib_factor_lab.autoresearch.cross_sectional_oracle import _lane_payload

        payload = _lane_payload(
            lane_name="risk_structure",
            run_id="run1",
            factor_specs=[{"name": "downside_vol_20"}],
            factors_frame=frame,
            artifact_dir=Path("reports/autoresearch/runs/risk_structure_run1"),
            elapsed_sec=0.1,
        )

        self.assertEqual(payload["status"], "discard_candidate")
        self.assertEqual(payload["decision_reason"], "no reviewed risk_structure candidate")


if __name__ == "__main__":
    unittest.main()
