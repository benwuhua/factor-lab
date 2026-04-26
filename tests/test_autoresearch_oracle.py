import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from qlib_factor_lab.autoresearch.contracts import load_expression_contract
from qlib_factor_lab.autoresearch.oracle import (
    build_expression_summary_payload,
    compute_complexity_score,
    determine_expression_status,
    render_summary_block,
    run_expression_oracle,
)
from qlib_factor_lab.config import ProjectConfig


class AutoresearchOracleTests(unittest.TestCase):
    def _eval_frame(self, neutralization: str, h5_rank_ic: float, h20_rank_ic: float) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "factor": "mom_skip_60_5_v1",
                    "category": "autoresearch_momentum",
                    "direction": 1,
                    "horizon": 5,
                    "neutralization": neutralization,
                    "rank_ic_mean": h5_rank_ic,
                    "ic_mean": h5_rank_ic / 2,
                    "long_short_mean_return": 0.001,
                    "top_quantile_turnover": 0.25,
                    "observations": 1000,
                },
                {
                    "factor": "mom_skip_60_5_v1",
                    "category": "autoresearch_momentum",
                    "direction": 1,
                    "horizon": 20,
                    "neutralization": neutralization,
                    "rank_ic_mean": h20_rank_ic,
                    "ic_mean": h20_rank_ic / 2,
                    "long_short_mean_return": 0.0038,
                    "top_quantile_turnover": 0.34,
                    "observations": 981234,
                },
            ]
        )

    def test_build_expression_summary_payload_prefers_neutral_h20_primary_metric(self):
        raw = self._eval_frame("none", 0.0182, 0.0415)
        neutral = self._eval_frame("size_proxy", 0.0114, 0.0273)

        payload = build_expression_summary_payload(
            run_id="run1",
            candidate_name="mom_skip_60_5_v1",
            commit="abc1234",
            contract_name="csi500_current_v1",
            universe="csi500_current",
            horizons=(5, 20),
            raw_eval=raw,
            neutralized_eval=neutral,
            complexity_score=0.18,
            artifact_dir="reports/autoresearch/runs/run1",
        )

        self.assertEqual(payload["primary_metric"], 0.0273)
        self.assertEqual(payload["secondary_metric"], 0.0415)
        self.assertEqual(payload["guard_metric"], 0.34)
        self.assertEqual(payload["neutral_rank_ic_mean_h20"], 0.0273)
        self.assertEqual(payload["long_short_mean_return_h20"], 0.0038)
        self.assertEqual(payload["observations_h20"], 981234)

    def test_render_summary_block_wraps_payload_in_yaml_like_fences(self):
        block = render_summary_block(
            {
                "loop": "expression",
                "run_id": "run1",
                "candidate": "mom_skip_60_5_v1",
                "rank_ic_mean_h20": 0.0415,
                "status": "review",
            }
        )

        self.assertTrue(block.startswith("---\nloop: expression"))
        self.assertIn("candidate: mom_skip_60_5_v1", block)
        self.assertTrue(block.endswith("\n---\n"))

    def test_compute_complexity_score_increases_with_expression_length_and_operators(self):
        simple = compute_complexity_score("Ref($close, 5) / Ref($close, 60) - 1")
        complex_score = compute_complexity_score("Mean(Abs($close / Ref($close, 1) - 1), 20) + Std($volume, 60)")

        self.assertGreater(complex_score, simple)
        self.assertGreaterEqual(simple, 0.0)
        self.assertLessEqual(complex_score, 1.0)

    def test_determine_expression_status_discards_low_observations(self):
        contract = load_expression_contract(self._write_contract(Path(tempfile.mkdtemp()), minimum_observations=10000))
        payload = {"observations_h20": 100, "primary_metric": 0.05, "complexity_score": 0.2}

        status, reason = determine_expression_status(payload, contract)

        self.assertEqual(status, "discard_candidate")
        self.assertIn("observations_h20 below minimum_observations", reason)

    def test_determine_expression_status_discards_weak_neutral_metric(self):
        contract = load_expression_contract(self._write_contract(Path(tempfile.mkdtemp()), minimum_observations=100))
        payload = {"observations_h20": 10000, "primary_metric": -0.006, "complexity_score": 0.2}

        status, reason = determine_expression_status(payload, contract)

        self.assertEqual(status, "discard_candidate")
        self.assertIn("primary_metric below discard threshold", reason)

    def test_run_expression_oracle_writes_crash_ledger_for_invalid_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            contract_path = self._write_contract(root)
            space_path = root / "space.yaml"
            space_path.write_text(
                yaml.safe_dump(
                    {
                        "fields": ["close"],
                        "windows": [5],
                        "operators": ["Ref"],
                        "families": ["momentum"],
                    }
                ),
                encoding="utf-8",
            )
            candidate_path = root / "candidate.yaml"
            candidate_path.write_text(
                yaml.safe_dump(
                    {
                        "name": "bad_candidate",
                        "family": "momentum",
                        "expression": "Std($close, 20)",
                        "direction": 1,
                        "description": "Invalid operator.",
                    }
                ),
                encoding="utf-8",
            )

            payload, block = run_expression_oracle(
                contract_path=contract_path,
                space_path=space_path,
                candidate_path=candidate_path,
                project_root=root,
            )

            self.assertEqual(payload["status"], "crash")
            self.assertIn("disallowed operator: Std", payload["decision_reason"])
            self.assertIn("status: crash", block)
            ledger = root / "reports/autoresearch/expression_results.tsv"
            self.assertIn("bad_candidate", ledger.read_text(encoding="utf-8"))
            self.assertTrue((Path(payload["artifact_dir"]) / "summary.txt").exists())

    def test_run_expression_oracle_respects_disabled_size_proxy_eval(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            contract_path = self._write_contract(root, size_proxy=False)
            space_path = root / "space.yaml"
            space_path.write_text(
                yaml.safe_dump(
                    {
                        "fields": ["close"],
                        "windows": [5, 20],
                        "operators": ["Ref"],
                        "families": ["momentum"],
                    }
                ),
                encoding="utf-8",
            )
            candidate_path = root / "candidate.yaml"
            candidate_path.write_text(
                yaml.safe_dump(
                    {
                        "name": "mom",
                        "family": "momentum",
                        "expression": "Ref($close, 5) / Ref($close, 20) - 1",
                        "direction": 1,
                        "description": "Momentum.",
                    }
                ),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.oracle.load_project_config") as load_config:
                with patch("qlib_factor_lab.autoresearch.oracle.evaluate_factor") as evaluate:
                    load_config.return_value = ProjectConfig(
                        provider_uri=Path("data"),
                        region="cn",
                        market="csi500_current",
                        benchmark="SH000905",
                        freq="day",
                        start_time="2015-01-01",
                        end_time="2026-04-20",
                    )
                    evaluate.return_value = self._eval_frame("none", 0.02, 0.03)

                    payload, _ = run_expression_oracle(
                        contract_path=contract_path,
                        space_path=space_path,
                        candidate_path=candidate_path,
                        project_root=root,
                    )

            self.assertEqual(evaluate.call_count, 1)
            self.assertTrue(pd.isna(payload["neutral_rank_ic_mean_h20"]))
            self.assertEqual(payload["secondary_metric"], 0.03)

    def test_run_expression_oracle_passes_contract_purification_to_evaluations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            contract_path = self._write_contract(root, purification={"steps": ["mad", "rank"], "mad_n": 2.5})
            space_path = root / "space.yaml"
            space_path.write_text(
                yaml.safe_dump(
                    {
                        "fields": ["close"],
                        "windows": [5, 20],
                        "operators": ["Ref"],
                        "families": ["momentum"],
                    }
                ),
                encoding="utf-8",
            )
            candidate_path = root / "candidate.yaml"
            candidate_path.write_text(
                yaml.safe_dump(
                    {
                        "name": "mom",
                        "family": "momentum",
                        "expression": "Ref($close, 5) / Ref($close, 20) - 1",
                        "direction": 1,
                        "description": "Momentum.",
                    }
                ),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.oracle.load_project_config") as load_config:
                with patch("qlib_factor_lab.autoresearch.oracle.evaluate_factor") as evaluate:
                    load_config.return_value = ProjectConfig(
                        provider_uri=Path("data"),
                        region="cn",
                        market="csi500_current",
                        benchmark="SH000905",
                        freq="day",
                        start_time="2015-01-01",
                        end_time="2026-04-20",
                    )
                    evaluate.return_value = self._eval_frame("none", 0.02, 0.03)

                    payload, _ = run_expression_oracle(
                        contract_path=contract_path,
                        space_path=space_path,
                        candidate_path=candidate_path,
                        project_root=root,
                    )

            eval_configs = [call.args[2] for call in evaluate.call_args_list]
            self.assertEqual({config.purification_steps for config in eval_configs}, {("mad", "rank")})
            self.assertEqual({config.purification_mad_n for config in eval_configs}, {2.5})
            self.assertEqual(payload["purification"], "mad+rank")

    def test_run_expression_oracle_overrides_contract_window_for_smoke_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            contract_path = self._write_contract(root)
            space_path = root / "space.yaml"
            space_path.write_text(
                yaml.safe_dump(
                    {
                        "fields": ["close"],
                        "windows": [5, 20],
                        "operators": ["Ref"],
                        "families": ["momentum"],
                    }
                ),
                encoding="utf-8",
            )
            candidate_path = root / "candidate.yaml"
            candidate_path.write_text(
                yaml.safe_dump(
                    {
                        "name": "mom",
                        "family": "momentum",
                        "expression": "Ref($close, 5) / Ref($close, 20) - 1",
                        "direction": 1,
                        "description": "Momentum.",
                    }
                ),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.oracle.load_project_config") as load_config:
                with patch("qlib_factor_lab.autoresearch.oracle.evaluate_factor") as evaluate:
                    load_config.return_value = ProjectConfig(
                        provider_uri=Path("data"),
                        region="cn",
                        market="csi500_current",
                        benchmark="SH000905",
                        freq="day",
                        start_time="2015-01-01",
                        end_time="2026-04-20",
                    )
                    evaluate.return_value = self._eval_frame("none", 0.02, 0.03)

                    run_expression_oracle(
                        contract_path=contract_path,
                        space_path=space_path,
                        candidate_path=candidate_path,
                        project_root=root,
                        start_time="2026-01-01",
                        end_time="2026-04-20",
                    )

            configs = [call.args[0] for call in evaluate.call_args_list]
            self.assertEqual({config.start_time for config in configs}, {"2026-01-01"})
            self.assertEqual({config.end_time for config in configs}, {"2026-04-20"})

    def _write_contract(
        self,
        root: Path,
        minimum_observations: int = 1000,
        raw: bool = True,
        size_proxy: bool = True,
        purification=None,
    ) -> Path:
        path = root / "contract.yaml"
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
                    "neutralization": {"raw": raw, "size_proxy": size_proxy},
                    **({"purification": purification} if purification is not None else {}),
                    "minimum_observations": minimum_observations,
                    "artifact_root": "reports/autoresearch/runs",
                    "ledger_path": "reports/autoresearch/expression_results.tsv",
                }
            ),
            encoding="utf-8",
        )
        return path


if __name__ == "__main__":
    unittest.main()
