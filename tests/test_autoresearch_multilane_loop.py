import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from qlib_factor_lab.autoresearch.multilane import MultiLaneReport
from qlib_factor_lab.autoresearch.multilane_loop import run_multilane_loop


class AutoresearchMultilaneLoopTests(unittest.TestCase):
    def test_loop_runs_multiple_iterations_and_writes_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            outputs: list[Path] = []

            def fake_runner(**kwargs):
                output = Path(kwargs["output_path"])
                outputs.append(output)
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text("# fake multilane\n", encoding="utf-8")
                return MultiLaneReport(
                    (
                        {
                            "lane": "expression_price_volume",
                            "activation_status": "active",
                            "run_status": "completed",
                            "candidate": "demo",
                            "primary_metric": 0.01,
                            "artifact_dir": "reports/demo",
                            "detail": "review",
                        },
                    ),
                    output_path=output,
                )

            result = run_multilane_loop(
                project_root=root,
                lane_space_path="lane_space.yaml",
                contract_path="contract.yaml",
                expression_space_path="space.yaml",
                expression_candidate_path="candidate.yaml",
                mining_config_path="mining.yaml",
                provider_config_path="provider.yaml",
                output_root="reports/autoresearch/multilane_loop",
                max_iterations=2,
                max_crashes=3,
                sleep_sec=0,
                runner=fake_runner,
            )

            self.assertEqual(result.iterations_started, 2)
            self.assertEqual(result.crash_count, 0)
            self.assertEqual(result.stop_reason, "max_iterations")
            self.assertEqual([path.name for path in outputs], ["multilane_iteration_001.md", "multilane_iteration_002.md"])
            self.assertTrue((result.log_dir / "summary.txt").exists())
            self.assertEqual(
                (root / "reports/autoresearch/multilane_loop/latest_log_dir.txt").read_text(encoding="utf-8").strip(),
                str(result.log_dir),
            )
            summary = json.loads((result.log_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(len(summary["iterations"]), 2)
            self.assertEqual(summary["iterations"][0]["status"], "completed")

    def test_loop_records_lane_crashes_without_stopping_at_runner_crash_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_runner(**kwargs):
                return MultiLaneReport(
                    (
                        {
                            "lane": "risk_structure",
                            "activation_status": "active",
                            "run_status": "crash",
                            "candidate": "",
                            "primary_metric": float("nan"),
                            "artifact_dir": "",
                            "detail": "boom",
                        },
                    ),
                    output_path=Path(kwargs["output_path"]),
                )

            result = run_multilane_loop(
                project_root=root,
                lane_space_path="lane_space.yaml",
                contract_path="contract.yaml",
                expression_space_path="space.yaml",
                expression_candidate_path="candidate.yaml",
                mining_config_path="mining.yaml",
                provider_config_path="provider.yaml",
                output_root="reports/autoresearch/multilane_loop",
                max_iterations=5,
                max_crashes=2,
                sleep_sec=0,
                runner=fake_runner,
            )

            self.assertEqual(result.iterations_started, 5)
            self.assertEqual(result.crash_count, 0)
            self.assertEqual(result.stop_reason, "max_iterations")
            summary = json.loads((result.log_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["lane_crash_count"], 5)

    def test_loop_records_runner_exceptions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            def fake_runner(**kwargs):
                raise RuntimeError("provider exploded")

            result = run_multilane_loop(
                project_root=root,
                lane_space_path="lane_space.yaml",
                contract_path="contract.yaml",
                expression_space_path="space.yaml",
                expression_candidate_path="candidate.yaml",
                mining_config_path="mining.yaml",
                provider_config_path="provider.yaml",
                output_root="reports/autoresearch/multilane_loop",
                max_iterations=3,
                max_crashes=1,
                sleep_sec=0,
                runner=fake_runner,
            )

            self.assertEqual(result.iterations_started, 1)
            self.assertEqual(result.crash_count, 1)
            self.assertEqual(result.stop_reason, "max_crashes")
            self.assertIn("provider exploded", (result.log_dir / "iteration_001_error.txt").read_text(encoding="utf-8"))

    def test_loop_prioritizes_non_reversal_expression_candidates_and_non_reversal_lanes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidate_dir = root / "configs/autoresearch/candidates"
            candidate_dir.mkdir(parents=True)
            (candidate_dir / "candidate_momentum.yaml").write_text(
                "name: candidate_momentum\nfamily: momentum\nexpression: $close\n",
                encoding="utf-8",
            )
            (candidate_dir / "candidate_reversal.yaml").write_text(
                "name: candidate_reversal\nfamily: reversal\nexpression: Ref($close, 20) / $close - 1\n",
                encoding="utf-8",
            )
            calls: list[dict] = []

            def fake_runner(**kwargs):
                calls.append(kwargs)
                output = Path(kwargs["output_path"])
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text("# fake multilane\n", encoding="utf-8")
                return MultiLaneReport(
                    (
                        {
                            "lane": "expression_price_volume",
                            "activation_status": "active",
                            "run_status": "completed",
                            "candidate": Path(kwargs["expression_candidate_path"]).stem,
                            "primary_metric": 0.01,
                            "artifact_dir": "",
                            "detail": "review",
                        },
                    ),
                    output_path=output,
                )

            run_multilane_loop(
                project_root=root,
                lane_space_path="lane_space.yaml",
                contract_path="contract.yaml",
                expression_space_path="space.yaml",
                expression_candidate_path="configs/autoresearch/candidates/candidate_a.yaml",
                expression_candidate_glob="configs/autoresearch/candidates/*.yaml",
                mining_config_path="mining.yaml",
                provider_config_path="provider.yaml",
                output_root="reports/autoresearch/multilane_loop",
                max_iterations=3,
                max_crashes=3,
                sleep_sec=0,
                lane_factor_batch_size=1,
                runner=fake_runner,
            )

            self.assertEqual(
                [Path(call["expression_candidate_path"]).name for call in calls],
                ["candidate_momentum.yaml", "candidate_momentum.yaml", "candidate_momentum.yaml"],
            )
            self.assertEqual(calls[0]["lane_factor_name_overrides"]["pattern_event"], ["wangji-factor1"])
            self.assertGreater(len(calls[0]["lane_factor_name_overrides"]["emotion_atmosphere"]), 1)
            self.assertGreater(len(calls[0]["lane_factor_name_overrides"]["liquidity_microstructure"]), 1)
            self.assertGreater(len(calls[0]["lane_factor_name_overrides"]["risk_structure"]), 1)
            self.assertNotEqual(
                calls[0]["lane_factor_name_overrides"]["risk_structure"],
                calls[1]["lane_factor_name_overrides"]["risk_structure"],
            )
            summary = json.loads(calls[0]["output_path"].parent.joinpath("summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["iterations"][0]["rotation"]["expression_candidate"], "candidate_momentum.yaml")
            self.assertEqual(summary["iterations"][0]["rotation"]["policy"], "non_reversal_priority")
            self.assertEqual(summary["skipped_expression_candidates"][0]["candidate"], "candidate_reversal.yaml")
            self.assertEqual(summary["skipped_expression_candidates"][0]["reason"], "reversal_priority_filter")

    def test_loop_can_include_reversal_expression_candidates_when_explicitly_requested(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidate_dir = root / "configs/autoresearch/candidates"
            candidate_dir.mkdir(parents=True)
            (candidate_dir / "candidate_momentum.yaml").write_text(
                "name: candidate_momentum\nfamily: momentum\nexpression: $close\n",
                encoding="utf-8",
            )
            (candidate_dir / "candidate_reversal.yaml").write_text(
                "name: candidate_reversal\nfamily: reversal\nexpression: Ref($close, 20) / $close - 1\n",
                encoding="utf-8",
            )
            calls: list[dict] = []

            def fake_runner(**kwargs):
                calls.append(kwargs)
                output = Path(kwargs["output_path"])
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text("# fake multilane\n", encoding="utf-8")
                return MultiLaneReport((), output_path=output)

            run_multilane_loop(
                project_root=root,
                lane_space_path="lane_space.yaml",
                contract_path="contract.yaml",
                expression_space_path="space.yaml",
                expression_candidate_path="configs/autoresearch/candidates/candidate_momentum.yaml",
                expression_candidate_glob="configs/autoresearch/candidates/*.yaml",
                mining_config_path="mining.yaml",
                provider_config_path="provider.yaml",
                output_root="reports/autoresearch/multilane_loop",
                max_iterations=2,
                max_crashes=3,
                sleep_sec=0,
                lane_factor_batch_size=1,
                include_reversal_expression_candidates=True,
                runner=fake_runner,
            )

            self.assertEqual(
                [Path(call["expression_candidate_path"]).name for call in calls],
                ["candidate_momentum.yaml", "candidate_reversal.yaml"],
            )

    def test_loop_can_stop_when_rotation_repeats(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidate_dir = root / "configs/autoresearch/candidates"
            candidate_dir.mkdir(parents=True)
            (candidate_dir / "candidate_a.yaml").write_text(
                "name: candidate_a\nfamily: momentum\nexpression: $close\n",
                encoding="utf-8",
            )
            (candidate_dir / "candidate_b.yaml").write_text(
                "name: candidate_b\nfamily: momentum\nexpression: $open\n",
                encoding="utf-8",
            )
            calls: list[dict] = []

            def fake_runner(**kwargs):
                calls.append(kwargs)
                output = Path(kwargs["output_path"])
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text("# fake multilane\n", encoding="utf-8")
                return MultiLaneReport((), output_path=output)

            with patch.dict(
                "qlib_factor_lab.autoresearch.multilane_loop.DEFAULT_ROTATION_FACTOR_NAMES",
                {"pattern_event": ["pattern_a", "pattern_b"]},
                clear=True,
            ):
                result = run_multilane_loop(
                    project_root=root,
                    lane_space_path="lane_space.yaml",
                    contract_path="contract.yaml",
                    expression_space_path="space.yaml",
                    expression_candidate_path="configs/autoresearch/candidates/candidate_a.yaml",
                    expression_candidate_glob="configs/autoresearch/candidates/*.yaml",
                    mining_config_path="mining.yaml",
                    provider_config_path="provider.yaml",
                    output_root="reports/autoresearch/multilane_loop",
                    max_iterations=10,
                    max_crashes=3,
                    sleep_sec=0,
                    lane_factor_batch_size=1,
                    stop_on_rotation_exhausted=True,
                    runner=fake_runner,
                )

            self.assertEqual(result.stop_reason, "rotation_exhausted")
            self.assertEqual(result.iterations_started, 2)
            self.assertEqual(
                [Path(call["expression_candidate_path"]).name for call in calls],
                ["candidate_a.yaml", "candidate_b.yaml"],
            )
            summary = json.loads((result.log_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["stop_reason"], "rotation_exhausted")

    def test_loop_seeds_expression_candidates_from_strategy_dictionary_before_rotation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidate_dir = root / "configs/autoresearch/candidates"
            candidate_dir.mkdir(parents=True)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True, exist_ok=True)
            lane_space.write_text(
                """
strategy_dictionary:
  enabled: true
  path: configs/strategy_dictionary/151_trading_strategies_equity.yaml
  combo_spec: configs/combo_specs/balanced_multifactor_v1.yaml
  lanes:
    - expression
  limit_per_run: 2
  write_candidates: true
  candidate_output_dir: configs/autoresearch/candidates
""",
                encoding="utf-8",
            )
            dictionary_path = root / "configs/strategy_dictionary/151_trading_strategies_equity.yaml"
            dictionary_path.parent.mkdir(parents=True, exist_ok=True)
            dictionary_path.write_text(
                """
strategies:
  - strategy_id: stock_low_volatility
    strategy_name: Low volatility
    strategy_family: low_vol
    candidate_lane: expression
    template_formula: "-Std(Return, 120)"
    a_share_transferability: high
  - strategy_id: stock_price_momentum
    strategy_name: Price momentum
    strategy_family: momentum
    candidate_lane: expression
    template_formula: "Return(120) skip 20"
    a_share_transferability: high
""",
                encoding="utf-8",
            )
            combo_path = root / "configs/combo_specs/balanced_multifactor_v1.yaml"
            combo_path.parent.mkdir(parents=True, exist_ok=True)
            combo_path.write_text("name: combo\nmembers: []\n", encoding="utf-8")
            calls: list[dict] = []

            def fake_runner(**kwargs):
                calls.append(kwargs)
                output = Path(kwargs["output_path"])
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text("# fake multilane\n", encoding="utf-8")
                return MultiLaneReport((), output_path=output)

            result = run_multilane_loop(
                project_root=root,
                lane_space_path="configs/autoresearch/lane_space.yaml",
                contract_path="contract.yaml",
                expression_space_path="space.yaml",
                expression_candidate_path="configs/autoresearch/candidates/fallback.yaml",
                expression_candidate_glob="configs/autoresearch/candidates/*.yaml",
                mining_config_path="mining.yaml",
                provider_config_path="provider.yaml",
                output_root="reports/autoresearch/multilane_loop",
                max_iterations=1,
                max_crashes=3,
                sleep_sec=0,
                runner=fake_runner,
            )

            self.assertTrue((candidate_dir / "low_vol_120_v1.yaml").exists())
            self.assertTrue((candidate_dir / "momentum_120_skip_20_v1.yaml").exists())
            self.assertEqual(Path(calls[0]["expression_candidate_path"]).name, "low_vol_120_v1.yaml")
            summary = json.loads((result.log_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["strategy_dictionary_seed"]["written_candidates"], 2)


if __name__ == "__main__":
    unittest.main()
