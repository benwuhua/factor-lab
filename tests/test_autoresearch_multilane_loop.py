import json
import tempfile
import unittest
from pathlib import Path

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

    def test_loop_counts_lane_crashes_and_stops_at_crash_budget(self):
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

            self.assertEqual(result.iterations_started, 2)
            self.assertEqual(result.crash_count, 2)
            self.assertEqual(result.stop_reason, "max_crashes")

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


if __name__ == "__main__":
    unittest.main()
