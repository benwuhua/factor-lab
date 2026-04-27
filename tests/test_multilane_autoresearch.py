import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from qlib_factor_lab.autoresearch.multilane import _event_factor_specs, run_multilane_autoresearch


class MultilaneAutoresearchTests(unittest.TestCase):
    def test_runner_executes_expression_and_records_shadow_lanes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump(
                    {
                        "lanes": {
                            "expression_price_volume": {
                                "activation_status": "active",
                                "editable_space": "configs/autoresearch/expression_space.yaml",
                            },
                            "emotion_atmosphere": {
                                "activation_status": "shadow",
                                "editable_space": "configs/autoresearch/emotion_space.yaml",
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            mining_config = root / "configs/factor_mining.yaml"
            mining_config.write_text(
                yaml.safe_dump(
                    {
                        "templates": [
                            {"name": "wangji-factor1", "expression": "$close", "direction": 1},
                            {"name": "arbr_26", "expression": "$close", "direction": -1},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_expression_oracle") as oracle:
                oracle.return_value = (
                    {
                        "candidate": "demo_expr",
                        "status": "review",
                        "primary_metric": 0.031,
                        "artifact_dir": "reports/autoresearch/runs/demo",
                    },
                    "---\nstatus: review\n---\n",
                )
                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    contract_path="contract.yaml",
                    expression_space_path="space.yaml",
                    expression_candidate_path="candidate.yaml",
                    output_path="reports/autoresearch/multilane_summary.md",
                    max_workers=2,
                )
                oracle.assert_called_once()

            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["expression_price_volume", "run_status"], "completed")
            self.assertEqual(frame.loc["expression_price_volume", "candidate"], "demo_expr")
            self.assertEqual(frame.loc["emotion_atmosphere", "run_status"], "shadow_skipped")
            self.assertTrue((root / "reports/autoresearch/multilane_summary.md").exists())
            self.assertTrue((root / "reports/autoresearch/multilane_summary.json").exists())

    def test_runner_dispatches_pattern_and_emotion_event_oracles(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump(
                    {
                        "lanes": {
                            "pattern_event": {"activation_status": "active"},
                            "emotion_atmosphere": {"activation_status": "active"},
                        }
                    }
                ),
                encoding="utf-8",
            )
            mining_config = root / "configs/factor_mining.yaml"
            mining_config.write_text(
                yaml.safe_dump(
                    {
                        "templates": [
                            {"name": "wangji-factor1", "expression": "$close", "direction": 1},
                            {"name": "arbr_26", "expression": "$close", "direction": -1},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_event_lane_oracle") as event_oracle:
                event_oracle.side_effect = [
                    ({"candidate": "wangji-factor1", "status": "review", "primary_metric": 0.02, "artifact_dir": "a"}, ""),
                    ({"candidate": "arbr_26", "status": "review", "primary_metric": 0.01, "artifact_dir": "b"}, ""),
                ]

                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    mining_config_path=mining_config,
                    start_time="2026-01-01",
                    end_time="2026-04-20",
                )

            self.assertEqual(event_oracle.call_count, 2)
            for call in event_oracle.call_args_list:
                self.assertEqual(call.kwargs["start_time"], "2026-01-01")
                self.assertEqual(call.kwargs["end_time"], "2026-04-20")
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["pattern_event", "run_status"], "completed")
            self.assertEqual(frame.loc["emotion_atmosphere", "run_status"], "completed")

    def test_runner_applies_data_governance_activation_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump(
                    {
                        "lanes": {
                            "emotion_atmosphere": {"activation_status": "active"},
                        }
                    }
                ),
                encoding="utf-8",
            )
            governance = root / "reports/data_governance.csv"
            governance.parent.mkdir(parents=True)
            governance.write_text(
                "domain,activation_lane,activation_status\n"
                "emotion_atmosphere,emotion_atmosphere,shadow\n",
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_event_lane_oracle") as event_oracle:
                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    data_governance_report_path=governance,
                )

            event_oracle.assert_not_called()
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["emotion_atmosphere", "activation_status"], "shadow")
            self.assertEqual(frame.loc["emotion_atmosphere", "run_status"], "shadow_skipped")

    def test_runner_can_include_shadow_after_data_governance_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump({"lanes": {"emotion_atmosphere": {"activation_status": "active"}}}),
                encoding="utf-8",
            )
            governance = root / "reports/data_governance.csv"
            governance.parent.mkdir(parents=True)
            governance.write_text(
                "domain,activation_lane,activation_status\n"
                "emotion_atmosphere,emotion_atmosphere,shadow\n",
                encoding="utf-8",
            )
            mining_config = root / "configs/factor_mining.yaml"
            mining_config.write_text(
                yaml.safe_dump({"templates": [{"name": "arbr_26", "expression": "$close", "direction": -1}]}),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_event_lane_oracle") as event_oracle:
                event_oracle.return_value = (
                    {"candidate": "arbr_26", "status": "review", "primary_metric": 0.01, "artifact_dir": "b"},
                    "",
                )
                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    mining_config_path=mining_config,
                    data_governance_report_path=governance,
                    include_shadow=True,
                )

            event_oracle.assert_called_once()
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["emotion_atmosphere", "activation_status"], "shadow")
            self.assertEqual(frame.loc["emotion_atmosphere", "run_status"], "completed")

    def test_runner_maps_governance_block_to_disabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump({"lanes": {"expression_price_volume": {"activation_status": "active"}}}),
                encoding="utf-8",
            )
            governance = root / "reports/data_governance.csv"
            governance.parent.mkdir(parents=True)
            governance.write_text(
                "domain,activation_lane,activation_status\n"
                "market_ohlcv,expression_price_volume,block\n",
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_expression_oracle") as oracle:
                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    data_governance_report_path=governance,
                )

            oracle.assert_not_called()
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["expression_price_volume", "activation_status"], "disabled")
            self.assertEqual(frame.loc["expression_price_volume", "run_status"], "disabled_skipped")

    def test_runner_uses_lane_space_when_governance_report_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump({"lanes": {"expression_price_volume": {"activation_status": "active"}}}),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_expression_oracle") as oracle:
                oracle.return_value = (
                    {"candidate": "demo", "status": "review", "primary_metric": 0.01, "artifact_dir": "a"},
                    "",
                )
                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    data_governance_report_path="reports/missing_governance.md",
                )

            oracle.assert_called_once()
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["expression_price_volume", "activation_status"], "active")
            self.assertEqual(frame.loc["expression_price_volume", "run_status"], "completed")

    def test_runner_passes_smoke_window_to_expression_oracle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump({"lanes": {"expression_price_volume": {"activation_status": "active"}}}),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_expression_oracle") as oracle:
                oracle.return_value = (
                    {"candidate": "demo", "status": "review", "primary_metric": 0.01, "artifact_dir": "a"},
                    "",
                )

                run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    start_time="2026-01-01",
                    end_time="2026-04-20",
                )

            self.assertEqual(oracle.call_args.kwargs["start_time"], "2026-01-01")
            self.assertEqual(oracle.call_args.kwargs["end_time"], "2026-04-20")

    def test_emotion_lane_includes_heat_limit_and_breadth_proxy_factors(self):
        repo_root = Path(__file__).resolve().parents[1]

        names = {spec["name"] for spec in _event_factor_specs(repo_root, "configs/factor_mining.yaml", "emotion_atmosphere")}

        self.assertIn("limit_pressure_5", names)
        self.assertIn("heat_cooling_5_20", names)
        self.assertIn("breadth_proxy_20", names)


if __name__ == "__main__":
    unittest.main()
