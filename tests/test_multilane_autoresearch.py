import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from qlib_factor_lab.autoresearch.multilane import run_multilane_autoresearch


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
                )

            self.assertEqual(event_oracle.call_count, 2)
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["pattern_event", "run_status"], "completed")
            self.assertEqual(frame.loc["emotion_atmosphere", "run_status"], "completed")


if __name__ == "__main__":
    unittest.main()
