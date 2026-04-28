import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from qlib_factor_lab.autoresearch.multilane import _event_factor_specs, _lane_factor_specs, run_multilane_autoresearch


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

    def test_runner_applies_lane_factor_name_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump(
                    {
                        "lanes": {
                            "pattern_event": {"activation_status": "active"},
                            "risk_structure": {"activation_status": "active"},
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
                            {"name": "quiet_breakout_{window}", "expression": "$close", "windows": [20, 60], "direction": 1},
                            {"name": "max_drawdown_{window}", "expression": "$close", "windows": [20, 60], "direction": -1},
                            {"name": "downside_vol_{window}", "expression": "$close", "windows": [20], "direction": -1},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_event_lane_oracle") as event_oracle, patch(
                "qlib_factor_lab.autoresearch.multilane.run_cross_sectional_lane_oracle"
            ) as cross_oracle:
                event_oracle.return_value = (
                    {"candidate": "quiet_breakout_60", "status": "review", "primary_metric": 0.02, "artifact_dir": "a"},
                    "",
                )
                cross_oracle.return_value = (
                    {"candidate": "downside_vol_20", "status": "review", "primary_metric": 0.03, "artifact_dir": "b"},
                    "",
                )
                run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    mining_config_path=mining_config,
                    lane_factor_name_overrides={
                        "pattern_event": ["quiet_breakout_60"],
                        "risk_structure": ["downside_vol_20"],
                    },
                )

            self.assertEqual([spec["name"] for spec in event_oracle.call_args.kwargs["factor_specs"]], ["quiet_breakout_60"])
            self.assertEqual([spec["name"] for spec in cross_oracle.call_args.kwargs["factor_specs"]], ["downside_vol_20"])

    def test_runner_serializes_qlib_oracle_execution(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump(
                    {
                        "lanes": {
                            "pattern_event": {"activation_status": "active"},
                            "risk_structure": {"activation_status": "active"},
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
                            {"name": "max_drawdown_{window}", "expression": "$close", "windows": [20], "direction": -1},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            active = 0
            max_seen = 0
            lock = threading.Lock()

            def fake_oracle(**kwargs):
                nonlocal active, max_seen
                with lock:
                    active += 1
                    max_seen = max(max_seen, active)
                time.sleep(0.02)
                with lock:
                    active -= 1
                return (
                    {
                        "candidate": kwargs["factor_specs"][0]["name"],
                        "status": "review",
                        "primary_metric": 0.01,
                        "artifact_dir": "a",
                    },
                    "",
                )

            with patch("qlib_factor_lab.autoresearch.multilane.run_event_lane_oracle", side_effect=fake_oracle), patch(
                "qlib_factor_lab.autoresearch.multilane.run_cross_sectional_lane_oracle", side_effect=fake_oracle
            ):
                run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    mining_config_path=mining_config,
                    max_workers=2,
                )

            self.assertEqual(max_seen, 1)

    def test_runner_dispatches_liquidity_cross_sectional_oracle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump({"lanes": {"liquidity_microstructure": {"activation_status": "active"}}}),
                encoding="utf-8",
            )
            mining_config = root / "configs/factor_mining.yaml"
            mining_config.write_text(
                yaml.safe_dump(
                    {
                        "templates": [
                            {"name": "amount_mean_{window}", "expression": "$amount", "windows": [5], "direction": 1, "category": "candidate_liquidity"},
                            {"name": "amihud_illiq_{window}", "expression": "$close", "windows": [20], "direction": -1, "category": "candidate_liquidity"},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_cross_sectional_lane_oracle") as oracle:
                oracle.return_value = (
                    {
                        "candidate": "amount_mean_5",
                        "status": "review",
                        "primary_metric": 0.014,
                        "artifact_dir": "reports/autoresearch/runs/liquidity",
                    },
                    "",
                )
                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    mining_config_path=mining_config,
                    start_time="2026-01-01",
                    end_time="2026-04-20",
                )

            oracle.assert_called_once()
            self.assertEqual(oracle.call_args.kwargs["lane_name"], "liquidity_microstructure")
            self.assertEqual(oracle.call_args.kwargs["start_time"], "2026-01-01")
            self.assertEqual(oracle.call_args.kwargs["end_time"], "2026-04-20")
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["liquidity_microstructure", "run_status"], "completed")
            self.assertEqual(frame.loc["liquidity_microstructure", "candidate"], "amount_mean_5")

    def test_runner_dispatches_risk_cross_sectional_oracle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump({"lanes": {"risk_structure": {"activation_status": "active"}}}),
                encoding="utf-8",
            )
            mining_config = root / "configs/factor_mining.yaml"
            mining_config.write_text(
                yaml.safe_dump(
                    {
                        "templates": [
                            {"name": "max_drawdown_{window}", "expression": "$close", "windows": [20], "direction": -1, "category": "candidate_drawdown_quality"},
                            {"name": "downside_vol_{window}", "expression": "$close", "windows": [20], "direction": -1, "category": "candidate_downside_volatility"},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_cross_sectional_lane_oracle") as oracle:
                oracle.return_value = (
                    {
                        "candidate": "max_drawdown_20",
                        "status": "review",
                        "primary_metric": 0.021,
                        "artifact_dir": "reports/autoresearch/runs/risk",
                    },
                    "",
                )
                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    mining_config_path=mining_config,
                )

            oracle.assert_called_once()
            self.assertEqual(oracle.call_args.kwargs["lane_name"], "risk_structure")
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["risk_structure", "run_status"], "completed")
            self.assertEqual(frame.loc["risk_structure", "candidate"], "max_drawdown_20")

    def test_runner_dispatches_regime_oracle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump({"lanes": {"regime": {"activation_status": "active"}}}),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_regime_lane_oracle") as oracle:
                oracle.return_value = (
                    {
                        "candidate": "",
                        "status": "review",
                        "primary_metric": 3.0,
                        "artifact_dir": "reports/autoresearch/runs/regime",
                    },
                    "",
                )
                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    provider_config_path="configs/provider_current.yaml",
                    start_time="2026-01-01",
                    end_time="2026-04-20",
                )

            oracle.assert_called_once()
            self.assertEqual(oracle.call_args.kwargs["lane_name"], "regime")
            self.assertEqual(oracle.call_args.kwargs["start_time"], "2026-01-01")
            self.assertEqual(oracle.call_args.kwargs["end_time"], "2026-04-20")
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["regime", "run_status"], "completed")

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

    def test_liquidity_lane_includes_amount_amihud_and_turnover_factors(self):
        repo_root = Path(__file__).resolve().parents[1]

        names = {spec["name"] for spec in _lane_factor_specs(repo_root, "configs/factor_mining.yaml", "liquidity_microstructure")}

        self.assertIn("amount_mean_20", names)
        self.assertIn("amihud_illiq_20", names)
        self.assertIn("turnover_mean_20", names)
        self.assertIn("turnover_volatility_20", names)

    def test_risk_lane_includes_drawdown_downside_gap_and_excursion_factors(self):
        repo_root = Path(__file__).resolve().parents[1]

        names = {spec["name"] for spec in _lane_factor_specs(repo_root, "configs/factor_mining.yaml", "risk_structure")}

        self.assertIn("max_drawdown_20", names)
        self.assertIn("downside_vol_20", names)
        self.assertIn("gap_risk_20", names)
        self.assertIn("intraday_excursion_20", names)

    def test_runner_dispatches_fundamental_quality_oracle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lane_space = root / "configs/autoresearch/lane_space.yaml"
            lane_space.parent.mkdir(parents=True)
            lane_space.write_text(
                yaml.safe_dump({"lanes": {"fundamental_quality": {"activation_status": "active"}}}),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.autoresearch.multilane.run_fundamental_lane_oracle") as oracle:
                oracle.return_value = (
                    {"candidate": "roe", "status": "review", "primary_metric": 0.04, "artifact_dir": "fund"},
                    "",
                )

                report = run_multilane_autoresearch(
                    lane_space_path=lane_space,
                    project_root=root,
                    start_time="2026-01-01",
                    end_time="2026-04-20",
                )

            oracle.assert_called_once()
            self.assertEqual(oracle.call_args.kwargs["lane_name"], "fundamental_quality")
            self.assertEqual(oracle.call_args.kwargs["start_time"], "2026-01-01")
            self.assertEqual(oracle.call_args.kwargs["end_time"], "2026-04-20")
            frame = report.to_frame().set_index("lane")
            self.assertEqual(frame.loc["fundamental_quality", "run_status"], "completed")


if __name__ == "__main__":
    unittest.main()
