import tempfile
import unittest
from pathlib import Path

import yaml

from qlib_factor_lab.config import ProjectConfig
from qlib_factor_lab.model_workflow import build_qrun_command, render_lgb_workflow_config


class ModelWorkflowTests(unittest.TestCase):
    def test_render_lgb_workflow_config_injects_local_provider_uri(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / "workflow.yaml"
            config = ProjectConfig(
                provider_uri=root / "data/qlib/cn_data",
                region="cn",
                market="csi500",
                benchmark="SH000905",
                start_time="2016-01-01",
                end_time="2020-08-01",
            )

            render_lgb_workflow_config(config, output)

            data = yaml.safe_load(output.read_text(encoding="utf-8"))
            self.assertEqual(data["qlib_init"]["provider_uri"], str(config.provider_uri))
            self.assertEqual(data["market"], "csi500")
            self.assertEqual(data["benchmark"], "SH000905")
            self.assertEqual(data["task"]["model"]["class"], "LGBModel")

    def test_build_qrun_command_points_to_config(self):
        command = build_qrun_command(Path("configs/workflow.yaml"), qrun_bin="qrun")

        self.assertEqual(command, ["qrun", "configs/workflow.yaml"])

    def test_render_lgb_workflow_config_uses_recent_split_for_current_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            calendar_dir = root / "data/qlib/cn_data_current/calendars"
            calendar_dir.mkdir(parents=True)
            calendar_dir.joinpath("day.txt").write_text("2026-04-17\n2026-04-20\n", encoding="utf-8")
            instrument_dir = root / "data/qlib/cn_data_current/instruments"
            instrument_dir.mkdir()
            instrument_dir.joinpath("all.txt").write_text(
                "SH600000\t2015-01-05\t2026-04-20\nSZ000001\t2015-01-05\t2026-04-20\n",
                encoding="utf-8",
            )
            output = root / "workflow.yaml"
            config = ProjectConfig(
                provider_uri=root / "data/qlib/cn_data_current",
                region="cn",
                market="all",
                benchmark="SH000905",
                start_time="2015-01-01",
                end_time="2026-04-20",
            )

            render_lgb_workflow_config(config, output)

            data = yaml.safe_load(output.read_text(encoding="utf-8"))
            segments = data["task"]["dataset"]["kwargs"]["segments"]
            self.assertEqual(segments["train"], ["2015-01-01", "2021-12-31"])
            self.assertEqual(segments["valid"], ["2022-01-01", "2023-12-31"])
            self.assertEqual(segments["test"], ["2024-01-01", "2026-04-17"])
            benchmark = data["port_analysis_config"]["backtest"]["benchmark"]
            self.assertEqual(benchmark, ["SH600000", "SZ000001"])


if __name__ == "__main__":
    unittest.main()
