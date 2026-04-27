import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from qlib_factor_lab.autoresearch.regime_oracle import (
    build_regime_summary_payload,
    run_regime_lane_oracle,
)
from qlib_factor_lab.config import ProjectConfig


class AutoresearchRegimeOracleTests(unittest.TestCase):
    def test_build_regime_summary_payload_reports_allocator_context(self):
        regime = pd.DataFrame(
            {
                "market_regime": ["sideways", "up", "up", "down", "sideways"],
                "trend_return": [0.0, 0.04, 0.05, -0.03, 0.0],
            },
            index=pd.date_range("2026-01-01", periods=5, freq="D", name="datetime"),
        )

        payload = build_regime_summary_payload(
            lane_name="regime",
            run_id="run1",
            regime=regime,
            artifact_dir="reports/autoresearch/runs/regime_run1",
            elapsed_sec=0.1,
        )

        self.assertEqual(payload["loop"], "regime")
        self.assertEqual(payload["candidate"], "")
        self.assertEqual(payload["active_regime"], "sideways")
        self.assertEqual(payload["regime_counts"], {"sideways": 2, "up": 2, "down": 1})
        self.assertEqual(payload["switch_count"], 3)
        self.assertEqual(payload["status"], "review")
        self.assertEqual(payload["primary_metric"], 3.0)

    def test_run_regime_lane_oracle_writes_summary_and_regime_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            frame = self._market_frame()

            with patch("qlib_factor_lab.autoresearch.regime_oracle.load_project_config") as load_config:
                with patch("qlib_factor_lab.autoresearch.regime_oracle.init_qlib") as init_qlib:
                    with patch("qlib_factor_lab.autoresearch.regime_oracle.fetch_market_regime_frame") as fetch_frame:
                        load_config.return_value = ProjectConfig(
                            provider_uri=Path("data"),
                            region="cn",
                            market="csi500_current",
                            benchmark="SH000905",
                            freq="day",
                            start_time="2026-01-01",
                            end_time="2026-01-08",
                        )
                        fetch_frame.return_value = frame

                        payload, block = run_regime_lane_oracle(
                            lane_name="regime",
                            provider_config="configs/provider_current.yaml",
                            project_root=root,
                            artifact_root="reports/autoresearch/runs",
                            fast_window=2,
                            slow_window=3,
                            trend_window=1,
                            trend_threshold=0.02,
                        )

            artifact_dir = Path(payload["artifact_dir"])
            self.assertEqual(payload["loop"], "regime")
            self.assertEqual(payload["candidate"], "")
            self.assertIn(payload["active_regime"], {"up", "down", "sideways"})
            self.assertGreater(payload["switch_count"], 0)
            self.assertIn("active_regime:", block)
            self.assertTrue((artifact_dir / "summary.json").exists())
            self.assertTrue((artifact_dir / "summary.txt").exists())
            self.assertTrue((artifact_dir / "market_regime.csv").exists())
            self.assertEqual(json.loads((artifact_dir / "summary.json").read_text())["artifact_dir"], str(artifact_dir))
            self.assertIn("market_regime", pd.read_csv(artifact_dir / "market_regime.csv").columns)
            init_qlib.assert_called_once()

    def test_run_regime_lane_oracle_discards_when_no_regime_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            empty = pd.DataFrame(
                columns=["close"],
                index=pd.MultiIndex.from_arrays(
                    [pd.to_datetime([]), []],
                    names=["datetime", "instrument"],
                ),
            )

            with patch("qlib_factor_lab.autoresearch.regime_oracle.load_project_config") as load_config:
                with patch("qlib_factor_lab.autoresearch.regime_oracle.init_qlib"):
                    with patch("qlib_factor_lab.autoresearch.regime_oracle.fetch_market_regime_frame") as fetch_frame:
                        load_config.return_value = ProjectConfig(
                            provider_uri=Path("data"),
                            region="cn",
                            market="csi500_current",
                            benchmark="SH000905",
                            freq="day",
                            start_time="2026-01-01",
                            end_time="2026-01-08",
                        )
                        fetch_frame.return_value = empty

                        payload, _ = run_regime_lane_oracle(
                            lane_name="regime",
                            provider_config="configs/provider_current.yaml",
                            project_root=root,
                        )

            self.assertEqual(payload["status"], "discard_candidate")
            self.assertEqual(payload["decision_reason"], "no market regime observations")
            self.assertEqual(payload["active_regime"], "unknown")

    def _market_frame(self) -> pd.DataFrame:
        dates = pd.date_range("2026-01-01", periods=8, freq="D")
        index = pd.MultiIndex.from_product([dates, ["A", "B"]], names=["datetime", "instrument"])
        frame = pd.DataFrame(index=index)
        frame["close"] = [
            10.0,
            20.0,
            11.0,
            22.0,
            12.0,
            24.0,
            13.0,
            26.0,
            12.0,
            24.0,
            11.0,
            22.0,
            10.0,
            20.0,
            10.0,
            20.0,
        ]
        return frame


if __name__ == "__main__":
    unittest.main()
