import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from qlib_factor_lab.autoresearch.event_oracle import run_event_lane_oracle
from qlib_factor_lab.config import ProjectConfig


class AutoresearchEventOracleTests(unittest.TestCase):
    def _frame(self):
        dates = pd.date_range("2026-01-01", periods=8, freq="D")
        index = pd.MultiIndex.from_product([dates, ["AAA", "BBB"]], names=["datetime", "instrument"])
        return pd.DataFrame(
            {
                "demo": list(range(16)),
                "demo2": list(reversed(range(16))),
                "open": [10.0] * 16,
                "high": [11.0] * 16,
                "low": [9.5] * 16,
                "close": [10.5] * 16,
                "volume": [1000.0] * 16,
            },
            index=index,
        )

    def test_event_lane_oracle_runs_configured_factors_and_writes_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch("qlib_factor_lab.autoresearch.event_oracle.load_project_config") as load_config:
                with patch("qlib_factor_lab.autoresearch.event_oracle.init_qlib") as init_qlib:
                    with patch("qlib_factor_lab.autoresearch.event_oracle.fetch_event_backtest_frame") as fetch_frame:
                        load_config.return_value = object()
                        fetch_frame.return_value = self._frame()

                        payload, block = run_event_lane_oracle(
                            lane_name="pattern_event",
                            factor_specs=[
                                {
                                    "name": "demo",
                                    "expression": "$close / Ref($close, 1) - 1",
                                    "direction": 1,
                                },
                                {
                                    "name": "demo2",
                                    "expression": "$volume / Mean($volume, 5)",
                                    "direction": -1,
                                },
                            ],
                            provider_config="configs/provider_current.yaml",
                            project_root=root,
                            artifact_root="reports/autoresearch/runs",
                            horizons=(3,),
                        )

            self.assertEqual(payload["loop"], "pattern_event")
            self.assertEqual(payload["status"], "review")
            self.assertEqual(payload["factor_count"], 2)
            self.assertIn("event_mean_return_h3", payload)
            self.assertIn("loop: pattern_event", block)
            artifact = Path(payload["artifact_dir"])
            self.assertTrue((artifact / "summary.csv").exists())
            self.assertTrue((artifact / "trades.csv").exists())
            self.assertTrue((artifact / "summary.txt").exists())
            init_qlib.assert_called_once()
            fetch_frame.assert_called_once()

    def test_event_lane_oracle_overrides_provider_window_for_smoke_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch("qlib_factor_lab.autoresearch.event_oracle.load_project_config") as load_config:
                with patch("qlib_factor_lab.autoresearch.event_oracle.init_qlib"):
                    with patch("qlib_factor_lab.autoresearch.event_oracle.fetch_event_backtest_frame") as fetch_frame:
                        load_config.return_value = ProjectConfig(
                            provider_uri=Path("data"),
                            start_time="2015-01-01",
                            end_time="2026-04-20",
                        )
                        fetch_frame.return_value = self._frame()

                        run_event_lane_oracle(
                            lane_name="emotion_atmosphere",
                            factor_specs=[{"name": "demo", "expression": "$close", "direction": 1}],
                            provider_config="configs/provider_current.yaml",
                            project_root=root,
                            start_time="2026-01-01",
                            end_time="2026-04-20",
                            horizons=(3,),
                        )

            config = fetch_frame.call_args.args[0]
            self.assertEqual(config.start_time, "2026-01-01")
            self.assertEqual(config.end_time, "2026-04-20")


if __name__ == "__main__":
    unittest.main()
