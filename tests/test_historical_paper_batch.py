import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from qlib_factor_lab.config import ProjectConfig
from qlib_factor_lab.historical_paper_batch import BatchPaths, build_historical_targets, write_historical_batch_summary
from qlib_factor_lab.portfolio import PortfolioConfig
from qlib_factor_lab.risk import RiskConfig
from qlib_factor_lab.signal import SignalConfig, SignalFactor
from qlib_factor_lab.tradability import TradabilityConfig


class HistoricalPaperBatchTests(unittest.TestCase):
    def test_write_historical_batch_summary_records_pipeline_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = BatchPaths(
                signal_paths=[root / "reports/signals_20260401.csv"],
                target_paths=[root / "reports/target_portfolio_20260401.csv"],
                batch_summary_csv=root / "runs/paper_batch_summary.csv",
                batch_summary_md=root / "runs/paper_batch_summary.md",
            )
            paths.batch_summary_csv.parent.mkdir(parents=True)
            pd.DataFrame([{"days": 1, "average_turnover": 0.1, "reconciliation_failures": 0}]).to_csv(
                paths.batch_summary_csv,
                index=False,
            )

            output = write_historical_batch_summary(paths, root / "reports/historical_paper_batch.md")

            self.assertTrue(output.exists())
            text = output.read_text(encoding="utf-8")
            self.assertIn("Historical Paper Batch", text)
            self.assertIn("signals_20260401.csv", text)

    def test_historical_paper_batch_cli_accepts_fixture_targets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_dir = root / "reports/historical_targets"
            target_dir.mkdir(parents=True)
            for day in range(2):
                date = pd.Timestamp("2026-04-01") + pd.Timedelta(days=day)
                pd.DataFrame(
                    {
                        "date": [date.strftime("%Y-%m-%d")],
                        "instrument": ["AAA"],
                        "target_weight": [0.1],
                        "last_price": [10.0],
                    }
                ).to_csv(target_dir / f"target_portfolio_{date.strftime('%Y%m%d')}.csv", index=False)
            config = root / "configs/execution.yaml"
            config.parent.mkdir(parents=True)
            config.write_text(
                yaml.safe_dump(
                    {
                        "orders": {"total_equity": 100000, "min_order_value": 100, "lot_size": 100},
                        "paper_broker": {"fill_ratio": 1.0},
                        "reconcile": {"weight_tolerance": 0.001},
                        "batch": {
                            "max_days": 2,
                            "run_root": "runs/paper_batch",
                            "summary_csv": "runs/paper_batch_summary.csv",
                            "summary_md": "runs/paper_batch_summary.md",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/run_historical_paper_batch.py"),
                    "--target-glob",
                    "reports/historical_targets/target_portfolio_*.csv",
                    "--execution-config",
                    str(config.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((root / "reports/historical_paper_batch.md").exists())
            self.assertIn("wrote:", result.stdout)

    def test_build_historical_targets_reuses_existing_qlib_initialization(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            project_config = ProjectConfig(
                provider_uri=root / "data/qlib",
                region="cn",
                market="csi500",
                benchmark="SH000905",
                start_time="2026-04-01",
                end_time="2026-04-02",
                freq="day",
            )
            factors = [
                SignalFactor(
                    name="core_alpha",
                    expression="$close",
                    direction=1,
                    family="test",
                    approval_status="core",
                    regime_profile="all_weather",
                )
            ]
            signal_config = SignalConfig(
                approved_factors_path=Path("reports/approved_factors.yaml"),
                provider_config=Path("configs/provider_current.yaml"),
                run_date="2026-04-01",
                active_regime="sideways",
                status_weights={"core": 1.0},
                regime_weights={"all_weather": {"sideways": 1.0}},
                rule_weight=1.0,
                model_weight=0.0,
                signals_output_path=Path("reports/signals.csv"),
                summary_output_path=Path("reports/signals.md"),
                execution_calendar_path=None,
            )
            run_dates = ["2026-04-01", "2026-04-02"]
            calls: list[tuple[str, bool]] = []

            def fake_fetch(_project_config, _factors, run_date, *, initialize=True):
                calls.append((run_date, initialize))
                return pd.DataFrame(
                    {
                        "date": [run_date, run_date],
                        "instrument": ["AAA", "BBB"],
                        "tradable": [True, True],
                        "core_alpha": [2.0, 1.0],
                        "amount_20d": [100_000_000, 90_000_000],
                        "last_price": [10.0, 20.0],
                    }
                )

            with patch("qlib_factor_lab.historical_paper_batch.fetch_daily_factor_exposures", side_effect=fake_fetch):
                build_historical_targets(
                    project_config,
                    factors,
                    signal_config,
                    TradabilityConfig(min_amount_20d=10_000_000),
                    PortfolioConfig(top_k=1, cash_buffer=0.0, max_single_weight=1.0),
                    RiskConfig(max_single_weight=1.0, min_positions=1, min_signal_coverage=0.1),
                    run_dates,
                    root / "signals",
                    root / "targets",
                )

            self.assertEqual(calls, [("2026-04-01", False), ("2026-04-02", False)])


if __name__ == "__main__":
    unittest.main()
