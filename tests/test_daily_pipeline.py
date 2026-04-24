import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml


class DailyPipelineTests(unittest.TestCase):
    def test_daily_pipeline_cli_writes_run_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_fixture(root)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/run_daily_pipeline.py"),
                    "--project-root",
                    str(root),
                    "--signal-config",
                    "configs/signal.yaml",
                    "--trading-config",
                    "configs/trading.yaml",
                    "--portfolio-config",
                    "configs/portfolio.yaml",
                    "--risk-config",
                    "configs/risk.yaml",
                    "--execution-config",
                    "configs/execution.yaml",
                    "--exposures-csv",
                    "data/exposures.csv",
                    "--current-positions-csv",
                    "state/current_positions.csv",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            run_dir = root / "runs/20260423"
            self.assertTrue((run_dir / "signals.csv").exists())
            self.assertTrue((run_dir / "signal_summary.md").exists())
            self.assertTrue((run_dir / "target_portfolio.csv").exists())
            self.assertTrue((run_dir / "target_portfolio_summary.md").exists())
            self.assertTrue((run_dir / "expert_review_packet.md").exists())
            self.assertTrue((run_dir / "expert_review_result.md").exists())
            self.assertTrue((run_dir / "risk_report.md").exists())
            self.assertTrue((run_dir / "orders.csv").exists())
            self.assertTrue((run_dir / "fills.csv").exists())
            self.assertTrue((run_dir / "positions_expected.csv").exists())
            self.assertTrue((run_dir / "reconciliation.md").exists())
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["run_date"], "2026-04-23")
            self.assertEqual(manifest["status"], "pass")
            self.assertTrue(manifest["risk_passed"])
            self.assertIn("signals", manifest["artifacts"])
            self.assertIn("expert_review_packet", manifest["artifacts"])
            self.assertIn("expert_review_result", manifest["artifacts"])
            self.assertEqual(manifest["expert_review"]["decision"], "not_run")
            self.assertIn("orders", manifest["artifacts"])
            self.assertIn("wrote:", result.stdout)

    def test_daily_pipeline_stops_before_orders_when_risk_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_fixture(root, min_positions=3)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/run_daily_pipeline.py"),
                    "--project-root",
                    str(root),
                    "--signal-config",
                    "configs/signal.yaml",
                    "--trading-config",
                    "configs/trading.yaml",
                    "--portfolio-config",
                    "configs/portfolio.yaml",
                    "--risk-config",
                    "configs/risk.yaml",
                    "--execution-config",
                    "configs/execution.yaml",
                    "--exposures-csv",
                    "data/exposures.csv",
                    "--current-positions-csv",
                    "state/current_positions.csv",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            run_dir = root / "runs/20260423"
            self.assertTrue((run_dir / "signals.csv").exists())
            self.assertTrue((run_dir / "target_portfolio.csv").exists())
            self.assertTrue((run_dir / "expert_review_packet.md").exists())
            self.assertTrue((run_dir / "expert_review_result.md").exists())
            self.assertTrue((run_dir / "risk_report.md").exists())
            self.assertFalse((run_dir / "orders.csv").exists())
            self.assertFalse((run_dir / "fills.csv").exists())
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "risk_failed")
            self.assertFalse(manifest["risk_passed"])
            self.assertNotIn("orders", manifest["artifacts"])

    def test_daily_pipeline_runs_configured_expert_review_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_fixture(root)
            execution_path = root / "configs/execution.yaml"
            data = yaml.safe_load(execution_path.read_text(encoding="utf-8"))
            data["expert_review"] = {
                "enabled": True,
                "command": [
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('research_review_status: caution\\nreason: concentrated factor family')",
                ],
            }
            execution_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/run_daily_pipeline.py"),
                    "--project-root",
                    str(root),
                    "--signal-config",
                    "configs/signal.yaml",
                    "--trading-config",
                    "configs/trading.yaml",
                    "--portfolio-config",
                    "configs/portfolio.yaml",
                    "--risk-config",
                    "configs/risk.yaml",
                    "--execution-config",
                    "configs/execution.yaml",
                    "--exposures-csv",
                    "data/exposures.csv",
                    "--current-positions-csv",
                    "state/current_positions.csv",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            manifest = json.loads((root / "runs/20260423/manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["expert_review"]["status"], "completed")
            self.assertEqual(manifest["expert_review"]["decision"], "caution")
            self.assertIn("concentrated factor family", (root / "runs/20260423/expert_review_result.md").read_text(encoding="utf-8"))

    def _write_fixture(self, root: Path, min_positions: int = 1) -> None:
        (root / "configs").mkdir(parents=True)
        (root / "reports").mkdir(parents=True)
        (root / "data").mkdir(parents=True)
        (root / "state").mkdir(parents=True)
        (root / "reports/approved_factors.yaml").write_text(
            yaml.safe_dump(
                {
                    "approved_factors": [
                        {
                            "name": "core_alpha",
                            "expression": "$close",
                            "direction": 1,
                            "family": "test_family",
                            "approval_status": "core",
                            "regime_profile": "all_weather",
                        }
                    ]
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        (root / "configs/signal.yaml").write_text(
            yaml.safe_dump(
                {
                    "approved_factors_path": "reports/approved_factors.yaml",
                    "provider_config": "configs/provider_current.yaml",
                    "run_date": "2026-04-23",
                    "active_regime": "sideways",
                    "weights": {
                        "approval_status": {"core": 1.0, "challenger": 0.5, "reserve": 0.0},
                        "regime": {"all_weather": {"down": 1.0, "sideways": 1.0, "up": 1.0}},
                        "ensemble": {"rule_score": 1.0, "model_score": 0.0},
                    },
                    "output": {
                        "signals": "reports/signals_{run_yyyymmdd}.csv",
                        "summary": "reports/signal_summary_{run_yyyymmdd}.md",
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        (root / "configs/trading.yaml").write_text(
            yaml.safe_dump(
                {
                    "data_quality": {"min_coverage_ratio": 0.5},
                    "tradability": {"require_tradable": True, "min_amount_20d": 0.0},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        (root / "configs/portfolio.yaml").write_text(
            yaml.safe_dump(
                {
                    "portfolio": {
                        "top_k": 2,
                        "cash_buffer": 0.0,
                        "max_single_weight": 0.6,
                        "score_column": "ensemble_score",
                    },
                    "output": {
                        "target_portfolio": "reports/target_portfolio_{run_yyyymmdd}.csv",
                        "summary": "reports/target_portfolio_summary_{run_yyyymmdd}.md",
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        (root / "configs/risk.yaml").write_text(
            yaml.safe_dump(
                {
                    "risk": {
                        "max_single_weight": 0.6,
                        "min_positions": min_positions,
                        "min_signal_coverage": 0.5,
                        "max_turnover": 2.0,
                    },
                    "output": {"report": "reports/portfolio_risk_{run_yyyymmdd}.md"},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        (root / "configs/execution.yaml").write_text(
            yaml.safe_dump(
                {
                    "orders": {"total_equity": 100_000, "min_order_value": 100, "lot_size": 100},
                    "paper_broker": {"fill_ratio": 1.0, "min_trade_value": 100},
                    "reconcile": {"weight_tolerance": 0.001},
                    "output": {"run_dir": "runs/{run_yyyymmdd}"},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "BBB", "CCC"],
                "tradable": [True, True, False],
                "core_alpha": [3.0, 2.0, 1.0],
                "last_price": [10.0, 20.0, 30.0],
                "amount_20d": [1_000_000, 1_000_000, 1_000_000],
            }
        ).to_csv(root / "data/exposures.csv", index=False)
        pd.DataFrame(columns=["instrument", "current_weight", "last_price"]).to_csv(
            root / "state/current_positions.csv",
            index=False,
        )


if __name__ == "__main__":
    unittest.main()
