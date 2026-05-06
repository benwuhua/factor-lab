import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from qlib_factor_lab.daily_pipeline import _load_announcement_evidence, check_provider_data_freshness


class DailyPipelineTests(unittest.TestCase):
    def test_load_announcement_evidence_uses_low_memory_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            (root / "data/announcement_evidence.csv").write_text(
                "event_id,instrument,event_type\n1,AAA,financial_report_disclosure\n",
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.daily_pipeline.pd.read_csv", wraps=pd.read_csv) as read_csv:
                _load_announcement_evidence(root)

            read_csv.assert_called_once_with(root / "data" / "announcement_evidence.csv", low_memory=False)

    def test_provider_data_freshness_fails_when_latest_calendar_is_stale(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "configs").mkdir()
            qlib_dir = root / "data/qlib/cn_data_current"
            (qlib_dir / "calendars").mkdir(parents=True)
            (qlib_dir / "calendars/day.txt").write_text("2026-04-22\n2026-04-23\n", encoding="utf-8")
            (root / "configs/provider_current.yaml").write_text(
                yaml.safe_dump(
                    {
                        "provider_uri": "data/qlib/cn_data_current",
                        "freq": "day",
                        "end_time": "2026-04-23",
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            report = check_provider_data_freshness(
                root,
                Path("configs/provider_current.yaml"),
                now=pd.Timestamp("2026-04-28 09:00:00"),
                max_age_days=3,
            )

            self.assertFalse(report["passed"])
            self.assertEqual(report["latest_calendar_date"], "2026-04-23")
            self.assertIn("stale", report["detail"])

    def test_provider_data_freshness_passes_for_latest_complete_daily_bar(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "configs").mkdir()
            qlib_dir = root / "data/qlib/cn_data_current"
            (qlib_dir / "calendars").mkdir(parents=True)
            (qlib_dir / "calendars/day.txt").write_text("2026-04-24\n2026-04-27\n", encoding="utf-8")
            (root / "configs/provider_current.yaml").write_text(
                yaml.safe_dump(
                    {
                        "provider_uri": "data/qlib/cn_data_current",
                        "freq": "day",
                        "end_time": "2026-04-27",
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            report = check_provider_data_freshness(
                root,
                Path("configs/provider_current.yaml"),
                now=pd.Timestamp("2026-04-28 09:00:00"),
                max_age_days=3,
            )

            self.assertTrue(report["passed"])
            self.assertEqual(report["provider_end_time"], "2026-04-27")

    def test_provider_data_freshness_uses_latest_available_vendor_date_for_holidays(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "configs").mkdir()
            qlib_dir = root / "data/qlib/cn_data_current"
            (qlib_dir / "calendars").mkdir(parents=True)
            (qlib_dir / "calendars/day.txt").write_text("2026-04-29\n2026-04-30\n", encoding="utf-8")
            (root / "configs/provider_current.yaml").write_text(
                yaml.safe_dump(
                    {
                        "provider_uri": "data/qlib/cn_data_current",
                        "freq": "day",
                        "end_time": "2026-04-30",
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            report = check_provider_data_freshness(
                root,
                Path("configs/provider_current.yaml"),
                now=pd.Timestamp("2026-05-05 16:00:00"),
                max_age_days=3,
                latest_available_data_date="2026-04-30",
            )

            self.assertTrue(report["passed"], report["detail"])
            self.assertEqual(report["latest_available_data_date"], "2026-04-30")
            self.assertEqual(report["age_days"], 5)
            self.assertEqual(report["freshness_basis"], "latest_available_data_date")

    def test_provider_data_freshness_fails_when_local_calendar_lags_vendor_available_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "configs").mkdir()
            qlib_dir = root / "data/qlib/cn_data_current"
            (qlib_dir / "calendars").mkdir(parents=True)
            (qlib_dir / "calendars/day.txt").write_text("2026-04-29\n2026-04-30\n", encoding="utf-8")
            (root / "configs/provider_current.yaml").write_text(
                yaml.safe_dump(
                    {
                        "provider_uri": "data/qlib/cn_data_current",
                        "freq": "day",
                        "end_time": "2026-04-30",
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            report = check_provider_data_freshness(
                root,
                Path("configs/provider_current.yaml"),
                now=pd.Timestamp("2026-05-05 16:00:00"),
                max_age_days=3,
                latest_available_data_date="2026-05-05",
            )

            self.assertFalse(report["passed"])
            self.assertIn("calendar_behind_available_data", report["detail"])

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
            self.assertTrue((run_dir / "data_governance.md").exists())
            self.assertTrue((run_dir / "data_governance.csv").exists())
            self.assertTrue((run_dir / "research_portfolio.csv").exists())
            self.assertTrue((run_dir / "research_portfolio_summary.md").exists())
            self.assertTrue((run_dir / "execution_portfolio.csv").exists())
            self.assertTrue((run_dir / "execution_portfolio_summary.md").exists())
            self.assertTrue((run_dir / "target_portfolio.csv").exists())
            self.assertTrue((run_dir / "target_portfolio_summary.md").exists())
            self.assertTrue((run_dir / "stock_cards.jsonl").exists())
            self.assertTrue((run_dir / "expert_review_packet.md").exists())
            self.assertTrue((run_dir / "expert_review_result.md").exists())
            self.assertTrue((run_dir / "risk_report.md").exists())
            self.assertTrue((run_dir / "portfolio_gate_explanation.md").exists())
            self.assertTrue((run_dir / "run_summary.md").exists())
            self.assertTrue((run_dir / "orders.csv").exists())
            self.assertTrue((run_dir / "fills.csv").exists())
            self.assertTrue((run_dir / "positions_expected.csv").exists())
            self.assertTrue((run_dir / "reconciliation.md").exists())
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["run_date"], "2026-04-23")
            self.assertEqual(manifest["status"], "pass")
            self.assertTrue(manifest["risk_passed"])
            self.assertIn("signals", manifest["artifacts"])
            self.assertIn("data_governance", manifest["artifacts"])
            self.assertIn("data_governance_csv", manifest["artifacts"])
            self.assertIn("research_portfolio", manifest["artifacts"])
            self.assertIn("research_portfolio_summary", manifest["artifacts"])
            self.assertIn("execution_portfolio", manifest["artifacts"])
            self.assertIn("execution_portfolio_summary", manifest["artifacts"])
            self.assertIn("target_portfolio", manifest["artifacts"])
            self.assertIn("stock_cards", manifest["artifacts"])
            self.assertIn("expert_review_packet", manifest["artifacts"])
            self.assertIn("expert_review_result", manifest["artifacts"])
            self.assertIn("portfolio_gate_explanation", manifest["artifacts"])
            self.assertIn("run_summary", manifest["artifacts"])
            self.assertEqual(manifest["expert_review"]["decision"], "not_run")
            self.assertIn("orders", manifest["artifacts"])
            packet = (run_dir / "expert_review_packet.md").read_text(encoding="utf-8")
            self.assertIn("## Stock Research Cards", packet)
            self.assertIn("AAA", packet)
            self.assertIn("# Research Portfolio Summary", (run_dir / "research_portfolio_summary.md").read_text(encoding="utf-8"))
            self.assertIn("# Execution Portfolio Summary", (run_dir / "execution_portfolio_summary.md").read_text(encoding="utf-8"))
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
            self.assertTrue((run_dir / "portfolio_gate_explanation.md").exists())
            self.assertTrue((run_dir / "run_summary.md").exists())
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

    def test_daily_pipeline_scales_portfolio_when_expert_review_is_caution(self):
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
                    "import sys; sys.stdin.read(); print('research_review_status: caution')",
                ],
                "caution_action": "scale",
                "caution_weight_multiplier": 0.5,
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
            run_dir = root / "runs/20260423"
            research = pd.read_csv(run_dir / "research_portfolio.csv")
            execution = pd.read_csv(run_dir / "execution_portfolio.csv")
            target = pd.read_csv(run_dir / "target_portfolio.csv")
            self.assertAlmostEqual(float(research["target_weight"].sum()), 1.0)
            self.assertAlmostEqual(float(execution["target_weight"].sum()), 0.5)
            self.assertAlmostEqual(float(target["target_weight"].sum()), 0.5)
            self.assertTrue(execution["risk_flags"].str.contains("expert_review_caution_scaled").all())
            self.assertFalse(research.get("risk_flags", pd.Series(dtype=str)).astype(str).str.contains("expert_review_caution_scaled").any())
            manifest = json.loads((root / "runs/20260423/manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["expert_review_gate"]["status"], "scaled")
            self.assertEqual(Path(manifest["artifacts"]["execution_portfolio"]).resolve(), (run_dir / "execution_portfolio.csv").resolve())

    def test_daily_pipeline_blocks_orders_when_expert_review_rejects(self):
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
                    "import sys; sys.stdin.read(); print('research_review_status: reject')",
                ],
            }
            execution_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
            stale_run_dir = root / "runs/20260423"
            stale_run_dir.mkdir(parents=True)
            (stale_run_dir / "orders.csv").write_text("instrument,qty\nAAA,100\n", encoding="utf-8")
            (stale_run_dir / "risk_report.md").write_text("# stale risk report\n", encoding="utf-8")
            (stale_run_dir / "portfolio_gate_explanation.md").write_text("# stale gate\n", encoding="utf-8")
            (stale_run_dir / "research_stock_cards.md").write_text("# stale research cards\n", encoding="utf-8")
            (stale_run_dir / "research_stock_cards.jsonl").write_text("{}\n", encoding="utf-8")
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
            self.assertFalse((run_dir / "orders.csv").exists())
            self.assertFalse((run_dir / "risk_report.md").exists())
            self.assertFalse((run_dir / "portfolio_gate_explanation.md").exists())
            self.assertFalse((run_dir / "research_stock_cards.md").exists())
            self.assertFalse((run_dir / "research_stock_cards.jsonl").exists())
            self.assertTrue((run_dir / "block_report.md").exists())
            self.assertTrue((run_dir / "run_summary.md").exists())
            self.assertIn("expert_review_blocked", (run_dir / "block_report.md").read_text(encoding="utf-8"))
            self.assertIn("expert_review_blocked", (run_dir / "run_summary.md").read_text(encoding="utf-8"))
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "expert_review_blocked")
            self.assertEqual(manifest["expert_review_gate"]["status"], "blocked")
            self.assertIn("block_report", manifest["artifacts"])
            self.assertIn("run_summary", manifest["artifacts"])
            self.assertNotIn("orders", manifest["artifacts"])
            self.assertNotIn("risk_report", manifest["artifacts"])
            self.assertNotIn("portfolio_gate_explanation", manifest["artifacts"])

    def test_daily_pipeline_allows_orders_after_manual_confirmation_override(self):
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
                    "import sys; sys.stdin.read(); print('research_review_status: caution')",
                ],
                "caution_action": "manual_confirmation",
                "caution_weight_multiplier": 0.5,
                "manual_confirmation": {
                    "enabled": True,
                    "reviewer": "ryan",
                    "reason": "checked chart and event packet",
                },
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

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            run_dir = root / "runs/20260423"
            self.assertTrue((run_dir / "orders.csv").exists())
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "pass")
            self.assertEqual(manifest["expert_review_gate"]["status"], "manual_confirmed")
            self.assertEqual(manifest["expert_review_gate"]["reviewer"], "ryan")
            portfolio = pd.read_csv(run_dir / "target_portfolio.csv")
            aaa = portfolio[portfolio["instrument"] == "AAA"].iloc[0]
            self.assertIn("expert_manual_confirmed", aaa["risk_flags"])
            stock_cards = (run_dir / "stock_cards.jsonl").read_text(encoding="utf-8")
            self.assertIn("manual_confirmed", stock_cards)
            self.assertIn("expert_manual_confirmed", stock_cards)

    def test_daily_pipeline_writes_event_risk_snapshot_and_enriches_portfolio(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_fixture(root)
            self._write_event_risk_fixture(root, severity="watch", event_type="earnings_watch")
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
                    "--event-risk-config",
                    "configs/event_risk.yaml",
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
            self.assertTrue((run_dir / "event_risk_snapshot.csv").exists())
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertIn("event_risk_snapshot", manifest["artifacts"])
            portfolio = pd.read_csv(run_dir / "target_portfolio.csv")
            self.assertIn("industry_sw", portfolio.columns)
            self.assertIn("event_risk_summary", portfolio.columns)
            aaa = portfolio[portfolio["instrument"] == "AAA"].iloc[0]
            self.assertEqual(aaa["industry_sw"], "Pharma")
            self.assertIn("earnings_watch", aaa["event_risk_summary"])
            stock_cards = (run_dir / "stock_cards.jsonl").read_text(encoding="utf-8")
            self.assertIn("earnings_watch", stock_cards)
            self.assertIn("Alpha A", stock_cards)

    def test_daily_pipeline_stops_before_signal_when_data_governance_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_fixture(root)
            (root / "configs/data_governance.yaml").write_text(
                yaml.safe_dump(
                    {
                        "data_governance": {
                            "expected_universe_path": "data/universe.csv",
                            "domains": {
                                "required_vendor_feed": {
                                    "path": "data/missing_vendor.csv",
                                    "required_fields": ["instrument"],
                                    "activation_if_missing": "block",
                                    "activation_lane": "expression_price_volume",
                                }
                            },
                        }
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            repo = Path(__file__).resolve().parents[1]
            (root / "data/exposures.csv").unlink()

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
            self.assertTrue((run_dir / "data_governance.md").exists())
            self.assertTrue((run_dir / "data_governance.csv").exists())
            self.assertTrue((run_dir / "run_summary.md").exists())
            self.assertFalse((run_dir / "signals.csv").exists())
            self.assertFalse((run_dir / "orders.csv").exists())
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "data_governance_failed")
            self.assertFalse(manifest["risk_passed"])
            self.assertIn("data_governance", manifest["artifacts"])

    def test_daily_pipeline_event_risk_enrichment_preserves_duplicate_signal_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_fixture(root)
            self._write_event_risk_fixture(root, severity="watch", event_type="earnings_watch")
            exposures_path = root / "data/exposures.csv"
            exposures = pd.read_csv(exposures_path)
            exposures = pd.concat([exposures, exposures.iloc[[2]]], ignore_index=True)
            exposures.to_csv(exposures_path, index=False)
            original_row_count = len(exposures)
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
                    "--event-risk-config",
                    "configs/event_risk.yaml",
                    "--exposures-csv",
                    "data/exposures.csv",
                    "--current-positions-csv",
                    "state/current_positions.csv",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            run_dir = root / "runs/20260423"
            signals = pd.read_csv(run_dir / "signals.csv")
            event_snapshot = pd.read_csv(run_dir / "event_risk_snapshot.csv")
            self.assertEqual(len(signals), original_row_count)
            self.assertEqual(len(event_snapshot), original_row_count)

    def test_daily_pipeline_stops_before_orders_when_event_risk_blocks_selected_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_fixture(root)
            self._write_event_risk_fixture(root, severity="block", event_type="disciplinary_action")
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
                    "--event-risk-config",
                    "configs/event_risk.yaml",
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
            self.assertFalse((run_dir / "orders.csv").exists())
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "risk_failed")
            self.assertFalse(manifest["risk_passed"])
            self.assertTrue((run_dir / "block_report.md").exists())
            self.assertTrue((run_dir / "run_summary.md").exists())
            block_report = (run_dir / "block_report.md").read_text(encoding="utf-8")
            run_summary = (run_dir / "run_summary.md").read_text(encoding="utf-8")
            self.assertIn("risk_failed", block_report)
            self.assertIn("event_blocked_positions", block_report)
            self.assertIn("event_blocked_positions", run_summary)
            self.assertIn("block_report", manifest["artifacts"])
            self.assertIn("run_summary", manifest["artifacts"])
            risk_report = (run_dir / "risk_report.md").read_text(encoding="utf-8")
            self.assertIn("event_blocked_positions", risk_report)
            self.assertIn("AAA:", risk_report)

    def test_daily_pipeline_cli_runs_combo_spec_through_formal_review_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_fixture(root)
            (root / "configs/combo_specs").mkdir(parents=True)
            (root / "configs/combo_specs/quality_gap_breakout_v1.yaml").write_text(
                yaml.safe_dump(
                    {
                        "name": "quality_gap_breakout_v1",
                        "description": "Quality base plus gap/risk structure.",
                        "members": [
                            {
                                "name": "quality_low_leverage",
                                "source": "fundamental_quality",
                                "family": "fundamental_quality",
                                "logic_bucket": "quality_low_leverage",
                                "direction": 1,
                                "weight": 0.6,
                                "components": [
                                    {"field": "roe", "direction": 1, "weight": 1.0},
                                    {"field": "debt_ratio", "direction": -1, "weight": 0.25},
                                ],
                            },
                            {
                                "name": "core_alpha",
                                "source": "approved_factor",
                                "family": "test_family",
                                "logic_bucket": "test_logic",
                                "direction": 1,
                                "weight": 0.4,
                            },
                        ],
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            pd.DataFrame(
                {
                    "instrument": ["AAA", "BBB", "CCC"],
                    "available_at": ["2026-04-01", "2026-04-01", "2026-04-01"],
                    "roe": [12.0, 6.0, 3.0],
                    "debt_ratio": [20.0, 80.0, 50.0],
                }
            ).to_csv(root / "data/fundamental_quality.csv", index=False)
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
                    "--combo-spec",
                    "configs/combo_specs/quality_gap_breakout_v1.yaml",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            run_dir = root / "runs/20260423"
            signals = pd.read_csv(run_dir / "signals.csv")
            self.assertIn("quality_low_leverage_contribution", signals.columns)
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["inputs"]["combo_spec"], "configs/combo_specs/quality_gap_breakout_v1.yaml")
            self.assertIn("combo_spec", manifest["artifacts"])
            packet = (run_dir / "expert_review_packet.md").read_text(encoding="utf-8")
            self.assertIn("quality_low_leverage", packet)
            self.assertIn("source=fundamental_quality; direction=1; weight=0.6", packet)
            self.assertIn("source=approved_factor; direction=1; weight=0.4", packet)
            stock_cards = (run_dir / "stock_cards.jsonl").read_text(encoding="utf-8")
            self.assertIn("quality_low_leverage", stock_cards)

    def test_daily_pipeline_cli_manual_confirmation_override_releases_caution_gate(self):
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
                    "import sys; sys.stdin.read(); print('结论：`caution`\\n硬人工复核：AAA')",
                ],
                "caution_action": "manual_confirmation",
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
                    "--expert-manual-confirm",
                    "--expert-reviewer",
                    "ryan",
                    "--expert-confirm-reason",
                    "checked event and chart context",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            run_dir = root / "runs/20260423"
            manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual("pass", manifest["status"])
            self.assertEqual("manual_confirmed", manifest["expert_review_gate"]["status"])
            self.assertEqual("ryan", manifest["expert_review_gate"]["reviewer"])
            self.assertTrue((run_dir / "orders.csv").exists())
            portfolio = pd.read_csv(run_dir / "target_portfolio.csv")
            aaa = portfolio[portfolio["instrument"] == "AAA"].iloc[0]
            self.assertIn("expert_manual_confirmed", aaa["risk_flags"])

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
                "turnover_20d": [0.02, 0.01, 0.0],
                "industry": ["医药", "电力设备", "机械"],
                "limit_up": [False, False, False],
                "limit_down": [False, True, False],
                "suspended": [False, False, False],
                "abnormal_event": ["", "earnings_warning", ""],
                "announcement_flag": [False, True, False],
            }
        ).to_csv(root / "data/exposures.csv", index=False)
        pd.DataFrame({"instrument": ["AAA", "BBB", "CCC"]}).to_csv(root / "data/universe.csv", index=False)
        (root / "configs/data_governance.yaml").write_text(
            yaml.safe_dump(
                {
                    "data_governance": {
                        "expected_universe_path": "data/universe.csv",
                        "domains": {
                            "market_ohlcv": {
                                "path": "data/exposures.csv",
                                "required_fields": ["date", "instrument", "core_alpha"],
                                "pit_fields": ["date"],
                                "min_coverage_ratio": 0.5,
                                "activation_lane": "expression_price_volume",
                            }
                        },
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        pd.DataFrame(columns=["instrument", "current_weight", "last_price"]).to_csv(
            root / "state/current_positions.csv",
            index=False,
        )

    def _write_event_risk_fixture(self, root: Path, severity: str, event_type: str) -> None:
        (root / "configs/event_risk.yaml").write_text(
            yaml.safe_dump(
                {
                    "event_risk": {
                        "security_master_path": "data/security_master.csv",
                        "events_path": "data/company_events.csv",
                        "default_lookback_days": 30,
                        "block_event_types": ["disciplinary_action"],
                        "block_severities": ["block"],
                        "max_events_per_name": 3,
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        pd.DataFrame(
            {
                "instrument": ["AAA", "BBB", "CCC"],
                "name": ["Alpha A", "Beta B", "Gamma C"],
                "exchange": ["XSHG", "XSHE", "XSHG"],
                "board": ["main", "main", "main"],
                "industry_sw": ["Pharma", "Power Equipment", "Machinery"],
                "industry_csrc": ["Healthcare", "Manufacturing", "Manufacturing"],
                "is_st": [False, False, False],
                "listing_date": ["2020-01-01", "2020-01-01", "2020-01-01"],
                "delisting_date": ["", "", ""],
                "valid_from": ["2020-01-01", "2020-01-01", "2020-01-01"],
                "valid_to": ["", "", ""],
            }
        ).to_csv(root / "data/security_master.csv", index=False)
        pd.DataFrame(
            {
                "event_id": ["evt-1"],
                "instrument": ["AAA"],
                "event_type": [event_type],
                "event_date": ["2026-04-20"],
                "source": ["exchange"],
                "source_url": ["https://example.test/events/evt-1"],
                "title": ["AAA event"],
                "severity": [severity],
                "summary": ["selected name event context"],
                "evidence": ["fixture"],
                "active_until": [""],
            }
        ).to_csv(root / "data/company_events.csv", index=False)


if __name__ == "__main__":
    unittest.main()
