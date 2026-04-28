from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qlib_factor_lab.replay import replay_daily_run, write_replay_report


class ReplayTest(unittest.TestCase):
    def test_replay_passes_when_manifest_artifacts_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = root / "runs/20260423"
            run_dir.mkdir(parents=True)
            for name in ["signals.csv", "target_portfolio.csv", "orders.csv", "fills.csv", "run_summary.md"]:
                (run_dir / name).write_text("ok\n", encoding="utf-8")
            (run_dir / "run_summary.md").write_text("- status: pass\n- risk_passed: True\n", encoding="utf-8")
            _write_manifest(
                run_dir,
                {
                    "run_date": "2026-04-23",
                    "status": "pass",
                    "risk_passed": True,
                    "artifacts": {
                        "signals": str(run_dir / "signals.csv"),
                        "target_portfolio": str(run_dir / "target_portfolio.csv"),
                        "orders": str(run_dir / "orders.csv"),
                        "fills": str(run_dir / "fills.csv"),
                        "run_summary": str(run_dir / "run_summary.md"),
                    },
                },
            )

            report = replay_daily_run(run_dir)

            self.assertTrue(report.passed)
            frame = report.to_frame()
            self.assertEqual("pass", frame.set_index("check").loc["manifest_exists", "status"])
            self.assertEqual("pass", frame.set_index("check").loc["artifact:signals", "status"])

    def test_replay_fails_when_required_pass_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs/20260423"
            run_dir.mkdir(parents=True)
            (run_dir / "signals.csv").write_text("ok\n", encoding="utf-8")
            _write_manifest(
                run_dir,
                {
                    "run_date": "2026-04-23",
                    "status": "pass",
                    "risk_passed": True,
                    "artifacts": {
                        "signals": str(run_dir / "signals.csv"),
                        "target_portfolio": str(run_dir / "missing_target.csv"),
                    },
                },
            )

            report = replay_daily_run(run_dir)

            self.assertFalse(report.passed)
            rows = report.to_frame().set_index("check")
            self.assertEqual("fail", rows.loc["required_artifact:orders", "status"])
            self.assertEqual("fail", rows.loc["artifact:target_portfolio", "status"])

    def test_replay_failed_risk_run_does_not_require_orders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs/20260423"
            run_dir.mkdir(parents=True)
            (run_dir / "run_summary.md").write_text("- status: risk_failed\n- risk_passed: False\n", encoding="utf-8")
            _write_manifest(
                run_dir,
                {
                    "run_date": "2026-04-23",
                    "status": "risk_failed",
                    "risk_passed": False,
                    "artifacts": {"run_summary": str(run_dir / "run_summary.md")},
                },
            )

            report = replay_daily_run(run_dir)

            self.assertTrue(report.passed)
            self.assertNotIn("required_artifact:orders", set(report.to_frame()["check"]))

    def test_write_replay_report_writes_markdown_and_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "runs/20260423"
            run_dir.mkdir(parents=True)
            _write_manifest(run_dir, {"run_date": "2026-04-23", "status": "risk_failed", "artifacts": {}})
            report = replay_daily_run(run_dir)

            path = write_replay_report(report, run_dir / "replay_report.md")

            self.assertTrue(path.exists())
            self.assertTrue(path.with_suffix(".json").exists())
            self.assertIn("# Replay Report", path.read_text(encoding="utf-8"))
            data = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
            self.assertEqual("2026-04-23", data["run_date"])


def _write_manifest(run_dir: Path, payload: dict) -> None:
    (run_dir / "manifest.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
