from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from qlib_factor_lab.data_update import DailyDataUpdateConfig, build_daily_data_update_plan, write_update_manifest


class DailyDataUpdateTest(unittest.TestCase):
    def test_plan_updates_both_fixed_universes_before_context_and_governance(self) -> None:
        config = DailyDataUpdateConfig(project_root=Path("/repo"), as_of_date="2026-04-27")

        steps = build_daily_data_update_plan(config)
        names = [step.name for step in steps]

        self.assertEqual(
            [
                "market_data_csi500",
                "market_data_csi300",
                "research_context",
                "research_data_domains",
                "data_governance",
            ],
            names,
        )
        self.assertIn("--universe", steps[0].command)
        self.assertIn("csi500", steps[0].command)
        self.assertIn("configs/provider_current.yaml", steps[0].command)
        self.assertIn("csi300", steps[1].command)
        self.assertIn("configs/provider_csi300_current.yaml", steps[1].command)

    def test_plan_can_skip_market_data_for_fast_context_refresh(self) -> None:
        config = DailyDataUpdateConfig(project_root=Path("/repo"), as_of_date="2026-04-27", skip_market_data=True)

        steps = build_daily_data_update_plan(config)

        self.assertEqual(["research_context", "research_data_domains", "data_governance"], [step.name for step in steps])

    def test_write_update_manifest_records_step_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "manifest.md"
            steps = build_daily_data_update_plan(
                DailyDataUpdateConfig(project_root=Path(tmp), as_of_date="2026-04-27", skip_market_data=True)
            )
            path = write_update_manifest(output, as_of_date="2026-04-27", rows=[(steps[0], "pass", 0, "")])

            text = path.read_text(encoding="utf-8")
            self.assertIn("# Daily Data Update", text)
            self.assertIn("research_context", text)
            self.assertIn("pass", text)


if __name__ == "__main__":
    unittest.main()
