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
                "liquidity_microstructure_csi500",
                "liquidity_microstructure_csi300",
                "emotion_atmosphere",
                "research_context",
                "research_data_domains",
                "data_governance",
            ],
            names,
        )
        self.assertIn("--universe", steps[0].command)
        self.assertIn("scripts/build_tushare_qlib_data.py", steps[0].command)
        self.assertIn("csi500", steps[0].command)
        self.assertIn("configs/provider_current.yaml", steps[0].command)
        self.assertIn("csi300", steps[1].command)
        self.assertIn("configs/provider_csi300_current.yaml", steps[1].command)
        self.assertEqual("liquidity_microstructure_csi500", steps[2].name)
        self.assertIn("scripts/build_liquidity_microstructure.py", steps[2].command)
        self.assertIn("configs/provider_current.yaml", steps[2].command)
        self.assertIn("--merge-existing", steps[2].command)
        self.assertEqual("liquidity_microstructure_csi300", steps[3].name)
        self.assertIn("configs/provider_csi300_current.yaml", steps[3].command)
        self.assertEqual("emotion_atmosphere", steps[4].name)
        self.assertIn("scripts/build_emotion_atmosphere.py", steps[4].command)
        self.assertIn("--merge-existing", steps[4].command)
        self.assertIn("--merge-existing-events", steps[5].command)

    def test_plan_can_fallback_to_akshare_market_data_provider(self) -> None:
        config = DailyDataUpdateConfig(project_root=Path("/repo"), as_of_date="2026-04-27", market_data_provider="akshare")

        steps = build_daily_data_update_plan(config)

        self.assertIn("scripts/build_akshare_qlib_data.py", steps[0].command)
        self.assertIn("data/akshare/source_csi500", steps[0].command)

    def test_plan_can_force_market_data_start_date(self) -> None:
        config = DailyDataUpdateConfig(
            project_root=Path("/repo"),
            as_of_date="2026-04-30",
            market_data_provider="tushare",
            force_market_start="20260430",
        )

        steps = build_daily_data_update_plan(config)

        self.assertIn("--force-start", steps[0].command)
        self.assertIn("20260430", steps[0].command)
        self.assertIn("--force-start", steps[1].command)

    def test_plan_can_use_tushare_market_data_provider_for_both_universes(self) -> None:
        config = DailyDataUpdateConfig(project_root=Path("/repo"), as_of_date="2026-04-27", market_data_provider="tushare")

        steps = build_daily_data_update_plan(config)

        self.assertEqual("market_data_csi500", steps[0].name)
        self.assertIn("scripts/build_tushare_qlib_data.py", steps[0].command)
        self.assertIn("data/tushare/source_csi500", steps[0].command)
        self.assertEqual("market_data_csi300", steps[1].name)
        self.assertIn("scripts/build_tushare_qlib_data.py", steps[1].command)
        self.assertIn("data/tushare/source_csi300", steps[1].command)

    def test_plan_can_skip_market_data_for_fast_context_refresh(self) -> None:
        config = DailyDataUpdateConfig(project_root=Path("/repo"), as_of_date="2026-04-27", skip_market_data=True)

        steps = build_daily_data_update_plan(config)

        self.assertEqual(
            [
                "liquidity_microstructure_csi500",
                "liquidity_microstructure_csi300",
                "emotion_atmosphere",
                "research_context",
                "research_data_domains",
                "data_governance",
            ],
            [step.name for step in steps],
        )

    def test_plan_can_batch_research_data_domain_refresh_with_offset(self) -> None:
        config = DailyDataUpdateConfig(
            project_root=Path("/repo"),
            as_of_date="2026-04-27",
            fetch_fundamentals=True,
            fetch_cninfo_dividends=True,
            limit=50,
            offset=100,
        )

        steps = build_daily_data_update_plan(config)
        research_step = steps[-2]

        self.assertEqual("research_data_domains", research_step.name)
        self.assertIn("--limit", research_step.command)
        self.assertIn("50", research_step.command)
        self.assertIn("--offset", research_step.command)
        self.assertIn("100", research_step.command)

    def test_plan_defaults_to_tushare_fundamental_provider(self) -> None:
        config = DailyDataUpdateConfig(
            project_root=Path("/repo"),
            as_of_date="2026-04-27",
            fetch_fundamentals=True,
        )

        steps = build_daily_data_update_plan(config)
        research_step = steps[-2]

        self.assertEqual("research_data_domains", research_step.name)
        self.assertIn("--fundamental-provider", research_step.command)
        self.assertIn("tushare", research_step.command)

    def test_plan_can_fallback_to_akshare_fundamental_provider(self) -> None:
        config = DailyDataUpdateConfig(
            project_root=Path("/repo"),
            as_of_date="2026-04-27",
            fetch_fundamentals=True,
            fundamental_provider="akshare",
        )

        steps = build_daily_data_update_plan(config)
        research_step = steps[-2]

        self.assertIn("--fundamental-provider", research_step.command)
        self.assertIn("akshare", research_step.command)

    def test_write_update_manifest_records_step_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "manifest.md"
            steps = build_daily_data_update_plan(
                DailyDataUpdateConfig(project_root=Path(tmp), as_of_date="2026-04-27", skip_market_data=True)
            )
            path = write_update_manifest(output, as_of_date="2026-04-27", rows=[(steps[0], "pass", 0, "")])

            text = path.read_text(encoding="utf-8")
            self.assertIn("# Daily Data Update", text)
            self.assertIn(steps[0].name, text)
            self.assertIn("pass", text)


if __name__ == "__main__":
    unittest.main()
