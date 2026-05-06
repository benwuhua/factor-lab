import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.theme_scanner import (
    build_theme_candidates,
    combine_signal_with_supplemental,
    load_theme_universe,
    missing_theme_instruments,
    write_theme_candidate_report,
    write_theme_candidates,
)


class ThemeScannerTests(unittest.TestCase):
    def test_build_theme_candidates_outputs_business_scores_tiers_and_reasons(self):
        with tempfile.TemporaryDirectory() as tmp:
            theme_path = Path(tmp) / "theme.yaml"
            theme_path.write_text(
                """
theme_id: ai_semiconductor
display_name: AI产业链
members:
  - instrument: SH688981
    name: 中芯国际
    supply_chain_role: 晶圆代工
    sub_chain: foundry
    theme_exposure: 1.0
  - instrument: SZ300456
    name: 赛微电子
    supply_chain_role: 半导体设备
    sub_chain: semiconductor_equipment
    theme_exposure: 0.7
  - instrument: SZ002261
    name: 拓维信息
    supply_chain_role: 整机集成
    sub_chain: compute_integration
    theme_exposure: 0.4
""",
                encoding="utf-8",
            )
            universe = load_theme_universe(theme_path)

        signal = pd.DataFrame(
            [
                {
                    "date": "2026-04-30",
                    "instrument": "SH688981",
                    "ensemble_score": 0.95,
                    "family_quality_score": 0.8,
                    "family_growth_improvement_score": 0.7,
                    "quiet_breakout_20_contribution": 0.6,
                    "family_event_catalyst_score": 0.5,
                    "event_blocked": False,
                    "risk_flags": "",
                    "top_factor_1": "family_quality",
                    "top_factor_2": "quiet_breakout_20",
                },
                {
                    "date": "2026-04-30",
                    "instrument": "SZ300456",
                    "ensemble_score": 0.7,
                    "family_quality_score": 0.4,
                    "family_growth_improvement_score": 0.9,
                    "quiet_breakout_20_contribution": 0.3,
                    "family_event_catalyst_score": 0.2,
                    "event_blocked": False,
                    "risk_flags": "",
                    "top_factor_1": "family_growth_improvement",
                    "top_factor_2": "family_quality",
                },
                {
                    "date": "2026-04-30",
                    "instrument": "SZ002261",
                    "ensemble_score": 0.85,
                    "family_quality_score": 0.3,
                    "family_growth_improvement_score": 0.5,
                    "quiet_breakout_20_contribution": 0.7,
                    "family_event_catalyst_score": -0.4,
                    "event_blocked": True,
                    "risk_flags": "event_blocked",
                    "top_factor_1": "quiet_breakout_20",
                },
            ]
        )

        candidates = build_theme_candidates(signal, universe, top_k=10)

        for column in [
            "sub_chain",
            "theme_score",
            "quality_score",
            "growth_score",
            "momentum_score",
            "event_score",
            "risk_penalty",
            "total_score",
            "tier",
            "reason",
        ]:
            self.assertIn(column, candidates.columns)

        smic = candidates[candidates["instrument"] == "SH688981"].iloc[0]
        blocked = candidates[candidates["instrument"] == "SZ002261"].iloc[0]
        self.assertEqual(smic["tier"], "A重点研究")
        self.assertEqual(blocked["tier"], "C风险复核")
        self.assertGreater(smic["total_score"], blocked["total_score"])
        self.assertIn("晶圆代工", smic["reason"])
        self.assertIn("事件/交易风险", blocked["reason"])

    def test_ai_semiconductor_theme_config_loads_required_sub_chains(self):
        universe = load_theme_universe(Path("configs/themes/ai_semiconductor.yaml"))

        self.assertEqual(universe.theme_id, "ai_semiconductor")
        self.assertIn("非投资建议", universe.thesis)
        sub_chains = set(universe.members["sub_chain"].astype(str))
        for expected in {
            "chip_design",
            "foundry",
            "semiconductor_equipment",
            "semiconductor_materials",
            "memory_storage",
            "advanced_packaging_interconnect",
            "compute_integration",
        }:
            self.assertIn(expected, sub_chains)

    def test_build_theme_candidates_enriches_signal_with_supply_chain_roles(self):
        with tempfile.TemporaryDirectory() as tmp:
            theme_path = Path(tmp) / "theme.yaml"
            theme_path.write_text(
                """
theme_id: deepseek_ascend_semiconductor
display_name: DeepSeek / Ascend semiconductor supply chain
as_of_date: 2026-04-28
thesis: Theme hypothesis, not investment advice.
score:
  signal_weight: 0.7
  theme_weight: 0.3
members:
  - instrument: SH688981
    name: 中芯国际
    supply_chain_role: 晶圆代工
    theme_exposure: 1.0
    confidence: high
  - instrument: SZ300456
    name: 赛微电子
    supply_chain_role: OCS 光交换
    theme_exposure: 0.5
    confidence: medium
""",
                encoding="utf-8",
            )
            universe = load_theme_universe(theme_path)

        signal = pd.DataFrame(
            [
                {
                    "date": "2026-04-27",
                    "instrument": "SH688981",
                    "ensemble_score": 0.8,
                    "amount_20d": 1_000_000_000,
                    "event_blocked": False,
                    "risk_flags": "",
                },
                {
                    "date": "2026-04-27",
                    "instrument": "SZ300456",
                    "ensemble_score": 0.9,
                    "amount_20d": 500_000_000,
                    "event_blocked": True,
                    "risk_flags": "event_blocked",
                },
                {
                    "date": "2026-04-27",
                    "instrument": "SH600000",
                    "ensemble_score": 2.0,
                    "amount_20d": 600_000_000,
                    "event_blocked": False,
                    "risk_flags": "",
                },
            ]
        )

        candidates = build_theme_candidates(signal, universe, top_k=10)

        self.assertEqual(candidates["instrument"].tolist(), ["SH688981", "SZ300456"])
        self.assertEqual(candidates.loc[0, "supply_chain_role"], "晶圆代工")
        self.assertEqual(candidates.loc[0, "research_status"], "research_candidate")
        self.assertEqual(candidates.loc[1, "research_status"], "risk_review")
        self.assertEqual(candidates.loc[0, "recommendation_type"], "research_candidate_not_advice")
        self.assertGreater(candidates.loc[0, "theme_research_score"], candidates.loc[1, "theme_research_score"])

    def test_missing_theme_instruments_and_supplemental_signal_fill_watch_only_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            theme_path = Path(tmp) / "theme.yaml"
            theme_path.write_text(
                """
theme_id: deepseek_ascend_semiconductor
display_name: DeepSeek / Ascend semiconductor supply chain
members:
  - instrument: SH688981
    name: 中芯国际
    supply_chain_role: 晶圆代工
    theme_exposure: 1.0
  - instrument: SZ300456
    name: 赛微电子
    supply_chain_role: OCS 光交换
    theme_exposure: 0.5
""",
                encoding="utf-8",
            )
            universe = load_theme_universe(theme_path)
            primary = pd.DataFrame(
                [
                    {
                        "date": "2026-04-27",
                        "instrument": "SH688981",
                        "ensemble_score": 0.4,
                        "event_blocked": False,
                    }
                ]
            )
            supplemental = pd.DataFrame(
                [
                    {
                        "date": "2026-04-27",
                        "instrument": "SZ300456",
                        "ensemble_score": 0.9,
                        "event_blocked": False,
                    },
                    {
                        "date": "2026-04-27",
                        "instrument": "SH688981",
                        "ensemble_score": -1.0,
                        "event_blocked": False,
                    },
                ]
            )

            self.assertEqual(missing_theme_instruments(primary, universe), ["SZ300456"])
            combined = combine_signal_with_supplemental(primary, supplemental)
            candidates = build_theme_candidates(combined, universe, top_k=10)

            self.assertEqual(
                candidates[candidates["instrument"] == "SZ300456"]["research_status"].iloc[0],
                "research_candidate",
            )
            self.assertEqual(candidates[candidates["instrument"] == "SH688981"]["ensemble_score"].iloc[0], 0.4)

    def test_write_theme_outputs_csv_and_markdown_report(self):
        candidates = pd.DataFrame(
            [
                {
                    "date": "2026-04-27",
                    "instrument": "SH688981",
                    "name": "中芯国际",
                    "supply_chain_role": "晶圆代工",
                    "ensemble_score": 0.8,
                    "theme_research_score": 0.86,
                    "research_status": "research_candidate",
                    "recommendation_type": "research_candidate_not_advice",
                }
            ]
        )

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = write_theme_candidates(candidates, Path(tmp) / "theme_candidates.csv")
            report_path = write_theme_candidate_report(
                candidates,
                Path(tmp) / "theme_report.md",
                theme_display_name="DeepSeek / Ascend semiconductor supply chain",
                thesis="Theme hypothesis, not investment advice.",
                sources=["https://example.test/source"],
            )

            self.assertIn("SH688981", csv_path.read_text(encoding="utf-8"))
            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("研究候选", report_text)
            self.assertIn("非投资建议", report_text)
            self.assertIn("https://example.test/source", report_text)


if __name__ == "__main__":
    unittest.main()
