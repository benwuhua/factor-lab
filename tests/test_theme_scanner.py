import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.theme_scanner import (
    build_theme_candidates,
    load_theme_universe,
    write_theme_candidate_report,
    write_theme_candidates,
)


class ThemeScannerTests(unittest.TestCase):
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
