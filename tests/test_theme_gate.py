import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.theme_gate import (
    ThemeGateConfig,
    check_theme_gate,
    write_theme_gate_report,
)


class ThemeGateTests(unittest.TestCase):
    def test_theme_gate_allows_small_single_theme_research_shortlist(self):
        candidates = pd.DataFrame(
            [
                {
                    "instrument": "AAA",
                    "research_status": "research_candidate",
                    "theme_research_score": 0.8,
                    "theme_exposure": 1.0,
                    "ensemble_score": 0.5,
                    "amount_20d": 1_000_000_000,
                    "tradable": True,
                    "buy_blocked": False,
                    "event_blocked": False,
                },
                {
                    "instrument": "BBB",
                    "research_status": "research_candidate",
                    "theme_research_score": 0.7,
                    "theme_exposure": 0.3,
                    "ensemble_score": 1.1,
                    "amount_20d": 500_000_000,
                    "tradable": True,
                    "buy_blocked": False,
                    "event_blocked": False,
                },
                {
                    "instrument": "CCC",
                    "research_status": "research_candidate",
                    "theme_research_score": 0.6,
                    "theme_exposure": 0.25,
                    "ensemble_score": 0.7,
                    "amount_20d": 700_000_000,
                    "tradable": True,
                    "buy_blocked": False,
                    "event_blocked": False,
                },
            ]
        )

        report = check_theme_gate(candidates, ThemeGateConfig(min_research_candidates=3))

        self.assertEqual(report.decision, "pass")
        self.assertTrue(report.passed)
        self.assertEqual(report.to_frame().set_index("check").loc["min_research_candidates", "status"], "pass")

    def test_theme_gate_rejects_blocked_events_but_marks_watch_only_as_caution(self):
        candidates = pd.DataFrame(
            [
                {
                    "instrument": "AAA",
                    "research_status": "research_candidate",
                    "theme_research_score": 0.8,
                    "theme_exposure": 1.0,
                    "ensemble_score": 0.5,
                    "amount_20d": 1_000_000_000,
                    "tradable": True,
                    "buy_blocked": False,
                    "event_blocked": True,
                    "event_risk_summary": "减持",
                },
                {
                    "instrument": "BBB",
                    "research_status": "watch_only",
                    "theme_research_score": 0.4,
                    "theme_exposure": 0.4,
                    "ensemble_score": pd.NA,
                    "amount_20d": pd.NA,
                },
            ]
        )

        report = check_theme_gate(candidates, ThemeGateConfig(min_research_candidates=1))
        frame = report.to_frame().set_index("check")

        self.assertEqual(report.decision, "reject")
        self.assertEqual(frame.loc["event_blocked_positions", "status"], "fail")
        self.assertEqual(frame.loc["watch_only_positions", "status"], "caution")

    def test_write_theme_gate_report_renders_decision_and_checks(self):
        report = check_theme_gate(
            pd.DataFrame(
                [
                    {
                        "instrument": "AAA",
                        "research_status": "research_candidate",
                        "theme_research_score": 0.8,
                        "theme_exposure": 1.0,
                        "ensemble_score": 0.5,
                        "amount_20d": 1_000_000_000,
                    }
                ]
            ),
            ThemeGateConfig(min_research_candidates=1),
        )

        with tempfile.TemporaryDirectory() as tmp:
            output = write_theme_gate_report(report, Path(tmp) / "theme_gate.md")
            text = output.read_text(encoding="utf-8")

        self.assertIn("Theme Gate Report", text)
        self.assertIn("decision: pass", text)
        self.assertIn("min_research_candidates", text)


if __name__ == "__main__":
    unittest.main()
