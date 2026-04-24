import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.expert_review import build_expert_review_packet


class ExpertReviewTests(unittest.TestCase):
    def test_build_expert_review_packet_includes_portfolio_drivers_and_factor_warnings(self):
        packet = build_expert_review_packet(
            target_portfolio=self._target_portfolio(),
            factor_diagnostics=self._factor_diagnostics(),
            run_date="2026-04-23",
        )

        self.assertIn("# Expert Portfolio Review Packet", packet)
        self.assertIn("run_date: 2026-04-23", packet)
        self.assertIn("AAA", packet)
        self.assertIn("main drivers", packet)
        self.assertIn("shadow_review", packet)
        self.assertIn("Questions For Expert LLM", packet)
        self.assertNotIn(" nan ", packet)

    def test_build_expert_review_packet_cli_writes_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_path = root / "runs/20260423/target_portfolio.csv"
            diagnostics_path = root / "reports/diagnostics.csv"
            output_path = root / "runs/20260423/expert_review_packet.md"
            target_path.parent.mkdir(parents=True)
            diagnostics_path.parent.mkdir(parents=True)
            self._target_portfolio().to_csv(target_path, index=False)
            self._factor_diagnostics().to_csv(diagnostics_path, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_expert_review_packet.py"),
                    "--target-portfolio",
                    str(target_path.relative_to(root)),
                    "--factor-diagnostics",
                    str(diagnostics_path.relative_to(root)),
                    "--run-date",
                    "2026-04-23",
                    "--output",
                    str(output_path.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(output_path.exists())
            self.assertIn("Expert Portfolio Review Packet", output_path.read_text(encoding="utf-8"))
            self.assertIn("wrote:", result.stdout)

    def _target_portfolio(self):
        return pd.DataFrame(
            {
                "instrument": ["AAA", "BBB"],
                "rank": [1, 2],
                "target_weight": [0.0475, 0.0475],
                "ensemble_score": [5.0, 4.0],
                "selection_explanation": [
                    "selected by ensemble_score 5; main drivers: alpha_a 3, alpha_b 2",
                    "selected by ensemble_score 4; main drivers: alpha_c 4",
                ],
                "top_factor_1": ["alpha_a", "alpha_c"],
                "top_factor_1_contribution": [3.0, 4.0],
                "risk_flags": ["", ""],
                "amount_20d": [100_000_000, 80_000_000],
            }
        )

    def _factor_diagnostics(self):
        return pd.DataFrame(
            {
                "factor": ["alpha_a", "alpha_b"],
                "family": ["family_one", "family_one"],
                "suggested_role": ["core_candidate", "shadow_review"],
                "neutral_rank_ic_h20": [0.04, 0.034],
                "neutral_long_short_h20": [0.003, -0.001],
                "concerns": ["", "negative_neutral_long_short"],
            }
        )


if __name__ == "__main__":
    unittest.main()
