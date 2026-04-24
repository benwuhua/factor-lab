import tempfile
import subprocess
import sys
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.factor_diagnostics import (
    build_single_factor_diagnostics,
    write_single_factor_diagnostics_markdown,
)


class FactorDiagnosticsTests(unittest.TestCase):
    def test_build_single_factor_diagnostics_merges_raw_neutral_and_marks_family_representatives(self):
        diagnostics = build_single_factor_diagnostics(
            raw_eval=self._raw_eval(),
            neutral_eval=self._neutral_eval(),
            metadata=self._metadata(),
        )

        self.assertEqual(list(diagnostics["factor"]), ["alpha_a", "alpha_c", "alpha_b"])
        alpha_a = diagnostics.set_index("factor").loc["alpha_a"]
        self.assertEqual(alpha_a["family"], "family_one")
        self.assertEqual(alpha_a["suggested_role"], "core_candidate")
        self.assertTrue(bool(alpha_a["family_representative"]))
        self.assertAlmostEqual(float(alpha_a["neutral_retention_h20"]), 0.8)

        alpha_b = diagnostics.set_index("factor").loc["alpha_b"]
        self.assertEqual(alpha_b["suggested_role"], "shadow_review")
        self.assertIn("negative_neutral_long_short", alpha_b["concerns"])
        self.assertFalse(bool(alpha_b["family_representative"]))

    def test_write_single_factor_diagnostics_markdown_highlights_keep_and_review_lists(self):
        diagnostics = build_single_factor_diagnostics(
            raw_eval=self._raw_eval(),
            neutral_eval=self._neutral_eval(),
            metadata=self._metadata(),
        )
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "diagnostics.md"

            write_single_factor_diagnostics_markdown(diagnostics, output)

            text = output.read_text(encoding="utf-8")
            self.assertIn("# Single Factor Diagnostics", text)
            self.assertIn("alpha_a", text)
            self.assertIn("negative_neutral_long_short", text)
            self.assertIn("Family Representatives", text)

    def test_summarize_single_factor_eval_cli_writes_csv_and_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw_path = root / "reports/raw.csv"
            neutral_path = root / "reports/neutral.csv"
            approved_path = root / "reports/approved_factors.yaml"
            output_csv = root / "reports/diagnostics.csv"
            output_md = root / "reports/diagnostics.md"
            raw_path.parent.mkdir(parents=True)
            self._raw_eval().to_csv(raw_path, index=False)
            self._neutral_eval().to_csv(neutral_path, index=False)
            approved_path.write_text(
                yaml.safe_dump(
                    {
                        "approved_factors": [
                            {"name": "alpha_a", "family": "family_one", "approval_status": "core"},
                            {"name": "alpha_b", "family": "family_one", "approval_status": "challenger"},
                        ]
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/summarize_single_factor_eval.py"),
                    "--raw-eval",
                    str(raw_path.relative_to(root)),
                    "--neutral-eval",
                    str(neutral_path.relative_to(root)),
                    "--approved-factors",
                    str(approved_path.relative_to(root)),
                    "--output-csv",
                    str(output_csv.relative_to(root)),
                    "--output-md",
                    str(output_md.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(output_csv.exists())
            self.assertTrue(output_md.exists())
            diagnostics = pd.read_csv(output_csv)
            self.assertIn("suggested_role", diagnostics.columns)
            self.assertIn("wrote:", result.stdout)

    def _raw_eval(self):
        return pd.DataFrame(
            {
                "factor": ["alpha_a", "alpha_b", "alpha_c"],
                "horizon": [20, 20, 20],
                "rank_ic_mean": [0.05, 0.06, 0.03],
                "rank_icir": [0.4, 0.35, 0.3],
                "long_short_mean_return": [0.008, 0.004, 0.006],
                "top_quantile_turnover": [0.12, 0.2, 0.18],
                "observations": [1000, 1000, 1000],
            }
        )

    def _neutral_eval(self):
        return pd.DataFrame(
            {
                "factor": ["alpha_a", "alpha_b", "alpha_c"],
                "horizon": [20, 20, 20],
                "rank_ic_mean": [0.04, 0.034, 0.029],
                "rank_icir": [0.32, 0.26, 0.28],
                "long_short_mean_return": [0.003, -0.001, 0.007],
                "top_quantile_turnover": [0.13, 0.22, 0.19],
                "observations": [1000, 1000, 1000],
            }
        )

    def _metadata(self):
        return pd.DataFrame(
            {
                "factor": ["alpha_a", "alpha_b", "alpha_c"],
                "family": ["family_one", "family_one", "family_two"],
                "approval_status": ["core", "challenger", "challenger"],
            }
        )


if __name__ == "__main__":
    unittest.main()
