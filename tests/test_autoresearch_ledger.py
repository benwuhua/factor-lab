import tempfile
import unittest
from pathlib import Path

from qlib_factor_lab.autoresearch.ledger import (
    append_expression_ledger_row,
    render_expression_ledger_status_report,
    summarize_expression_ledger,
)


class AutoresearchLedgerTests(unittest.TestCase):
    def test_append_expression_ledger_row_creates_header_and_appends(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "expression_results.tsv"
            row = {
                "timestamp": "2026-04-22T19:30:00",
                "run_id": "run1",
                "commit": "abc1234",
                "loop": "expression",
                "contract": "csi500_current_v1",
                "candidate_name": "mom_skip_60_5_v1",
                "candidate_file": "candidate.yaml",
                "candidate_hash": "hash1",
                "status": "review",
                "decision_reason": "",
                "primary_metric": 0.0273,
                "secondary_metric": 0.0415,
                "guard_metric": 0.34,
                "rank_ic_mean_h5": 0.0182,
                "rank_ic_mean_h20": 0.0415,
                "neutral_rank_ic_mean_h5": 0.0114,
                "neutral_rank_ic_mean_h20": 0.0273,
                "long_short_mean_return_h20": 0.0038,
                "top_quantile_turnover_h20": 0.34,
                "observations_h20": 981234,
                "complexity_score": 0.18,
                "artifact_dir": "reports/autoresearch/runs/run1",
                "elapsed_sec": 3.2,
            }

            append_expression_ledger_row(ledger, row)
            append_expression_ledger_row(ledger, {**row, "run_id": "run2"})

            lines = ledger.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 3)
            self.assertTrue(lines[0].startswith("timestamp\trun_id\tcommit\tloop"))
            self.assertIn("\trun1\t", lines[1])
            self.assertIn("\trun2\t", lines[2])

    def test_summarize_expression_ledger_groups_statuses_and_top_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "expression_results.tsv"
            base = {
                "timestamp": "2026-04-22T19:30:00",
                "commit": "abc1234",
                "loop": "expression",
                "contract": "csi500_current_v1",
                "candidate_file": "candidate.yaml",
                "candidate_hash": "hash1",
                "decision_reason": "",
                "secondary_metric": 0.01,
                "guard_metric": 0.10,
                "rank_ic_mean_h5": 0.01,
                "rank_ic_mean_h20": 0.02,
                "neutral_rank_ic_mean_h5": 0.01,
                "neutral_rank_ic_mean_h20": 0.02,
                "long_short_mean_return_h20": 0.003,
                "top_quantile_turnover_h20": 0.10,
                "observations_h20": 1000,
                "complexity_score": 0.20,
                "artifact_dir": "reports/autoresearch/runs/run",
                "elapsed_sec": 1.0,
            }
            append_expression_ledger_row(
                ledger,
                {**base, "run_id": "run-review", "candidate_name": "good", "status": "review", "primary_metric": 0.04},
            )
            append_expression_ledger_row(
                ledger,
                {
                    **base,
                    "run_id": "run-discard",
                    "candidate_name": "weak",
                    "status": "discard_candidate",
                    "primary_metric": -0.01,
                    "decision_reason": "primary_metric below discard threshold",
                },
            )
            append_expression_ledger_row(
                ledger,
                {
                    **base,
                    "run_id": "run-crash",
                    "candidate_name": "bad",
                    "status": "crash",
                    "primary_metric": "",
                    "decision_reason": "disallowed operator: Std",
                },
            )

            summary = summarize_expression_ledger(ledger, top_n=2)

            self.assertEqual(summary.status_counts, {"review": 1, "discard_candidate": 1, "crash": 1})
            self.assertEqual([row["candidate_name"] for row in summary.top_review], ["good"])
            self.assertEqual(summary.discard_reasons["primary_metric below discard threshold"], 1)
            self.assertEqual(summary.crash_reasons["disallowed operator: Std"], 1)

    def test_render_expression_ledger_status_report_is_human_readable(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Path(tmp) / "expression_results.tsv"
            append_expression_ledger_row(
                ledger,
                {
                    "timestamp": "2026-04-22T19:30:00",
                    "run_id": "run-review",
                    "commit": "abc1234",
                    "loop": "expression",
                    "contract": "csi500_current_v1",
                    "candidate_name": "good",
                    "candidate_file": "candidate.yaml",
                    "candidate_hash": "hash1",
                    "status": "review",
                    "decision_reason": "",
                    "primary_metric": 0.04,
                    "secondary_metric": 0.02,
                    "guard_metric": 0.10,
                    "rank_ic_mean_h5": 0.01,
                    "rank_ic_mean_h20": 0.02,
                    "neutral_rank_ic_mean_h5": 0.01,
                    "neutral_rank_ic_mean_h20": 0.04,
                    "long_short_mean_return_h20": 0.003,
                    "top_quantile_turnover_h20": 0.10,
                    "observations_h20": 1000,
                    "complexity_score": 0.20,
                    "artifact_dir": "reports/autoresearch/runs/run-review",
                    "elapsed_sec": 1.0,
                },
            )

            report = render_expression_ledger_status_report(summarize_expression_ledger(ledger))

            self.assertIn("# Expression Autoresearch Ledger", report)
            self.assertIn("| review | 1 |", report)
            self.assertIn("| good | 0.040000 | 0.020000 | 0.100000 | run-review |", report)


if __name__ == "__main__":
    unittest.main()
