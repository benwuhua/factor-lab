import tempfile
import unittest
from pathlib import Path

from qlib_factor_lab.autoresearch.ledger import append_expression_ledger_row


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


if __name__ == "__main__":
    unittest.main()
