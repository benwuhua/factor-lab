import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.workbench import (
    build_portfolio_gate_explanation,
    load_autoresearch_queue,
    load_workbench_snapshot,
    summarize_autoresearch_queue,
)


class WorkbenchTests(unittest.TestCase):
    def test_load_autoresearch_queue_sorts_latest_first_and_summarizes_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = root / "reports/autoresearch/expression_results.tsv"
            ledger.parent.mkdir(parents=True)
            pd.DataFrame(
                [
                    {
                        "timestamp": "2026-04-22T21:00:00",
                        "candidate_name": "old",
                        "status": "discard_candidate",
                        "primary_metric": 0.001,
                        "neutral_rank_ic_mean_h20": 0.001,
                        "complexity_score": 0.2,
                        "decision_reason": "weak",
                        "artifact_dir": "reports/autoresearch/runs/old",
                    },
                    {
                        "timestamp": "2026-04-22T22:00:00",
                        "candidate_name": "new",
                        "status": "review",
                        "primary_metric": 0.04,
                        "neutral_rank_ic_mean_h20": 0.04,
                        "complexity_score": 0.3,
                        "decision_reason": "",
                        "artifact_dir": "reports/autoresearch/runs/new",
                    },
                ]
            ).to_csv(ledger, sep="\t", index=False)

            queue = load_autoresearch_queue(root)
            summary = summarize_autoresearch_queue(queue)

        self.assertEqual(list(queue["candidate_name"]), ["new", "old"])
        self.assertEqual(summary["review"], 1)
        self.assertEqual(summary["discard_candidate"], 1)
        self.assertEqual(summary["crash"], 0)

    def test_portfolio_gate_explanation_marks_exposure_failures_as_caution(self):
        portfolio = pd.DataFrame(
            {
                "instrument": ["AAA", "BBB"],
                "target_weight": [0.4, 0.4],
                "industry": ["tech", "tech"],
                "top_factor_1": ["mom_20", "mom_20"],
                "top_factor_1_contribution": [2.0, 1.0],
            }
        )

        result = build_portfolio_gate_explanation(
            portfolio,
            risk_config={
                "max_single_weight": 0.5,
                "min_positions": 2,
                "min_signal_coverage": 0.2,
                "max_industry_weight": 0.6,
                "min_factor_family_count": 2,
                "max_factor_family_concentration": 0.7,
            },
            factor_family_map={"mom_20": "momentum"},
        )

        self.assertEqual(result.decision, "caution")
        failed = set(result.checks.query("status == 'fail'")["check"])
        self.assertIn("max_industry_weight", failed)
        self.assertIn("min_factor_family_count", failed)
        self.assertIn("max_factor_family_concentration", failed)
        self.assertEqual(list(result.industry["industry"]), ["tech"])
        self.assertEqual(list(result.family["family"]), ["momentum"])

    def test_workbench_snapshot_counts_approved_factors_and_latest_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "reports").mkdir()
            (root / "reports/approved_factors.yaml").write_text(
                yaml.safe_dump({"approved_factors": [{"name": "a"}, {"name": "b"}]}),
                encoding="utf-8",
            )
            pd.DataFrame({"instrument": ["AAA"], "target_weight": [0.1]}).to_csv(
                root / "reports/target_portfolio_20260423.csv",
                index=False,
            )
            pd.DataFrame({"instrument": ["BBB"], "target_weight": [0.1]}).to_csv(
                root / "reports/target_portfolio_20260424.csv",
                index=False,
            )

            snapshot = load_workbench_snapshot(root)

        self.assertEqual(snapshot.approved_factor_count, 2)
        self.assertEqual(snapshot.latest_target_portfolio.name, "target_portfolio_20260424.csv")


if __name__ == "__main__":
    unittest.main()
