import unittest

import pandas as pd

from qlib_factor_lab.autoresearch.oracle import (
    build_expression_summary_payload,
    compute_complexity_score,
    render_summary_block,
)


class AutoresearchOracleTests(unittest.TestCase):
    def _eval_frame(self, neutralization: str, h5_rank_ic: float, h20_rank_ic: float) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "factor": "mom_skip_60_5_v1",
                    "category": "autoresearch_momentum",
                    "direction": 1,
                    "horizon": 5,
                    "neutralization": neutralization,
                    "rank_ic_mean": h5_rank_ic,
                    "ic_mean": h5_rank_ic / 2,
                    "long_short_mean_return": 0.001,
                    "top_quantile_turnover": 0.25,
                    "observations": 1000,
                },
                {
                    "factor": "mom_skip_60_5_v1",
                    "category": "autoresearch_momentum",
                    "direction": 1,
                    "horizon": 20,
                    "neutralization": neutralization,
                    "rank_ic_mean": h20_rank_ic,
                    "ic_mean": h20_rank_ic / 2,
                    "long_short_mean_return": 0.0038,
                    "top_quantile_turnover": 0.34,
                    "observations": 981234,
                },
            ]
        )

    def test_build_expression_summary_payload_prefers_neutral_h20_primary_metric(self):
        raw = self._eval_frame("none", 0.0182, 0.0415)
        neutral = self._eval_frame("size_proxy", 0.0114, 0.0273)

        payload = build_expression_summary_payload(
            run_id="run1",
            candidate_name="mom_skip_60_5_v1",
            commit="abc1234",
            contract_name="csi500_current_v1",
            universe="csi500_current",
            horizons=(5, 20),
            raw_eval=raw,
            neutralized_eval=neutral,
            complexity_score=0.18,
            artifact_dir="reports/autoresearch/runs/run1",
        )

        self.assertEqual(payload["primary_metric"], 0.0273)
        self.assertEqual(payload["secondary_metric"], 0.0415)
        self.assertEqual(payload["guard_metric"], 0.34)
        self.assertEqual(payload["neutral_rank_ic_mean_h20"], 0.0273)
        self.assertEqual(payload["long_short_mean_return_h20"], 0.0038)
        self.assertEqual(payload["observations_h20"], 981234)

    def test_render_summary_block_wraps_payload_in_yaml_like_fences(self):
        block = render_summary_block(
            {
                "loop": "expression",
                "run_id": "run1",
                "candidate": "mom_skip_60_5_v1",
                "rank_ic_mean_h20": 0.0415,
                "status": "review",
            }
        )

        self.assertTrue(block.startswith("---\nloop: expression"))
        self.assertIn("candidate: mom_skip_60_5_v1", block)
        self.assertTrue(block.endswith("\n---\n"))

    def test_compute_complexity_score_increases_with_expression_length_and_operators(self):
        simple = compute_complexity_score("Ref($close, 5) / Ref($close, 60) - 1")
        complex_score = compute_complexity_score("Mean(Abs($close / Ref($close, 1) - 1), 20) + Std($volume, 60)")

        self.assertGreater(complex_score, simple)
        self.assertGreaterEqual(simple, 0.0)
        self.assertLessEqual(complex_score, 1.0)


if __name__ == "__main__":
    unittest.main()
