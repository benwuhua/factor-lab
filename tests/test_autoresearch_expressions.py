import tempfile
import unittest
from pathlib import Path

import yaml

from qlib_factor_lab.autoresearch.expressions import load_expression_candidate, load_expression_space


class AutoresearchExpressionTests(unittest.TestCase):
    def _write_space(self, directory: str) -> Path:
        path = Path(directory) / "space.yaml"
        path.write_text(
            yaml.safe_dump(
                {
                    "fields": ["close", "volume"],
                    "windows": [5, 20, 60],
                    "operators": ["Ref", "Mean", "Abs"],
                    "families": ["momentum", "reversal"],
                    "complexity": {
                        "max_expression_length": 200,
                            "max_operator_count": 3,
                        "max_window_count": 2,
                    },
                }
            ),
            encoding="utf-8",
        )
        return path

    def _write_candidate(self, directory: str, expression: str) -> Path:
        path = Path(directory) / "candidate.yaml"
        path.write_text(
            yaml.safe_dump(
                {
                    "name": "mom_skip_60_5_v1",
                    "family": "momentum",
                    "expression": expression,
                    "direction": 1,
                    "description": "60 day momentum skipping the most recent 5 sessions.",
                    "expected_behavior": "Higher values should indicate persistent strength.",
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_valid_candidate_converts_to_factor_def(self):
        with tempfile.TemporaryDirectory() as tmp:
            space = load_expression_space(self._write_space(tmp))
            candidate_path = self._write_candidate(tmp, "Ref($close, 5) / Ref($close, 60) - 1")

            candidate = load_expression_candidate(candidate_path, space)
            factor = candidate.to_factor_def()

            self.assertEqual(factor.name, "mom_skip_60_5_v1")
            self.assertEqual(factor.expression, "Ref($close, 5) / Ref($close, 60) - 1")
            self.assertEqual(factor.category, "autoresearch_momentum")
            self.assertEqual(factor.direction, 1)

    def test_candidate_rejects_disallowed_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            space = load_expression_space(self._write_space(tmp))
            candidate_path = self._write_candidate(tmp, "Ref($high, 5) / Ref($close, 60) - 1")

            with self.assertRaisesRegex(ValueError, "disallowed field: high"):
                load_expression_candidate(candidate_path, space)

    def test_candidate_rejects_disallowed_operator(self):
        with tempfile.TemporaryDirectory() as tmp:
            space = load_expression_space(self._write_space(tmp))
            candidate_path = self._write_candidate(tmp, "Std($close, 20)")

            with self.assertRaisesRegex(ValueError, "disallowed operator: Std"):
                load_expression_candidate(candidate_path, space)

    def test_candidate_rejects_disallowed_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            space = load_expression_space(self._write_space(tmp))
            candidate_path = self._write_candidate(tmp, "Ref($close, 120) / Ref($close, 60) - 1")

            with self.assertRaisesRegex(ValueError, "disallowed window: 120"):
                load_expression_candidate(candidate_path, space)

    def test_candidate_rejects_too_many_operators(self):
        with tempfile.TemporaryDirectory() as tmp:
            space = load_expression_space(self._write_space(tmp))
            candidate_path = self._write_candidate(
                tmp,
                "Mean(Abs(Ref($close, 5) / Ref($close, 20) - 1), 60)",
            )

            with self.assertRaisesRegex(ValueError, "operator count exceeds max_operator_count"):
                load_expression_candidate(candidate_path, space)

    def test_candidate_rejects_too_many_unique_windows(self):
        with tempfile.TemporaryDirectory() as tmp:
            space = load_expression_space(self._write_space(tmp))
            candidate_path = self._write_candidate(
                tmp,
                "Ref($close, 5) / Ref($close, 20) + Mean($volume, 60)",
            )

            with self.assertRaisesRegex(ValueError, "window count exceeds max_window_count"):
                load_expression_candidate(candidate_path, space)

    def test_candidate_rejects_disallowed_outer_window_in_nested_expression(self):
        with tempfile.TemporaryDirectory() as tmp:
            space_path = Path(tmp) / "space.yaml"
            space_path.write_text(
                yaml.safe_dump(
                    {
                        "fields": ["close"],
                        "windows": [1, 20, 60],
                        "operators": ["Ref", "Mean", "Abs"],
                        "families": ["volatility"],
                    }
                ),
                encoding="utf-8",
            )
            space = load_expression_space(space_path)
            candidate_path = Path(tmp) / "candidate.yaml"
            candidate_path.write_text(
                yaml.safe_dump(
                    {
                        "name": "nested_vol",
                        "family": "volatility",
                        "expression": "Mean(Abs($close / Ref($close, 1) - 1), 120)",
                        "direction": -1,
                        "description": "Nested volatility expression.",
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "disallowed window: 120"):
                load_expression_candidate(candidate_path, space)


if __name__ == "__main__":
    unittest.main()
