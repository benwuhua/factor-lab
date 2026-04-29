import tempfile
import unittest
import subprocess
import sys
from pathlib import Path

import yaml

from qlib_factor_lab.combo_spec import load_combo_spec
from qlib_factor_lab.strategy_dictionary import (
    build_expression_candidate_from_strategy,
    filter_strategy_entries,
    load_strategy_dictionary,
    propose_strategy_ideas,
    render_strategy_proposals_markdown,
)


class StrategyDictionaryTests(unittest.TestCase):
    def test_load_strategy_dictionary_reads_entries_and_filters_by_lane(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dictionary.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "source": "151 Trading Strategies",
                        "strategies": [
                            {
                                "strategy_id": "stock_price_momentum",
                                "strategy_name": "Price momentum",
                                "strategy_family": "momentum",
                                "candidate_lane": "expression",
                                "template_formula": "Return(120) skip 20",
                                "a_share_transferability": "high",
                            },
                            {
                                "strategy_id": "sector_momentum_rotation",
                                "strategy_name": "Sector momentum rotation",
                                "strategy_family": "sector_theme",
                                "candidate_lane": "theme",
                                "template_formula": "industry relative strength",
                                "a_share_transferability": "high",
                            },
                        ],
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            entries = load_strategy_dictionary(path)
            expression_entries = filter_strategy_entries(entries, candidate_lane="expression")

            self.assertEqual(["stock_price_momentum", "sector_momentum_rotation"], [entry.strategy_id for entry in entries])
            self.assertEqual(["stock_price_momentum"], [entry.strategy_id for entry in expression_entries])

    def test_load_strategy_dictionary_rejects_duplicate_strategy_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "dictionary.yaml"
            path.write_text(
                yaml.safe_dump(
                    {
                        "strategies": [
                            {"strategy_id": "duplicate", "strategy_name": "A", "strategy_family": "momentum"},
                            {"strategy_id": "duplicate", "strategy_name": "B", "strategy_family": "value"},
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "duplicate strategy_id"):
                load_strategy_dictionary(path)

    def test_propose_strategy_ideas_prefers_missing_families_from_combo(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dictionary_path = root / "dictionary.yaml"
            combo_path = root / "combo.yaml"
            dictionary_path.write_text(
                yaml.safe_dump(
                    {
                        "strategies": [
                            {
                                "strategy_id": "stock_value",
                                "strategy_name": "Value",
                                "strategy_family": "value",
                                "candidate_lane": "fundamental",
                                "template_formula": "EP + CFP",
                                "a_share_transferability": "high",
                            },
                            {
                                "strategy_id": "stock_low_volatility",
                                "strategy_name": "Low volatility",
                                "strategy_family": "low_vol",
                                "candidate_lane": "expression",
                                "template_formula": "-Std(Return, 120)",
                                "a_share_transferability": "high",
                            },
                        ]
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            combo_path.write_text(
                yaml.safe_dump(
                    {
                        "name": "combo",
                        "members": [
                            {"name": "value_ep_cfp", "source": "fundamental_quality", "family": "value", "weight": 0.2}
                        ],
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            proposals = propose_strategy_ideas(
                load_strategy_dictionary(dictionary_path),
                combo_spec=load_combo_spec(combo_path),
                limit=5,
            )
            markdown = render_strategy_proposals_markdown(proposals)

            self.assertEqual(["stock_low_volatility", "stock_value"], [proposal.strategy_id for proposal in proposals])
            self.assertIn("missing family", proposals[0].reason)
            self.assertIn("stock_low_volatility", markdown)
            self.assertIn("-Std(Return, 120)", markdown)

    def test_strategy_dictionary_cli_writes_markdown_and_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dictionary_path = root / "dictionary.yaml"
            combo_path = root / "combo.yaml"
            output_md = root / "proposals.md"
            output_csv = root / "proposals.csv"
            dictionary_path.write_text(
                yaml.safe_dump(
                    {
                        "strategies": [
                            {
                                "strategy_id": "stock_low_volatility",
                                "strategy_name": "Low volatility",
                                "strategy_family": "low_vol",
                                "candidate_lane": "expression",
                                "template_formula": "-Std(Return, 120)",
                                "a_share_transferability": "high",
                            }
                        ]
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            combo_path.write_text(
                yaml.safe_dump({"name": "combo", "members": [{"name": "value", "source": "approved_factor", "family": "value"}]}),
                encoding="utf-8",
            )
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/autoresearch/propose_from_strategy_dictionary.py"),
                    "--dictionary",
                    str(dictionary_path),
                    "--combo-spec",
                    str(combo_path),
                    "--output-md",
                    str(output_md),
                    "--output-csv",
                    str(output_csv),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("stock_low_volatility", output_md.read_text(encoding="utf-8"))
            self.assertIn("stock_low_volatility", output_csv.read_text(encoding="utf-8"))

    def test_strategy_dictionary_cli_writes_expression_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_dir = root / "candidates"
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/autoresearch/propose_from_strategy_dictionary.py"),
                    "--dictionary",
                    str(repo / "configs/strategy_dictionary/151_trading_strategies_equity.yaml"),
                    "--combo-spec",
                    str(repo / "configs/combo_specs/balanced_multifactor_v1.yaml"),
                    "--lane",
                    "expression",
                    "--limit",
                    "2",
                    "--write-expression-candidates",
                    "--candidate-output-dir",
                    str(output_dir),
                    "--output-md",
                    str(root / "proposals.md"),
                    "--output-csv",
                    str(root / "proposals.csv"),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((output_dir / "low_vol_120_v1.yaml").exists())
            self.assertTrue((output_dir / "momentum_120_skip_20_v1.yaml").exists())

    def test_build_expression_candidate_from_strategy_uses_mapped_formula(self):
        entry = load_strategy_dictionary(
            Path(__file__).resolve().parents[1] / "configs/strategy_dictionary/151_trading_strategies_equity.yaml"
        )
        by_id = {item.strategy_id: item for item in entry}

        low_vol = build_expression_candidate_from_strategy(by_id["stock_low_volatility"])
        momentum = build_expression_candidate_from_strategy(by_id["stock_price_momentum"])

        self.assertEqual(low_vol["name"], "low_vol_120_v1")
        self.assertEqual(low_vol["family"], "volatility")
        self.assertEqual(low_vol["expression"], "Std($close / Ref($close, 1) - 1, 120)")
        self.assertEqual(low_vol["direction"], -1)
        self.assertEqual(momentum["name"], "momentum_120_skip_20_v1")
        self.assertEqual(momentum["expression"], "Ref($close, 20) / Ref($close, 120) - 1")


if __name__ == "__main__":
    unittest.main()
