import tempfile
import unittest
import subprocess
import sys
from pathlib import Path

import yaml

from qlib_factor_lab.factor_selection import (
    build_factor_selection,
    load_factor_selection_config,
    write_approved_factors,
    write_factor_review,
)


class FactorSelectionTests(unittest.TestCase):
    def test_build_factor_selection_preserves_approval_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            registry_path, evidence_path, config_path = self._write_fixture(root)

            config = load_factor_selection_config(config_path)
            result = build_factor_selection(config, root=root)

            self.assertEqual([factor.name for factor in result.approved_factors], ["alpha_core", "alpha_challenger"])
            core = result.approved_factors[0]
            self.assertEqual(core.family, "test_family")
            self.assertEqual(core.factor_type, "cross_sectional")
            self.assertEqual(core.primary_horizon, 20)
            self.assertEqual(core.supported_universes, ["csi500_current", "csi300_current"])
            self.assertEqual(core.regime_profile, "down_sideways")
            self.assertEqual(core.approval_status, "core")
            self.assertEqual(core.expression, "$close / Mean($close, 20) - 1")
            self.assertEqual(core.evidence_paths, [str(evidence_path.relative_to(root))])
            self.assertAlmostEqual(core.evidence["csi500_neutral_rank_ic_h20"], 0.04)

    def test_build_factor_selection_groups_redundant_family_members(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, config_path = self._write_fixture(root)

            result = build_factor_selection(load_factor_selection_config(config_path), root=root)

            groups = {factor.name: factor.redundancy_group for factor in result.approved_factors}
            self.assertEqual(groups["alpha_core"], groups["alpha_challenger"])
            self.assertEqual(result.redundancy_rows[0]["representative"], "alpha_core")
            self.assertGreaterEqual(result.redundancy_rows[1]["similarity_to_representative"], 0.5)

    def test_build_factor_selection_rejects_missing_evidence_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, config_path = self._write_fixture(root, missing_evidence=True)

            with self.assertRaisesRegex(ValueError, "missing evidence path"):
                build_factor_selection(load_factor_selection_config(config_path), root=root)

    def test_writers_emit_approved_yaml_and_review_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, config_path = self._write_fixture(root)
            result = build_factor_selection(load_factor_selection_config(config_path), root=root)
            approved_path = root / "reports/approved_factors.yaml"
            review_path = root / "reports/factor_review_2026-04-23.md"

            write_approved_factors(result, approved_path)
            write_factor_review(result, review_path)

            approved = yaml.safe_load(approved_path.read_text(encoding="utf-8"))
            self.assertEqual(approved["approved_factors"][0]["name"], "alpha_core")
            self.assertEqual(approved["approved_factors"][0]["approval_status"], "core")
            review = review_path.read_text(encoding="utf-8")
            self.assertIn("# Factor Review", review)
            self.assertIn("alpha_core", review)
            self.assertIn("alpha_challenger", review)

    def test_select_factors_cli_writes_configured_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, config_path = self._write_fixture(root)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/select_factors.py"),
                    "--config",
                    str(config_path.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((root / "reports/approved_factors.yaml").exists())
            self.assertTrue((root / "reports/factor_review_2026-04-23.md").exists())
            self.assertIn("approved_count: 2", result.stdout)

    def test_project_factor_selection_config_approves_divergence_family(self):
        root = Path(__file__).resolve().parents[1]
        config = load_factor_selection_config(root / "configs/factor_selection.yaml")

        result = build_factor_selection(config, root=root)

        self.assertEqual(len(result.approved_factors), 5)
        statuses = {factor.name: factor.approval_status for factor in result.approved_factors}
        self.assertEqual(statuses["high_mean60_discount_volume_divergence_reversal_20_60_v1"], "core")
        self.assertIn("challenger", set(statuses.values()))
        self.assertTrue(all(factor.regime_profile == "down_sideways" for factor in result.approved_factors))

    def _write_fixture(self, root: Path, missing_evidence: bool = False):
        registry_path = root / "factors/registry.yaml"
        evidence_path = root / "docs/evidence.md"
        config_path = root / "configs/factor_selection.yaml"
        registry_path.parent.mkdir(parents=True)
        evidence_path.parent.mkdir(parents=True)
        config_path.parent.mkdir(parents=True)
        evidence_path.write_text("# evidence\n", encoding="utf-8")
        registry_path.write_text(
            yaml.safe_dump(
                {
                    "factors": [
                        {
                            "name": "alpha_core",
                            "expression": "$close / Mean($close, 20) - 1",
                            "direction": 1,
                            "category": "autoresearch_divergence",
                            "description": "core test factor",
                        },
                        {
                            "name": "alpha_challenger",
                            "expression": "$high / Mean($high, 20) - 1",
                            "direction": 1,
                            "category": "autoresearch_divergence",
                            "description": "similar challenger",
                        },
                    ]
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        evidence_value = "docs/missing.md" if missing_evidence else str(evidence_path.relative_to(root))
        config_path.write_text(
            yaml.safe_dump(
                {
                    "registry_path": str(registry_path.relative_to(root)),
                    "approval_date": "2026-04-23",
                    "output": {
                        "approved_factors": "reports/approved_factors.yaml",
                        "review_markdown": "reports/factor_review_2026-04-23.md",
                    },
                    "redundancy": {"similarity_threshold": 0.35},
                    "approved_factors": [
                        {
                            "name": "alpha_core",
                            "family": "test_family",
                            "type": "cross_sectional",
                            "primary_horizon": 20,
                            "supported_universes": ["csi500_current", "csi300_current"],
                            "regime_profile": "down_sideways",
                            "turnover_profile": "medium",
                            "approval_status": "core",
                            "evidence_paths": [evidence_value],
                            "evidence": {
                                "csi500_neutral_rank_ic_h20": 0.04,
                                "csi300_neutral_rank_ic_h20": 0.05,
                                "weakest_year": 2023,
                                "weakest_year_neutral_rank_ic_h20": -0.03,
                            },
                            "review_notes": "Core test factor.",
                        },
                        {
                            "name": "alpha_challenger",
                            "family": "test_family",
                            "type": "cross_sectional",
                            "primary_horizon": 20,
                            "supported_universes": ["csi500_current"],
                            "regime_profile": "down_sideways",
                            "turnover_profile": "medium",
                            "approval_status": "challenger",
                            "evidence_paths": [evidence_value],
                            "evidence": {
                                "csi500_neutral_rank_ic_h20": 0.03,
                                "csi300_neutral_rank_ic_h20": 0.04,
                                "weakest_year": 2023,
                                "weakest_year_neutral_rank_ic_h20": -0.04,
                            },
                            "review_notes": "Challenger test factor.",
                        },
                    ],
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return registry_path, evidence_path, config_path


if __name__ == "__main__":
    unittest.main()
