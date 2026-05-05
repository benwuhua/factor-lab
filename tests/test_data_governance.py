import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.data_governance import (
    build_data_governance_report,
    load_data_governance_config,
    write_data_governance_report,
)


class DataGovernanceTests(unittest.TestCase):
    def test_report_scores_coverage_pit_completeness_and_activation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame({"instrument": ["AAA", "BBB", "CCC"]}).to_csv(root / "data/universe.csv", index=False)
            pd.DataFrame(
                {
                    "instrument": ["AAA", "BBB"],
                    "valid_from": ["2026-01-01", "2026-01-01"],
                    "valid_to": ["2026-12-31", "2026-12-31"],
                }
            ).to_csv(root / "data/security_master.csv", index=False)
            config_path = root / "configs/data_governance.yaml"
            config_path.parent.mkdir()
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "data_governance": {
                            "expected_universe_path": "data/universe.csv",
                            "domains": {
                                "security_master": {
                                    "path": "data/security_master.csv",
                                    "required_fields": ["instrument"],
                                    "pit_fields": ["valid_from", "valid_to"],
                                    "min_coverage_ratio": 0.5,
                                    "activation_lane": "security_master",
                                },
                                "shareholder_capital": {
                                    "path": "data/shareholder.csv",
                                    "required_fields": ["instrument"],
                                    "pit_fields": ["announce_date"],
                                    "min_coverage_ratio": 0.7,
                                    "activation_lane": "shareholder_capital",
                                },
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            config = load_data_governance_config(config_path)
            report = build_data_governance_report(config, project_root=root)

            frame = report.to_frame().set_index("domain")
            self.assertEqual(frame.loc["security_master", "status"], "pass")
            self.assertEqual(frame.loc["security_master", "activation_status"], "active")
            self.assertEqual(frame.loc["security_master", "activation_lane"], "security_master")
            self.assertAlmostEqual(frame.loc["security_master", "coverage_ratio"], 2 / 3)
            self.assertEqual(frame.loc["security_master", "pit_field_completeness"], 1.0)
            self.assertEqual(frame.loc["shareholder_capital", "status"], "missing")
            self.assertEqual(frame.loc["shareholder_capital", "activation_status"], "shadow")
            self.assertTrue(report.passed)

            output = write_data_governance_report(report, root / "reports/data_governance.md")
            self.assertIn("security_master", output.read_text(encoding="utf-8"))

    def test_missing_configured_freshness_field_fails_and_stays_shadow(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame({"instrument": ["AAA"]}).to_csv(root / "data/universe.csv", index=False)
            pd.DataFrame({"instrument": ["AAA"], "valid_from": ["2026-01-01"]}).to_csv(
                root / "data/security_master.csv",
                index=False,
            )
            config_path = root / "configs/data_governance.yaml"
            config_path.parent.mkdir()
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "data_governance": {
                            "expected_universe_path": "data/universe.csv",
                            "domains": {
                                "security_master": {
                                    "path": "data/security_master.csv",
                                    "required_fields": ["instrument"],
                                    "pit_fields": ["valid_from"],
                                    "freshness_date_column": "available_at",
                                    "max_age_days": 1,
                                },
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = build_data_governance_report(
                load_data_governance_config(config_path),
                project_root=root,
                as_of_date="2026-04-20",
            )

            row = report.to_frame().iloc[0]
            self.assertEqual(row["freshness_status"], "missing")
            self.assertEqual(row["status"], "fail")
            self.assertEqual(row["activation_status"], "shadow")
            self.assertTrue(report.passed)

    def test_blank_pit_fields_are_incomplete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame({"instrument": ["AAA"]}).to_csv(root / "data/universe.csv", index=False)
            pd.DataFrame(
                {
                    "instrument": ["AAA"],
                    "report_period": ["2026-03-31"],
                    "announce_date": [""],
                    "available_at": ["2026-04-20"],
                }
            ).to_csv(root / "data/fundamental_quality.csv", index=False)
            config_path = root / "configs/data_governance.yaml"
            config_path.parent.mkdir()
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "data_governance": {
                            "expected_universe_path": "data/universe.csv",
                            "domains": {
                                "fundamental_quality": {
                                    "path": "data/fundamental_quality.csv",
                                    "required_fields": ["instrument", "report_period", "announce_date", "available_at"],
                                    "pit_fields": ["report_period", "announce_date", "available_at"],
                                    "min_coverage_ratio": 0.7,
                                }
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = build_data_governance_report(load_data_governance_config(config_path), project_root=root)

            row = report.to_frame().iloc[0]
            self.assertEqual(row["status"], "fail")
            self.assertEqual(row["activation_status"], "shadow")
            self.assertLess(row["pit_field_completeness"], 1.0)
            self.assertIn("pit_incomplete", row["detail"])

    def test_blocking_activation_failure_marks_report_failed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame({"instrument": ["AAA"]}).to_csv(root / "data/universe.csv", index=False)
            config_path = root / "configs/data_governance.yaml"
            config_path.parent.mkdir()
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "data_governance": {
                            "expected_universe_path": "data/universe.csv",
                            "domains": {
                                "security_master": {
                                    "path": "data/missing.csv",
                                    "required_fields": ["instrument"],
                                    "activation_if_missing": "block",
                                },
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = build_data_governance_report(load_data_governance_config(config_path), project_root=root)

            self.assertFalse(report.passed)

    def test_trusted_source_ratio_can_shadow_current_snapshot_pit_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame({"instrument": ["AAA", "BBB"]}).to_csv(root / "data/universe.csv", index=False)
            pd.DataFrame(
                {
                    "instrument": ["AAA", "BBB"],
                    "valid_from": ["2020-01-01", "2021-01-01"],
                    "as_of_date": ["2026-04-30", "2026-04-30"],
                    "source": ["vendor_pit", "current_snapshot_backfilled"],
                }
            ).to_csv(root / "data/security_master_history.csv", index=False)
            config_path = root / "configs/data_governance.yaml"
            config_path.parent.mkdir()
            config_path.write_text(
                yaml.safe_dump(
                    {
                        "data_governance": {
                            "expected_universe_path": "data/universe.csv",
                            "domains": {
                                "security_master_history": {
                                    "path": "data/security_master_history.csv",
                                    "required_fields": ["instrument", "valid_from", "as_of_date", "source"],
                                    "pit_fields": ["valid_from", "as_of_date"],
                                    "trusted_source_field": "source",
                                    "trusted_sources": ["vendor_pit", "official_exchange_pit"],
                                    "min_trusted_source_ratio": 0.8,
                                    "activation_if_failed": "shadow",
                                }
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            report = build_data_governance_report(load_data_governance_config(config_path), project_root=root)

            row = report.to_frame().iloc[0]
            self.assertEqual(row["status"], "fail")
            self.assertEqual(row["activation_status"], "shadow")
            self.assertAlmostEqual(row["trusted_source_ratio"], 0.5)
            self.assertIn("trusted_source_below_0.8", row["detail"])
            self.assertTrue(report.passed)


if __name__ == "__main__":
    unittest.main()
