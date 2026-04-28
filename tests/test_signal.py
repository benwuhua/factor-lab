import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from qlib_factor_lab.config import ProjectConfig
from qlib_factor_lab.signal import (
    build_daily_signal,
    fetch_daily_factor_exposures,
    load_approved_signal_factors,
    load_signal_config,
)


class SignalTests(unittest.TestCase):
    def test_build_daily_signal_blends_zscored_approved_factors(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved_path, config_path = self._write_fixture(root)
            config = load_signal_config(config_path)
            factors = load_approved_signal_factors(approved_path)
            exposures = self._exposures()

            signal = build_daily_signal(exposures, factors, config)

            self.assertEqual(list(signal["instrument"]), ["AAA", "BBB", "CCC"])
            self.assertIn("rule_score", signal.columns)
            self.assertIn("ensemble_score", signal.columns)
            self.assertGreater(signal.loc[signal["instrument"] == "AAA", "rule_score"].iloc[0], 0)
            self.assertLess(signal.loc[signal["instrument"] == "CCC", "rule_score"].iloc[0], 0)
            self.assertEqual(signal.loc[0, "active_regime"], "down")
            self.assertEqual(signal.loc[0, "top_factor_1"], "core_alpha")
            self.assertEqual(signal.loc[1, "risk_flags"], "")
            self.assertEqual(signal.loc[2, "risk_flags"], "not_tradable")
            self.assertIn("limit_up", signal.columns)
            self.assertFalse(bool(signal.loc[0, "limit_up"]))
            self.assertFalse(bool(signal.loc[0, "suspended"]))

    def test_build_daily_signal_applies_execution_calendar_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved_path, config_path = self._write_fixture(root)
            calendar_path = root / "data/execution_calendar.csv"
            calendar_path.parent.mkdir(parents=True)
            pd.DataFrame(
                {
                    "date": ["2026-04-23", "2026-04-23"],
                    "instrument": ["AAA", "BBB"],
                    "suspended": [True, False],
                    "limit_up": [False, True],
                    "buy_blocked": [False, True],
                }
            ).to_csv(calendar_path, index=False)
            config = load_signal_config(config_path)
            config = config.__class__(**{**config.__dict__, "execution_calendar_path": calendar_path})
            factors = load_approved_signal_factors(approved_path)

            signal = build_daily_signal(self._exposures(), factors, config)

            by_instrument = signal.set_index("instrument")
            self.assertTrue(bool(by_instrument.loc["AAA", "suspended"]))
            self.assertTrue(bool(by_instrument.loc["BBB", "limit_up"]))
            self.assertTrue(bool(by_instrument.loc["BBB", "buy_blocked"]))

    def test_build_daily_signal_materializes_execution_calendar_template_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved_path, config_path = self._write_fixture(root)
            calendar_path = root / "data/execution_calendar_20260423.csv"
            calendar_path.parent.mkdir(parents=True)
            pd.DataFrame(
                {"date": ["2026-04-23"], "instrument": ["AAA"], "limit_up": [True]}
            ).to_csv(calendar_path, index=False)
            config = load_signal_config(config_path)
            config = config.__class__(
                **{**config.__dict__, "execution_calendar_path": root / "data/execution_calendar_{run_yyyymmdd}.csv"}
            )
            factors = load_approved_signal_factors(approved_path)

            signal = build_daily_signal(self._exposures(), factors, config)

            self.assertTrue(bool(signal.set_index("instrument").loc["AAA", "limit_up"]))

    def test_build_daily_signal_gates_down_sideways_factors_in_up_regime(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved_path, config_path = self._write_fixture(root, active_regime="up")
            config = load_signal_config(config_path)
            factors = load_approved_signal_factors(approved_path)

            signal = build_daily_signal(self._exposures(), factors, config)

            self.assertTrue((signal["rule_score"] == 0.0).all())
            self.assertTrue((signal["ensemble_score"] == 0.0).all())
            self.assertEqual(signal.loc[0, "risk_flags"], "regime_gated")

    def test_build_daily_signal_can_combine_by_family_and_shadow_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved_path, config_path = self._write_fixture(root)
            config_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            config_data["weights"]["approval_status"]["shadow"] = 0.0
            config_data["combination"] = {"mode": "family_first"}
            config_path.write_text(yaml.safe_dump(config_data, sort_keys=False), encoding="utf-8")
            approved_data = yaml.safe_load(approved_path.read_text(encoding="utf-8"))
            approved_data["approved_factors"].append(
                {
                    "name": "shadow_alpha",
                    "expression": "$amount",
                    "direction": 1,
                    "family": "test_family",
                    "approval_status": "shadow",
                    "regime_profile": "down_sideways",
                }
            )
            approved_path.write_text(yaml.safe_dump(approved_data, sort_keys=False), encoding="utf-8")
            exposures = self._exposures()
            exposures["shadow_alpha"] = [300.0, 200.0, 100.0]

            signal = build_daily_signal(exposures, load_approved_signal_factors(approved_path), load_signal_config(config_path))

            first = signal.loc[signal["instrument"] == "AAA"].iloc[0]
            self.assertAlmostEqual(first["core_alpha_contribution"], 1.0)
            self.assertAlmostEqual(first["challenger_alpha_contribution"], 0.5)
            self.assertAlmostEqual(first["shadow_alpha_contribution"], 0.0)
            self.assertAlmostEqual(first["family_test_family_score"], 0.75)
            self.assertAlmostEqual(first["logic_reversal_repair_score"], 0.75)
            self.assertAlmostEqual(first["rule_score"], 0.75)

    def test_family_first_caps_single_family_score(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved_path, config_path = self._write_fixture(root)
            config_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            config_data["combination"] = {
                "mode": "family_first",
                "family_score_cap": 0.25,
            }
            config_path.write_text(yaml.safe_dump(config_data, sort_keys=False), encoding="utf-8")

            signal = build_daily_signal(
                self._exposures(),
                load_approved_signal_factors(approved_path),
                load_signal_config(config_path),
            )

            first = signal.loc[signal["instrument"] == "AAA"].iloc[0]
            self.assertAlmostEqual(first["family_test_family_score"], 0.25)
            self.assertAlmostEqual(first["rule_score"], 0.25)

    def test_build_daily_signal_cli_writes_csv_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved_path, config_path = self._write_fixture(root)
            exposures_path = root / "exposures.csv"
            self._exposures().to_csv(exposures_path, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_daily_signal.py"),
                    "--config",
                    str(config_path.relative_to(root)),
                    "--project-root",
                    str(root),
                    "--exposures-csv",
                    str(exposures_path.relative_to(root)),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((root / "reports/signals_20260423.csv").exists())
            self.assertTrue((root / "reports/signal_summary_20260423.md").exists())
            output = pd.read_csv(root / "reports/signals_20260423.csv")
            self.assertEqual(output.loc[0, "instrument"], "AAA")
            self.assertIn("wrote:", result.stdout)

    def test_build_daily_signal_cli_resolves_relative_execution_calendar(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved_path, config_path = self._write_fixture(root)
            config_data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            config_data["execution_calendar_path"] = "data/execution_calendar.csv"
            config_path.write_text(yaml.safe_dump(config_data, sort_keys=False), encoding="utf-8")
            exposures_path = root / "exposures.csv"
            self._exposures().to_csv(exposures_path, index=False)
            calendar_path = root / "data/execution_calendar.csv"
            calendar_path.parent.mkdir(parents=True)
            pd.DataFrame(
                {"date": ["2026-04-23"], "instrument": ["AAA"], "suspended": [True]}
            ).to_csv(calendar_path, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_daily_signal.py"),
                    "--config",
                    str(config_path.relative_to(root)),
                    "--project-root",
                    str(root),
                    "--exposures-csv",
                    str(exposures_path.relative_to(root)),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            output = pd.read_csv(root / "reports/signals_20260423.csv")
            self.assertTrue(bool(output.set_index("instrument").loc["AAA", "suspended"]))

    def test_fetch_daily_factor_exposures_can_limit_to_explicit_instruments(self):
        class FakeD:
            requested_instruments = None

            @classmethod
            def features(cls, instruments, fields, start_time, end_time, freq):
                cls.requested_instruments = instruments
                index = pd.MultiIndex.from_product(
                    [["SH688981", "SZ300456"], [pd.Timestamp("2026-04-27")]],
                    names=["instrument", "datetime"],
                )
                return pd.DataFrame([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], index=index)

        fake_qlib_data = types.SimpleNamespace(D=FakeD)
        with tempfile.TemporaryDirectory() as tmp:
            factor = load_approved_signal_factors(self._write_fixture(Path(tmp))[0])[0]
            project_config = ProjectConfig(provider_uri=Path("/tmp/qlib"), market="csi500", end_time="2026-04-27")

            with patch.dict(sys.modules, {"qlib.data": fake_qlib_data}):
                with patch("qlib_factor_lab.signal.init_qlib"), patch(
                    "qlib_factor_lab.signal.resolve_run_date",
                    return_value="2026-04-27",
                ):
                    exposures = fetch_daily_factor_exposures(
                        project_config,
                        [factor],
                        "latest",
                        instruments=["SZ300456", "SH688981", "SH688981"],
                    )

        self.assertEqual(FakeD.requested_instruments, ["SH688981", "SZ300456"])
        self.assertEqual(set(exposures["instrument"]), {"SH688981", "SZ300456"})

    def _write_fixture(self, root: Path, active_regime: str = "down"):
        approved_path = root / "reports/approved_factors.yaml"
        config_path = root / "configs/signal.yaml"
        approved_path.parent.mkdir(parents=True)
        config_path.parent.mkdir(parents=True)
        approved_path.write_text(
            yaml.safe_dump(
                {
                    "approved_factors": [
                        {
                            "name": "core_alpha",
                            "expression": "$close",
                            "direction": 1,
                            "family": "test_family",
                            "logic_bucket": "reversal_repair",
                            "approval_status": "core",
                            "regime_profile": "down_sideways",
                        },
                        {
                            "name": "challenger_alpha",
                            "expression": "$volume",
                            "direction": 1,
                            "family": "test_family",
                            "logic_bucket": "reversal_repair",
                            "approval_status": "challenger",
                            "regime_profile": "down_sideways",
                        },
                    ]
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        config_path.write_text(
            yaml.safe_dump(
                {
                    "approved_factors_path": str(approved_path.relative_to(root)),
                    "provider_config": "configs/provider_current.yaml",
                    "run_date": "2026-04-23",
                    "active_regime": active_regime,
                    "weights": {
                        "approval_status": {"core": 1.0, "challenger": 0.5, "reserve": 0.25},
                        "regime": {"down_sideways": {"down": 1.0, "sideways": 1.0, "up": 0.0}},
                        "ensemble": {"rule_score": 1.0, "model_score": 0.0},
                    },
                    "output": {
                        "signals": "reports/signals_20260423.csv",
                        "summary": "reports/signal_summary_20260423.md",
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return approved_path, config_path

    def _exposures(self):
        return pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "BBB", "CCC"],
                "tradable": [True, True, False],
                "core_alpha": [3.0, 2.0, 1.0],
                "challenger_alpha": [30.0, 20.0, 10.0],
            }
        )


if __name__ == "__main__":
    unittest.main()
