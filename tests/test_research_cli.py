import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from qlib_factor_lab.signal import SignalConfig


class ResearchCliTests(unittest.TestCase):
    def test_check_data_governance_script_exposes_main(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts" / "check_data_governance.py"

        spec = importlib.util.spec_from_file_location("check_data_governance", script)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        self.assertTrue(callable(module.main))

    def test_build_stock_cards_script_exposes_main(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts" / "build_stock_cards.py"

        spec = importlib.util.spec_from_file_location("build_stock_cards", script)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        self.assertTrue(callable(module.main))

    def test_run_theme_scanner_script_exposes_main(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts" / "run_theme_scanner.py"

        spec = importlib.util.spec_from_file_location("run_theme_scanner", script)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        self.assertTrue(callable(module.main))

    def test_theme_scanner_provider_fill_keeps_original_signal_when_provider_has_no_factor_coverage(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts" / "run_theme_scanner.py"
        spec = importlib.util.spec_from_file_location("run_theme_scanner", script)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        universe = type("Universe", (), {"members": pd.DataFrame([{"instrument": "AAA"}, {"instrument": "BBB"}])})()
        signal = pd.DataFrame([{"date": "2026-04-27", "instrument": "AAA", "ensemble_score": 1.0}])

        with patch.object(module, "load_signal_config") as load_config, patch.object(
            module,
            "load_approved_signal_factors",
            return_value=[object()],
        ), patch.object(module, "load_project_config") as load_project, patch.object(
            module,
            "fetch_daily_factor_exposures",
            side_effect=ValueError("no factor exposures were available for 2026-04-27"),
        ):
            load_config.return_value = SignalConfig(
                approved_factors_path=Path("reports/approved_factors.yaml"),
                provider_config=Path("configs/provider_current.yaml"),
                run_date="latest",
                active_regime="sideways",
                status_weights={},
                regime_weights={},
                rule_weight=1.0,
                model_weight=0.0,
                signals_output_path=Path("reports/signals.csv"),
                summary_output_path=Path("reports/signal_summary.md"),
            )
            load_project.return_value = object()

            filled = module._fill_missing_signal_from_provider(
                root=root,
                signal=signal,
                universe=universe,
                run_date="2026-04-27",
                signal_config_path="configs/signal.yaml",
                provider_config_path=None,
            )

        self.assertEqual(filled.to_dict(orient="records"), signal.to_dict(orient="records"))


if __name__ == "__main__":
    unittest.main()
