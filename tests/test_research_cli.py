import importlib.util
import unittest
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
