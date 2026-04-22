import importlib.util
import unittest
from pathlib import Path


class AutoresearchCliTests(unittest.TestCase):
    def test_run_expression_loop_script_exposes_main(self):
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts" / "autoresearch" / "run_expression_loop.py"

        spec = importlib.util.spec_from_file_location("run_expression_loop", script)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        self.assertTrue(callable(module.main))


if __name__ == "__main__":
    unittest.main()
