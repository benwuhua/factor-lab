import importlib.util
import unittest
import warnings
from pathlib import Path


class BootstrapTests(unittest.TestCase):
    def test_suppress_runtime_warnings_filters_urllib3_openssl_noise(self):
        repo = Path(__file__).resolve().parents[1]
        spec = importlib.util.spec_from_file_location("script_bootstrap", repo / "scripts/_bootstrap.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("default")
            module.suppress_runtime_warnings()
            from urllib3.exceptions import NotOpenSSLWarning

            warnings.warn("noisy ssl warning", NotOpenSSLWarning)

        self.assertEqual(captured, [])


if __name__ == "__main__":
    unittest.main()
