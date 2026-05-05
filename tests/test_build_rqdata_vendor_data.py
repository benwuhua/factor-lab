from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


class BuildRQDataVendorDataCliTest(unittest.TestCase):
    def test_script_exposes_cli_flags_without_importing_rqdatac(self) -> None:
        root = Path(__file__).resolve().parents[1]
        script = root / "scripts/build_rqdata_vendor_data.py"

        spec = importlib.util.spec_from_file_location("build_rqdata_vendor_data", script)
        self.assertIsNotNone(spec)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)

        parser = module.build_parser()
        args = parser.parse_args(
            [
                "--instruments",
                "SH600000",
                "SZ000001",
                "--start-date",
                "2026-01-01",
                "--end-date",
                "2026-05-05",
            ]
        )
        self.assertEqual(args.instruments, ["SH600000", "SZ000001"])
        self.assertEqual(args.output, "data/vendor/security_master_history_rqdata.csv")


if __name__ == "__main__":
    unittest.main()
