import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.akshare_data import (
    build_dump_bin_command,
    normalize_akshare_history,
    qlib_symbol_from_code,
    write_instrument_alias,
    write_provider_config,
)


class AkShareDataTests(unittest.TestCase):
    def test_qlib_symbol_from_code_adds_exchange_prefix(self):
        self.assertEqual(qlib_symbol_from_code("600000"), "SH600000")
        self.assertEqual(qlib_symbol_from_code("000001"), "SZ000001")
        self.assertEqual(qlib_symbol_from_code("1"), "SZ000001")
        self.assertEqual(qlib_symbol_from_code("300750"), "SZ300750")
        self.assertEqual(qlib_symbol_from_code("688111"), "SH688111")

    def test_normalize_akshare_history_maps_columns_and_computes_vwap(self):
        raw = pd.DataFrame(
            {
                "日期": ["2024-01-02", "2024-01-03"],
                "开盘": [10.0, 11.0],
                "收盘": [11.0, 12.0],
                "最高": [11.5, 12.5],
                "最低": [9.5, 10.5],
                "成交量": [1000.0, 2000.0],
                "成交额": [1050000.0, 2300000.0],
                "涨跌幅": [1.0, 2.0],
            }
        )

        result = normalize_akshare_history(raw, "600000")

        self.assertEqual(result["symbol"].tolist(), ["SH600000", "SH600000"])
        self.assertEqual(result["date"].tolist(), ["2024-01-02", "2024-01-03"])
        self.assertIn("vwap", result.columns)
        self.assertAlmostEqual(result.loc[0, "vwap"], 10.5)
        self.assertAlmostEqual(result.loc[0, "volume"], 100000.0)
        self.assertEqual(result["factor"].tolist(), [1.0, 1.0])

    def test_normalize_akshare_history_keeps_english_volume_in_shares(self):
        raw = pd.DataFrame(
            {
                "date": ["2024-01-02"],
                "open": [10.0],
                "close": [11.0],
                "high": [11.5],
                "low": [9.5],
                "volume": [100000.0],
                "amount": [1050000.0],
            }
        )

        result = normalize_akshare_history(raw, "600000")

        self.assertAlmostEqual(result.loc[0, "volume"], 100000.0)
        self.assertAlmostEqual(result.loc[0, "vwap"], 10.5)

    def test_build_dump_bin_command_uses_qlib_dump_all(self):
        command = build_dump_bin_command(
            dump_bin_path=Path("scripts/dump_bin.py"),
            source_dir=Path("data/akshare/normalized"),
            qlib_dir=Path("data/qlib/cn_data_current"),
            python_bin="python",
            max_workers=2,
        )

        self.assertEqual(command[0:3], ["python", "scripts/dump_bin.py", "dump_all"])
        self.assertIn("--data_path", command)
        self.assertIn("--qlib_dir", command)
        self.assertIn("--exclude_fields", command)
        self.assertIn("date,symbol", command)

    def test_write_provider_config_points_to_current_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "provider_current.yaml"
            qlib_dir = Path(tmp) / "data/qlib/cn_data_current"

            write_provider_config(path, qlib_dir, market="csi500_current", benchmark="SH000905", end_time="2026-04-20")

            data = yaml.safe_load(path.read_text(encoding="utf-8"))
            self.assertEqual(data["provider_uri"], str(qlib_dir))
            self.assertEqual(data["market"], "csi500_current")
            self.assertEqual(data["benchmark"], "SH000905")
            self.assertEqual(data["end_time"], "2026-04-20")

    def test_write_instrument_alias_copies_all_universe(self):
        with tempfile.TemporaryDirectory() as tmp:
            qlib_dir = Path(tmp)
            instruments = qlib_dir / "instruments"
            instruments.mkdir()
            instruments.joinpath("all.txt").write_text("SH600000\t2015-01-01\t2026-04-20\n", encoding="utf-8")

            target = write_instrument_alias(qlib_dir, "csi500_current")

            self.assertEqual(target.name, "csi500_current.txt")
            self.assertEqual(target.read_text(encoding="utf-8"), "SH600000\t2015-01-01\t2026-04-20\n")


if __name__ == "__main__":
    unittest.main()
