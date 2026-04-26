import tempfile
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import yaml

from qlib_factor_lab.akshare_data import (
    build_dump_bin_command,
    filter_frame_to_universes,
    fetch_company_notices,
    fetch_universe_symbols,
    normalize_akshare_notices,
    normalize_security_master_snapshot,
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

    def test_fetch_universe_symbols_rejects_non_fixed_universe(self):
        with self.assertRaisesRegex(ValueError, "only supports csi300 and csi500"):
            fetch_universe_symbols("all")

    def test_filter_frame_to_universes_keeps_only_csi300_and_csi500_symbols(self):
        frame = pd.DataFrame(
            {
                "instrument": ["SH600000", "SZ300750", "SH600999"],
                "value": [1, 2, 3],
            }
        )
        universe_symbols = {"csi300": ["SH600000"], "csi500": ["SZ300750"]}

        result = filter_frame_to_universes(frame, universe_symbols)

        self.assertEqual(result["instrument"].tolist(), ["SH600000", "SZ300750"])
        self.assertEqual(result["research_universes"].tolist(), ["csi300", "csi500"])

    def test_normalize_security_master_snapshot_maps_current_a_share_metadata(self):
        raw = pd.DataFrame(
            {
                "代码": ["600000", "300750", "688111"],
                "名称": ["浦发银行", "宁德时代", "*ST科创"],
            }
        )

        result = normalize_security_master_snapshot(raw, as_of_date="2026-04-24")

        self.assertEqual(result["instrument"].tolist(), ["SH600000", "SZ300750", "SH688111"])
        self.assertEqual(result["exchange"].tolist(), ["SSE", "SZSE", "SSE"])
        self.assertEqual(result["board"].tolist(), ["main", "ChiNext", "STAR"])
        self.assertEqual(result["valid_from"].tolist(), ["2026-04-24", "2026-04-24", "2026-04-24"])
        self.assertTrue(bool(result[result["instrument"] == "SH688111"]["is_st"].iloc[0]))

    def test_normalize_akshare_notices_classifies_event_risk(self):
        raw = pd.DataFrame(
            {
                "代码": ["600000", "000001", "300750"],
                "公告标题": ["关于收到纪律处分决定书的公告", "股东减持计划公告", "年度报告"],
                "公告日期": ["2026-04-20", "2026-04-21", "2026-04-22"],
                "公告类型": ["监管", "股东", "定期报告"],
                "网址": ["https://example.test/a", "https://example.test/b", ""],
            }
        )

        result = normalize_akshare_notices(raw)

        self.assertEqual(len(result), 3)
        disciplinary = result[result["instrument"] == "SH600000"].iloc[0]
        self.assertEqual(disciplinary["event_type"], "disciplinary_action")
        self.assertEqual(disciplinary["severity"], "block")
        reduction = result[result["instrument"] == "SZ000001"].iloc[0]
        self.assertEqual(reduction["event_type"], "shareholder_reduction")
        self.assertEqual(reduction["severity"], "risk")
        self.assertEqual(reduction["active_until"], "2026-06-20")

    def test_fetch_company_notices_skips_upstream_schema_errors(self):
        class FakeAkshare:
            def stock_notice_report(self, symbol, date):
                raise KeyError("代码")

        with patch("qlib_factor_lab.akshare_data._get_akshare", return_value=FakeAkshare()):
            result = fetch_company_notices("2026-04-20", "2026-04-20", delay=0)

        self.assertTrue(result.empty)
        self.assertIn("instrument", result.columns)

    def test_build_research_context_data_cli_normalizes_local_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "raw").mkdir()
            raw_master = root / "raw/security.csv"
            raw_notices = root / "raw/notices.csv"
            raw_universes = root / "raw/universes.csv"
            raw_master.write_text("代码,名称\n600000,浦发银行\n", encoding="utf-8")
            raw_notices.write_text(
                "代码,公告标题,公告日期,公告类型,网址\n600000,关于收到监管函的公告,2026-04-20,监管,https://example.test/r\n000001,年度报告,2026-04-20,定期报告,\n",
                encoding="utf-8",
            )
            raw_universes.write_text("universe,instrument\ncsi300,SH600000\ncsi500,SZ300750\n", encoding="utf-8")
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_research_context_data.py"),
                    "--project-root",
                    str(root),
                    "--security-master-source-csv",
                    str(raw_master.relative_to(root)),
                    "--notice-source-csv",
                    str(raw_notices.relative_to(root)),
                    "--security-master-output",
                    "data/security_master.csv",
                    "--company-events-output",
                    "data/company_events.csv",
                    "--universe-symbols-csv",
                    "raw/universes.csv",
                    "--as-of-date",
                    "2026-04-24",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((root / "data/security_master.csv").exists())
            self.assertTrue((root / "data/company_events.csv").exists())
            master = pd.read_csv(root / "data/security_master.csv")
            self.assertEqual(master["instrument"].tolist(), ["SH600000"])
            self.assertEqual(master["research_universes"].tolist(), ["csi300"])
            events = pd.read_csv(root / "data/company_events.csv")
            self.assertEqual(events.loc[0, "event_type"], "regulatory_inquiry")
            self.assertIn("wrote:", result.stdout)


if __name__ == "__main__":
    unittest.main()
    fetch_company_notices,
