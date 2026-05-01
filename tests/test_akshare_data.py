import tempfile
import subprocess
import sys
import unittest
import importlib.util
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

from qlib_factor_lab.akshare_data import (
    build_dump_bin_command,
    dump_csvs_to_qlib,
    enrich_security_master_industries,
    filter_source_csvs_to_existing_qlib_fields,
    filter_frame_to_universes,
    fetch_company_notices,
    fetch_security_industry_overrides,
    fetch_universe_symbols,
    normalize_akshare_notices,
    normalize_security_master_snapshot,
    normalize_akshare_history,
    normalize_cninfo_industry_override,
    qlib_symbol_from_code,
    read_latest_qlib_calendar_date,
    today_for_daily_data,
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

    def test_today_for_daily_data_uses_current_day_after_a_share_close(self):
        now = datetime(2026, 4, 28, 15, 1, tzinfo=ZoneInfo("Asia/Shanghai"))

        self.assertEqual(today_for_daily_data(now), "2026-04-28")

    def test_today_for_daily_data_uses_previous_day_before_a_share_close(self):
        now = datetime(2026, 4, 28, 14, 59, tzinfo=ZoneInfo("Asia/Shanghai"))

        self.assertEqual(today_for_daily_data(now), "2026-04-27")

    def test_today_for_daily_data_treats_date_input_as_closed_session(self):
        self.assertEqual(today_for_daily_data(date(2026, 4, 28)), "2026-04-28")

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

    def test_build_dump_bin_command_can_use_qlib_dump_update(self):
        command = build_dump_bin_command(
            dump_bin_path=Path("scripts/dump_bin.py"),
            source_dir=Path("data/akshare/incremental"),
            qlib_dir=Path("data/qlib/cn_data_current"),
            python_bin="python",
            max_workers=2,
            mode="dump_update",
        )

        self.assertEqual(command[0:3], ["python", "scripts/dump_bin.py", "dump_update"])

    def test_dump_csvs_to_qlib_update_preserves_existing_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            qlib_dir = root / "qlib"
            qlib_dir.mkdir()
            sentinel = qlib_dir / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")

            with patch("qlib_factor_lab.akshare_data.subprocess.run") as run:
                dump_csvs_to_qlib(
                    root / "source",
                    qlib_dir,
                    root / "dump_bin.py",
                    python_bin="python",
                    max_workers=1,
                    update=True,
                )

            self.assertTrue(sentinel.exists())
            self.assertEqual(run.call_args.args[0][1], str(root / "dump_bin.py"))
            self.assertEqual(run.call_args.args[0][2], "dump_update")

    def test_filter_source_csvs_to_existing_qlib_fields_drops_new_incremental_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            source.mkdir()
            pd.DataFrame(
                [
                    {"date": "2026-04-30", "symbol": "SH600000", "close": 10.0, "pe_ttm": 8.0},
                ]
            ).to_csv(source / "sh600000.csv", index=False)
            (root / "qlib/features/sh600000").mkdir(parents=True)
            (root / "qlib/features/sh600000/close.day.bin").write_bytes(b"existing")

            filtered, dropped = filter_source_csvs_to_existing_qlib_fields(source, root / "qlib")

            result = pd.read_csv(filtered / "sh600000.csv")
            self.assertEqual(["date", "symbol", "close"], result.columns.tolist())
            self.assertEqual({"pe_ttm"}, dropped)

    def test_filter_source_csvs_to_existing_qlib_fields_pads_symbol_calendar_gaps(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            source.mkdir()
            pd.DataFrame(
                [
                    {"date": "2026-04-30", "symbol": "SH600000", "close": 10.0, "pe_ttm": 8.0},
                ]
            ).to_csv(source / "sh600000.csv", index=False)
            (root / "qlib/calendars").mkdir(parents=True)
            (root / "qlib/calendars/day.txt").write_text("2026-04-28\n2026-04-29\n2026-04-30\n", encoding="utf-8")
            (root / "qlib/features/sh600000").mkdir(parents=True)
            # Qlib .bin stores the calendar start index as the first float, followed by values.
            (root / "qlib/features/sh600000/close.day.bin").write_bytes(pd.Series([0.0, 9.0], dtype="float32").values.tobytes())

            filtered, dropped = filter_source_csvs_to_existing_qlib_fields(source, root / "qlib")

            result = pd.read_csv(filtered / "sh600000.csv")
            self.assertEqual(["2026-04-29", "2026-04-30"], result["date"].tolist())
            self.assertTrue(pd.isna(result.loc[0, "close"]))
            self.assertEqual(10.0, result.loc[1, "close"])
            self.assertEqual({"pe_ttm"}, dropped)

    def test_dump_csvs_to_qlib_update_rewinds_instrument_end_to_feature_bin_before_dump(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source"
            source.mkdir()
            pd.DataFrame([{"date": "2026-04-30", "symbol": "SH600000", "close": 10.0}]).to_csv(source / "sh600000.csv", index=False)
            (root / "qlib/calendars").mkdir(parents=True)
            (root / "qlib/calendars/day.txt").write_text("2026-04-28\n2026-04-29\n2026-04-30\n", encoding="utf-8")
            (root / "qlib/instruments").mkdir(parents=True)
            (root / "qlib/instruments/all.txt").write_text("SH600000\t2026-04-28\t2026-04-30\n", encoding="utf-8")
            (root / "qlib/features/sh600000").mkdir(parents=True)
            (root / "qlib/features/sh600000/close.day.bin").write_bytes(pd.Series([0.0, 9.0], dtype="float32").values.tobytes())

            with patch("qlib_factor_lab.akshare_data.subprocess.run"):
                dump_csvs_to_qlib(
                    source,
                    root / "qlib",
                    root / "dump_bin.py",
                    python_bin="python",
                    max_workers=1,
                    update=True,
                    update_existing_fields_only=True,
                )

            self.assertEqual("SH600000\t2026-04-28\t2026-04-28\n", (root / "qlib/instruments/all.txt").read_text(encoding="utf-8"))

    def test_read_latest_qlib_calendar_date_reads_last_calendar_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            calendar = Path(tmp) / "calendars"
            calendar.mkdir()
            calendar.joinpath("day.txt").write_text("2026-04-23\n2026-04-24\n", encoding="utf-8")

            self.assertEqual(read_latest_qlib_calendar_date(Path(tmp)), "2026-04-24")

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

    def test_enrich_security_master_industries_fills_blank_industries(self):
        master = normalize_security_master_snapshot(
            pd.DataFrame({"代码": ["600000", "300750"], "名称": ["浦发银行", "宁德时代"]}),
            as_of_date="2026-04-24",
        )
        industries = pd.DataFrame(
            {
                "证券代码": ["600000", "300750"],
                "行业中类": ["银行", "电池"],
                "行业大类": ["金融", "电力设备"],
            }
        )

        result = enrich_security_master_industries(master, industries)

        by_instrument = result.set_index("instrument")
        self.assertEqual(by_instrument.loc["SH600000", "industry_sw"], "银行")
        self.assertEqual(by_instrument.loc["SZ300750", "industry_csrc"], "电力设备")

    def test_normalize_cninfo_industry_override_prefers_latest_sw_and_official(self):
        raw = pd.DataFrame(
            {
                "新证券简称": ["样本股份", "样本股份", "样本股份"],
                "行业中类": ["旧行业", "软件开发", None],
                "行业大类": ["旧大类", "计算机", "软件和信息技术服务业"],
                "行业门类": ["旧门类", "信息技术", "信息传输、软件和信息技术服务业"],
                "分类标准": ["申银万国行业分类标准", "申银万国行业分类标准", "中国上市公司协会上市公司行业分类标准"],
                "证券代码": ["000001", "000001", "000001"],
                "变更日期": ["2020-01-01", "2024-01-01", "2024-02-01"],
            }
        )

        result = normalize_cninfo_industry_override(raw, "SZ000001", "2026-04-23")

        self.assertEqual(result["证券代码"], "000001")
        self.assertEqual(result["行业中类"], "软件开发")
        self.assertEqual(result["行业大类"], "软件和信息技术服务业")
        self.assertEqual(result["行业门类"], "信息传输、软件和信息技术服务业")
        self.assertEqual(result["更新截止"], "20260423")

    def test_fetch_security_industry_overrides_normalizes_symbols(self):
        class FakeAkshare:
            def stock_industry_change_cninfo(self, symbol, start_date, end_date):
                return pd.DataFrame(
                    {
                        "新证券简称": ["样本股份"],
                        "行业中类": ["软件开发"],
                        "行业大类": ["软件和信息技术服务业"],
                        "行业门类": ["信息传输、软件和信息技术服务业"],
                        "分类标准": ["申银万国行业分类标准"],
                        "证券代码": [symbol],
                        "变更日期": ["2024-01-01"],
                    }
                )

        with patch("qlib_factor_lab.akshare_data._get_akshare", return_value=FakeAkshare()):
            result = fetch_security_industry_overrides(["SZ000001"], "2026-04-23", delay=0)

        self.assertEqual(result["证券代码"].tolist(), ["000001"])
        self.assertEqual(result["行业中类"].tolist(), ["软件开发"])

    def test_normalize_akshare_notices_classifies_event_risk(self):
        raw = pd.DataFrame(
            {
                "代码": ["600000", "000001", "300750", "601106", "002156", "688981"],
                "公告标题": [
                    "关于收到纪律处分决定书的公告",
                    "股东减持计划公告",
                    "年度报告",
                    "关于以集中竞价方式回购股份方案的公告",
                    "控股股东增持股份计划公告",
                    "股东户数变化情况公告",
                ],
                "公告日期": ["2026-04-20", "2026-04-21", "2026-04-22", "2026-04-23", "2026-04-24", "2026-04-25"],
                "公告类型": ["监管", "股东", "定期报告", "回购", "股东", "股东"],
                "网址": ["https://example.test/a", "https://example.test/b", "", "", "", ""],
            }
        )

        result = normalize_akshare_notices(raw)

        self.assertEqual(len(result), 6)
        disciplinary = result[result["instrument"] == "SH600000"].iloc[0]
        self.assertEqual(disciplinary["event_type"], "disciplinary_action")
        self.assertEqual(disciplinary["severity"], "block")
        reduction = result[result["instrument"] == "SZ000001"].iloc[0]
        self.assertEqual(reduction["event_type"], "shareholder_reduction")
        self.assertEqual(reduction["severity"], "risk")
        self.assertEqual(reduction["active_until"], "2026-06-20")
        self.assertEqual(result[result["instrument"] == "SH601106"].iloc[0]["event_type"], "buyback")
        self.assertEqual(result[result["instrument"] == "SZ002156"].iloc[0]["event_type"], "shareholder_increase")
        self.assertEqual(result[result["instrument"] == "SH688981"].iloc[0]["event_type"], "holder_count_change")

    def test_fetch_company_notices_skips_upstream_schema_errors(self):
        class FakeAkshare:
            def stock_notice_report(self, symbol, date):
                raise KeyError("代码")

        with patch("qlib_factor_lab.akshare_data._get_akshare", return_value=FakeAkshare()):
            result = fetch_company_notices("2026-04-20", "2026-04-20", delay=0)

        self.assertTrue(result.empty)
        self.assertIn("instrument", result.columns)

    def test_fetch_company_notices_skips_failed_legacy_fallback_call(self):
        class FakeAkshare:
            def stock_notice_report(self, **kwargs):
                if "symbol" in kwargs:
                    raise TypeError("legacy signature")
                raise ConnectionError("stream interrupted")

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

    def test_build_research_context_prefers_current_provider_universe_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data/qlib/cn_data_csi300_current/instruments").mkdir(parents=True)
            (root / "data/qlib/cn_data_current/instruments").mkdir(parents=True)
            (root / "data/qlib/cn_data_csi300_current/instruments/csi300_current.txt").write_text(
                "SH600000\t2015-01-01\t2026-04-29\n",
                encoding="utf-8",
            )
            (root / "data/qlib/cn_data_current/instruments/csi500_current.txt").write_text(
                "SZ000001\t2015-01-01\t2026-04-29\n",
                encoding="utf-8",
            )
            repo = Path(__file__).resolve().parents[1]
            spec = importlib.util.spec_from_file_location(
                "build_research_context_data_for_test",
                repo / "scripts/build_research_context_data.py",
            )
            self.assertIsNotNone(spec)
            module = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            sys.path.insert(0, str(repo / "scripts"))
            try:
                spec.loader.exec_module(module)
            finally:
                sys.path.pop(0)
            args = SimpleNamespace(universes=["csi300", "csi500"], universe_symbols_csv=None)

            with patch.object(module, "fetch_universe_symbols", side_effect=RuntimeError("network unavailable")):
                result = module._load_universe_symbols(root, args)

            self.assertEqual(result, {"csi300": ["SH600000"], "csi500": ["SZ000001"]})


if __name__ == "__main__":
    unittest.main()
    fetch_company_notices,
