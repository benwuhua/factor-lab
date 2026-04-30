from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from qlib_factor_lab.research_data_domains import (
    build_announcement_evidence_index,
    build_shareholder_capital_from_events,
    derive_fundamental_quality_fields,
    derive_fundamental_valuation_fields,
    normalize_fundamental_quality,
    normalize_cninfo_dividend,
    read_close_prices_from_source_dirs,
    write_research_data_domains,
)


class ResearchDataDomainsTest(unittest.TestCase):
    def test_normalize_fundamental_quality_keeps_point_in_time_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "证券代码": "600000",
                    "报告期": "2026-03-31",
                    "公告日期": "2026-04-20",
                    "净资产收益率": "8.5",
                    "销售毛利率": "31.2",
                    "资产负债率": "42.1",
                    "营业收入同比增长率": "12.3",
                    "净利润同比增长率": "-5.5",
                    "盈利收益率": "4.8",
                    "经营现金流市值比": "3.2",
                    "股息率": "2.1",
                    "摊薄每股收益(元)": "1.2",
                    "每股经营性现金流(元)": "0.8",
                }
            ]
        )

        result = normalize_fundamental_quality(raw, as_of_date="2026-04-27")

        self.assertEqual(["SH600000"], result["instrument"].tolist())
        self.assertEqual(["2026-03-31"], result["report_period"].tolist())
        self.assertEqual(["2026-04-20"], result["announce_date"].tolist())
        self.assertEqual(["2026-04-20"], result["available_at"].tolist())
        self.assertAlmostEqual(8.5, float(result.loc[0, "roe"]))
        self.assertAlmostEqual(31.2, float(result.loc[0, "gross_margin"]))
        self.assertAlmostEqual(4.8, float(result.loc[0, "ep"]))
        self.assertAlmostEqual(3.2, float(result.loc[0, "cfp"]))
        self.assertAlmostEqual(2.1, float(result.loc[0, "dividend_yield"]))
        self.assertAlmostEqual(1.2, float(result.loc[0, "eps"]))
        self.assertAlmostEqual(0.8, float(result.loc[0, "operating_cashflow_per_share"]))
        self.assertEqual("akshare_financial_indicator", result.loc[0, "source"])

    def test_normalize_fundamental_quality_keeps_p1_quality_fields_when_available(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "证券代码": "600000",
                    "报告期": "2026-03-31",
                    "公告日期": "2026-04-20",
                    "投入资本回报率": "9.1",
                    "应计比率": "-2.3",
                    "经营现金流同比增长率": "18.2",
                }
            ]
        )

        result = normalize_fundamental_quality(raw, as_of_date="2026-04-27")

        for column in [
            "roic",
            "accrual_ratio",
            "gross_margin_change_yoy",
            "revenue_growth_change_yoy",
            "net_profit_growth_change_yoy",
            "cashflow_growth_change_yoy",
            "dividend_stability",
            "dividend_cashflow_coverage",
        ]:
            self.assertIn(column, result.columns)
        self.assertAlmostEqual(9.1, float(result.loc[0, "roic"]))
        self.assertAlmostEqual(-2.3, float(result.loc[0, "accrual_ratio"]))
        self.assertTrue(pd.isna(pd.to_numeric(result.loc[0, "dividend_stability"], errors="coerce")))

    def test_derive_fundamental_quality_fields_derives_changes_and_dividend_metrics(self) -> None:
        fundamentals = pd.DataFrame(
            [
                {
                    "instrument": "SH600000",
                    "report_period": "2025-12-31",
                    "announce_date": "2026-04-20",
                    "available_at": "2026-04-20",
                    "gross_margin": 30.0,
                    "revenue_growth_yoy": 8.0,
                    "net_profit_growth_yoy": 5.0,
                    "cashflow_growth_yoy": 12.0,
                    "operating_cashflow_per_share": 1.2,
                },
                {
                    "instrument": "SH600000",
                    "report_period": "2026-03-31",
                    "announce_date": "2026-04-28",
                    "available_at": "2026-04-28",
                    "gross_margin": 34.5,
                    "revenue_growth_yoy": 11.0,
                    "net_profit_growth_yoy": 2.0,
                    "cashflow_growth_yoy": 17.0,
                    "operating_cashflow_per_share": 1.5,
                },
            ]
        )
        dividends = pd.DataFrame(
            [
                {"instrument": "SH600000", "available_at": "2025-05-20", "dividend_cash_per_10": 2.0},
                {"instrument": "SH600000", "available_at": "2026-04-25", "dividend_cash_per_10": 3.0},
            ]
        )

        result = derive_fundamental_quality_fields(fundamentals, dividends=dividends)
        current = result[result["report_period"] == "2026-03-31"].iloc[0]

        self.assertAlmostEqual(4.5, float(current["gross_margin_change_yoy"]))
        self.assertAlmostEqual(3.0, float(current["revenue_growth_change_yoy"]))
        self.assertAlmostEqual(-3.0, float(current["net_profit_growth_change_yoy"]))
        self.assertAlmostEqual(5.0, float(current["cashflow_growth_change_yoy"]))
        self.assertAlmostEqual(0.5, float(current["dividend_stability"]))
        self.assertAlmostEqual(5.0, float(current["dividend_cashflow_coverage"]))

    def test_normalize_fundamental_quality_derives_ratios_from_price_when_raw_ratios_missing(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "证券代码": "600000",
                    "报告期": "2026-03-31",
                    "公告日期": "2026-04-20",
                    "摊薄每股收益(元)": "1.5",
                    "每股经营性现金流(元)": "0.6",
                    "close": "30",
                    "dividend_cash_per_10": "3",
                }
            ]
        )

        result = normalize_fundamental_quality(raw, as_of_date="2026-04-27")

        self.assertAlmostEqual(5.0, float(result.loc[0, "ep"]))
        self.assertAlmostEqual(2.0, float(result.loc[0, "cfp"]))
        self.assertAlmostEqual(1.0, float(result.loc[0, "dividend_yield"]))

    def test_normalize_cninfo_dividend_keeps_cash_dividend_and_pit_dates(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "实施方案公告日期": "2025-05-19",
                    "派息比例": "28.1",
                    "除权日": "2025-05-27",
                    "报告时间": "2024年报",
                }
            ]
        )

        result = normalize_cninfo_dividend(raw, instrument="SZ002032")

        self.assertEqual(["SZ002032"], result["instrument"].tolist())
        self.assertEqual(["2025-05-27"], result["available_at"].tolist())
        self.assertAlmostEqual(28.1, float(result.loc[0, "dividend_cash_per_10"]))
        self.assertEqual("cninfo_dividend", result.loc[0, "source"])

    def test_derive_fundamental_valuation_fields_uses_pit_close_and_cninfo_dividend(self) -> None:
        fundamentals = pd.DataFrame(
            [
                {
                    "instrument": "SH600000",
                    "report_period": "2026-03-31",
                    "announce_date": "2026-04-20",
                    "available_at": "2026-04-20",
                    "eps": 1.2,
                    "operating_cashflow_per_share": 0.6,
                }
            ]
        )
        prices = pd.DataFrame(
            [
                {"instrument": "SH600000", "trade_date": "2026-04-19", "close": 20.0},
                {"instrument": "SH600000", "trade_date": "2026-04-20", "close": 24.0},
            ]
        )
        dividends = pd.DataFrame(
            [
                {
                    "instrument": "SH600000",
                    "announce_date": "2026-04-10",
                    "available_at": "2026-04-18",
                    "dividend_cash_per_10": 2.4,
                    "source": "cninfo_dividend",
                }
            ]
        )

        result = derive_fundamental_valuation_fields(fundamentals, prices=prices, dividends=dividends)

        self.assertAlmostEqual(5.0, float(result.loc[0, "ep"]))
        self.assertAlmostEqual(2.5, float(result.loc[0, "cfp"]))
        self.assertAlmostEqual(1.0, float(result.loc[0, "dividend_yield"]))
        self.assertEqual("eps_to_pit_close;ocfps_to_pit_close;cninfo_dividend_to_pit_close", result.loc[0, "valuation_source"])

    def test_derive_fundamental_valuation_fields_records_only_used_sources(self) -> None:
        fundamentals = pd.DataFrame(
            [
                {
                    "instrument": "SH600000",
                    "report_period": "2026-03-31",
                    "announce_date": "2026-04-20",
                    "available_at": "2026-04-20",
                    "eps": 1.2,
                    "operating_cashflow_per_share": "",
                }
            ]
        )
        prices = pd.DataFrame([{"instrument": "SH600000", "trade_date": "2026-04-20", "close": 24.0}])

        result = derive_fundamental_valuation_fields(fundamentals, prices=prices)

        self.assertAlmostEqual(5.0, float(result.loc[0, "ep"]))
        self.assertTrue(pd.isna(pd.to_numeric(result.loc[0, "cfp"], errors="coerce")))
        self.assertEqual("eps_to_pit_close", result.loc[0, "valuation_source"])

    def test_read_close_prices_from_source_dirs_reads_nested_tushare_refresh_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "data/tushare/source_csi300/csi300_current_20260430_20260430"
            source.mkdir(parents=True)
            pd.DataFrame(
                [
                    {"date": "2026-04-30", "symbol": "SH600000", "close": 12.3},
                ]
            ).to_csv(source / "sh600000.csv", index=False)

            result = read_close_prices_from_source_dirs(root, ["data/tushare/source_csi300"])

        self.assertEqual(["SH600000"], result["instrument"].tolist())
        self.assertEqual(["2026-04-30"], result["trade_date"].tolist())
        self.assertEqual([12.3], result["close"].tolist())

    def test_normalize_fundamental_quality_accepts_compact_as_of_date(self) -> None:
        raw = pd.DataFrame([{"证券代码": "000001", "报告期": "2026-03-31", "公告日期": "2026-04-20"}])

        result = normalize_fundamental_quality(raw, as_of_date="20260427")

        self.assertEqual(["2026-04-20"], result["available_at"].tolist())

    def test_normalize_fundamental_quality_uses_conservative_announce_date_when_missing(self) -> None:
        raw = pd.DataFrame([{"证券代码": "000001", "报告期": "2025-12-31", "净资产收益率": "10"}])

        result = normalize_fundamental_quality(raw, as_of_date="20260427")

        self.assertEqual(["2026-04-30"], result["announce_date"].tolist())
        self.assertEqual(["2026-04-30"], result["available_at"].tolist())

    def test_shareholder_capital_extracts_capital_events(self) -> None:
        events = pd.DataFrame(
            [
                {
                    "instrument": "SZ000001",
                    "event_type": "shareholder_reduction",
                    "event_date": "2026-04-20",
                    "title": "股东减持计划公告",
                    "severity": "warning",
                    "source_url": "https://example.com/a",
                    "active_until": "2026-05-20",
                },
                {
                    "instrument": "SZ000002",
                    "event_type": "announcement",
                    "event_date": "2026-04-20",
                    "title": "普通公告",
                    "severity": "info",
                    "source_url": "https://example.com/b",
                    "active_until": "2026-05-20",
                },
            ]
        )

        result = build_shareholder_capital_from_events(events, as_of_date="2026-04-27")

        self.assertEqual(1, len(result))
        self.assertEqual("SZ000001", result.loc[0, "instrument"])
        self.assertEqual("shareholder_reduction", result.loc[0, "event_type"])
        self.assertEqual("2026-04-20", result.loc[0, "announce_date"])
        self.assertEqual("2026-04-27", result.loc[0, "available_at"])

    def test_announcement_evidence_index_splits_searchable_event_text(self) -> None:
        events = pd.DataFrame(
            [
                {
                    "event_id": "e1",
                    "instrument": "SH600000",
                    "event_type": "announcement",
                    "event_date": "2026-04-20",
                    "title": "一季度业绩说明",
                    "summary": "收入增长，现金流改善。",
                    "evidence": "公告披露经营现金流同比改善。",
                    "source_url": "https://example.com/a",
                    "severity": "info",
                }
            ]
        )

        result = build_announcement_evidence_index(events, as_of_date="2026-04-27", chunk_size=12)

        self.assertGreaterEqual(len(result), 2)
        self.assertEqual({"event_id", "instrument", "chunk_id", "chunk_text", "available_at"} <= set(result.columns), True)
        self.assertTrue(result["chunk_text"].str.len().le(12).all())

    def test_write_research_data_domains_writes_all_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame(
                [
                    {
                        "instrument": "SZ000001",
                        "event_type": "pledge_risk",
                        "event_date": "2026-04-20",
                        "title": "质押风险公告",
                        "severity": "warning",
                        "source_url": "https://example.com/a",
                        "active_until": "2026-05-20",
                    }
                ]
            ).to_csv(root / "data/company_events.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "证券代码": "000001",
                        "报告期": "2026-03-31",
                        "公告日期": "2026-04-20",
                        "净资产收益率": "10",
                    }
                ]
            ).to_csv(root / "fundamental_source.csv", index=False)

            manifest = write_research_data_domains(
                root,
                as_of_date="2026-04-27",
                fundamental_source=root / "fundamental_source.csv",
            )

            self.assertTrue(Path(manifest["fundamental_quality"]).exists())
            self.assertTrue(Path(manifest["shareholder_capital"]).exists())
            self.assertTrue(Path(manifest["announcement_evidence"]).exists())

    def test_write_research_data_domains_merges_limited_live_refresh_with_existing_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame({"instrument": ["SH600000", "SZ000001"]}).to_csv(root / "data/security_master.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "instrument": "SH600000",
                        "report_period": "2025-12-31",
                        "announce_date": "2026-04-30",
                        "available_at": "2026-04-30",
                        "roe": 10.0,
                    }
                ]
            ).to_csv(root / "data/fundamental_quality.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "instrument": "SH600000",
                        "announce_date": "2025-05-10",
                        "available_at": "2025-05-20",
                        "dividend_cash_per_10": 1.0,
                        "source": "cninfo_dividend",
                    }
                ]
            ).to_csv(root / "data/cninfo_dividends.csv", index=False)

            live_fundamentals = pd.DataFrame(
                [
                    {
                        "instrument": "SZ000001",
                        "report_period": "2025-12-31",
                        "announce_date": "2026-04-30",
                        "available_at": "2026-04-30",
                        "roe": 9.0,
                    }
                ]
            )
            live_dividends = pd.DataFrame(
                [
                    {
                        "instrument": "SZ000001",
                        "announce_date": "2025-05-10",
                        "available_at": "2025-05-20",
                        "dividend_cash_per_10": 2.0,
                        "source": "cninfo_dividend",
                    }
                ]
            )

            with patch("qlib_factor_lab.research_data_domains.fetch_fundamental_quality_from_akshare", return_value=live_fundamentals), patch(
                "qlib_factor_lab.research_data_domains.fetch_cninfo_dividends_from_akshare",
                return_value=live_dividends,
            ):
                write_research_data_domains(
                    root,
                    as_of_date="2026-04-29",
                    fetch_fundamentals=True,
                    fetch_cninfo_dividends=True,
                    limit=1,
                )

            fundamentals = pd.read_csv(root / "data/fundamental_quality.csv")
            dividends = pd.read_csv(root / "data/cninfo_dividends.csv")
            self.assertEqual({"SH600000", "SZ000001"}, set(fundamentals["instrument"]))
            self.assertEqual({"SH600000", "SZ000001"}, set(dividends["instrument"]))
            self.assertIn("dividend_stability", fundamentals.columns)

    def test_write_research_data_domains_can_fetch_fundamentals_from_tushare(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame([{"instrument": "SZ000001", "name": "平安银行"}]).to_csv(
                root / "data/security_master.csv",
                index=False,
            )

            fetched = pd.DataFrame(
                [
                    {
                        "instrument": "SZ000001",
                        "report_period": "2026-03-31",
                        "announce_date": "2026-04-20",
                        "available_at": "2026-04-20",
                        "roe": 8.5,
                        "gross_margin": 31.2,
                        "debt_ratio": 42.1,
                        "source": "tushare_fina_indicator_vip",
                    }
                ]
            )

            with patch("qlib_factor_lab.research_data_domains.fetch_fundamental_quality_from_tushare", return_value=fetched) as mocked:
                write_research_data_domains(
                    root,
                    as_of_date="2026-04-30",
                    fetch_fundamentals=True,
                    fundamental_provider="tushare",
                )

            mocked.assert_called_once()
            result = pd.read_csv(root / "data/fundamental_quality.csv")
            self.assertEqual(["SZ000001"], result["instrument"].tolist())
            self.assertEqual(["tushare_fina_indicator_vip"], result["source"].tolist())

    def test_write_research_data_domains_passes_offset_to_live_fetchers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame({"instrument": ["SH600000", "SZ000001", "SZ000002"]}).to_csv(root / "data/security_master.csv", index=False)

            with patch("qlib_factor_lab.research_data_domains.fetch_fundamental_quality_from_akshare", return_value=pd.DataFrame()) as fundamentals, patch(
                "qlib_factor_lab.research_data_domains.fetch_cninfo_dividends_from_akshare",
                return_value=pd.DataFrame(),
            ) as dividends:
                write_research_data_domains(
                    root,
                    as_of_date="2026-04-29",
                    fetch_fundamentals=True,
                    fetch_cninfo_dividends=True,
                    limit=1,
                    offset=2,
                )

            self.assertEqual(2, fundamentals.call_args.kwargs["offset"])
            self.assertEqual(2, dividends.call_args.kwargs["offset"])

    def test_write_research_data_domains_preserves_existing_fundamentals_without_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            existing = pd.DataFrame(
                [
                    {
                        "instrument": "SZ000001",
                        "report_period": "2026-03-31",
                        "announce_date": "2026-04-20",
                        "available_at": "2026-04-20",
                        "roe": 10.0,
                        "gross_margin": 20.0,
                        "debt_ratio": 30.0,
                        "revenue_growth_yoy": 1.0,
                        "net_profit_growth_yoy": 2.0,
                        "operating_cashflow_to_net_profit": 3.0,
                        "source": "existing",
                    }
                ]
            )
            existing.to_csv(root / "data/fundamental_quality.csv", index=False)
            pd.DataFrame(columns=["event_id", "instrument", "event_type", "event_date"]).to_csv(root / "data/company_events.csv", index=False)
            pd.DataFrame({"instrument": ["SZ000001"]}).to_csv(root / "data/security_master.csv", index=False)

            write_research_data_domains(root, as_of_date="2026-04-28")

            result = pd.read_csv(root / "data/fundamental_quality.csv")
            self.assertEqual(["SZ000001"], result["instrument"].tolist())
            self.assertEqual({"ep", "cfp", "dividend_yield"} <= set(result.columns), True)
            self.assertEqual(["existing"], result["source"].tolist())


if __name__ == "__main__":
    unittest.main()
