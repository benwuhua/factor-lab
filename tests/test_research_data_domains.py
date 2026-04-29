from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.research_data_domains import (
    build_announcement_evidence_index,
    build_shareholder_capital_from_events,
    normalize_fundamental_quality,
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
        self.assertEqual("akshare_financial_indicator", result.loc[0, "source"])

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
