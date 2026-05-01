import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.stock_cards import build_stock_cards, write_stock_card_report, write_stock_cards


class StockCardsTests(unittest.TestCase):
    def test_build_stock_cards_combines_portfolio_evidence_and_audit_fields(self):
        portfolio = pd.DataFrame(
            [
                {
                    "date": "2026-04-24",
                    "instrument": "AAA",
                    "rank": 1,
                    "target_weight": 0.08,
                    "ensemble_score": 1.23,
                    "top_factor_1": "mom_20",
                    "top_factor_1_contribution": 0.7,
                    "industry": "tech",
                    "event_count": 1,
                    "max_event_severity": "watch",
                    "active_event_types": "buyback",
                    "positive_event_types": "buyback",
                    "positive_event_summary": "buyback plan",
                    "risk_event_types": "",
                    "risk_event_summary": "",
                    "event_risk_summary": "buyback plan",
                    "event_source_urls": "https://example.test/a",
                    "risk_flags": "announcement_watch",
                    "selection_explanation": "selected by ensemble",
                }
            ]
        )
        gate_checks = pd.DataFrame(
            [{"check": "max_industry_weight", "status": "pass", "value": 0.2, "limit": 0.5}]
        )

        cards = build_stock_cards(
            portfolio,
            run_id="run-1",
            as_of_date="2026-04-24",
            card_version="v1",
            gate_decision="caution",
            gate_checks=gate_checks,
            factor_version="factors-v1",
        )

        self.assertEqual(len(cards), 1)
        card = cards[0]
        self.assertEqual(card["instrument"], "AAA")
        self.assertEqual(card["audit"]["run_id"], "run-1")
        self.assertEqual(card["audit"]["review_decision"], "caution")
        self.assertEqual(card["current_signal"]["top_factor_1"], "mom_20")
        self.assertEqual(card["evidence"]["event_count"], 1)
        self.assertEqual(card["evidence"]["positive_event_types"], "buyback")
        self.assertEqual(card["evidence"]["positive_event_summary"], "buyback plan")
        self.assertEqual(card["evidence"]["risk_event_types"], "")
        self.assertEqual(card["evidence"]["risk_event_summary"], "")
        self.assertEqual(card["review_questions"]["gate_reason"], "max_industry_weight:pass")
        self.assertEqual(card["selection_thesis"]["why_selected"], "selected by ensemble")
        self.assertEqual(card["factor_contributions"][0]["factor"], "mom_20")
        self.assertEqual(card["factor_contributions"][0]["contribution"], 0.7)
        self.assertEqual(card["counter_evidence"]["risks"], "announcement_watch")
        self.assertEqual(card["announcement_evidence"]["positive_event_types"], "buyback")
        self.assertEqual(card["announcement_evidence"]["source_urls"], ["https://example.test/a"])
        self.assertEqual(card["financial_anomalies"], [])
        self.assertTrue(card["manual_review_actions"]["announcement_review"])
        self.assertEqual(card["tracking"]["status"], "")

        with tempfile.TemporaryDirectory() as tmp:
            output = write_stock_cards(cards, Path(tmp) / "stock_cards.jsonl")
            text = output.read_text(encoding="utf-8")
            self.assertIn('"instrument": "AAA"', text)

    def test_stock_cards_tolerate_missing_optional_columns(self):
        cards = build_stock_cards(
            pd.DataFrame([{"instrument": "AAA"}]),
            run_id="run-1",
            as_of_date="2026-04-24",
        )

        self.assertEqual(cards[0]["instrument"], "AAA")
        self.assertEqual(cards[0]["current_signal"]["ensemble_score"], None)
        self.assertEqual(cards[0]["evidence"]["source_urls"], [])
        self.assertEqual(cards[0]["factor_contributions"], [])
        self.assertEqual(cards[0]["financial_anomalies"], [])
        self.assertEqual(cards[0]["manual_review_actions"]["manual_chart_needed"], False)

    def test_stock_cards_include_rolling_announcement_evidence_chunks(self):
        portfolio = pd.DataFrame(
            [
                {
                    "date": "2026-04-24",
                    "instrument": "AAA",
                    "name": "Alpha A",
                    "event_count": 0,
                }
            ]
        )
        evidence = pd.DataFrame(
            [
                {
                    "event_id": "e1",
                    "instrument": "AAA",
                    "event_type": "buyback",
                    "event_date": "2026-04-20",
                    "available_at": "2026-04-21",
                    "severity": "watch",
                    "title": "回购股份方案公告",
                    "source_url": "https://example.test/e1",
                    "chunk_id": "e1_000",
                    "chunk_text": "公司公告回购股份方案，后续需核实资金来源。",
                    "keywords": "回购,资金",
                },
                {
                    "event_id": "future",
                    "instrument": "AAA",
                    "event_type": "announcement",
                    "event_date": "2026-05-01",
                    "available_at": "2026-05-01",
                    "severity": "info",
                    "title": "未来公告",
                    "source_url": "https://example.test/future",
                    "chunk_id": "future_000",
                    "chunk_text": "未来不可见",
                    "keywords": "",
                },
            ]
        )

        cards = build_stock_cards(
            portfolio,
            run_id="run-1",
            as_of_date="2026-04-24",
            announcement_evidence=evidence,
        )

        rolling = cards[0]["announcement_evidence"]["rolling_evidence"]
        self.assertEqual(1, rolling["chunks"])
        self.assertEqual(["buyback"], rolling["event_types"])
        self.assertEqual(["https://example.test/e1"], rolling["source_urls"])
        self.assertIn("回购股份方案公告", rolling["items"][0]["title"])

    def test_write_stock_card_report_renders_candidate_review_markdown(self):
        cards = build_stock_cards(
            pd.DataFrame(
                [
                    {
                        "date": "2026-04-24",
                        "instrument": "AAA",
                        "name": "Alpha A",
                        "rank": 1,
                        "target_weight": 0.08,
                        "selection_explanation": "selected by ensemble",
                        "top_factor_1": "mom_20",
                        "top_factor_1_contribution": 0.7,
                        "top_factor_2": "quality",
                        "top_factor_2_contribution": 0.4,
                        "positive_event_types": "buyback",
                        "positive_event_summary": "buyback plan",
                        "risk_event_types": "pledge_risk",
                        "risk_event_summary": "pledge watch",
                        "event_source_urls": "https://example.test/a; https://example.test/b",
                        "risk_flags": "announcement_watch",
                        "financial_anomaly_flags": "negative_cashflow",
                    }
                ]
            ),
            run_id="run-1",
            as_of_date="2026-04-24",
            gate_decision="caution",
            gate_checks=pd.DataFrame([{"check": "max_industry_weight", "status": "pass"}]),
        )

        with tempfile.TemporaryDirectory() as tmp:
            output = write_stock_card_report(cards, Path(tmp) / "report.md")
            text = output.read_text(encoding="utf-8")

        self.assertIn("# Stock Candidate Report", text)
        self.assertIn("## AAA Alpha A", text)
        self.assertIn("Why selected: selected by ensemble", text)
        self.assertIn("Top drivers: mom_20 (+0.7); quality (+0.4)", text)
        self.assertIn("Risks: announcement_watch; pledge watch", text)
        self.assertIn("Evidence urls: https://example.test/a; https://example.test/b", text)
        self.assertIn("Manual review action: Review announcement evidence; Review financial anomalies; Review chart/risk flags", text)
        self.assertIn("Tracking: status=; next_review_date=; owner=", text)

    def test_build_stock_cards_cli_can_write_jsonl_and_markdown_report(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            portfolio_path = tmp_path / "portfolio.csv"
            portfolio_path.write_text(
                "\n".join(
                    [
                        "date,instrument,name,selection_explanation,top_factor_1,top_factor_1_contribution,event_source_urls",
                        "2026-04-24,AAA,Alpha A,selected by ensemble,mom_20,0.7,https://example.test/a",
                    ]
                ),
                encoding="utf-8",
            )
            cards_path = tmp_path / "cards.jsonl"
            report_path = tmp_path / "report.md"

            completed = subprocess.run(
                [
                    sys.executable,
                    str(repo_root / "scripts" / "build_stock_cards.py"),
                    "--target-portfolio",
                    str(portfolio_path),
                    "--as-of-date",
                    "2026-04-24",
                    "--output",
                    str(cards_path),
                    "--report-output",
                    str(report_path),
                    "--project-root",
                    str(repo_root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertIn("wrote:", completed.stdout)
            self.assertIn("wrote report:", completed.stdout)
            self.assertIn('"instrument": "AAA"', cards_path.read_text(encoding="utf-8"))
            self.assertIn("Why selected: selected by ensemble", report_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
