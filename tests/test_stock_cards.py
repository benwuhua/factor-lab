import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.stock_cards import build_stock_cards, write_stock_cards


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
        self.assertEqual(card["review_questions"]["gate_reason"], "max_industry_weight:pass")

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


if __name__ == "__main__":
    unittest.main()
