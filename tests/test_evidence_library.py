from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.evidence_library import search_announcement_evidence, summarize_announcement_evidence


class EvidenceLibraryTest(unittest.TestCase):
    def test_search_filters_by_instrument_type_severity_and_keyword(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "announcement_evidence.csv"
            self._fixture().to_csv(path, index=False)

            rows = search_announcement_evidence(
                path,
                instruments=["AAA"],
                event_types=["buyback"],
                severities=["watch"],
                keyword="回购",
            )

        self.assertEqual(list(rows["instrument"]), ["AAA"])
        self.assertEqual(list(rows["source_url"]), ["https://example.test/a"])

    def test_search_respects_available_at_point_in_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "announcement_evidence.csv"
            self._fixture().to_csv(path, index=False)

            rows = search_announcement_evidence(path, as_of_date="2026-04-21")

        self.assertEqual(list(rows["event_id"]), ["e1"])

    def test_summary_counts_events_instruments_and_source_urls(self) -> None:
        summary = summarize_announcement_evidence(self._fixture())

        self.assertEqual(summary["chunks"], 3)
        self.assertEqual(summary["events"], 3)
        self.assertEqual(summary["instruments"], 2)
        self.assertEqual(summary["source_urls"], 2)

    def _fixture(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "event_id": "e1",
                    "instrument": "AAA",
                    "event_type": "buyback",
                    "event_date": "2026-04-20",
                    "available_at": "2026-04-21",
                    "severity": "watch",
                    "title": "回购公告",
                    "source_url": "https://example.test/a",
                    "chunk_id": "e1_000",
                    "chunk_text": "公司公告回购股份。",
                    "keywords": "回购,公告",
                },
                {
                    "event_id": "e2",
                    "instrument": "BBB",
                    "event_type": "disciplinary_action",
                    "event_date": "2026-04-22",
                    "available_at": "2026-04-22",
                    "severity": "block",
                    "title": "监管处罚",
                    "source_url": "https://example.test/b",
                    "chunk_id": "e2_000",
                    "chunk_text": "交易所纪律处分。",
                    "keywords": "监管,处罚",
                },
                {
                    "event_id": "e3",
                    "instrument": "AAA",
                    "event_type": "earnings_watch",
                    "event_date": "2026-04-23",
                    "available_at": "2026-04-23",
                    "severity": "watch",
                    "title": "业绩预告",
                    "source_url": "",
                    "chunk_id": "e3_000",
                    "chunk_text": "业绩预告修复。",
                    "keywords": "业绩,修复",
                },
            ]
        )


if __name__ == "__main__":
    unittest.main()
