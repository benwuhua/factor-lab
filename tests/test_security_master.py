import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.security_master import (
    SECURITY_MASTER_COLUMNS,
    enrich_with_security_master,
    load_security_master,
)


class SecurityMasterTests(unittest.TestCase):
    def test_security_master_selects_row_valid_on_trade_date(self):
        signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["AAA"], "score": [1.0]})
        master = pd.DataFrame(
            {
                "instrument": ["AAA", "AAA"],
                "name": ["Old Name", "New Name"],
                "exchange": ["SSE", "SSE"],
                "board": ["main", "main"],
                "industry_sw": ["old_sw", "new_sw"],
                "industry_csrc": ["old_csrc", "new_csrc"],
                "is_st": [False, True],
                "listing_date": ["2020-01-01", "2020-01-01"],
                "delisting_date": ["", ""],
                "valid_from": ["2020-01-01", "2026-01-01"],
                "valid_to": ["2025-12-31", ""],
            }
        )

        enriched = enrich_with_security_master(signal, master)

        self.assertEqual(enriched.loc[0, "name"], "New Name")
        self.assertTrue(bool(enriched.loc[0, "is_st"]))
        self.assertFalse(bool(enriched.loc[0, "security_master_missing"]))

    def test_security_master_marks_missing_metadata(self):
        signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["MISSING"], "score": [1.0]})
        master = pd.DataFrame(columns=SECURITY_MASTER_COLUMNS)

        enriched = enrich_with_security_master(signal, master)

        self.assertTrue(bool(enriched.loc[0, "security_master_missing"]))
        self.assertTrue(pd.isna(enriched.loc[0, "name"]))

    def test_security_master_clears_stale_metadata_when_missing(self):
        signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["MISSING"], "name": ["stale"]})
        master = pd.DataFrame(columns=SECURITY_MASTER_COLUMNS)

        enriched = enrich_with_security_master(signal, master)

        self.assertTrue(bool(enriched.loc[0, "security_master_missing"]))
        self.assertTrue(pd.isna(enriched.loc[0, "name"]))

    def test_security_master_handles_duplicate_signal_index_labels(self):
        signal = pd.DataFrame(
            {"date": ["2026-04-23", "2026-04-23"], "instrument": ["AAA", "BBB"]},
            index=[0, 0],
        )
        master = pd.DataFrame(
            {
                "instrument": ["AAA", "BBB"],
                "name": ["AAA Name", "BBB Name"],
                "exchange": ["SSE", "SZSE"],
                "board": ["main", "main"],
                "industry_sw": ["sw_a", "sw_b"],
                "industry_csrc": ["csrc_a", "csrc_b"],
                "is_st": [False, False],
                "listing_date": ["2020-01-01", "2020-01-01"],
                "delisting_date": ["", ""],
                "valid_from": ["2020-01-01", "2020-01-01"],
                "valid_to": ["", ""],
            }
        )

        enriched = enrich_with_security_master(signal, master)

        self.assertEqual(list(enriched["name"]), ["AAA Name", "BBB Name"])
        self.assertEqual(list(enriched["exchange"]), ["SSE", "SZSE"])

    def test_load_security_master_returns_empty_required_columns_when_path_none_or_missing(self):
        none_loaded = load_security_master(None)

        self.assertTrue(none_loaded.empty)
        self.assertEqual(list(none_loaded.columns), SECURITY_MASTER_COLUMNS)

        with tempfile.TemporaryDirectory() as tmp:
            missing_loaded = load_security_master(Path(tmp) / "missing.csv")

        self.assertTrue(missing_loaded.empty)
        self.assertEqual(list(missing_loaded.columns), SECURITY_MASTER_COLUMNS)


if __name__ == "__main__":
    unittest.main()
