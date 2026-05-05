from __future__ import annotations

import unittest
from datetime import date
from types import SimpleNamespace

import pandas as pd

from qlib_factor_lab.rqdata_data import (
    build_security_master_history_from_rqdata,
    normalize_rqdata_instruments,
    qlib_symbol_from_rqdata,
    rqdata_code_from_qlib,
)


class RQDataDataTest(unittest.TestCase):
    def test_symbol_conversion_round_trips_a_share_codes(self) -> None:
        self.assertEqual(rqdata_code_from_qlib("SH600000"), "600000.XSHG")
        self.assertEqual(rqdata_code_from_qlib("SZ000001"), "000001.XSHE")
        self.assertEqual(qlib_symbol_from_rqdata("600000.XSHG"), "SH600000")
        self.assertEqual(qlib_symbol_from_rqdata("000001.XSHE"), "SZ000001")

    def test_normalize_rqdata_instruments_maps_master_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "order_book_id": "600000.XSHG",
                    "symbol": "浦发银行",
                    "exchange": "XSHG",
                    "board_type": "MainBoard",
                    "industry_name": "银行",
                    "industry_code": "J66",
                    "special_type": "ST",
                    "listed_date": "1999-11-10",
                    "de_listed_date": "0000-00-00",
                }
            ]
        )

        result = normalize_rqdata_instruments(raw, as_of_date="2026-05-05", research_universe="csi300")

        self.assertEqual(result.loc[0, "instrument"], "SH600000")
        self.assertEqual(result.loc[0, "name"], "浦发银行")
        self.assertEqual(result.loc[0, "exchange"], "SSE")
        self.assertEqual(result.loc[0, "board"], "MainBoard")
        self.assertEqual(result.loc[0, "industry_csrc"], "银行")
        self.assertEqual(result.loc[0, "industry_sw"], "J66")
        self.assertEqual(result.loc[0, "is_st"], True)
        self.assertEqual(result.loc[0, "source"], "rqdata_instruments")
        self.assertEqual(result.loc[0, "valid_from"], "2026-05-05")

    def test_build_security_master_history_from_rqdata_collapses_daily_snapshots(self) -> None:
        client = FakeRQDataClient()

        result = build_security_master_history_from_rqdata(
            client,
            instruments=["SH600000"],
            start_date="2026-05-01",
            end_date="2026-05-03",
            as_of_date="2026-05-05",
            research_universe="csi300",
        )

        self.assertEqual(["2026-05-01", "2026-05-03"], result["valid_from"].tolist())
        self.assertEqual(["2026-05-02", ""], result["valid_to"].tolist())
        self.assertEqual([False, True], result["is_st"].tolist())
        self.assertEqual(["银行", "非银金融"], result["industry_sw"].tolist())
        self.assertEqual({"rqdata_pit"}, set(result["source"]))


class FakeRQDataClient:
    def all_instruments(self, type=None, date=None, market="cn"):
        return pd.DataFrame(
            [
                {
                    "order_book_id": "600000.XSHG",
                    "symbol": "浦发银行",
                    "exchange": "XSHG",
                    "board_type": "MainBoard",
                    "industry_name": "银行",
                    "industry_code": "J66",
                    "special_type": "Normal",
                    "listed_date": "1999-11-10",
                    "de_listed_date": "0000-00-00",
                }
            ]
        )

    def get_instrument_industry(self, order_book_ids, source="sws", level=1, date=None, market="cn"):
        name = "银行" if str(date) < "2026-05-03" else "非银金融"
        return pd.Series({order_book_ids[0]: SimpleNamespace(name=name, code="801780")})

    def is_st_stock(self, order_book_ids, start_date=None, end_date=None, market="cn"):
        value = pd.Timestamp(start_date).date() >= date(2026, 5, 3)
        return pd.DataFrame({order_book_ids[0]: [value]}, index=[pd.Timestamp(start_date)])


if __name__ == "__main__":
    unittest.main()
