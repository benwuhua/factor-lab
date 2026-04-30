from __future__ import annotations

import unittest

import pandas as pd

from qlib_factor_lab.tushare_data import (
    TushareApiError,
    call_tushare_api,
    download_tushare_history_csvs,
    fetch_fundamental_quality_from_tushare,
    format_permission_probe_rows,
    normalize_tushare_history,
    normalize_tushare_fina_indicator,
    probe_tushare_permissions,
    qlib_symbol_from_tushare,
    resolve_latest_tushare_daily_date,
    tushare_code_from_qlib,
)

from pathlib import Path
import tempfile


class TushareDataTest(unittest.TestCase):
    def test_symbol_conversion_round_trips_between_qlib_and_tushare(self) -> None:
        self.assertEqual("000001.SZ", tushare_code_from_qlib("SZ000001"))
        self.assertEqual("600000.SH", tushare_code_from_qlib("SH600000"))
        self.assertEqual("SZ000001", qlib_symbol_from_tushare("000001.SZ"))
        self.assertEqual("SH600000", qlib_symbol_from_tushare("600000.SH"))
        self.assertEqual("SZ000001", qlib_symbol_from_tushare("000001"))

    def test_call_tushare_api_posts_token_and_returns_dataframe(self) -> None:
        seen: dict[str, object] = {}

        def fake_transport(_endpoint: str, payload: dict[str, object], _timeout: float) -> dict[str, object]:
            seen.update(payload)
            return {
                "code": 0,
                "msg": "",
                "data": {
                    "fields": ["ts_code", "trade_date", "close"],
                    "items": [["000001.SZ", "20260430", 12.34]],
                },
            }

        result = call_tushare_api(
            "daily",
            params={"trade_date": "20260430"},
            fields=["ts_code", "trade_date", "close"],
            token="secret-token",
            transport=fake_transport,
        )

        self.assertEqual("daily", seen["api_name"])
        self.assertEqual("secret-token", seen["token"])
        self.assertEqual(["000001.SZ"], result["ts_code"].tolist())
        self.assertEqual([12.34], result["close"].tolist())

    def test_call_tushare_api_error_never_includes_token(self) -> None:
        def fake_transport(_endpoint: str, _payload: dict[str, object], _timeout: float) -> dict[str, object]:
            return {"code": 2002, "msg": "权限不足", "data": {"fields": [], "items": []}}

        with self.assertRaises(TushareApiError) as ctx:
            call_tushare_api("income_vip", token="very-sensitive-token", transport=fake_transport)

        message = str(ctx.exception)
        self.assertIn("income_vip", message)
        self.assertIn("权限不足", message)
        self.assertNotIn("very-sensitive-token", message)

    def test_normalize_tushare_history_merges_adjustment_and_daily_basic_fields(self) -> None:
        daily = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260429",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "change": 0.2,
                    "pct_chg": 1.94,
                    "vol": 1000.0,
                    "amount": 1050.0,
                }
            ]
        )
        adj = pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260429", "adj_factor": 2.5}])
        basic = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260429",
                    "turnover_rate": 1.2,
                    "pe_ttm": 8.5,
                    "pb": 0.8,
                    "dv_ratio": 3.1,
                    "total_mv": 100000.0,
                    "circ_mv": 80000.0,
                }
            ]
        )

        result = normalize_tushare_history(daily, adj_factors=adj, daily_basic=basic)

        self.assertEqual(["2026-04-29"], result["date"].tolist())
        self.assertEqual(["SZ000001"], result["symbol"].tolist())
        self.assertAlmostEqual(100000.0, result.loc[0, "volume"])
        self.assertAlmostEqual(1050000.0, result.loc[0, "amount"])
        self.assertAlmostEqual(10.5, result.loc[0, "vwap"])
        self.assertAlmostEqual(2.5, result.loc[0, "factor"])
        self.assertAlmostEqual(1.94, result.loc[0, "change"])
        self.assertAlmostEqual(0.2, result.loc[0, "change_amount"])
        self.assertAlmostEqual(1.2, result.loc[0, "turnover"])
        self.assertAlmostEqual(8.5, result.loc[0, "pe_ttm"])
        self.assertAlmostEqual(3.1, result.loc[0, "dividend_yield"])

    def test_resolve_latest_tushare_daily_date_skips_empty_unpublished_day(self) -> None:
        def fake_transport(_endpoint: str, payload: dict[str, object], _timeout: float) -> dict[str, object]:
            api_name = str(payload["api_name"])
            params = payload["params"]
            if api_name == "trade_cal":
                return {
                    "code": 0,
                    "msg": "",
                    "data": {
                        "fields": ["cal_date", "is_open"],
                        "items": [["20260429", 1], ["20260430", 1]],
                    },
                }
            if api_name == "daily" and params == {"trade_date": "20260430"}:
                return {"code": 0, "msg": "", "data": {"fields": ["ts_code"], "items": []}}
            if api_name == "daily" and params == {"trade_date": "20260429"}:
                return {"code": 0, "msg": "", "data": {"fields": ["ts_code"], "items": [["000001.SZ"]]}}
            raise AssertionError(payload)

        result = resolve_latest_tushare_daily_date("20260430", token="token", transport=fake_transport)

        self.assertEqual("20260429", result)

    def test_download_tushare_history_csvs_writes_normalized_symbol_files(self) -> None:
        calls: list[tuple[str, dict[str, object]]] = []

        def fake_transport(_endpoint: str, payload: dict[str, object], _timeout: float) -> dict[str, object]:
            api_name = str(payload["api_name"])
            params = dict(payload["params"])
            calls.append((api_name, params))
            if api_name == "daily":
                return {
                    "code": 0,
                    "msg": "",
                    "data": {
                        "fields": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"],
                        "items": [["000001.SZ", "20260429", 10.0, 11.0, 9.5, 10.5, 1000.0, 1050.0]],
                    },
                }
            if api_name == "adj_factor":
                return {
                    "code": 0,
                    "msg": "",
                    "data": {"fields": ["ts_code", "trade_date", "adj_factor"], "items": [["000001.SZ", "20260429", 2.5]]},
                }
            if api_name == "daily_basic":
                return {
                    "code": 0,
                    "msg": "",
                    "data": {"fields": ["ts_code", "trade_date", "pe_ttm"], "items": [["000001.SZ", "20260429", 8.5]]},
                }
            raise AssertionError(payload)

        with tempfile.TemporaryDirectory() as tmp:
            paths = download_tushare_history_csvs(
                ["SZ000001"],
                Path(tmp),
                start="20260429",
                end="20260430",
                token="token",
                transport=fake_transport,
            )

            self.assertEqual(1, len(paths))
            frame = pd.read_csv(paths[0])
            self.assertEqual(["SZ000001"], frame["symbol"].tolist())
            self.assertAlmostEqual(8.5, frame.loc[0, "pe_ttm"])
            self.assertEqual(
                [
                    ("daily", {"ts_code": "000001.SZ", "start_date": "20260429", "end_date": "20260430"}),
                    ("adj_factor", {"ts_code": "000001.SZ", "start_date": "20260429", "end_date": "20260430"}),
                    ("daily_basic", {"ts_code": "000001.SZ", "start_date": "20260429", "end_date": "20260430"}),
                ],
                calls,
            )

    def test_download_tushare_history_csvs_retries_transient_vendor_errors(self) -> None:
        attempts = {"daily": 0}

        def fake_transport(_endpoint: str, payload: dict[str, object], _timeout: float) -> dict[str, object]:
            api_name = str(payload["api_name"])
            if api_name == "daily":
                attempts["daily"] += 1
                if attempts["daily"] == 1:
                    raise OSError("temporary ssl eof")
                return {
                    "code": 0,
                    "msg": "",
                    "data": {
                        "fields": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"],
                        "items": [["000001.SZ", "20260430", 10.0, 11.0, 9.5, 10.5, 1000.0, 1050.0]],
                    },
                }
            if api_name == "adj_factor":
                return {
                    "code": 0,
                    "msg": "",
                    "data": {"fields": ["ts_code", "trade_date", "adj_factor"], "items": [["000001.SZ", "20260430", 2.5]]},
                }
            if api_name == "daily_basic":
                return {
                    "code": 0,
                    "msg": "",
                    "data": {"fields": ["ts_code", "trade_date", "pe_ttm"], "items": [["000001.SZ", "20260430", 8.5]]},
                }
            raise AssertionError(payload)

        with tempfile.TemporaryDirectory() as tmp:
            paths = download_tushare_history_csvs(
                ["SZ000001"],
                Path(tmp),
                start="20260430",
                end="20260430",
                token="token",
                transport=fake_transport,
            )

            self.assertEqual(1, len(paths))
            self.assertEqual(2, attempts["daily"])

    def test_probe_tushare_permissions_marks_required_endpoints(self) -> None:
        def fake_transport(_endpoint: str, payload: dict[str, object], _timeout: float) -> dict[str, object]:
            api_name = str(payload["api_name"])
            if api_name == "fina_indicator_vip":
                return {"code": 2002, "msg": "权限不足", "data": {"fields": [], "items": []}}
            return {"code": 0, "msg": "", "data": {"fields": ["ts_code"], "items": [["000001.SZ"]]}}

        result = probe_tushare_permissions(token="token", transport=fake_transport)
        by_api = {item["api_name"]: item for item in result}

        self.assertEqual("ok", by_api["daily"]["status"])
        self.assertEqual("fail", by_api["fina_indicator_vip"]["status"])
        self.assertEqual(2002, by_api["fina_indicator_vip"]["code"])
        self.assertNotIn("token", str(result))

    def test_format_permission_probe_rows_is_token_safe_and_readable(self) -> None:
        report = format_permission_probe_rows(
            [
                {"api_name": "daily", "status": "ok", "code": 0, "rows": 10, "msg": ""},
                {"api_name": "income_vip", "status": "fail", "code": 2002, "rows": 0, "msg": "权限不足 secret-token"},
            ],
            token="secret-token",
        )

        self.assertIn("daily", report)
        self.assertIn("income_vip", report)
        self.assertIn("[REDACTED]", report)
        self.assertNotIn("secret-token", report)

    def test_normalize_tushare_fina_indicator_maps_pit_fundamental_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "end_date": "20260331",
                    "ann_date": "20260420",
                    "roe": "8.5",
                    "grossprofit_margin": "31.2",
                    "debt_to_assets": "42.1",
                    "or_yoy": "12.3",
                    "netprofit_yoy": "-5.5",
                    "eps": "1.2",
                    "cfps": "0.8",
                }
            ]
        )

        result = normalize_tushare_fina_indicator(raw, as_of_date="2026-04-30")

        self.assertEqual(["SZ000001"], result["instrument"].tolist())
        self.assertEqual(["2026-03-31"], result["report_period"].tolist())
        self.assertEqual(["2026-04-20"], result["announce_date"].tolist())
        self.assertEqual(["2026-04-20"], result["available_at"].tolist())
        self.assertAlmostEqual(8.5, float(result.loc[0, "roe"]))
        self.assertAlmostEqual(31.2, float(result.loc[0, "gross_margin"]))
        self.assertAlmostEqual(42.1, float(result.loc[0, "debt_ratio"]))
        self.assertAlmostEqual(12.3, float(result.loc[0, "revenue_growth_yoy"]))
        self.assertAlmostEqual(-5.5, float(result.loc[0, "net_profit_growth_yoy"]))
        self.assertAlmostEqual(1.2, float(result.loc[0, "eps"]))
        self.assertAlmostEqual(0.8, float(result.loc[0, "operating_cashflow_per_share"]))
        self.assertEqual("tushare_fina_indicator_vip", result.loc[0, "source"])

    def test_fetch_fundamental_quality_from_tushare_uses_vip_period_batch_and_filters_symbols(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_transport(_endpoint: str, payload: dict[str, object], _timeout: float) -> dict[str, object]:
            calls.append(payload)
            return {
                "code": 0,
                "msg": "",
                "data": {
                    "fields": ["ts_code", "end_date", "ann_date", "roe", "grossprofit_margin", "debt_to_assets"],
                    "items": [
                        ["000001.SZ", "20260331", "20260420", 8.5, 31.2, 42.1],
                        ["600000.SH", "20260331", "20260422", 7.0, 25.0, 50.0],
                    ],
                },
            }

        result = fetch_fundamental_quality_from_tushare(
            ["SZ000001"],
            as_of_date="2026-04-30",
            periods=["20260331"],
            token="token",
            transport=fake_transport,
        )

        self.assertEqual(["fina_indicator_vip"], [str(call["api_name"]) for call in calls])
        self.assertEqual({"period": "20260331"}, calls[0]["params"])
        self.assertEqual(["SZ000001"], result["instrument"].tolist())
        self.assertEqual(["2026-03-31"], result["report_period"].tolist())


if __name__ == "__main__":
    unittest.main()
