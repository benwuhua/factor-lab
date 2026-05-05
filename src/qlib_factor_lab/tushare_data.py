from __future__ import annotations

import datetime as dt
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping
from urllib.request import Request, urlopen

import pandas as pd


TUSHARE_ENDPOINT = "https://api.tushare.pro"

FUNDAMENTAL_QUALITY_COLUMNS = [
    "instrument",
    "report_period",
    "announce_date",
    "available_at",
    "roe",
    "roic",
    "gross_margin",
    "debt_ratio",
    "revenue_growth_yoy",
    "net_profit_growth_yoy",
    "cashflow_growth_yoy",
    "operating_cashflow_to_net_profit",
    "accrual_ratio",
    "eps",
    "operating_cashflow_per_share",
    "ep",
    "cfp",
    "dividend_yield",
    "gross_margin_change_yoy",
    "revenue_growth_change_yoy",
    "net_profit_growth_change_yoy",
    "cashflow_growth_change_yoy",
    "dividend_stability",
    "dividend_cashflow_coverage",
    "valuation_source",
    "source",
]

TUSHARE_FINA_INDICATOR_FIELDS = [
    "ts_code",
    "ann_date",
    "end_date",
    "eps",
    "cfps",
    "grossprofit_margin",
    "roe",
    "roic",
    "debt_to_assets",
    "or_yoy",
    "netprofit_yoy",
    "ocf_yoy",
    "ocf_to_np",
    "accrual_ratio",
]

TUSHARE_INCOME_FIELDS = [
    "ts_code",
    "ann_date",
    "end_date",
    "n_income_attr_p",
    "n_income",
]

TUSHARE_BALANCESHEET_FIELDS = [
    "ts_code",
    "ann_date",
    "end_date",
    "total_assets",
]

TUSHARE_CASHFLOW_FIELDS = [
    "ts_code",
    "ann_date",
    "end_date",
    "n_cashflow_act",
]

TUSHARE_DAILY_FIELDS = [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
]

TUSHARE_DAILY_BASIC_FIELDS = [
    "ts_code",
    "trade_date",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
]

TUSHARE_ADJ_FACTOR_FIELDS = ["ts_code", "trade_date", "adj_factor"]

TUSHARE_DIVIDEND_FIELDS = [
    "ts_code",
    "ann_date",
    "end_date",
    "cash_div_tax",
    "record_date",
    "ex_date",
    "pay_date",
]

TUSHARE_DISCLOSURE_DATE_FIELDS = [
    "ts_code",
    "ann_date",
    "end_date",
    "pre_date",
    "actual_date",
    "modify_date",
]

TUSHARE_STOCK_BASIC_FIELDS = [
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "market",
    "exchange",
    "list_date",
    "delist_date",
    "is_hs",
]

TUSHARE_NAMECHANGE_FIELDS = [
    "ts_code",
    "name",
    "start_date",
    "end_date",
    "ann_date",
    "change_reason",
]

SECURITY_MASTER_HISTORY_COLUMNS = [
    "instrument",
    "name",
    "exchange",
    "board",
    "industry_sw",
    "industry_csrc",
    "is_st",
    "listing_date",
    "delisting_date",
    "valid_from",
    "valid_to",
    "research_universes",
    "source",
    "as_of_date",
]


Transport = Callable[[str, dict[str, object], float], dict[str, object]]


@dataclass(frozen=True)
class TushareApiError(RuntimeError):
    api_name: str
    code: int | str
    msg: str

    def __str__(self) -> str:
        return f"Tushare API {self.api_name} failed with code={self.code}: {self.msg}"


DEFAULT_PERMISSION_PROBES = [
    {
        "api_name": "trade_cal",
        "params": {"exchange": "SSE", "start_date": "20260401", "end_date": "20260430"},
        "fields": ["exchange", "cal_date", "is_open"],
    },
    {
        "api_name": "stock_basic",
        "params": {"list_status": "L"},
        "fields": ["ts_code", "symbol", "name", "area", "industry", "list_date"],
    },
    {
        "api_name": "daily",
        "params": {"trade_date": "20260430"},
        "fields": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"],
    },
    {
        "api_name": "daily_basic",
        "params": {"trade_date": "20260430"},
        "fields": ["ts_code", "trade_date", "turnover_rate", "pe_ttm", "pb", "dv_ratio", "total_mv", "circ_mv"],
    },
    {
        "api_name": "adj_factor",
        "params": {"trade_date": "20260430"},
        "fields": ["ts_code", "trade_date", "adj_factor"],
    },
    {
        "api_name": "stk_limit",
        "params": {"trade_date": "20260430"},
        "fields": ["ts_code", "trade_date", "up_limit", "down_limit"],
    },
    {
        "api_name": "index_weight",
        "params": {"index_code": "000300.SH", "trade_date": "20260430"},
        "fields": ["index_code", "con_code", "trade_date", "weight"],
    },
    {
        "api_name": "fina_indicator_vip",
        "params": {"period": "20260331"},
        "fields": TUSHARE_FINA_INDICATOR_FIELDS,
    },
    {
        "api_name": "income_vip",
        "params": {"period": "20260331"},
        "fields": TUSHARE_INCOME_FIELDS,
    },
    {
        "api_name": "balancesheet_vip",
        "params": {"period": "20260331"},
        "fields": TUSHARE_BALANCESHEET_FIELDS,
    },
    {
        "api_name": "cashflow_vip",
        "params": {"period": "20260331"},
        "fields": TUSHARE_CASHFLOW_FIELDS,
    },
    {
        "api_name": "dividend",
        "params": {"ann_date": "20250430"},
        "fields": TUSHARE_DIVIDEND_FIELDS,
    },
    {
        "api_name": "disclosure_date",
        "params": {"end_date": "20260331"},
        "fields": TUSHARE_DISCLOSURE_DATE_FIELDS,
    },
]


def get_tushare_token(*, env: Mapping[str, str] | None = None, env_var: str = "TUSHARE_TOKEN") -> str:
    source = os.environ if env is None else env
    token = str(source.get(env_var, "")).strip()
    if not token:
        raise RuntimeError(f"{env_var} is not set")
    return token


def call_tushare_api(
    api_name: str,
    *,
    params: Mapping[str, object] | None = None,
    fields: Iterable[str] | str | None = None,
    token: str | None = None,
    endpoint: str = TUSHARE_ENDPOINT,
    timeout: float = 30.0,
    transport: Transport | None = None,
) -> pd.DataFrame:
    actual_token = token or get_tushare_token()
    payload: dict[str, object] = {
        "api_name": str(api_name),
        "token": actual_token,
        "params": dict(params or {}),
        "fields": _fields_to_string(fields),
    }
    response = (transport or _post_json)(endpoint, payload, float(timeout))
    code = response.get("code", "")
    msg = _redact(str(response.get("msg", "")), actual_token)
    if code != 0:
        raise TushareApiError(api_name=str(api_name), code=code, msg=msg)
    data = response.get("data") or {}
    if not isinstance(data, dict):
        return pd.DataFrame()
    result_fields = list(data.get("fields") or [])
    items = list(data.get("items") or [])
    if not result_fields:
        return pd.DataFrame()
    return pd.DataFrame(items, columns=result_fields)


def probe_tushare_permissions(
    *,
    token: str | None = None,
    probes: Iterable[Mapping[str, object]] = DEFAULT_PERMISSION_PROBES,
    transport: Transport | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for probe in probes:
        api_name = str(probe.get("api_name", ""))
        try:
            frame = call_tushare_api(
                api_name,
                params=probe.get("params") if isinstance(probe.get("params"), Mapping) else None,
                fields=probe.get("fields") if isinstance(probe.get("fields"), (list, tuple, str)) else None,
                token=token,
                transport=transport,
            )
            rows.append(
                {
                    "api_name": api_name,
                    "status": "ok",
                    "code": 0,
                    "rows": int(len(frame)),
                    "msg": "",
                }
            )
        except TushareApiError as exc:
            rows.append(
                {
                    "api_name": api_name,
                    "status": "fail",
                    "code": exc.code,
                    "rows": 0,
                    "msg": exc.msg,
                }
            )
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            rows.append(
                {
                    "api_name": api_name,
                    "status": "fail",
                    "code": "exception",
                    "rows": 0,
                    "msg": str(exc),
                }
            )
    return rows


def format_permission_probe_rows(rows: Iterable[Mapping[str, object]], *, token: str | None = None) -> str:
    lines = [
        "# Tushare Permission Check",
        "",
        "| api | status | code | rows | message |",
        "|---|---|---:|---:|---|",
    ]
    for row in rows:
        msg = _redact(str(row.get("msg", "")), token or "")
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("api_name", "")),
                    str(row.get("status", "")),
                    str(row.get("code", "")),
                    str(row.get("rows", "")),
                    msg.replace("|", "\\|"),
                ]
            )
            + " |"
        )
    return "\n".join(lines).rstrip() + "\n"


def tushare_code_from_qlib(symbol: str) -> str:
    text = str(symbol).strip().upper()
    if text.startswith("SH") and len(text) == 8:
        return f"{text[2:]}.SH"
    if text.startswith("SZ") and len(text) == 8:
        return f"{text[2:]}.SZ"
    if "." in text:
        return text
    if len(text) == 6 and text.isdigit():
        suffix = "SH" if text.startswith(("5", "6", "9")) else "SZ"
        return f"{text}.{suffix}"
    raise ValueError(f"invalid qlib symbol: {symbol}")


def qlib_symbol_from_tushare(ts_code: str) -> str:
    text = str(ts_code).strip().upper()
    if "." not in text:
        return qlib_symbol_from_tushare(tushare_code_from_qlib(text))
    code, exchange = text.split(".", 1)
    if exchange not in {"SH", "SZ"} or len(code) != 6 or not code.isdigit():
        raise ValueError(f"invalid Tushare ts_code: {ts_code}")
    return f"{exchange}{code}"


def normalize_tushare_security_master_history(
    stock_basic: pd.DataFrame,
    namechange: pd.DataFrame | None = None,
    *,
    as_of_date: str,
    research_universe: str = "",
) -> pd.DataFrame:
    if stock_basic is None or stock_basic.empty:
        return pd.DataFrame(columns=SECURITY_MASTER_HISTORY_COLUMNS)
    normalized_as_of = _normalize_date(as_of_date)
    name_frame = namechange.copy() if namechange is not None else pd.DataFrame()
    rows: list[dict[str, object]] = []
    for _, stock in stock_basic.iterrows():
        ts_code = str(stock.get("ts_code", "")).strip().upper()
        instrument = _safe_qlib_symbol_from_tushare(ts_code)
        if not instrument:
            continue
        listing_date = _normalize_date(stock.get("list_date", ""))
        delisting_date = _normalize_date(stock.get("delist_date", ""))
        changes = _namechange_rows_for_code(name_frame, ts_code)
        if changes.empty:
            changes = pd.DataFrame(
                [
                    {
                        "ts_code": ts_code,
                        "name": stock.get("name", ""),
                        "start_date": listing_date,
                        "end_date": delisting_date,
                    }
                ]
            )
        for _, change in changes.iterrows():
            name = str(change.get("name", "") or stock.get("name", "") or "").strip()
            valid_from = _normalize_date(change.get("start_date", "")) or listing_date or normalized_as_of
            valid_to = _normalize_date(change.get("end_date", "")) or delisting_date
            rows.append(
                {
                    "instrument": instrument,
                    "name": name,
                    "exchange": _exchange_from_tushare(ts_code, stock.get("exchange", "")),
                    "board": str(stock.get("market", "") or ""),
                    "industry_sw": "",
                    "industry_csrc": str(stock.get("industry", "") or ""),
                    "is_st": _is_st_name(name),
                    "listing_date": listing_date,
                    "delisting_date": delisting_date,
                    "valid_from": valid_from,
                    "valid_to": valid_to,
                    "research_universes": research_universe,
                    "source": "tushare_pit",
                    "as_of_date": normalized_as_of,
                }
            )
    if not rows:
        return pd.DataFrame(columns=SECURITY_MASTER_HISTORY_COLUMNS)
    frame = pd.DataFrame(rows, columns=SECURITY_MASTER_HISTORY_COLUMNS)
    return (
        frame.sort_values(["instrument", "valid_from", "valid_to", "name"])
        .drop_duplicates(["instrument", "valid_from", "valid_to", "name"], keep="last")
        .reset_index(drop=True)
    )


def fetch_security_master_history_from_tushare(
    instruments: Iterable[str],
    *,
    as_of_date: str,
    start_date: str | None = None,
    research_universe: str = "",
    token: str | None = None,
    transport: Transport | None = None,
    delay: float = 0.2,
) -> pd.DataFrame:
    symbols = [str(item).strip().upper() for item in instruments if str(item).strip()]
    ts_codes = sorted({tushare_code_from_qlib(symbol) for symbol in symbols})
    stock_basic = call_tushare_api(
        "stock_basic",
        params={"list_status": "L"},
        fields=TUSHARE_STOCK_BASIC_FIELDS,
        token=token,
        transport=transport,
    )
    if ts_codes and not stock_basic.empty and "ts_code" in stock_basic.columns:
        stock_basic = stock_basic[stock_basic["ts_code"].astype(str).str.upper().isin(ts_codes)].copy()
    frames: list[pd.DataFrame] = []
    for ts_code in ts_codes:
        try:
            frame = call_tushare_api(
                "namechange",
                params={"ts_code": ts_code},
                fields=TUSHARE_NAMECHANGE_FIELDS,
                token=token,
                transport=transport,
            )
        except TushareApiError as exc:
            print(f"skip tushare_namechange {ts_code}: {exc}")
            continue
        if not frame.empty:
            frames.append(frame)
        if delay > 0:
            time.sleep(delay)
    namechange = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=TUSHARE_NAMECHANGE_FIELDS)
    result = normalize_tushare_security_master_history(
        stock_basic,
        namechange,
        as_of_date=as_of_date,
        research_universe=research_universe,
    )
    if start_date and not result.empty:
        start = pd.Timestamp(_normalize_date(start_date))
        valid_to = pd.to_datetime(result["valid_to"], errors="coerce").fillna(pd.Timestamp.max)
        result = result[valid_to >= start].reset_index(drop=True)
    return result.loc[:, SECURITY_MASTER_HISTORY_COLUMNS]


def write_security_master_history_from_tushare(
    output_path: str | Path,
    *,
    instruments: Iterable[str],
    as_of_date: str,
    start_date: str | None = None,
    research_universe: str = "",
    token: str | None = None,
    transport: Transport | None = None,
    delay: float = 0.2,
) -> Path:
    frame = fetch_security_master_history_from_tushare(
        instruments,
        as_of_date=as_of_date,
        start_date=start_date,
        research_universe=research_universe,
        token=token,
        transport=transport,
        delay=delay,
    )
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return output


def normalize_tushare_history(
    daily: pd.DataFrame,
    *,
    adj_factors: pd.DataFrame | None = None,
    daily_basic: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if daily is None or daily.empty:
        return pd.DataFrame()
    required = {"ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"}
    missing = required - set(daily.columns)
    if missing:
        raise ValueError(f"Tushare daily is missing columns: {sorted(missing)}")

    frame = daily.copy()
    frame["date"] = frame["trade_date"].map(_normalize_date)
    frame["symbol"] = frame["ts_code"].map(_safe_qlib_symbol_from_tushare)
    frame["volume"] = pd.to_numeric(frame["vol"], errors="coerce") * 100.0
    frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce") * 1000.0
    frame = frame.rename(columns={"change": "change_amount", "pct_chg": "change"})

    if adj_factors is not None and not adj_factors.empty and {"ts_code", "trade_date", "adj_factor"} <= set(adj_factors.columns):
        factor_frame = adj_factors.loc[:, ["ts_code", "trade_date", "adj_factor"]].rename(columns={"adj_factor": "factor"})
        frame = frame.merge(factor_frame, on=["ts_code", "trade_date"], how="left")
    if "factor" not in frame.columns:
        frame["factor"] = 1.0
    frame["factor"] = pd.to_numeric(frame["factor"], errors="coerce").fillna(1.0)

    if daily_basic is not None and not daily_basic.empty and {"ts_code", "trade_date"} <= set(daily_basic.columns):
        basic = daily_basic.rename(
            columns={
                "turnover_rate": "turnover",
                "dv_ratio": "dividend_yield",
            }
        ).copy()
        keep = [
            "ts_code",
            "trade_date",
            "turnover",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "dividend_yield",
            "dv_ttm",
            "total_share",
            "float_share",
            "free_share",
            "total_mv",
            "circ_mv",
        ]
        frame = frame.merge(basic.loc[:, [column for column in keep if column in basic.columns]], on=["ts_code", "trade_date"], how="left")

    for column in ["open", "high", "low", "close", "volume", "amount", "factor", "change", "change_amount", "turnover"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["vwap"] = frame["amount"] / frame["volume"].replace(0, pd.NA)
    frame["vwap"] = frame["vwap"].fillna(frame["close"])

    base_columns = ["date", "symbol", "open", "close", "high", "low", "volume", "amount", "vwap", "factor"]
    optional = [
        column
        for column in [
            "change",
            "change_amount",
            "turnover",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "dividend_yield",
            "dv_ttm",
            "total_share",
            "float_share",
            "free_share",
            "total_mv",
            "circ_mv",
        ]
        if column in frame.columns
    ]
    output = frame.loc[:, base_columns + optional]
    output = output.dropna(subset=["date", "symbol", "open", "close", "high", "low", "volume"])
    return output.sort_values("date").drop_duplicates("date", keep="last").reset_index(drop=True)


def resolve_latest_tushare_daily_date(
    requested_end: str,
    *,
    lookback_days: int = 10,
    token: str | None = None,
    transport: Transport | None = None,
) -> str:
    end = dt.datetime.strptime(_yyyymmdd(requested_end), "%Y%m%d").date()
    start = end - dt.timedelta(days=max(1, int(lookback_days)))
    calendar = call_tushare_api(
        "trade_cal",
        params={"exchange": "SSE", "start_date": start.strftime("%Y%m%d"), "end_date": end.strftime("%Y%m%d")},
        fields=["cal_date", "is_open"],
        token=token,
        transport=transport,
    )
    if calendar.empty:
        raise RuntimeError(f"Tushare trade_cal returned no rows through {requested_end}")
    calendar = calendar.copy()
    calendar["is_open"] = pd.to_numeric(calendar.get("is_open"), errors="coerce")
    open_dates = sorted(calendar.loc[calendar["is_open"].eq(1), "cal_date"].astype(str).tolist(), reverse=True)
    for trade_date in open_dates:
        probe = call_tushare_api(
            "daily",
            params={"trade_date": trade_date},
            fields=["ts_code"],
            token=token,
            transport=transport,
        )
        if not probe.empty:
            return trade_date
    raise RuntimeError(f"Tushare daily returned no rows for open dates through {requested_end}")


def download_tushare_history_csvs(
    symbols: Iterable[str],
    output_dir: str | Path,
    *,
    start: str,
    end: str,
    delay: float = 0.2,
    limit: int | None = None,
    retries: int = 2,
    token: str | None = None,
    transport: Transport | None = None,
) -> list[Path]:
    selected = list(symbols)
    if limit is not None:
        selected = selected[:limit]
    output_paths: list[Path] = []
    for index, symbol in enumerate(selected, start=1):
        ts_code = tushare_code_from_qlib(symbol)
        params = {"ts_code": ts_code, "start_date": _yyyymmdd(start), "end_date": _yyyymmdd(end)}
        for attempt in range(max(0, int(retries)) + 1):
            try:
                daily = call_tushare_api("daily", params=params, fields=TUSHARE_DAILY_FIELDS, token=token, transport=transport)
                adj_factors = call_tushare_api(
                    "adj_factor",
                    params=params,
                    fields=TUSHARE_ADJ_FACTOR_FIELDS,
                    token=token,
                    transport=transport,
                )
                basic = call_tushare_api(
                    "daily_basic",
                    params=params,
                    fields=TUSHARE_DAILY_BASIC_FIELDS,
                    token=token,
                    transport=transport,
                )
                break
            except Exception as exc:  # pragma: no cover - network/vendor dependent
                if attempt < max(0, int(retries)):
                    print(f"retry tushare history {symbol}: {exc}")
                    if delay > 0:
                        time.sleep(delay)
                    continue
                print(f"skip tushare history {symbol}: {exc}")
        else:
            continue
        frame = normalize_tushare_history(daily, adj_factors=adj_factors, daily_basic=basic)
        if frame.empty:
            continue
        path = _write_symbol_csv(frame, output_dir, str(symbol).upper())
        output_paths.append(path)
        print(f"[{index}/{len(selected)}] {symbol}: {len(frame)} rows -> {path}")
        if delay > 0:
            time.sleep(delay)
    return output_paths


def normalize_tushare_fina_indicator(
    raw: pd.DataFrame,
    *,
    as_of_date: str,
    source: str = "tushare_fina_indicator_vip",
) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=FUNDAMENTAL_QUALITY_COLUMNS)

    rows: list[dict[str, object]] = []
    for _, item in raw.iterrows():
        ts_code = str(item.get("ts_code", "")).strip()
        if not ts_code:
            continue
        try:
            instrument = qlib_symbol_from_tushare(ts_code)
        except ValueError:
            continue
        report_period = _date_from_row(item, ["end_date", "period", "report_period"])
        if not report_period:
            continue
        announce_date = _date_from_row(item, ["ann_date", "f_ann_date", "announce_date"]) or _fallback_announce_date(report_period)
        available_at = _date_from_row(item, ["available_at"]) or announce_date or _normalize_date(as_of_date)
        rows.append(
            {
                "instrument": instrument,
                "report_period": report_period,
                "announce_date": announce_date,
                "available_at": available_at,
                "roe": _number_from_row(item, ["roe", "roe_waa", "roe_dt"]),
                "roic": _number_from_row(item, ["roic"]),
                "gross_margin": _number_from_row(item, ["grossprofit_margin", "gross_margin"]),
                "debt_ratio": _number_from_row(item, ["debt_to_assets", "debt_asset_ratio"]),
                "revenue_growth_yoy": _number_from_row(item, ["or_yoy", "tr_yoy", "revenue_yoy"]),
                "net_profit_growth_yoy": _number_from_row(item, ["netprofit_yoy", "dt_netprofit_yoy"]),
                "cashflow_growth_yoy": _number_from_row(item, ["ocf_yoy", "cashflow_growth_yoy"]),
                "operating_cashflow_to_net_profit": _number_from_row(item, ["ocf_to_np", "ocf_to_profit", "cashflow_to_np"]),
                "accrual_ratio": _number_from_row(item, ["accrual_ratio"]),
                "eps": _number_from_row(item, ["eps", "basic_eps"]),
                "operating_cashflow_per_share": _number_from_row(item, ["cfps", "ocfps", "net_cashflow_ps"]),
                "ep": _number_from_row(item, ["ep", "earnings_to_price"]),
                "cfp": _number_from_row(item, ["cfp", "cashflow_to_price"]),
                "dividend_yield": _number_from_row(item, ["dividend_yield", "dv_ratio", "dv_ttm"]),
                "valuation_source": "",
                "source": source,
            }
        )
    if not rows:
        return pd.DataFrame(columns=FUNDAMENTAL_QUALITY_COLUMNS)
    frame = pd.DataFrame(rows, columns=FUNDAMENTAL_QUALITY_COLUMNS)
    frame = frame.sort_values(["instrument", "report_period", "announce_date"]).drop_duplicates(
        ["instrument", "report_period"],
        keep="last",
    )
    return _derive_change_fields(frame)


def fetch_fundamental_quality_from_tushare(
    instruments: Iterable[str],
    *,
    as_of_date: str,
    periods: Iterable[str] | None = None,
    start_year: int = 2015,
    limit: int | None = None,
    offset: int = 0,
    delay: float = 0.2,
    token: str | None = None,
    transport: Transport | None = None,
) -> pd.DataFrame:
    symbols = list(dict.fromkeys(str(item).upper() for item in instruments if str(item).strip()))
    if offset > 0:
        symbols = symbols[offset:]
    if limit is not None:
        symbols = symbols[:limit]
    selected = set(symbols)
    if not selected:
        return pd.DataFrame(columns=FUNDAMENTAL_QUALITY_COLUMNS)

    frames: list[pd.DataFrame] = []
    accrual_frames: list[pd.DataFrame] = []
    period_values = list(periods or _quarter_periods(start_year=start_year, as_of_date=as_of_date))
    for period in period_values:
        try:
            raw = call_tushare_api(
                "fina_indicator_vip",
                params={"period": _yyyymmdd(period)},
                fields=TUSHARE_FINA_INDICATOR_FIELDS,
                token=token,
                transport=transport,
            )
        except Exception as exc:  # pragma: no cover - network/vendor dependent
            print(f"skip tushare_fina_indicator_vip period={period}: {exc}")
            continue
        if raw is None or raw.empty or "ts_code" not in raw.columns:
            continue
        filtered = raw.copy()
        filtered["instrument"] = filtered["ts_code"].map(_safe_qlib_symbol_from_tushare)
        filtered = filtered[filtered["instrument"].isin(selected)]
        if filtered.empty:
            continue
        frames.append(normalize_tushare_fina_indicator(filtered, as_of_date=as_of_date))
        accruals = _fetch_statement_accruals_for_period(
            period,
            selected=selected,
            token=token,
            transport=transport,
        )
        if not accruals.empty:
            accrual_frames.append(accruals)
        if delay > 0:
            time.sleep(delay)
    if not frames:
        return pd.DataFrame(columns=FUNDAMENTAL_QUALITY_COLUMNS)
    combined = pd.concat(frames, ignore_index=True).sort_values(["instrument", "report_period"]).drop_duplicates(
        ["instrument", "report_period"],
        keep="last",
    )
    if accrual_frames:
        combined = _fill_statement_accruals(combined, pd.concat(accrual_frames, ignore_index=True))
    return _derive_change_fields(combined)


def normalize_tushare_dividend(
    raw: pd.DataFrame,
    *,
    source: str = "tushare_dividend",
) -> pd.DataFrame:
    columns = ["instrument", "announce_date", "available_at", "dividend_cash_per_10", "source"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, object]] = []
    for _, item in raw.iterrows():
        ts_code = str(item.get("ts_code", "")).strip()
        if not ts_code:
            continue
        instrument = _safe_qlib_symbol_from_tushare(ts_code)
        if not instrument:
            continue
        announce_date = _date_from_row(item, ["ann_date", "announce_date"])
        available_at = announce_date or _date_from_row(item, ["record_date", "ex_date", "pay_date"])
        cash = _number_from_row(item, ["cash_div_tax", "cash_div", "dividend_cash_per_10"])
        if not available_at or cash is None:
            continue
        rows.append(
            {
                "instrument": instrument,
                "announce_date": announce_date,
                "available_at": available_at,
                "dividend_cash_per_10": cash,
                "source": source,
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)
    return (
        pd.DataFrame(rows, columns=columns)
        .sort_values(["instrument", "available_at", "dividend_cash_per_10"])
        .drop_duplicates(["instrument", "available_at", "dividend_cash_per_10"], keep="last")
        .reset_index(drop=True)
    )


def fetch_tushare_dividends(
    instruments: Iterable[str],
    *,
    limit: int | None = None,
    offset: int = 0,
    delay: float = 0.2,
    token: str | None = None,
    transport: Transport | None = None,
) -> pd.DataFrame:
    symbols = list(dict.fromkeys(str(item).upper() for item in instruments if str(item).strip()))
    if offset > 0:
        symbols = symbols[offset:]
    if limit is not None:
        symbols = symbols[:limit]
    frames: list[pd.DataFrame] = []
    for symbol in symbols:
        try:
            raw = call_tushare_api(
                "dividend",
                params={"ts_code": tushare_code_from_qlib(symbol)},
                fields=TUSHARE_DIVIDEND_FIELDS,
                token=token,
                transport=transport,
            )
        except Exception as exc:  # pragma: no cover - network/vendor dependent
            print(f"skip tushare_dividend {symbol}: {exc}")
            continue
        normalized = normalize_tushare_dividend(raw)
        if not normalized.empty:
            frames.append(normalized)
        if delay > 0:
            time.sleep(delay)
    if not frames:
        return pd.DataFrame(columns=["instrument", "announce_date", "available_at", "dividend_cash_per_10", "source"])
    return pd.concat(frames, ignore_index=True)


def normalize_tushare_disclosure_dates(
    raw: pd.DataFrame,
    *,
    as_of_date: str,
    source: str = "tushare_disclosure_date",
) -> pd.DataFrame:
    columns = [
        "event_id",
        "instrument",
        "event_type",
        "event_date",
        "source",
        "source_url",
        "title",
        "severity",
        "summary",
        "evidence",
        "active_until",
    ]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, object]] = []
    normalized_as_of = pd.Timestamp(_normalize_date(as_of_date))
    for _, item in raw.iterrows():
        instrument = _safe_qlib_symbol_from_tushare(item.get("ts_code"))
        report_period = _date_from_row(item, ["end_date", "report_period"])
        event_date = _date_from_row(item, ["actual_date", "ann_date", "pre_date"])
        if not instrument or not report_period or not event_date:
            continue
        event_ts = pd.Timestamp(event_date)
        if event_ts > normalized_as_of:
            continue
        active_until = (event_ts + pd.Timedelta(days=30)).strftime("%Y-%m-%d")
        title = f"Financial report disclosure {report_period}"
        rows.append(
            {
                "event_id": f"{source}:{instrument}:{report_period}:{event_date}",
                "instrument": instrument,
                "event_type": "financial_report_disclosure",
                "event_date": event_date,
                "source": source,
                "source_url": "",
                "title": title,
                "severity": "info",
                "summary": f"Actual disclosure date {event_date}; report_period {report_period}.",
                "evidence": (
                    f"Tushare disclosure_date: ann_date={_date_from_row(item, ['ann_date'])}; "
                    f"pre_date={_date_from_row(item, ['pre_date'])}; actual_date={event_date}."
                ),
                "active_until": active_until,
            }
        )
    if not rows:
        return pd.DataFrame(columns=columns)
    return (
        pd.DataFrame(rows, columns=columns)
        .drop_duplicates(["event_id"], keep="last")
        .sort_values(["event_date", "instrument", "event_type"])
        .reset_index(drop=True)
    )


def fetch_tushare_disclosure_events(
    instruments: Iterable[str],
    *,
    as_of_date: str,
    periods: Iterable[str] | None = None,
    start_year: int = 2015,
    limit: int | None = None,
    offset: int = 0,
    delay: float = 0.2,
    token: str | None = None,
    transport: Transport | None = None,
) -> pd.DataFrame:
    symbols = list(dict.fromkeys(str(item).upper() for item in instruments if str(item).strip()))
    if offset > 0:
        symbols = symbols[offset:]
    if limit is not None:
        symbols = symbols[:limit]
    selected = set(symbols)
    if not selected:
        return pd.DataFrame(
            columns=[
                "event_id",
                "instrument",
                "event_type",
                "event_date",
                "source",
                "source_url",
                "title",
                "severity",
                "summary",
                "evidence",
                "active_until",
            ]
        )
    frames: list[pd.DataFrame] = []
    for period in list(periods or _quarter_periods(start_year=start_year, as_of_date=as_of_date)):
        try:
            raw = call_tushare_api(
                "disclosure_date",
                params={"end_date": _yyyymmdd(period)},
                fields=TUSHARE_DISCLOSURE_DATE_FIELDS,
                token=token,
                transport=transport,
            )
        except Exception as exc:  # pragma: no cover - network/vendor dependent
            print(f"skip tushare_disclosure_date period={period}: {exc}")
            continue
        if raw is None or raw.empty or "ts_code" not in raw.columns:
            continue
        filtered = raw.copy()
        filtered["instrument"] = filtered["ts_code"].map(_safe_qlib_symbol_from_tushare)
        filtered = filtered[filtered["instrument"].isin(selected)]
        if not filtered.empty:
            frames.append(normalize_tushare_disclosure_dates(filtered, as_of_date=as_of_date))
        if delay > 0:
            time.sleep(delay)
    if not frames:
        return normalize_tushare_disclosure_dates(pd.DataFrame(), as_of_date=as_of_date)
    return pd.concat(frames, ignore_index=True).drop_duplicates(["event_id"], keep="last").reset_index(drop=True)


def _fetch_statement_accruals_for_period(
    period: str,
    *,
    selected: set[str],
    token: str | None = None,
    transport: Transport | None = None,
) -> pd.DataFrame:
    params = {"period": _yyyymmdd(period)}
    try:
        income = call_tushare_api("income_vip", params=params, fields=TUSHARE_INCOME_FIELDS, token=token, transport=transport)
        balance = call_tushare_api(
            "balancesheet_vip",
            params=params,
            fields=TUSHARE_BALANCESHEET_FIELDS,
            token=token,
            transport=transport,
        )
        cashflow = call_tushare_api("cashflow_vip", params=params, fields=TUSHARE_CASHFLOW_FIELDS, token=token, transport=transport)
    except Exception as exc:  # pragma: no cover - network/vendor dependent
        print(f"skip tushare_statement_accrual period={period}: {exc}")
        return pd.DataFrame(columns=["instrument", "report_period", "accrual_ratio", "operating_cashflow_to_net_profit"])
    return _derive_statement_quality_metrics(income=income, balance=balance, cashflow=cashflow, selected=selected)


def _derive_statement_quality_metrics(
    *,
    income: pd.DataFrame,
    balance: pd.DataFrame,
    cashflow: pd.DataFrame,
    selected: set[str],
) -> pd.DataFrame:
    required = {"ts_code", "end_date"}
    if income is None or balance is None or cashflow is None:
        return pd.DataFrame(columns=["instrument", "report_period", "accrual_ratio", "operating_cashflow_to_net_profit"])
    if not required <= set(income.columns) or not required <= set(balance.columns) or not required <= set(cashflow.columns):
        return pd.DataFrame(columns=["instrument", "report_period", "accrual_ratio", "operating_cashflow_to_net_profit"])
    if income.empty or balance.empty or cashflow.empty:
        return pd.DataFrame(columns=["instrument", "report_period", "accrual_ratio", "operating_cashflow_to_net_profit"])

    inc = income.copy()
    bal = balance.copy()
    cf = cashflow.copy()
    merged = inc.merge(bal, on=["ts_code", "end_date"], how="inner", suffixes=("_income", "_balance"))
    merged = merged.merge(cf, on=["ts_code", "end_date"], how="inner", suffixes=("", "_cashflow"))
    if merged.empty:
        return pd.DataFrame(columns=["instrument", "report_period", "accrual_ratio", "operating_cashflow_to_net_profit"])
    rows: list[dict[str, object]] = []
    for _, item in merged.iterrows():
        instrument = _safe_qlib_symbol_from_tushare(item.get("ts_code"))
        if not instrument or instrument not in selected:
            continue
        total_assets = _number_from_row(item, ["total_assets"])
        operating_cashflow = _number_from_row(item, ["n_cashflow_act"])
        net_income = _number_from_row(item, ["n_income_attr_p", "n_income"])
        if total_assets in {None, 0} or operating_cashflow is None or net_income is None:
            continue
        report_period = _date_from_row(item, ["end_date"])
        if not report_period:
            continue
        rows.append(
            {
                "instrument": instrument,
                "report_period": report_period,
                "accrual_ratio": (float(net_income) - float(operating_cashflow)) / float(total_assets) * 100.0,
                "operating_cashflow_to_net_profit": (
                    float(operating_cashflow) / float(net_income) * 100.0
                    if float(net_income) != 0.0
                    else None
                ),
                "source": "tushare_statement_accrual",
            }
        )
    if not rows:
        return pd.DataFrame(columns=["instrument", "report_period", "accrual_ratio", "operating_cashflow_to_net_profit", "source"])
    return pd.DataFrame(rows).drop_duplicates(["instrument", "report_period"], keep="last")


def _fill_statement_accruals(fundamentals: pd.DataFrame, accruals: pd.DataFrame) -> pd.DataFrame:
    if fundamentals.empty or accruals.empty:
        return fundamentals
    keep = [
        column
        for column in ["instrument", "report_period", "accrual_ratio", "operating_cashflow_to_net_profit", "source"]
        if column in accruals.columns
    ]
    merged = fundamentals.merge(accruals.loc[:, keep], on=["instrument", "report_period"], how="left", suffixes=("", "_statement"))
    filled = pd.Series(False, index=merged.index)
    for column in ["accrual_ratio", "operating_cashflow_to_net_profit"]:
        current = pd.to_numeric(merged.get(column), errors="coerce")
        derived = pd.to_numeric(merged.get(f"{column}_statement"), errors="coerce")
        column_filled = current.isna() & derived.notna()
        filled = filled | column_filled
        merged[column] = current.combine_first(derived)
    merged["source"] = _append_source_flag(merged.get("source"), filled, "tushare_statement_accrual")
    return merged.drop(
        columns=[
            "accrual_ratio_statement",
            "operating_cashflow_to_net_profit_statement",
            "source_statement",
        ],
        errors="ignore",
    )


def _append_source_flag(current: pd.Series | None, mask: pd.Series, source_name: str) -> pd.Series:
    output = current.fillna("").astype(str).copy() if current is not None else pd.Series("", index=mask.index)
    for index in output.index:
        if not bool(mask.loc[index]):
            continue
        parts = [part for part in output.at[index].split(";") if part]
        if source_name not in parts:
            parts.append(source_name)
        output.at[index] = ";".join(parts)
    return output


def _post_json(endpoint: str, payload: dict[str, object], timeout: float) -> dict[str, object]:
    data = json.dumps(payload).encode("utf-8")
    request = Request(endpoint, data=data, headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _fields_to_string(fields: Iterable[str] | str | None) -> str:
    if fields is None:
        return ""
    if isinstance(fields, str):
        return fields
    return ",".join(str(item) for item in fields)


def _redact(value: str, secret: str) -> str:
    if secret and secret in value:
        return value.replace(secret, "[REDACTED]")
    return value


def _date_from_row(row: pd.Series, columns: Iterable[str]) -> str:
    for column in columns:
        if column not in row:
            continue
        normalized = _normalize_date(row.get(column))
        if normalized:
            return normalized
    return ""


def _normalize_date(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 8:
        return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _yyyymmdd(value: object) -> str:
    normalized = _normalize_date(value)
    if not normalized:
        raise ValueError(f"date must be YYYYMMDD or YYYY-MM-DD: {value}")
    return normalized.replace("-", "")


def _fallback_announce_date(report_period: str) -> str:
    normalized = _normalize_date(report_period)
    if not normalized:
        return ""
    year, month, _day = normalized.split("-")
    current_year = int(year)
    if month == "03":
        return f"{current_year}-04-30"
    if month == "06":
        return f"{current_year}-08-31"
    if month == "09":
        return f"{current_year}-10-31"
    return f"{current_year + 1}-04-30"


def _number_from_row(row: pd.Series, columns: Iterable[str]) -> float | None:
    for column in columns:
        if column not in row:
            continue
        value = pd.to_numeric(row.get(column), errors="coerce")
        if pd.notna(value):
            return float(value)
    return None


def _derive_change_fields(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    for column in FUNDAMENTAL_QUALITY_COLUMNS:
        if column not in output.columns:
            output[column] = pd.NA if column not in {"valuation_source", "source"} else ""
    output["_report_period_dt"] = pd.to_datetime(output["report_period"], errors="coerce")
    output = output.sort_values(["instrument", "_report_period_dt", "report_period"])
    for output_column, source_column in {
        "gross_margin_change_yoy": "gross_margin",
        "revenue_growth_change_yoy": "revenue_growth_yoy",
        "net_profit_growth_change_yoy": "net_profit_growth_yoy",
        "cashflow_growth_change_yoy": "cashflow_growth_yoy",
    }.items():
        current = pd.to_numeric(output[source_column], errors="coerce")
        previous = current.groupby(output["instrument"].astype(str)).shift(1)
        existing = pd.to_numeric(output[output_column], errors="coerce")
        output[output_column] = existing.combine_first(current - previous)
    return output.drop(columns=["_report_period_dt"], errors="ignore").loc[:, FUNDAMENTAL_QUALITY_COLUMNS]


def _quarter_periods(*, start_year: int, as_of_date: str) -> list[str]:
    cutoff = _yyyymmdd(as_of_date)
    periods: list[str] = []
    current_year = dt.datetime.strptime(cutoff, "%Y%m%d").year
    for year in range(int(start_year), current_year + 1):
        for suffix in ["0331", "0630", "0930", "1231"]:
            period = f"{year}{suffix}"
            if period <= cutoff:
                periods.append(period)
    return periods


def _safe_qlib_symbol_from_tushare(ts_code: object) -> str:
    try:
        return qlib_symbol_from_tushare(str(ts_code))
    except ValueError:
        return ""


def _namechange_rows_for_code(namechange: pd.DataFrame, ts_code: str) -> pd.DataFrame:
    if namechange is None or namechange.empty or "ts_code" not in namechange.columns:
        return pd.DataFrame(columns=TUSHARE_NAMECHANGE_FIELDS)
    frame = namechange[namechange["ts_code"].astype(str).str.upper() == str(ts_code).upper()].copy()
    if frame.empty:
        return frame
    if "start_date" not in frame.columns:
        frame["start_date"] = ""
    frame["_start"] = frame["start_date"].map(_normalize_date)
    return frame.sort_values(["_start", "name"]).drop(columns=["_start"], errors="ignore")


def _exchange_from_tushare(ts_code: str, exchange: object) -> str:
    value = str(exchange or "").strip().upper()
    if value in {"SSE", "SZSE", "BSE"}:
        return value
    text = str(ts_code).upper()
    if text.endswith(".SH"):
        return "SSE"
    if text.endswith(".SZ"):
        return "SZSE"
    if text.endswith(".BJ"):
        return "BSE"
    return value


def _is_st_name(name: object) -> bool:
    text = str(name or "").upper().replace(" ", "")
    return "ST" in text or "退" in text


def _write_symbol_csv(frame: pd.DataFrame, output_dir: str | Path, symbol: str) -> Path:
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"{str(symbol).lower()}.csv"
    frame.to_csv(path, index=False)
    return path
