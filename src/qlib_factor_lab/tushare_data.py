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
    "gross_margin",
    "debt_ratio",
    "revenue_growth_yoy",
    "net_profit_growth_yoy",
    "operating_cashflow_to_net_profit",
    "eps",
    "operating_cashflow_per_share",
    "ep",
    "cfp",
    "dividend_yield",
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
    "debt_to_assets",
    "or_yoy",
    "netprofit_yoy",
    "ocf_to_np",
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
        "fields": ["ts_code", "ann_date", "end_date", "total_revenue", "n_income_attr_p"],
    },
    {
        "api_name": "balancesheet_vip",
        "params": {"period": "20260331"},
        "fields": ["ts_code", "ann_date", "end_date", "total_assets", "total_liab"],
    },
    {
        "api_name": "cashflow_vip",
        "params": {"period": "20260331"},
        "fields": ["ts_code", "ann_date", "end_date", "n_cashflow_act"],
    },
    {
        "api_name": "dividend",
        "params": {"ann_date": "20250430"},
        "fields": ["ts_code", "ann_date", "end_date", "cash_div_tax"],
    },
    {
        "api_name": "disclosure_date",
        "params": {"end_date": "20260331"},
        "fields": ["ts_code", "ann_date", "end_date", "pre_date", "actual_date"],
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
        except Exception as exc:  # pragma: no cover - network/vendor dependent
            print(f"skip tushare history {symbol}: {exc}")
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
                "gross_margin": _number_from_row(item, ["grossprofit_margin", "gross_margin"]),
                "debt_ratio": _number_from_row(item, ["debt_to_assets", "debt_asset_ratio"]),
                "revenue_growth_yoy": _number_from_row(item, ["or_yoy", "tr_yoy", "revenue_yoy"]),
                "net_profit_growth_yoy": _number_from_row(item, ["netprofit_yoy", "dt_netprofit_yoy"]),
                "operating_cashflow_to_net_profit": _number_from_row(item, ["ocf_to_np", "ocf_to_profit", "cashflow_to_np"]),
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
    return frame.sort_values(["instrument", "report_period", "announce_date"]).drop_duplicates(
        ["instrument", "report_period"],
        keep="last",
    )


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
        if delay > 0:
            time.sleep(delay)
    if not frames:
        return pd.DataFrame(columns=FUNDAMENTAL_QUALITY_COLUMNS)
    return pd.concat(frames, ignore_index=True).sort_values(["instrument", "report_period"]).drop_duplicates(
        ["instrument", "report_period"],
        keep="last",
    )


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


def _write_symbol_csv(frame: pd.DataFrame, output_dir: str | Path, symbol: str) -> Path:
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"{str(symbol).lower()}.csv"
    frame.to_csv(path, index=False)
    return path
