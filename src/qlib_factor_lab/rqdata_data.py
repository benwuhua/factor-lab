from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd


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


@dataclass(frozen=True)
class RQDataCredentials:
    username: str
    password: str
    uri: str | None = None


def rqdata_code_from_qlib(symbol: str) -> str:
    text = str(symbol).strip().upper()
    if text.startswith("SH") and len(text) == 8:
        return f"{text[2:]}.XSHG"
    if text.startswith("SZ") and len(text) == 8:
        return f"{text[2:]}.XSHE"
    if text.endswith((".XSHG", ".XSHE")):
        return text
    raise ValueError(f"unsupported qlib A-share symbol: {symbol}")


def qlib_symbol_from_rqdata(order_book_id: str) -> str:
    text = str(order_book_id).strip().upper()
    if text.endswith(".XSHG"):
        return f"SH{text[:6]}"
    if text.endswith(".XSHE"):
        return f"SZ{text[:6]}"
    if text.startswith(("SH", "SZ")) and len(text) == 8:
        return text
    raise ValueError(f"unsupported RQData A-share order_book_id: {order_book_id}")


def credentials_from_env(env: Mapping[str, str] | None = None) -> RQDataCredentials:
    source = os.environ if env is None else env
    username = source.get("RQDATA_USERNAME") or source.get("RQDATAC_USERNAME") or ""
    password = source.get("RQDATA_PASSWORD") or source.get("RQDATAC_PASSWORD") or ""
    uri = source.get("RQDATA_URI") or source.get("RQDATAC_URI") or None
    if not username or not password:
        raise RuntimeError("RQData credentials are required: set RQDATA_USERNAME and RQDATA_PASSWORD")
    return RQDataCredentials(username=username, password=password, uri=uri)


def init_rqdata_client(credentials: RQDataCredentials | None = None):
    try:
        import rqdatac  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional vendor dependency
        raise RuntimeError("rqdatac is required for live RQData export: pip install rqdatac") from exc
    creds = credentials or credentials_from_env()
    if creds.uri:
        rqdatac.init(creds.username, creds.password, uri=creds.uri)
    else:
        rqdatac.init(creds.username, creds.password)
    return rqdatac


def normalize_rqdata_instruments(
    instruments: pd.DataFrame,
    *,
    as_of_date: str,
    research_universe: str = "",
) -> pd.DataFrame:
    if instruments is None or instruments.empty:
        return pd.DataFrame(columns=SECURITY_MASTER_HISTORY_COLUMNS)

    rows: list[dict[str, Any]] = []
    for _, item in instruments.iterrows():
        order_book_id = str(item.get("order_book_id", "")).strip()
        if not order_book_id.endswith((".XSHG", ".XSHE")):
            continue
        rows.append(
            {
                "instrument": qlib_symbol_from_rqdata(order_book_id),
                "name": str(item.get("symbol", "")),
                "exchange": _exchange_from_rqdata(item.get("exchange", "")),
                "board": str(item.get("board_type", "")),
                "industry_sw": str(item.get("industry_code", "")),
                "industry_csrc": str(item.get("industry_name", "")),
                "is_st": _is_st_value(item.get("special_type", "")),
                "listing_date": _normalize_date(item.get("listed_date", "")),
                "delisting_date": _normalize_delisting_date(item.get("de_listed_date", "")),
                "valid_from": _normalize_date(as_of_date),
                "valid_to": "",
                "research_universes": research_universe,
                "source": "rqdata_instruments",
                "as_of_date": _normalize_date(as_of_date),
            }
        )
    return pd.DataFrame(rows, columns=SECURITY_MASTER_HISTORY_COLUMNS)


def build_security_master_history_from_rqdata(
    client,
    *,
    instruments: Iterable[str],
    start_date: str,
    end_date: str,
    as_of_date: str,
    research_universe: str = "",
    industry_source: str = "sws",
    industry_level: int = 1,
) -> pd.DataFrame:
    order_book_ids = [rqdata_code_from_qlib(item) for item in dict.fromkeys(str(item).upper() for item in instruments)]
    dates = pd.date_range(_normalize_date(start_date), _normalize_date(end_date), freq="D")
    rows: list[dict[str, Any]] = []
    for current_date in dates:
        day = current_date.strftime("%Y-%m-%d")
        master = normalize_rqdata_instruments(
            client.all_instruments(type="CS", date=day, market="cn"),
            as_of_date=day,
            research_universe=research_universe,
        )
        if master.empty:
            continue
        master = master[master["instrument"].isin({qlib_symbol_from_rqdata(item) for item in order_book_ids})].copy()
        if master.empty:
            continue
        industry = _fetch_industry_snapshot(
            client,
            order_book_ids=order_book_ids,
            date=day,
            source=industry_source,
            level=industry_level,
        )
        st = _fetch_st_snapshot(client, order_book_ids=order_book_ids, date=day)
        for _, item in master.iterrows():
            rq_code = rqdata_code_from_qlib(item["instrument"])
            industry_item = industry.get(rq_code, {})
            row = item.to_dict()
            row["industry_sw"] = str(industry_item.get("name") or industry_item.get("code") or row.get("industry_sw", ""))
            row["industry_csrc"] = str(row.get("industry_csrc", ""))
            row["is_st"] = bool(st.get(rq_code, row.get("is_st", False)))
            row["valid_from"] = day
            row["valid_to"] = ""
            row["source"] = "rqdata_pit"
            row["as_of_date"] = _normalize_date(as_of_date)
            rows.append(row)
    if not rows:
        return pd.DataFrame(columns=SECURITY_MASTER_HISTORY_COLUMNS)
    frame = pd.DataFrame(rows, columns=SECURITY_MASTER_HISTORY_COLUMNS)
    return _collapse_security_master_history(frame)


def write_security_master_history_from_rqdata(
    output_path: str | Path,
    *,
    client=None,
    credentials: RQDataCredentials | None = None,
    instruments: Iterable[str],
    start_date: str,
    end_date: str,
    as_of_date: str,
    research_universe: str = "",
    industry_source: str = "sws",
    industry_level: int = 1,
) -> Path:
    actual_client = client or init_rqdata_client(credentials)
    frame = build_security_master_history_from_rqdata(
        actual_client,
        instruments=instruments,
        start_date=start_date,
        end_date=end_date,
        as_of_date=as_of_date,
        research_universe=research_universe,
        industry_source=industry_source,
        industry_level=industry_level,
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return output


def _fetch_industry_snapshot(client, *, order_book_ids: list[str], date: str, source: str, level: int) -> dict[str, dict[str, str]]:
    try:
        raw = client.get_instrument_industry(order_book_ids, source=source, level=level, date=date, market="cn")
    except TypeError:
        raw = client.get_instrument_industry(order_book_ids, source=source, level=level, date=date)
    if raw is None:
        return {}
    if isinstance(raw, pd.DataFrame):
        return _industry_from_frame(raw)
    if isinstance(raw, pd.Series):
        return {str(index): _industry_item(value) for index, value in raw.items()}
    if isinstance(raw, dict):
        return {str(key): _industry_item(value) for key, value in raw.items()}
    return {}


def _fetch_st_snapshot(client, *, order_book_ids: list[str], date: str) -> dict[str, bool]:
    raw = client.is_st_stock(order_book_ids, start_date=date, end_date=date, market="cn")
    if raw is None or getattr(raw, "empty", False):
        return {}
    if isinstance(raw, pd.DataFrame):
        row = raw.iloc[-1]
        return {str(column): bool(row[column]) for column in raw.columns}
    if isinstance(raw, pd.Series):
        return {str(index): bool(value) for index, value in raw.items()}
    return {}


def _industry_from_frame(frame: pd.DataFrame) -> dict[str, dict[str, str]]:
    output: dict[str, dict[str, str]] = {}
    work = frame.reset_index()
    for _, item in work.iterrows():
        code = str(item.get("order_book_id", item.get("index", "")))
        if not code:
            continue
        output[code] = {
            "name": str(item.get("name", item.get("industry_name", ""))),
            "code": str(item.get("code", item.get("industry_code", ""))),
        }
    return output


def _industry_item(value: object) -> dict[str, str]:
    if isinstance(value, dict):
        return {"name": str(value.get("name", value.get("industry_name", ""))), "code": str(value.get("code", ""))}
    return {
        "name": str(getattr(value, "name", "")),
        "code": str(getattr(value, "code", "")),
    }


def _collapse_security_master_history(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    compare_columns = [
        "name",
        "exchange",
        "board",
        "industry_sw",
        "industry_csrc",
        "is_st",
        "listing_date",
        "delisting_date",
        "research_universes",
    ]
    for instrument, group in frame.sort_values(["instrument", "valid_from"]).groupby("instrument", sort=False):
        active: dict[str, Any] | None = None
        previous_date: pd.Timestamp | None = None
        for _, item in group.iterrows():
            current = item.to_dict()
            current_date = pd.Timestamp(current["valid_from"])
            if active is None:
                active = current
                previous_date = current_date
                continue
            same = all(str(active.get(column, "")) == str(current.get(column, "")) for column in compare_columns)
            if same:
                previous_date = current_date
                continue
            active["valid_to"] = (current_date - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            rows.append(active)
            active = current
            previous_date = current_date
        if active is not None:
            active["valid_to"] = ""
            rows.append(active)
    return pd.DataFrame(rows, columns=SECURITY_MASTER_HISTORY_COLUMNS)


def _exchange_from_rqdata(exchange: object) -> str:
    text = str(exchange).upper()
    if text == "XSHG":
        return "SSE"
    if text == "XSHE":
        return "SZSE"
    return text


def _is_st_value(value: object) -> bool:
    text = str(value).strip().lower()
    return text in {"st", "starst", "*st", "pt"} or "st" in text


def _normalize_delisting_date(value: object) -> str:
    text = str(value or "").strip()
    if text in {"", "0000-00-00", "None", "nan", "NaT"}:
        return ""
    return _normalize_date(text)


def _normalize_date(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")
