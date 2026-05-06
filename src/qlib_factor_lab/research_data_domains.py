from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .akshare_data import akshare_code_from_qlib, classify_notice_event, qlib_symbol_from_code
from .company_events import COMPANY_EVENT_COLUMNS
from .tushare_data import fetch_fundamental_quality_from_tushare, fetch_tushare_disclosure_events, fetch_tushare_dividends


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
    "financial_disclosure_recency_30d",
    "financial_disclosure_days_since",
    "financial_disclosure_positive_score_90d",
    "financial_disclosure_negative_score_90d",
    "financial_disclosure_event_score_90d",
    "valuation_source",
    "source",
]

CNINFO_DIVIDEND_COLUMNS = [
    "instrument",
    "announce_date",
    "available_at",
    "dividend_cash_per_10",
    "source",
]

SHAREHOLDER_CAPITAL_COLUMNS = [
    "instrument",
    "event_date",
    "announce_date",
    "event_type",
    "severity",
    "title",
    "source_url",
    "active_until",
    "available_at",
    "source",
]

ANNOUNCEMENT_EVIDENCE_COLUMNS = [
    "event_id",
    "instrument",
    "event_type",
    "event_date",
    "available_at",
    "severity",
    "title",
    "document_title",
    "source",
    "source_tier",
    "official_source",
    "source_url",
    "evidence_url",
    "chunk_id",
    "chunk_text",
    "keywords",
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

CAPITAL_EVENT_TYPES = {
    "shareholder_reduction",
    "shareholder_increase",
    "buyback",
    "large_unlock",
    "pledge_risk",
    "pledge_release",
    "capital_structure_change",
    "holder_count_change",
}

POSITIVE_DISCLOSURE_EVENT_TYPES = {
    "buyback",
    "shareholder_increase",
    "order_contract",
    "earnings_preannouncement_up",
    "equity_incentive",
    "performance_warning_up",
    "performance_warning_repair",
    "dividend_plan",
}

NEGATIVE_DISCLOSURE_EVENT_TYPES = {
    "shareholder_reduction",
    "large_unlock",
    "regulatory_inquiry",
    "pledge_risk",
    "guarantee",
    "lawsuit",
    "disciplinary_action",
    "investigation",
    "st_risk",
    "delisting_risk",
    "nonstandard_audit",
    "major_penalty",
    "performance_warning_down",
    "impairment",
}

DISCLOSURE_SEVERITY_WEIGHTS = {
    "info": 0.5,
    "watch": 0.7,
    "risk": 1.0,
    "block": 1.5,
}


@dataclass(frozen=True)
class ResearchDataDomainPaths:
    fundamental_quality: Path
    shareholder_capital: Path
    announcement_evidence: Path
    security_master_history: Path


def normalize_fundamental_quality(raw: pd.DataFrame, *, as_of_date: str, source: str = "akshare_financial_indicator") -> pd.DataFrame:
    if raw is None or raw.empty:
        return _empty(FUNDAMENTAL_QUALITY_COLUMNS)

    rows: list[dict[str, Any]] = []
    for _, item in raw.iterrows():
        instrument = _instrument_from_row(item)
        if not instrument:
            continue
        report_period = _date_from_row(item, ["report_period", "报告期", "日期", "截止日期", "报告日期"])
        if not report_period:
            continue
        announce_date = _date_from_row(item, ["announce_date", "公告日期", "披露日期", "公布日期"]) or _fallback_announce_date(report_period)
        available_at = _date_from_row(item, ["available_at", "可用日期", "as_of_date"]) or announce_date
        rows.append(
            {
                "instrument": instrument,
                "report_period": report_period,
                "announce_date": announce_date,
                "available_at": available_at,
                "roe": _number_from_row(item, ["roe", "净资产收益率", "净资产收益率(%)", "加权净资产收益率"]),
                "roic": _number_from_row(item, ["roic", "ROIC", "投入资本回报率", "投入资本回报率(%)"]),
                "gross_margin": _number_from_row(item, ["gross_margin", "销售毛利率", "毛利率", "销售毛利率(%)"]),
                "debt_ratio": _number_from_row(item, ["debt_ratio", "资产负债率", "资产负债率(%)"]),
                "revenue_growth_yoy": _number_from_row(item, ["revenue_growth_yoy", "营业收入同比增长率", "营业总收入同比增长率", "营业收入同比增长率(%)"]),
                "net_profit_growth_yoy": _number_from_row(item, ["net_profit_growth_yoy", "净利润同比增长率", "归母净利润同比增长率", "净利润同比增长率(%)"]),
                "cashflow_growth_yoy": _number_from_row(
                    item,
                    ["cashflow_growth_yoy", "经营现金流同比增长率", "经营现金流量净额同比增长率", "经营活动现金流量净额同比增长率"],
                ),
                "operating_cashflow_to_net_profit": _number_from_row(
                    item,
                    ["operating_cashflow_to_net_profit", "经营现金流量净额/净利润", "经营现金流净额/净利润"],
                ),
                "accrual_ratio": _number_from_row(item, ["accrual_ratio", "应计比率", "应计项目比率"]),
                "eps": _number_from_row(item, ["eps", "EPS", "摊薄每股收益(元)", "加权每股收益(元)", "基本每股收益", "每股收益"]),
                "operating_cashflow_per_share": _number_from_row(
                    item,
                    ["operating_cashflow_per_share", "每股经营性现金流(元)", "每股经营现金流", "每股经营活动现金流量净额"],
                ),
                "ep": _valuation_or_derived_ratio(
                    item,
                    ["ep", "EP", "earnings_to_price", "盈利收益率", "市盈率倒数"],
                    ["eps", "EPS", "摊薄每股收益(元)", "加权每股收益(元)", "基本每股收益", "每股收益"],
                ),
                "cfp": _valuation_or_derived_ratio(
                    item,
                    ["cfp", "CFP", "cashflow_to_price", "operating_cashflow_to_market_cap", "经营现金流市值比", "现金流收益率"],
                    ["operating_cashflow_per_share", "每股经营性现金流(元)", "每股经营现金流", "每股经营活动现金流量净额"],
                ),
                "dividend_yield": _dividend_yield_from_row(item),
                "valuation_source": "",
                "source": source,
            }
        )
    if not rows:
        return _empty(FUNDAMENTAL_QUALITY_COLUMNS)
    frame = pd.DataFrame(rows, columns=FUNDAMENTAL_QUALITY_COLUMNS)
    frame = frame.sort_values(["instrument", "report_period"]).drop_duplicates(["instrument", "report_period"], keep="last")
    return derive_fundamental_quality_fields(frame)


def normalize_cninfo_dividend(raw: pd.DataFrame, *, instrument: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return _empty(CNINFO_DIVIDEND_COLUMNS)
    rows: list[dict[str, Any]] = []
    for _, item in raw.iterrows():
        announce_date = _date_from_row(item, ["announce_date", "实施方案公告日期", "公告日期"])
        available_at = _date_from_row(item, ["available_at", "除权日", "股权登记日", "派息日"]) or announce_date
        cash = _number_from_row(item, ["dividend_cash_per_10", "派息比例", "每10股派息", "每十股派息"])
        if not available_at or cash is None:
            continue
        rows.append(
            {
                "instrument": instrument,
                "announce_date": announce_date,
                "available_at": available_at,
                "dividend_cash_per_10": cash,
                "source": "cninfo_dividend",
            }
        )
    if not rows:
        return _empty(CNINFO_DIVIDEND_COLUMNS)
    return pd.DataFrame(rows, columns=CNINFO_DIVIDEND_COLUMNS).sort_values(["instrument", "available_at"])


def derive_fundamental_valuation_fields(
    fundamentals: pd.DataFrame,
    *,
    prices: pd.DataFrame,
    dividends: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if fundamentals is None or fundamentals.empty:
        return _empty(FUNDAMENTAL_QUALITY_COLUMNS)
    frame = fundamentals.copy()
    for column in FUNDAMENTAL_QUALITY_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    price_frame = _normalize_price_frame(prices)
    if price_frame.empty:
        return frame.loc[:, FUNDAMENTAL_QUALITY_COLUMNS]

    work = frame.reset_index(drop=True).copy()
    work["_row_id"] = work.index
    work["_available_at_dt"] = pd.to_datetime(work["available_at"], errors="coerce")
    pieces = []
    for instrument, group in work.dropna(subset=["_available_at_dt"]).sort_values("_available_at_dt").groupby("instrument", sort=False):
        source = price_frame[price_frame["instrument"].astype(str) == str(instrument)].sort_values("trade_date")
        if source.empty:
            pieces.append(group)
            continue
        pieces.append(
            pd.merge_asof(
                group.sort_values("_available_at_dt"),
                source,
                left_on="_available_at_dt",
                right_on="trade_date",
                by="instrument",
                direction="backward",
            )
        )
    merged = pd.concat(pieces, ignore_index=True) if pieces else work
    merged = _attach_latest_dividend(merged, dividends)
    close = pd.to_numeric(merged.get("close"), errors="coerce")
    eps = pd.to_numeric(merged.get("eps"), errors="coerce")
    cfps = pd.to_numeric(merged.get("operating_cashflow_per_share"), errors="coerce")
    cash_per_10 = pd.to_numeric(merged.get("dividend_cash_per_10"), errors="coerce")
    ep_current = pd.to_numeric(merged.get("ep"), errors="coerce")
    cfp_current = pd.to_numeric(merged.get("cfp"), errors="coerce")
    dividend_current = pd.to_numeric(merged.get("dividend_yield"), errors="coerce")
    ep_derived = eps / close * 100.0
    cfp_derived = cfps / close * 100.0
    dividend_derived = cash_per_10 / 10.0 / close * 100.0
    ep_filled = ep_current.isna() & pd.to_numeric(ep_derived, errors="coerce").notna()
    cfp_filled = cfp_current.isna() & pd.to_numeric(cfp_derived, errors="coerce").notna()
    dividend_filled = dividend_current.isna() & pd.to_numeric(dividend_derived, errors="coerce").notna()
    merged["ep"] = _fill_missing_numeric(merged.get("ep"), ep_derived)
    merged["cfp"] = _fill_missing_numeric(merged.get("cfp"), cfp_derived)
    merged["dividend_yield"] = _fill_missing_numeric(merged.get("dividend_yield"), dividend_derived)
    source = merged.get("valuation_source", pd.Series("", index=merged.index)).astype(str)
    merged["valuation_source"] = _append_valuation_sources(
        source,
        ep_filled=ep_filled,
        cfp_filled=cfp_filled,
        dividend_filled=dividend_filled,
    )
    output = work.drop(columns=[col for col in ["_available_at_dt"] if col in work.columns]).merge(
        merged[["_row_id", "ep", "cfp", "dividend_yield", "valuation_source"]],
        on="_row_id",
        how="left",
        suffixes=("", "_derived"),
    )
    for column in ["ep", "cfp", "dividend_yield", "valuation_source"]:
        derived = output[f"{column}_derived"]
        current = output[column]
        missing = current.isna() | current.astype(str).str.strip().eq("")
        output[column] = current.mask(missing, derived)
        output = output.drop(columns=[f"{column}_derived"])
    output = output.drop(columns=["_row_id"], errors="ignore")
    return output.loc[:, FUNDAMENTAL_QUALITY_COLUMNS]


def derive_fundamental_quality_fields(
    fundamentals: pd.DataFrame,
    dividends: pd.DataFrame | None = None,
    disclosures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if fundamentals is None or fundamentals.empty:
        return _empty(FUNDAMENTAL_QUALITY_COLUMNS)
    frame = fundamentals.copy()
    for column in FUNDAMENTAL_QUALITY_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA if column not in {"valuation_source", "source"} else ""

    frame["_report_period_dt"] = pd.to_datetime(frame["report_period"], errors="coerce")
    frame = frame.sort_values(["instrument", "_report_period_dt", "report_period"])
    change_pairs = {
        "gross_margin_change_yoy": "gross_margin",
        "revenue_growth_change_yoy": "revenue_growth_yoy",
        "net_profit_growth_change_yoy": "net_profit_growth_yoy",
        "cashflow_growth_change_yoy": "cashflow_growth_yoy",
    }
    for output_column, source_column in change_pairs.items():
        current = pd.to_numeric(frame[source_column], errors="coerce")
        previous = current.groupby(frame["instrument"].astype(str)).shift(1)
        existing = pd.to_numeric(frame[output_column], errors="coerce")
        frame[output_column] = existing.combine_first(current - previous)

    frame = _derive_dividend_quality_fields(frame, dividends)
    frame = _derive_financial_disclosure_fields(frame, disclosures)
    frame = frame.drop(columns=["_report_period_dt"], errors="ignore")
    return frame.loc[:, FUNDAMENTAL_QUALITY_COLUMNS]


def build_security_master_history(
    security_master: pd.DataFrame,
    *,
    prices: pd.DataFrame | None = None,
    history_source: pd.DataFrame | None = None,
    as_of_date: str,
) -> pd.DataFrame:
    if (security_master is None or security_master.empty) and (history_source is None or history_source.empty):
        return _empty(SECURITY_MASTER_HISTORY_COLUMNS)

    source_history = _normalize_security_master_history_source(history_source, as_of_date=as_of_date)
    source_instruments = set(source_history["instrument"].astype(str)) if not source_history.empty else set()

    frame = security_master.copy() if security_master is not None else pd.DataFrame()
    for column in SECURITY_MASTER_HISTORY_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""

    first_trade_dates: dict[str, str] = {}
    if prices is not None and not prices.empty and {"instrument", "trade_date"} <= set(prices.columns):
        price_frame = prices.copy()
        price_frame["_trade_date"] = pd.to_datetime(price_frame["trade_date"], errors="coerce")
        first_trade_dates = (
            price_frame.dropna(subset=["_trade_date"])
            .sort_values(["instrument", "_trade_date"])
            .groupby(price_frame["instrument"].astype(str))["_trade_date"]
            .first()
            .dt.strftime("%Y-%m-%d")
            .to_dict()
        )

    rows: list[dict[str, Any]] = []
    normalized_as_of = _normalize_date(as_of_date)
    for _, item in frame.iterrows():
        instrument = str(item.get("instrument", "")).strip().upper()
        if not instrument or instrument in source_instruments:
            continue
        listing_date = _normalize_date(item.get("listing_date", ""))
        first_trade = first_trade_dates.get(instrument, "")
        existing_valid_from = _normalize_date(item.get("valid_from", ""))
        valid_from = listing_date or first_trade or existing_valid_from or normalized_as_of
        if first_trade and (not listing_date) and (not existing_valid_from or existing_valid_from >= normalized_as_of):
            valid_from = first_trade
        source = "security_master_snapshot"
        if first_trade and valid_from == first_trade:
            source = "current_snapshot_backfilled"
        rows.append(
            {
                "instrument": instrument,
                "name": str(item.get("name", "")),
                "exchange": str(item.get("exchange", "")),
                "board": str(item.get("board", "")),
                "industry_sw": str(item.get("industry_sw", "")),
                "industry_csrc": str(item.get("industry_csrc", "")),
                "is_st": item.get("is_st", ""),
                "listing_date": listing_date,
                "delisting_date": _normalize_date(item.get("delisting_date", "")),
                "valid_from": valid_from,
                "valid_to": _normalize_date(item.get("valid_to", "")),
                "research_universes": str(item.get("research_universes", "")),
                "source": source,
                "as_of_date": normalized_as_of,
            }
        )
    if not rows:
        fallback = _empty(SECURITY_MASTER_HISTORY_COLUMNS)
    else:
        fallback = pd.DataFrame(rows, columns=SECURITY_MASTER_HISTORY_COLUMNS)
    return (
        pd.concat([source_history, fallback], ignore_index=True)
        .drop_duplicates(["instrument", "valid_from"], keep="last")
        .sort_values(["instrument", "valid_from"])
        .reset_index(drop=True)
    )


def _normalize_security_master_history_source(history_source: pd.DataFrame | None, *, as_of_date: str) -> pd.DataFrame:
    if history_source is None or history_source.empty:
        return _empty(SECURITY_MASTER_HISTORY_COLUMNS)
    rows: list[dict[str, Any]] = []
    normalized_as_of = _normalize_date(as_of_date)
    for _, item in history_source.iterrows():
        instrument = _instrument_from_row(item)
        valid_from = _date_from_row(item, ["valid_from", "生效日期", "start_date", "from_date"])
        if not instrument or not valid_from:
            continue
        rows.append(
            {
                "instrument": instrument,
                "name": str(item.get("name", item.get("名称", ""))),
                "exchange": str(item.get("exchange", "")),
                "board": str(item.get("board", "")),
                "industry_sw": str(item.get("industry_sw", item.get("申万行业", ""))),
                "industry_csrc": str(item.get("industry_csrc", item.get("证监会行业", ""))),
                "is_st": item.get("is_st", item.get("ST状态", "")),
                "listing_date": _date_from_row(item, ["listing_date", "上市日期", "list_date"]),
                "delisting_date": _date_from_row(item, ["delisting_date", "退市日期", "delist_date"]),
                "valid_from": valid_from,
                "valid_to": _date_from_row(item, ["valid_to", "失效日期", "end_date", "to_date"]),
                "research_universes": str(item.get("research_universes", item.get("universe", ""))),
                "source": str(item.get("source", "")).strip() or "external_pit_history",
                "as_of_date": _date_from_row(item, ["as_of_date", "数据日期"]) or normalized_as_of,
            }
        )
    if not rows:
        return _empty(SECURITY_MASTER_HISTORY_COLUMNS)
    return pd.DataFrame(rows, columns=SECURITY_MASTER_HISTORY_COLUMNS)


def build_shareholder_capital_from_events(events: pd.DataFrame, *, as_of_date: str) -> pd.DataFrame:
    if events is None or events.empty:
        return _empty(SHAREHOLDER_CAPITAL_COLUMNS)
    frame = events.copy()
    if "event_type" not in frame.columns:
        return _empty(SHAREHOLDER_CAPITAL_COLUMNS)
    frame = _with_inferred_event_types(frame)
    frame = frame[frame["event_type"].astype(str).isin(CAPITAL_EVENT_TYPES)].copy()
    if frame.empty:
        return _empty(SHAREHOLDER_CAPITAL_COLUMNS)
    for column in SHAREHOLDER_CAPITAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame["announce_date"] = frame["announce_date"] if "announce_date" in frame.columns else frame.get("event_date", "")
    frame.loc[frame["announce_date"].astype(str).str.strip() == "", "announce_date"] = frame.get("event_date", "")
    frame["available_at"] = [
        _event_available_date(row, fallback_as_of_date=as_of_date)
        for _, row in frame.iterrows()
    ]
    frame["source"] = frame["source"].where(frame["source"].astype(str).str.strip() != "", "company_events")
    return frame.loc[:, SHAREHOLDER_CAPITAL_COLUMNS].sort_values(["instrument", "event_date", "event_type"])


def build_announcement_evidence_index(
    events: pd.DataFrame,
    *,
    as_of_date: str,
    chunk_size: int = 220,
    lookback_days: int | None = None,
) -> pd.DataFrame:
    if events is None or events.empty:
        return _empty(ANNOUNCEMENT_EVIDENCE_COLUMNS)

    rows: list[dict[str, Any]] = []
    events = _with_inferred_event_types(events)
    as_of_ts = pd.Timestamp(_normalize_date(as_of_date))
    window_start = as_of_ts - pd.Timedelta(days=int(lookback_days)) if lookback_days is not None else None
    for idx, item in events.iterrows():
        available_at = _event_available_date(item, fallback_as_of_date=as_of_date)
        available_ts = pd.to_datetime(available_at, errors="coerce")
        if pd.isna(available_ts) or available_ts > as_of_ts:
            continue
        if window_start is not None and available_ts < window_start:
            continue
        event_id = str(item.get("event_id", "") or f"event_{idx}")
        title = _clean_text(item.get("title", ""))
        text = " ".join(
            part
            for part in [
                title,
                _clean_text(item.get("summary", "")),
                _clean_text(item.get("evidence", "")),
            ]
            if part
        )
        if not text:
            continue
        chunks = _chunk_text(text, max(1, int(chunk_size)))
        keywords = _keywords(text)
        source = str(item.get("source", "")).strip()
        source_url = str(item.get("source_url", "")).strip()
        source_tier = _announcement_source_tier(source=source, source_url=source_url)
        for chunk_index, chunk in enumerate(chunks):
            rows.append(
                {
                    "event_id": event_id,
                    "instrument": str(item.get("instrument", "")),
                    "event_type": str(item.get("event_type", "")),
                    "event_date": _normalize_date(item.get("event_date", "")),
                    "available_at": available_at,
                    "severity": str(item.get("severity", "")),
                    "title": title,
                    "document_title": title,
                    "source": source,
                    "source_tier": source_tier,
                    "official_source": source_tier == "official",
                    "source_url": source_url,
                    "evidence_url": source_url,
                    "chunk_id": f"{event_id}_{chunk_index:03d}",
                    "chunk_text": chunk,
                    "keywords": ",".join(keywords),
                }
            )
    if not rows:
        return _empty(ANNOUNCEMENT_EVIDENCE_COLUMNS)
    return pd.DataFrame(rows, columns=ANNOUNCEMENT_EVIDENCE_COLUMNS)


def _announcement_source_tier(*, source: str, source_url: str) -> str:
    text = f"{source} {source_url}".lower()
    if any(token in text for token in ("cninfo.com.cn", "sse.com.cn", "szse.cn", "bse.cn", "official_exchange")):
        return "official"
    if any(token in text for token in ("akshare", "eastmoney", "10jqka", "ifind", "choice", "wind")):
        return "aggregator"
    return "unknown"


def _event_available_date(item: pd.Series, *, fallback_as_of_date: str) -> str:
    for column in ["available_at", "announce_date", "event_date"]:
        value = item.get(column, "")
        normalized = _normalize_date(value)
        if normalized:
            return normalized
    return _normalize_date(fallback_as_of_date)


def _with_inferred_event_types(events: pd.DataFrame) -> pd.DataFrame:
    if events is None or events.empty or "title" not in events.columns:
        return events
    frame = events.copy()
    if "event_type" not in frame.columns:
        frame["event_type"] = ""
    if "severity" not in frame.columns:
        frame["severity"] = ""
    for index, row in frame.iterrows():
        current_type = str(row.get("event_type", "")).strip()
        inferred_type, inferred_severity = classify_notice_event(
            str(row.get("title", "")),
            str(row.get("category", "") or row.get("event_category", "")),
        )
        if current_type in {"", "announcement"} and inferred_type != "announcement":
            frame.at[index, "event_type"] = inferred_type
            if not str(row.get("severity", "")).strip() or str(row.get("severity", "")).strip() == "info":
                frame.at[index, "severity"] = inferred_severity
    return frame


def fetch_fundamental_quality_from_akshare(
    instruments: Iterable[str],
    *,
    as_of_date: str,
    limit: int | None = None,
    offset: int = 0,
    delay: float = 0.2,
) -> pd.DataFrame:
    try:
        import akshare as ak  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on optional local env
        raise RuntimeError("akshare is required to fetch live fundamental quality data") from exc

    frames: list[pd.DataFrame] = []
    symbols = list(dict.fromkeys(str(item).upper() for item in instruments if str(item).strip()))
    if offset > 0:
        symbols = symbols[offset:]
    if limit is not None:
        symbols = symbols[:limit]
    for instrument in symbols:
        try:
            raw = ak.stock_financial_analysis_indicator(symbol=akshare_code_from_qlib(instrument), start_year="2015")
        except Exception as exc:  # pragma: no cover - network/vendor dependent
            print(f"skip fundamental_quality {instrument}: {exc}")
            continue
        if raw is None or raw.empty:
            continue
        raw = raw.copy()
        raw["instrument"] = instrument
        frames.append(normalize_fundamental_quality(raw, as_of_date=as_of_date))
        if delay > 0:
            time.sleep(delay)
    if not frames:
        return _empty(FUNDAMENTAL_QUALITY_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def fetch_cninfo_dividends_from_akshare(
    instruments: Iterable[str],
    *,
    limit: int | None = None,
    offset: int = 0,
    delay: float = 0.2,
) -> pd.DataFrame:
    try:
        import akshare as ak  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on optional local env
        raise RuntimeError("akshare is required to fetch CNINFO dividend data") from exc

    frames: list[pd.DataFrame] = []
    symbols = list(dict.fromkeys(str(item).upper() for item in instruments if str(item).strip()))
    if offset > 0:
        symbols = symbols[offset:]
    if limit is not None:
        symbols = symbols[:limit]
    for instrument in symbols:
        try:
            raw = ak.stock_dividend_cninfo(symbol=akshare_code_from_qlib(instrument))
        except Exception as exc:  # pragma: no cover - network/vendor dependent
            print(f"skip cninfo_dividend {instrument}: {exc}")
            continue
        normalized = normalize_cninfo_dividend(raw, instrument=instrument)
        if not normalized.empty:
            frames.append(normalized)
        if delay > 0:
            time.sleep(delay)
    if not frames:
        return _empty(CNINFO_DIVIDEND_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def read_close_prices_from_source_dirs(root: str | Path, source_dirs: Iterable[str | Path]) -> pd.DataFrame:
    base = Path(root).expanduser().resolve()
    frames: list[pd.DataFrame] = []
    for source_dir in source_dirs:
        directory = _resolve(base, source_dir)
        if not directory.exists():
            continue
        for path in directory.rglob("*.csv"):
            frame = pd.read_csv(path, usecols=lambda column: column in {"date", "symbol", "close"})
            if frame.empty or {"date", "symbol", "close"} - set(frame.columns):
                continue
            frame = frame.rename(columns={"date": "trade_date", "symbol": "instrument"})
            frames.append(frame.loc[:, ["instrument", "trade_date", "close"]])
    if not frames:
        return pd.DataFrame(columns=["instrument", "trade_date", "close"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(["instrument", "trade_date"], keep="last")


def write_research_data_domains(
    project_root: str | Path,
    *,
    as_of_date: str,
    fundamental_source: str | Path | None = None,
    company_events_path: str | Path = "data/company_events.csv",
    security_master_path: str | Path = "data/security_master.csv",
    security_master_history_source: str | Path | None = None,
    fundamental_output: str | Path = "data/fundamental_quality.csv",
    security_master_history_output: str | Path = "data/security_master_history.csv",
    shareholder_output: str | Path = "data/shareholder_capital.csv",
    evidence_output: str | Path = "data/announcement_evidence.csv",
    evidence_jsonl_output: str | Path = "data/announcement_evidence.jsonl",
    dividend_output: str | Path = "data/cninfo_dividends.csv",
    price_source_dirs: Iterable[str | Path] = (
        "data/tushare/source_csi300_full",
        "data/tushare/source_csi500_full",
        "data/tushare/source_csi300",
        "data/tushare/source_csi500",
        "data/akshare/source_csi300",
        "data/akshare/source_csi500",
    ),
    evidence_lookback_days: int | None = 180,
    fetch_fundamentals: bool = False,
    fundamental_provider: str = "akshare",
    derive_valuation_fields: bool = False,
    fetch_cninfo_dividends: bool = False,
    fetch_dividends: bool = False,
    dividend_provider: str = "tushare",
    fetch_disclosure_events: bool = False,
    limit: int | None = None,
    offset: int = 0,
    delay: float = 0.2,
) -> dict[str, str]:
    root = Path(project_root).expanduser().resolve()
    events_path = _resolve(root, company_events_path)
    events = _read_csv_if_exists(events_path)
    security_master = _read_csv_if_exists(_resolve(root, security_master_path))
    security_history_source_frame = (
        pd.read_csv(_resolve(root, security_master_history_source)) if security_master_history_source is not None else None
    )

    existing_fundamentals = _read_existing_or_empty(_resolve(root, fundamental_output), FUNDAMENTAL_QUALITY_COLUMNS)
    if fundamental_source is not None:
        fundamentals = normalize_fundamental_quality(pd.read_csv(_resolve(root, fundamental_source)), as_of_date=as_of_date)
    elif fetch_fundamentals:
        instruments = security_master.get("instrument", pd.Series(dtype=str)).dropna().astype(str).tolist()
        provider = str(fundamental_provider or "akshare").strip().lower()
        if provider == "akshare":
            fetched_fundamentals = fetch_fundamental_quality_from_akshare(
                instruments,
                as_of_date=as_of_date,
                limit=limit,
                offset=offset,
                delay=delay,
            )
        elif provider == "tushare":
            fetched_fundamentals = fetch_fundamental_quality_from_tushare(
                instruments,
                as_of_date=as_of_date,
                limit=limit,
                offset=offset,
                delay=delay,
            )
        else:
            raise ValueError(f"unsupported fundamental_provider: {fundamental_provider}")
        fundamentals = _merge_domain_rows(
            existing_fundamentals,
            fetched_fundamentals,
            keys=["instrument", "report_period"],
            columns=FUNDAMENTAL_QUALITY_COLUMNS,
        )
    else:
        fundamentals = existing_fundamentals

    if fetch_disclosure_events:
        instruments = security_master.get("instrument", pd.Series(dtype=str)).dropna().astype(str).tolist()
        fetched_events = fetch_tushare_disclosure_events(
            instruments,
            as_of_date=as_of_date,
            limit=limit,
            offset=offset,
            delay=delay,
        )
        events = _merge_domain_rows(
            events,
            fetched_events,
            keys=["event_id"],
            columns=_domain_columns(COMPANY_EVENT_COLUMNS, events, fetched_events),
        )

    dividends_path = _resolve(root, dividend_output)
    dividends = _read_existing_or_empty(dividends_path, CNINFO_DIVIDEND_COLUMNS)
    if fetch_cninfo_dividends or fetch_dividends:
        instruments = security_master.get("instrument", pd.Series(dtype=str)).dropna().astype(str).tolist()
        provider = str(dividend_provider or "tushare").strip().lower()
        if fetch_cninfo_dividends and not fetch_dividends:
            provider = "cninfo"
        if provider in {"cninfo", "akshare"}:
            fetched_dividends = fetch_cninfo_dividends_from_akshare(
                instruments,
                limit=limit,
                offset=offset,
                delay=delay,
            )
        elif provider == "tushare":
            fetched_dividends = fetch_tushare_dividends(
                instruments,
                limit=limit,
                offset=offset,
                delay=delay,
            )
        else:
            raise ValueError(f"unsupported dividend_provider: {dividend_provider}")
        dividends = _merge_domain_rows(
            dividends,
            fetched_dividends,
            keys=["instrument", "available_at", "dividend_cash_per_10"],
            columns=CNINFO_DIVIDEND_COLUMNS,
        )
    prices = read_close_prices_from_source_dirs(root, price_source_dirs)
    if derive_valuation_fields:
        fundamentals = derive_fundamental_valuation_fields(fundamentals, prices=prices, dividends=dividends)
    fundamentals = derive_fundamental_quality_fields(fundamentals, dividends=dividends, disclosures=events)

    security_history = build_security_master_history(
        security_master,
        prices=prices,
        history_source=security_history_source_frame,
        as_of_date=as_of_date,
    )
    shareholder = build_shareholder_capital_from_events(events, as_of_date=as_of_date)
    evidence = build_announcement_evidence_index(events, as_of_date=as_of_date, lookback_days=evidence_lookback_days)

    paths = ResearchDataDomainPaths(
        fundamental_quality=_resolve(root, fundamental_output),
        shareholder_capital=_resolve(root, shareholder_output),
        announcement_evidence=_resolve(root, evidence_output),
        security_master_history=_resolve(root, security_master_history_output),
    )
    _write_csv(fundamentals, paths.fundamental_quality)
    _write_csv(dividends, dividends_path)
    if fetch_disclosure_events:
        _write_csv(events, events_path)
    _write_csv(security_history, paths.security_master_history)
    _write_csv(shareholder, paths.shareholder_capital)
    _write_csv(evidence, paths.announcement_evidence)
    _write_jsonl(evidence, _resolve(root, evidence_jsonl_output))

    return {
        "fundamental_quality": str(paths.fundamental_quality),
        "cninfo_dividends": str(dividends_path),
        "security_master_history": str(paths.security_master_history),
        "shareholder_capital": str(paths.shareholder_capital),
        "announcement_evidence": str(paths.announcement_evidence),
        "announcement_evidence_jsonl": str(_resolve(root, evidence_jsonl_output)),
    }


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_jsonl(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in frame.to_dict(orient="records"):
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def _read_existing_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return _empty(columns)
    frame = pd.read_csv(path, low_memory=False)
    if frame.empty:
        return _empty(columns)
    for column in columns:
        if column not in frame.columns:
            frame[column] = ""
    return frame.loc[:, columns]


def _merge_domain_rows(existing: pd.DataFrame, fetched: pd.DataFrame, *, keys: list[str], columns: list[str]) -> pd.DataFrame:
    frames = []
    for frame in [existing, fetched]:
        if frame is None or frame.empty:
            continue
        copy = frame.copy()
        for column in columns:
            if column not in copy.columns:
                copy[column] = ""
        frames.append(copy.loc[:, columns])
    if not frames:
        return _empty(columns)
    return pd.concat(frames, ignore_index=True).drop_duplicates(keys, keep="last").loc[:, columns]


def _domain_columns(base_columns: list[str], *frames: pd.DataFrame) -> list[str]:
    columns = list(base_columns)
    for frame in frames:
        for column in getattr(frame, "columns", []):
            if column not in columns:
                columns.append(str(column))
    return columns


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    return candidate if candidate.is_absolute() else root / candidate


def _instrument_from_row(row: pd.Series) -> str:
    for column in ["instrument", "证券代码", "代码", "symbol", "code"]:
        if column not in row.index:
            continue
        value = row.get(column, "")
        if pd.isna(value):
            continue
        text = str(value).strip().upper()
        if not text:
            continue
        if text.startswith(("SH", "SZ")) and len(text) == 8:
            return text
        digits = "".join(ch for ch in text if ch.isdigit())
        if digits:
            return qlib_symbol_from_code(digits)
    return ""


def _date_from_row(row: pd.Series, columns: list[str]) -> str:
    for column in columns:
        if column not in row.index:
            continue
        value = _normalize_date(row.get(column, ""))
        if value:
            return value
    return ""


def _normalize_date(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        return pd.Timestamp(text).strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        return text


def _fallback_announce_date(report_period: str) -> str:
    timestamp = pd.Timestamp(report_period)
    year = int(timestamp.year)
    month_day = timestamp.strftime("%m-%d")
    if month_day == "03-31":
        return f"{year}-04-30"
    if month_day == "06-30":
        return f"{year}-08-31"
    if month_day == "09-30":
        return f"{year}-10-31"
    if month_day == "12-31":
        return f"{year + 1}-04-30"
    return str((timestamp + pd.Timedelta(days=120)).date())


def _number_from_row(row: pd.Series, columns: list[str]) -> float | None:
    for column in columns:
        if column not in row.index:
            continue
        value = row.get(column)
        if value is None or pd.isna(value):
            continue
        text = str(value).strip().replace("%", "").replace(",", "")
        if text in {"", "--", "None", "nan"}:
            continue
        numeric = pd.to_numeric(text, errors="coerce")
        if pd.notna(numeric):
            return float(numeric)
    return None


def _valuation_or_derived_ratio(row: pd.Series, direct_columns: list[str], numerator_columns: list[str]) -> float | None:
    direct = _number_from_row(row, direct_columns)
    if direct is not None:
        return direct
    numerator = _number_from_row(row, numerator_columns)
    close = _number_from_row(row, ["close", "收盘价", "trade_close", "price", "最新价"])
    if numerator is None or close is None or close == 0:
        return None
    return numerator / close * 100.0


def _dividend_yield_from_row(row: pd.Series) -> float | None:
    direct = _number_from_row(row, ["dividend_yield", "股息率", "股息率(%)", "分红收益率", "现金分红收益率"])
    if direct is not None:
        return direct
    cash_per_10 = _number_from_row(row, ["dividend_cash_per_10", "派息比例", "每10股派息", "每十股派息"])
    close = _number_from_row(row, ["close", "收盘价", "trade_close", "price", "最新价"])
    if cash_per_10 is None or close is None or close == 0:
        return None
    return cash_per_10 / 10.0 / close * 100.0


def _normalize_price_frame(prices: pd.DataFrame) -> pd.DataFrame:
    if prices is None or prices.empty:
        return pd.DataFrame(columns=["instrument", "trade_date", "close"])
    frame = prices.copy()
    if "trade_date" not in frame.columns:
        if "date" in frame.columns:
            frame["trade_date"] = frame["date"]
        elif "datetime" in frame.columns:
            frame["trade_date"] = frame["datetime"]
    if "instrument" not in frame.columns or "trade_date" not in frame.columns or "close" not in frame.columns:
        return pd.DataFrame(columns=["instrument", "trade_date", "close"])
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame.dropna(subset=["instrument", "trade_date", "close"]).loc[:, ["instrument", "trade_date", "close"]]


def _attach_latest_dividend(frame: pd.DataFrame, dividends: pd.DataFrame | None) -> pd.DataFrame:
    if dividends is None or dividends.empty:
        frame["dividend_cash_per_10"] = pd.NA
        return frame
    div = dividends.copy()
    if "dividend_cash_per_10" not in div.columns:
        div = normalize_cninfo_dividend(div, instrument=str(div.get("instrument", pd.Series([""])).iloc[0] if not div.empty else ""))
    if div.empty or "available_at" not in div.columns:
        frame["dividend_cash_per_10"] = pd.NA
        return frame
    div["available_at"] = pd.to_datetime(div["available_at"], errors="coerce")
    div["dividend_cash_per_10"] = pd.to_numeric(div["dividend_cash_per_10"], errors="coerce")
    div = div.dropna(subset=["instrument", "available_at", "dividend_cash_per_10"]).sort_values("available_at")
    if div.empty:
        frame["dividend_cash_per_10"] = pd.NA
        return frame
    pieces = []
    for instrument, group in frame.sort_values("_available_at_dt").groupby("instrument", sort=False):
        source = div[div["instrument"].astype(str) == str(instrument)].sort_values("available_at")
        if source.empty:
            filled = group.copy()
            filled["dividend_cash_per_10"] = pd.NA
        else:
            filled = pd.merge_asof(
                group.sort_values("_available_at_dt"),
                source.loc[:, ["instrument", "available_at", "dividend_cash_per_10"]],
                left_on="_available_at_dt",
                right_on="available_at",
                by="instrument",
                direction="backward",
                suffixes=("", "_dividend"),
            )
        pieces.append(filled)
    return pd.concat(pieces, ignore_index=True) if pieces else frame


def _derive_dividend_quality_fields(frame: pd.DataFrame, dividends: pd.DataFrame | None) -> pd.DataFrame:
    output = frame.copy()
    if dividends is None or dividends.empty:
        return output
    if "available_at" not in output.columns or "instrument" not in output.columns:
        return output
    div = dividends.copy()
    if "dividend_cash_per_10" not in div.columns or "available_at" not in div.columns or "instrument" not in div.columns:
        return output
    div["available_at"] = pd.to_datetime(div["available_at"], errors="coerce")
    div["dividend_cash_per_10"] = pd.to_numeric(div["dividend_cash_per_10"], errors="coerce")
    div = div.dropna(subset=["instrument", "available_at", "dividend_cash_per_10"]).sort_values(["instrument", "available_at"])
    if div.empty:
        return output

    work = output.reset_index(drop=True).copy()
    work["_row_id"] = work.index
    work["_available_at_dt"] = pd.to_datetime(work["available_at"], errors="coerce")
    rows = []
    for _, item in work.iterrows():
        available_at = item.get("_available_at_dt")
        if pd.isna(available_at):
            rows.append({"_row_id": item["_row_id"], "latest_dividend_cash_per_10": pd.NA, "dividend_stability_derived": pd.NA})
            continue
        history = div[
            (div["instrument"].astype(str) == str(item.get("instrument", "")))
            & (div["available_at"] <= available_at)
        ].sort_values("available_at")
        if history.empty:
            rows.append({"_row_id": item["_row_id"], "latest_dividend_cash_per_10": pd.NA, "dividend_stability_derived": pd.NA})
            continue
        latest = float(history.iloc[-1]["dividend_cash_per_10"])
        stability = pd.NA
        if len(history) >= 2:
            previous = float(history.iloc[-2]["dividend_cash_per_10"])
            if previous != 0:
                stability = max(0.0, min(1.0, 1.0 - abs(latest - previous) / abs(previous)))
        rows.append(
            {
                "_row_id": item["_row_id"],
                "latest_dividend_cash_per_10": latest,
                "dividend_stability_derived": stability,
            }
        )
    derived = pd.DataFrame(rows)
    if derived.empty:
        return output
    merged = work.merge(derived, on="_row_id", how="left")
    existing_stability = pd.to_numeric(merged["dividend_stability"], errors="coerce")
    merged["dividend_stability"] = existing_stability.combine_first(pd.to_numeric(merged["dividend_stability_derived"], errors="coerce"))
    cashflow = pd.to_numeric(merged["operating_cashflow_per_share"], errors="coerce")
    dividend_per_share = pd.to_numeric(merged["latest_dividend_cash_per_10"], errors="coerce") / 10.0
    coverage = cashflow / dividend_per_share.replace(0, pd.NA)
    existing_coverage = pd.to_numeric(merged["dividend_cashflow_coverage"], errors="coerce")
    merged["dividend_cashflow_coverage"] = existing_coverage.combine_first(pd.to_numeric(coverage, errors="coerce"))
    return merged.drop(
        columns=[
            "_row_id",
            "_available_at_dt",
            "latest_dividend_cash_per_10",
            "dividend_stability_derived",
        ],
        errors="ignore",
    )


def _derive_financial_disclosure_fields(frame: pd.DataFrame, disclosures: pd.DataFrame | None) -> pd.DataFrame:
    output = frame.copy()
    if disclosures is None or disclosures.empty:
        return output
    if "available_at" not in output.columns or "instrument" not in output.columns:
        return output
    required = {"instrument", "event_type", "event_date"}
    if not required <= set(disclosures.columns):
        return output
    events = disclosures.copy()
    events["event_date"] = pd.to_datetime(events["event_date"], errors="coerce")
    events = events.dropna(subset=["instrument", "event_date"]).sort_values(["instrument", "event_date"])
    if events.empty:
        return output
    financial_events = events[events["event_type"].fillna("").astype(str) == "financial_report_disclosure"].copy()

    work = output.reset_index(drop=True).copy()
    work["_row_id"] = work.index
    work["_available_at_dt"] = pd.to_datetime(work["available_at"], errors="coerce")
    rows = []
    for _, item in work.iterrows():
        available_at = item.get("_available_at_dt")
        if pd.isna(available_at):
            rows.append(
                {
                    "_row_id": item["_row_id"],
                    "financial_disclosure_days_since_derived": pd.NA,
                    "financial_disclosure_positive_score_90d_derived": pd.NA,
                    "financial_disclosure_negative_score_90d_derived": pd.NA,
                    "financial_disclosure_event_score_90d_derived": pd.NA,
                }
            )
            continue
        instrument_events = events[events["instrument"].astype(str) == str(item.get("instrument", ""))].sort_values("event_date")
        history = financial_events[
            (financial_events["instrument"].astype(str) == str(item.get("instrument", "")))
            & (financial_events["event_date"] <= available_at)
        ].sort_values("event_date")
        if history.empty and instrument_events.empty:
            rows.append(
                {
                    "_row_id": item["_row_id"],
                    "financial_disclosure_days_since_derived": pd.NA,
                    "financial_disclosure_positive_score_90d_derived": 0.0,
                    "financial_disclosure_negative_score_90d_derived": 0.0,
                    "financial_disclosure_event_score_90d_derived": 0.0,
                }
            )
            continue
        days_since = pd.NA
        if not history.empty:
            latest_event_date = history.iloc[-1]["event_date"]
            days_since = max(float((available_at - latest_event_date).days), 0.0)
        positive_score, negative_score = _directional_disclosure_scores(instrument_events, available_at)
        rows.append(
            {
                "_row_id": item["_row_id"],
                "financial_disclosure_days_since_derived": days_since,
                "financial_disclosure_positive_score_90d_derived": positive_score,
                "financial_disclosure_negative_score_90d_derived": negative_score,
                "financial_disclosure_event_score_90d_derived": positive_score - negative_score,
            }
        )
    derived = pd.DataFrame(rows)
    if derived.empty:
        return output
    merged = work.merge(derived, on="_row_id", how="left")
    days = pd.to_numeric(merged["financial_disclosure_days_since_derived"], errors="coerce")
    recency = (1.0 - days / 30.0).clip(lower=0.0, upper=1.0)
    merged["financial_disclosure_days_since"] = _fill_missing_numeric(
        merged.get("financial_disclosure_days_since"),
        days,
    )
    merged["financial_disclosure_recency_30d"] = _fill_missing_numeric(
        merged.get("financial_disclosure_recency_30d"),
        recency,
    )
    for column in [
        "financial_disclosure_positive_score_90d",
        "financial_disclosure_negative_score_90d",
        "financial_disclosure_event_score_90d",
    ]:
        merged[column] = _fill_missing_numeric(
            merged.get(column),
            pd.to_numeric(merged[f"{column}_derived"], errors="coerce"),
        )
    return merged.drop(
        columns=[
            "_row_id",
            "_available_at_dt",
            "financial_disclosure_days_since_derived",
            "financial_disclosure_positive_score_90d_derived",
            "financial_disclosure_negative_score_90d_derived",
            "financial_disclosure_event_score_90d_derived",
        ],
        errors="ignore",
    )


def _directional_disclosure_scores(events: pd.DataFrame, available_at: pd.Timestamp) -> tuple[float, float]:
    lookback_start = available_at - pd.Timedelta(days=90)
    recent = events[(events["event_date"] >= lookback_start) & (events["event_date"] <= available_at)].copy()
    if recent.empty:
        return 0.0, 0.0
    positive = 0.0
    negative = 0.0
    for _, event in recent.iterrows():
        direction = _disclosure_event_direction(event)
        if direction == 0:
            continue
        days_since = max(float((available_at - event["event_date"]).days), 0.0)
        recency_weight = max(0.0, 1.0 - days_since / 90.0)
        severity_weight = _disclosure_severity_weight(event.get("severity"))
        contribution = recency_weight * severity_weight
        if direction > 0:
            positive += contribution
        else:
            negative += contribution
    return positive, negative


def _disclosure_event_direction(event: pd.Series) -> int:
    event_type = str(event.get("event_type", "")).strip().lower()
    severity = str(event.get("severity", "")).strip().lower()
    if event_type in POSITIVE_DISCLOSURE_EVENT_TYPES:
        return 1
    if event_type in NEGATIVE_DISCLOSURE_EVENT_TYPES:
        return -1
    if severity in {"risk", "block"}:
        return -1
    return 0


def _disclosure_severity_weight(value: Any) -> float:
    severity = str(value or "info").strip().lower()
    return DISCLOSURE_SEVERITY_WEIGHTS.get(severity, DISCLOSURE_SEVERITY_WEIGHTS["info"])


def _fill_missing_numeric(current: pd.Series | None, derived: pd.Series) -> pd.Series:
    if current is None:
        current = pd.Series(pd.NA, index=derived.index)
    current_numeric = pd.to_numeric(current, errors="coerce")
    return current_numeric.combine_first(pd.to_numeric(derived, errors="coerce"))


def _append_valuation_sources(
    current: pd.Series,
    *,
    ep_filled: pd.Series,
    cfp_filled: pd.Series,
    dividend_filled: pd.Series,
) -> pd.Series:
    output = current.fillna("").astype(str).copy()
    for index in output.index:
        parts = [part for part in output.at[index].split(";") if part]
        if bool(ep_filled.loc[index]) and "eps_to_pit_close" not in parts:
            parts.append("eps_to_pit_close")
        if bool(cfp_filled.loc[index]) and "ocfps_to_pit_close" not in parts:
            parts.append("ocfps_to_pit_close")
        if bool(dividend_filled.loc[index]) and "cninfo_dividend_to_pit_close" not in parts:
            parts.append("cninfo_dividend_to_pit_close")
        output.at[index] = ";".join(parts)
    return output


def _clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return " ".join(str(value).split())


def _chunk_text(text: str, chunk_size: int) -> list[str]:
    cleaned = _clean_text(text)
    if not cleaned:
        return []
    return [cleaned[index : index + chunk_size] for index in range(0, len(cleaned), chunk_size)]


def _keywords(text: str) -> list[str]:
    candidates = [
        "减持",
        "增持",
        "回购",
        "质押",
        "解禁",
        "业绩",
        "问询",
        "监管",
        "处罚",
        "担保",
        "诉讼",
        "现金流",
        "订单",
        "减值",
    ]
    return [keyword for keyword in candidates if keyword in text]
