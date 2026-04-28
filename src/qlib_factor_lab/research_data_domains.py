from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .akshare_data import akshare_code_from_qlib, qlib_symbol_from_code


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
    "source_url",
    "chunk_id",
    "chunk_text",
    "keywords",
]

CAPITAL_EVENT_TYPES = {
    "shareholder_reduction",
    "shareholder_increase",
    "buyback",
    "large_unlock",
    "pledge_risk",
    "capital_structure_change",
}


@dataclass(frozen=True)
class ResearchDataDomainPaths:
    fundamental_quality: Path
    shareholder_capital: Path
    announcement_evidence: Path


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
                "gross_margin": _number_from_row(item, ["gross_margin", "销售毛利率", "毛利率", "销售毛利率(%)"]),
                "debt_ratio": _number_from_row(item, ["debt_ratio", "资产负债率", "资产负债率(%)"]),
                "revenue_growth_yoy": _number_from_row(item, ["revenue_growth_yoy", "营业收入同比增长率", "营业总收入同比增长率", "营业收入同比增长率(%)"]),
                "net_profit_growth_yoy": _number_from_row(item, ["net_profit_growth_yoy", "净利润同比增长率", "归母净利润同比增长率", "净利润同比增长率(%)"]),
                "operating_cashflow_to_net_profit": _number_from_row(
                    item,
                    ["operating_cashflow_to_net_profit", "经营现金流量净额/净利润", "经营现金流净额/净利润"],
                ),
                "source": source,
            }
        )
    if not rows:
        return _empty(FUNDAMENTAL_QUALITY_COLUMNS)
    frame = pd.DataFrame(rows, columns=FUNDAMENTAL_QUALITY_COLUMNS)
    return frame.sort_values(["instrument", "report_period"]).drop_duplicates(["instrument", "report_period"], keep="last")


def build_shareholder_capital_from_events(events: pd.DataFrame, *, as_of_date: str) -> pd.DataFrame:
    if events is None or events.empty:
        return _empty(SHAREHOLDER_CAPITAL_COLUMNS)
    frame = events.copy()
    if "event_type" not in frame.columns:
        return _empty(SHAREHOLDER_CAPITAL_COLUMNS)
    frame = frame[frame["event_type"].astype(str).isin(CAPITAL_EVENT_TYPES)].copy()
    if frame.empty:
        return _empty(SHAREHOLDER_CAPITAL_COLUMNS)
    for column in SHAREHOLDER_CAPITAL_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame["announce_date"] = frame["announce_date"] if "announce_date" in frame.columns else frame.get("event_date", "")
    frame.loc[frame["announce_date"].astype(str).str.strip() == "", "announce_date"] = frame.get("event_date", "")
    frame["available_at"] = _normalize_date(as_of_date)
    frame["source"] = frame["source"].where(frame["source"].astype(str).str.strip() != "", "company_events")
    return frame.loc[:, SHAREHOLDER_CAPITAL_COLUMNS].sort_values(["instrument", "event_date", "event_type"])


def build_announcement_evidence_index(events: pd.DataFrame, *, as_of_date: str, chunk_size: int = 220) -> pd.DataFrame:
    if events is None or events.empty:
        return _empty(ANNOUNCEMENT_EVIDENCE_COLUMNS)

    rows: list[dict[str, Any]] = []
    available_at = _normalize_date(as_of_date)
    for idx, item in events.iterrows():
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
                    "source_url": str(item.get("source_url", "")),
                    "chunk_id": f"{event_id}_{chunk_index:03d}",
                    "chunk_text": chunk,
                    "keywords": ",".join(keywords),
                }
            )
    if not rows:
        return _empty(ANNOUNCEMENT_EVIDENCE_COLUMNS)
    return pd.DataFrame(rows, columns=ANNOUNCEMENT_EVIDENCE_COLUMNS)


def fetch_fundamental_quality_from_akshare(
    instruments: Iterable[str],
    *,
    as_of_date: str,
    limit: int | None = None,
    delay: float = 0.2,
) -> pd.DataFrame:
    try:
        import akshare as ak  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on optional local env
        raise RuntimeError("akshare is required to fetch live fundamental quality data") from exc

    frames: list[pd.DataFrame] = []
    symbols = list(dict.fromkeys(str(item).upper() for item in instruments if str(item).strip()))
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


def write_research_data_domains(
    project_root: str | Path,
    *,
    as_of_date: str,
    fundamental_source: str | Path | None = None,
    company_events_path: str | Path = "data/company_events.csv",
    security_master_path: str | Path = "data/security_master.csv",
    fundamental_output: str | Path = "data/fundamental_quality.csv",
    shareholder_output: str | Path = "data/shareholder_capital.csv",
    evidence_output: str | Path = "data/announcement_evidence.csv",
    evidence_jsonl_output: str | Path = "data/announcement_evidence.jsonl",
    fetch_fundamentals: bool = False,
    limit: int | None = None,
    delay: float = 0.2,
) -> dict[str, str]:
    root = Path(project_root).expanduser().resolve()
    events = _read_csv_if_exists(_resolve(root, company_events_path))
    security_master = _read_csv_if_exists(_resolve(root, security_master_path))

    if fundamental_source is not None:
        fundamentals = normalize_fundamental_quality(pd.read_csv(_resolve(root, fundamental_source)), as_of_date=as_of_date)
    elif fetch_fundamentals:
        fundamentals = fetch_fundamental_quality_from_akshare(
            security_master.get("instrument", pd.Series(dtype=str)).dropna().astype(str).tolist(),
            as_of_date=as_of_date,
            limit=limit,
            delay=delay,
        )
    else:
        fundamentals = _read_existing_or_empty(_resolve(root, fundamental_output), FUNDAMENTAL_QUALITY_COLUMNS)

    shareholder = build_shareholder_capital_from_events(events, as_of_date=as_of_date)
    evidence = build_announcement_evidence_index(events, as_of_date=as_of_date)

    paths = ResearchDataDomainPaths(
        fundamental_quality=_resolve(root, fundamental_output),
        shareholder_capital=_resolve(root, shareholder_output),
        announcement_evidence=_resolve(root, evidence_output),
    )
    _write_csv(fundamentals, paths.fundamental_quality)
    _write_csv(shareholder, paths.shareholder_capital)
    _write_csv(evidence, paths.announcement_evidence)
    _write_jsonl(evidence, _resolve(root, evidence_jsonl_output))

    return {
        "fundamental_quality": str(paths.fundamental_quality),
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
    return pd.read_csv(path)


def _read_existing_or_empty(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return _empty(columns)
    frame = pd.read_csv(path)
    if frame.empty:
        return _empty(columns)
    for column in columns:
        if column not in frame.columns:
            frame[column] = ""
    return frame.loc[:, columns]


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
