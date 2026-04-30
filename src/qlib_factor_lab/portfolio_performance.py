from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


PERFORMANCE_COLUMNS = [
    "date",
    "instrument",
    "display_name",
    "industry",
    "rank",
    "target_weight",
    "top_factor_1",
    "top_factor_2",
    "factor",
    "event_bucket",
    "prev_close",
    "current",
    "pct_today",
    "weighted_return_pct",
    "quote_time",
    "direction",
]


def build_intraday_performance(portfolio: pd.DataFrame, quotes: pd.DataFrame, *, run_date: str = "") -> pd.DataFrame:
    if portfolio.empty:
        return pd.DataFrame(columns=PERFORMANCE_COLUMNS)
    base = portfolio.copy()
    if "instrument" not in base.columns:
        raise ValueError("portfolio must contain instrument")
    if "target_weight" not in base.columns:
        raise ValueError("portfolio must contain target_weight")
    quote_frame = normalize_quote_frame(quotes)
    frame = base.merge(quote_frame, on="instrument", how="left", suffixes=("", "_quote"))
    frame["date"] = _first_nonblank_column(frame, ["date"], default=run_date)
    frame["display_name"] = _first_nonblank_column(frame, ["display_name", "name"], default="")
    frame["industry"] = _first_nonblank_column(frame, ["industry_sw", "industry", "industry_csrc"], default="unknown")
    frame["factor"] = _first_nonblank_column(frame, ["top_factor_1", "factor", "family"], default="unknown")
    frame["target_weight"] = pd.to_numeric(frame["target_weight"], errors="coerce").fillna(0.0)
    frame["prev_close"] = pd.to_numeric(frame.get("prev_close", pd.Series(dtype=float)), errors="coerce")
    frame["current"] = pd.to_numeric(frame.get("current", pd.Series(dtype=float)), errors="coerce")
    if "pct_today" not in frame.columns or frame["pct_today"].isna().all():
        frame["pct_today"] = (frame["current"] / frame["prev_close"] - 1.0) * 100.0
    frame["pct_today"] = pd.to_numeric(frame["pct_today"], errors="coerce").fillna(0.0)
    frame["weighted_return_pct"] = frame["target_weight"] * frame["pct_today"]
    frame["direction"] = frame["pct_today"].map(_direction)
    frame["event_bucket"] = frame.apply(_event_bucket, axis=1)
    for column in PERFORMANCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame.loc[:, PERFORMANCE_COLUMNS].reset_index(drop=True)


def normalize_quote_frame(quotes: pd.DataFrame) -> pd.DataFrame:
    if quotes.empty:
        return pd.DataFrame(columns=["instrument", "display_name", "prev_close", "current", "pct_today", "quote_time"])
    frame = quotes.copy()
    rename = {}
    aliases = {
        "instrument": ["instrument", "symbol", "代码", "证券代码"],
        "display_name": ["display_name", "name", "名称", "股票简称"],
        "prev_close": ["prev_close", "previous_close", "昨收", "昨收价"],
        "current": ["current", "price", "latest", "最新价", "现价"],
        "pct_today": ["pct_today", "pct_chg", "change_pct", "涨跌幅"],
        "quote_time": ["quote_time", "time", "更新时间"],
    }
    for canonical, candidates in aliases.items():
        column = _first_present(frame, candidates)
        if column is not None:
            rename[column] = canonical
    frame = frame.rename(columns=rename)
    if "instrument" not in frame.columns:
        raise ValueError("quotes must contain instrument/symbol/code")
    frame["instrument"] = frame["instrument"].map(_normalize_instrument)
    keep = [column for column in ["instrument", "display_name", "prev_close", "current", "pct_today", "quote_time"] if column in frame.columns]
    output = frame.loc[:, keep].copy()
    for column in ["prev_close", "current", "pct_today"]:
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")
    return output.drop_duplicates(subset=["instrument"], keep="last").reset_index(drop=True)


def fetch_akshare_spot_quotes() -> pd.DataFrame:
    from .akshare_data import _get_akshare

    ak = _get_akshare()
    raw = ak.stock_zh_a_spot_em()
    return normalize_quote_frame(raw)


def summarize_intraday_performance(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "summary": {"positions": 0, "weighted_return_pct": 0.0, "up_count": 0, "down_count": 0, "quote_time": ""},
            "industry": pd.DataFrame(columns=["industry", "count", "target_weight", "pct_today", "weighted_return_pct"]),
            "factor": pd.DataFrame(columns=["factor", "count", "target_weight", "pct_today", "weighted_return_pct"]),
            "event": pd.DataFrame(columns=["event_bucket", "count", "target_weight", "pct_today", "weighted_return_pct"]),
            "contributors": pd.DataFrame(),
        }
    source = frame.copy()
    for column in ["target_weight", "pct_today", "weighted_return_pct"]:
        source[column] = pd.to_numeric(source.get(column, pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    source["industry"] = _first_nonblank_column(source, ["industry"], default="unknown")
    source["factor"] = _first_nonblank_column(source, ["factor", "top_factor_1"], default="unknown")
    source["event_bucket"] = _first_nonblank_column(source, ["event_bucket"], default="no_event")
    contributors = source.sort_values("weighted_return_pct", ascending=True).reset_index(drop=True)
    return {
        "summary": {
            "positions": int(len(source)),
            "weighted_return_pct": float(source["weighted_return_pct"].sum()),
            "up_count": int((source["pct_today"] > 0).sum()),
            "down_count": int((source["pct_today"] < 0).sum()),
            "quote_time": str(source["quote_time"].dropna().astype(str).max()) if "quote_time" in source.columns and source["quote_time"].notna().any() else "",
        },
        "industry": _group_performance(source, "industry", "industry"),
        "factor": _group_performance(source, "factor", "factor"),
        "event": _group_performance(source, "event_bucket", "event_bucket"),
        "contributors": contributors,
    }


def write_intraday_performance_report(frame: pd.DataFrame, output_csv: str | Path, output_md: str | Path) -> tuple[Path, Path]:
    csv_path = Path(output_csv)
    md_path = Path(output_md)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False)
    summary = summarize_intraday_performance(frame)
    lines = [
        "# Portfolio Intraday Performance",
        "",
        f"- positions: {summary['summary']['positions']}",
        f"- weighted_return_pct: {summary['summary']['weighted_return_pct']:.6g}",
        f"- up_count: {summary['summary']['up_count']}",
        f"- down_count: {summary['summary']['down_count']}",
        f"- quote_time: {summary['summary']['quote_time']}",
        "",
        "## Worst Contributors",
        "",
        "| instrument | display_name | industry | factor | event_bucket | target_weight | pct_today | weighted_return_pct |",
        "|---|---|---|---|---|---:|---:|---:|",
    ]
    contributors = summary["contributors"]
    for _, row in contributors.head(12).iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("instrument", "")),
                    str(row.get("display_name", "")),
                    str(row.get("industry", "")),
                    str(row.get("factor", "")),
                    str(row.get("event_bucket", "")),
                    _fmt(row.get("target_weight")),
                    _fmt(row.get("pct_today")),
                    _fmt(row.get("weighted_return_pct")),
                ]
            )
            + " |"
        )
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return csv_path, md_path


def _first_present(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    return None


def _normalize_instrument(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text.startswith(("SH", "SZ", "BJ")):
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        code = digits[-6:]
        if code.startswith(("6", "9")):
            return f"SH{code}"
        if code.startswith(("0", "2", "3")):
            return f"SZ{code}"
        if code.startswith(("4", "8")):
            return f"BJ{code}"
    return text


def _first_nonblank_column(frame: pd.DataFrame, columns: list[str], *, default: str) -> pd.Series:
    output = pd.Series(default, index=frame.index, dtype="object")
    filled = pd.Series(False, index=frame.index)
    for column in columns:
        if column not in frame.columns:
            continue
        values = frame[column].fillna("").astype(str).str.strip()
        mask = (~filled) & values.ne("")
        output.loc[mask] = values.loc[mask]
        filled = filled | mask
    return output


def _event_bucket(row: pd.Series) -> str:
    if _truthy(row.get("event_blocked")):
        return "event_block"
    if _number(row.get("event_count")) > 0 or not _blank(row.get("event_risk_summary")) or not _blank(row.get("active_event_types")):
        return "event_watch"
    if _truthy(row.get("announcement_flag")):
        return "announcement_watch"
    return "no_event"


def _group_performance(frame: pd.DataFrame, group_column: str, label: str) -> pd.DataFrame:
    grouped = (
        frame.groupby(group_column, dropna=False)
        .agg(
            count=("instrument", "count"),
            target_weight=("target_weight", "sum"),
            pct_today=("pct_today", "mean"),
            weighted_return_pct=("weighted_return_pct", "sum"),
        )
        .reset_index()
        .rename(columns={group_column: label})
        .sort_values("weighted_return_pct", ascending=True)
        .reset_index(drop=True)
    )
    return grouped.loc[:, [label, "count", "target_weight", "pct_today", "weighted_return_pct"]]


def _direction(value: Any) -> str:
    number = _number(value)
    if number > 0:
        return "up"
    if number < 0:
        return "down"
    return "flat"


def _truthy(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _blank(value: Any) -> bool:
    return pd.isna(value) or str(value).strip() == ""


def _number(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _fmt(value: Any) -> str:
    try:
        return f"{float(value):.6g}"
    except (TypeError, ValueError):
        return str(value)
