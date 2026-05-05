from __future__ import annotations

from pathlib import Path
from typing import Iterable, Any

import pandas as pd


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


def search_announcement_evidence(
    evidence_path: str | Path,
    *,
    instruments: Iterable[str] | None = None,
    event_types: Iterable[str] | None = None,
    severities: Iterable[str] | None = None,
    keyword: str | None = None,
    as_of_date: str | None = None,
    lookback_days: int | None = None,
    limit: int = 200,
) -> pd.DataFrame:
    frame = _read_evidence(evidence_path)
    if frame.empty:
        return frame

    filtered = frame.copy()
    filtered = _filter_values(filtered, "instrument", instruments)
    filtered = _filter_values(filtered, "event_type", event_types)
    filtered = _filter_values(filtered, "severity", severities)
    if as_of_date:
        available = pd.to_datetime(filtered["available_at"], errors="coerce")
        cutoff = pd.Timestamp(as_of_date)
        filtered = filtered.loc[available.notna() & (available <= cutoff)]
        if lookback_days is not None:
            filtered = filtered.loc[available >= cutoff - pd.Timedelta(days=int(lookback_days))]
    if keyword and keyword.strip():
        needle = keyword.strip().lower()
        haystack = (
            filtered["title"].fillna("").astype(str)
            + " "
            + filtered["chunk_text"].fillna("").astype(str)
            + " "
            + filtered["keywords"].fillna("").astype(str)
        ).str.lower()
        filtered = filtered.loc[haystack.str.contains(needle, regex=False)]
    return filtered.sort_values(["available_at", "event_date", "instrument"], ascending=[False, False, True]).head(limit).reset_index(drop=True)


def summarize_announcement_evidence(evidence: pd.DataFrame | str | Path) -> dict[str, int]:
    frame = _read_evidence(evidence) if not isinstance(evidence, pd.DataFrame) else _normalize_columns(evidence.copy())
    return {
        "chunks": int(len(frame)),
        "events": int(frame["event_id"].dropna().astype(str).nunique()) if "event_id" in frame.columns else 0,
        "instruments": int(frame["instrument"].dropna().astype(str).nunique()) if "instrument" in frame.columns else 0,
        "source_urls": int(_nonblank(frame.get("source_url", pd.Series(dtype=str))).nunique()),
    }


def _read_evidence(evidence_path: str | Path) -> pd.DataFrame:
    path = Path(evidence_path)
    if not path.exists():
        return pd.DataFrame(columns=ANNOUNCEMENT_EVIDENCE_COLUMNS)
    return _normalize_columns(pd.read_csv(path, low_memory=False))


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    for column in ANNOUNCEMENT_EVIDENCE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    return frame.loc[:, ANNOUNCEMENT_EVIDENCE_COLUMNS]


def _filter_values(frame: pd.DataFrame, column: str, values: Iterable[str] | None) -> pd.DataFrame:
    wanted = {str(value).strip() for value in values or [] if str(value).strip()}
    if not wanted:
        return frame
    return frame.loc[frame[column].fillna("").astype(str).isin(wanted)]


def _nonblank(series: pd.Series) -> pd.Series:
    values = series.fillna("").astype(str).str.strip()
    return values.loc[values != ""]
