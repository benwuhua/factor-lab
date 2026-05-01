#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.akshare_data import (
    COMPANY_EVENT_COLUMNS,
    enrich_security_master_industries,
    fetch_company_notices,
    fetch_security_master_snapshot,
    fetch_universe_symbols,
    filter_frame_to_universes,
    load_symbols_from_existing_qlib,
    load_universe_symbols_csv,
    normalize_akshare_notices,
    normalize_security_master_snapshot,
    today_for_daily_data,
)


def main() -> int:
    default_root = project_root()
    parser = argparse.ArgumentParser(description="Build security master and company event CSVs for daily research risk.")
    parser.add_argument("--project-root", default=str(default_root))
    parser.add_argument("--as-of-date", default=today_for_daily_data())
    parser.add_argument("--notice-start", default=None, help="Notice start date, YYYY-MM-DD or YYYYMMDD. Defaults to as-of date.")
    parser.add_argument("--notice-end", default=None, help="Notice end date, YYYY-MM-DD or YYYYMMDD. Defaults to as-of date.")
    parser.add_argument("--security-master-output", default="data/security_master.csv")
    parser.add_argument("--company-events-output", default="data/company_events.csv")
    parser.add_argument(
        "--merge-existing-events",
        action="store_true",
        help="Merge fetched company events into the existing output CSV instead of replacing it.",
    )
    parser.add_argument("--universes", nargs="+", default=["csi300", "csi500"], choices=["csi300", "csi500"])
    parser.add_argument("--universe-symbols-csv", default=None, help="Optional CSV with universe,instrument columns.")
    parser.add_argument("--security-master-source-csv", default=None, help="Offline raw security CSV to normalize instead of AkShare.")
    parser.add_argument(
        "--industry-source-csv",
        default=None,
        help="Optional industry override/enrichment CSV. Defaults to data/security_industry_overrides.csv when present.",
    )
    parser.add_argument("--notice-source-csv", default=None, help="Offline raw notice CSV to normalize instead of AkShare.")
    parser.add_argument("--skip-security-master", action="store_true")
    parser.add_argument("--skip-company-events", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="Limit security rows for smoke tests.")
    parser.add_argument("--delay", type=float, default=0.2, help="Delay between AkShare notice-date calls.")
    args = parser.parse_args()

    root = Path(args.project_root).expanduser().resolve()
    universe_symbols = _load_universe_symbols(root, args)
    if not args.skip_security_master:
        if args.security_master_source_csv:
            raw_master = pd.read_csv(_resolve(root, args.security_master_source_csv))
            master = normalize_security_master_snapshot(raw_master, as_of_date=args.as_of_date)
        else:
            master = fetch_security_master_snapshot(args.as_of_date, limit=args.limit)
        industry_source = _default_industry_source(root, args.industry_source_csv)
        if industry_source is not None:
            master = enrich_security_master_industries(master, pd.read_csv(industry_source))
        master = filter_frame_to_universes(master, universe_symbols)
        _write_csv(master, _resolve(root, args.security_master_output))

    if not args.skip_company_events:
        if args.notice_source_csv:
            raw_notices = pd.read_csv(_resolve(root, args.notice_source_csv))
            events = normalize_akshare_notices(raw_notices)
        else:
            notice_start = _normalize_date_arg(args.notice_start or args.as_of_date)
            notice_end = _normalize_date_arg(args.notice_end or args.as_of_date)
            events = fetch_company_notices(notice_start, notice_end, delay=args.delay)
        events = filter_frame_to_universes(events, universe_symbols)
        events_output = _resolve(root, args.company_events_output)
        if args.merge_existing_events:
            events = _merge_company_events(_read_existing_events(events_output), events)
        _write_csv(events, events_output)

    return 0


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    print(f"wrote: {path}")


def _read_existing_events(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)
    try:
        return pd.read_csv(path)
    except EmptyDataError:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)


def _merge_company_events(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    columns = _merged_event_columns(existing, fetched)
    existing_norm = _normalize_event_frame(existing, columns)
    fetched_norm = _normalize_event_frame(fetched, columns)
    combined = pd.concat([existing_norm, fetched_norm], ignore_index=True)
    if combined.empty:
        return pd.DataFrame(columns=columns)

    event_id = combined["event_id"].astype(str).str.strip() if "event_id" in combined.columns else pd.Series("", index=combined.index)
    with_event_id = combined.loc[event_id != ""].drop_duplicates(["event_id"], keep="last")
    without_event_id = combined.loc[event_id == ""].drop_duplicates(_fallback_event_key_columns(combined), keep="last")
    merged = pd.concat([with_event_id, without_event_id], ignore_index=True)
    return _sort_events(merged, columns)


def _merged_event_columns(existing: pd.DataFrame, fetched: pd.DataFrame) -> list[str]:
    columns = list(COMPANY_EVENT_COLUMNS)
    for frame in (existing, fetched):
        for column in getattr(frame, "columns", []):
            if column not in columns:
                columns.append(str(column))
    return columns


def _normalize_event_frame(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)

    output = frame.copy()
    for column in columns:
        if column not in output.columns:
            output[column] = ""
    output = output.loc[:, columns]
    for column in output.columns:
        if column in {"event_date", "active_until"}:
            output[column] = output[column].map(_normalize_event_date)
        else:
            output[column] = output[column].map(_normalize_event_text)
    if "instrument" in output.columns:
        output["instrument"] = output["instrument"].str.upper()
    return output


def _normalize_event_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_event_date(value: object) -> str:
    text = _normalize_event_text(value)
    if not text:
        return ""
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return text
    return parsed.strftime("%Y-%m-%d")


def _fallback_event_key_columns(frame: pd.DataFrame) -> list[str]:
    return [
        column
        for column in ["instrument", "event_type", "event_date", "title", "source_url"]
        if column in frame.columns
    ]


def _sort_events(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    sort_columns = [
        column
        for column in ["event_date", "instrument", "event_type", "title", "source_url", "event_id"]
        if column in frame.columns
    ]
    if sort_columns:
        frame = frame.sort_values(sort_columns, kind="mergesort")
    return frame.loc[:, columns].reset_index(drop=True)


def _load_universe_symbols(root: Path, args: argparse.Namespace) -> dict[str, list[str]]:
    if args.universe_symbols_csv:
        loaded = load_universe_symbols_csv(_resolve(root, args.universe_symbols_csv))
        return {universe: symbols for universe, symbols in loaded.items() if universe in set(args.universes)}
    current_symbols = _load_current_provider_symbols(root, args.universes)
    if current_symbols:
        return current_symbols
    fallback_qlib_dir = root / "data/qlib/cn_data"
    return {
        universe: fetch_universe_symbols(universe, fallback_qlib_dir=fallback_qlib_dir).symbols
        for universe in args.universes
    }


def _load_current_provider_symbols(root: Path, universes: list[str]) -> dict[str, list[str]]:
    provider_specs = {
        "csi300": (root / "data/qlib/cn_data_csi300_current", "csi300_current"),
        "csi500": (root / "data/qlib/cn_data_current", "csi500_current"),
    }
    loaded: dict[str, list[str]] = {}
    for universe in universes:
        qlib_dir, market = provider_specs[universe]
        symbols = load_symbols_from_existing_qlib(qlib_dir, market)
        if not symbols:
            return {}
        loaded[universe] = symbols
    return loaded


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return root / candidate


def _default_industry_source(root: Path, explicit_path: str | None) -> Path | None:
    if explicit_path:
        return _resolve(root, explicit_path)
    default_path = root / "data/security_industry_overrides.csv"
    return default_path if default_path.exists() else None


def _normalize_date_arg(value: str) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


if __name__ == "__main__":
    raise SystemExit(main())
