#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

import pandas as pd

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.factor_eval import write_eval_report
from qlib_factor_lab.qlib_bootstrap import init_qlib
from qlib_factor_lab.wangji_patterns import FACTOR2_SIGNAL_COLUMN, compute_wangji_factor2_events


DEFAULT_SAMPLE_CASES: tuple[tuple[str, str, str], ...] = (
    ("SZ301326", "2026-04-29", "捷邦科技-钻石买点样本"),
    ("SZ300467", "2026-04-27", "迅游科技-钻石买点样本"),
    ("SZ002436", "2026-04-27", "兴森科技-钻石买点样本"),
    ("SZ301018", "2026-05-07", "申菱环境-平台回踩确认样本"),
    ("SH603986", "2026-01-16", "兆易创新-历史钻石样本"),
)


def parse_sample_case(raw: str) -> tuple[str, str, str]:
    parts = raw.split(":", 2)
    if len(parts) == 1:
        return parts[0].strip(), "", parts[0].strip()
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip(), parts[0].strip()
    return parts[0].strip(), parts[1].strip(), parts[2].strip()


def fetch_provider_events(provider_config_path: Path, instruments: list[str], end_time: str | None) -> pd.DataFrame:
    from qlib.data import D

    config = load_project_config(provider_config_path)
    if end_time:
        config = replace(config, end_time=end_time)
    init_qlib(config)
    frame = D.features(
        instruments,
        ["$open", "$high", "$low", "$close", "$volume"],
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    if frame.empty:
        return _empty_events()
    frame.columns = ["open", "high", "low", "close", "volume"]
    events = compute_wangji_factor2_events(frame.dropna(subset=["open", "high", "low", "close", "volume"]))
    events = events.reset_index()
    events.insert(0, "universe", config.market)
    events.insert(1, "provider_config", str(provider_config_path))
    return events


def fetch_csv_events(csv_dirs: list[Path], instruments: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for csv_dir in csv_dirs:
        for instrument in instruments:
            path = csv_dir / f"{instrument.lower()}.csv"
            if path.exists():
                frames.append(_read_ohlcv_csv(path, instrument))
    if not frames:
        return _empty_events()
    frame = pd.concat(frames).sort_index()
    events = compute_wangji_factor2_events(frame.dropna(subset=["open", "high", "low", "close", "volume"]))
    events = events.reset_index()
    events.insert(0, "universe", "local_csv")
    events.insert(1, "provider_config", ";".join(str(path) for path in csv_dirs))
    return events


def _read_ohlcv_csv(path: Path, instrument: str) -> pd.DataFrame:
    raw = pd.read_csv(path)
    required = {"date", "open", "high", "low", "close", "volume"}
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {sorted(missing)}")
    frame = raw[["date", "open", "high", "low", "close", "volume"]].copy()
    frame["datetime"] = pd.to_datetime(frame["date"])
    frame["instrument"] = instrument
    frame = frame.drop(columns=["date"]).set_index(["datetime", "instrument"]).sort_index()
    frame.index = frame.index.set_names(["datetime", "instrument"])
    return frame


def build_explanation_rows(
    cases: list[tuple[str, str, str]],
    provider_events: list[pd.DataFrame],
) -> pd.DataFrame:
    rows = []
    for instrument, requested_date, label in cases:
        target_date = pd.Timestamp(requested_date) if requested_date else None
        selected = None
        selected_status = "missing_from_provider"
        prior_selected = None
        prior_status = "missing_from_provider"
        for events in provider_events:
            if events.empty:
                continue
            candidate = events[events["instrument"] == instrument].copy()
            if candidate.empty:
                continue
            candidate["datetime"] = pd.to_datetime(candidate["datetime"])
            if target_date is not None:
                exact = candidate[candidate["datetime"] == target_date]
                if not exact.empty:
                    selected = exact.iloc[-1]
                    selected_status = "evaluated_exact_date"
                    break
                prior = candidate[candidate["datetime"] <= target_date]
                if prior_selected is None and not prior.empty:
                    prior_selected = prior.iloc[-1]
                    prior_status = "evaluated_previous_trading_date"
            else:
                selected = candidate.iloc[-1]
                selected_status = "evaluated_latest"
                break
        if selected is None and prior_selected is not None:
            selected = prior_selected
            selected_status = prior_status
        if selected is None:
            rows.append(
                {
                    "instrument": instrument,
                    "requested_date": requested_date,
                    "label": label,
                    "status": selected_status,
                    FACTOR2_SIGNAL_COLUMN: float("nan"),
                    "failure_reason": "missing_from_provider",
                }
            )
            continue
        row = selected.to_dict()
        row.update(
            {
                "requested_date": requested_date,
                "evaluated_date": str(pd.Timestamp(selected["datetime"]).date()),
                "label": label,
                "status": selected_status,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=["datetime", "instrument", FACTOR2_SIGNAL_COLUMN])


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(
        description="Explain Wangji factor2 2B pullback event evidence for selected instruments and dates."
    )
    parser.add_argument(
        "--provider-config",
        action="append",
        default=None,
        help="Provider config to search; may be repeated. Defaults to CSI500 and CSI300 current.",
    )
    parser.add_argument(
        "--sample-csv-dir",
        action="append",
        default=None,
        help="Directory containing normalized OHLCV CSVs named like sz002436.csv. May be repeated.",
    )
    parser.add_argument(
        "--sample",
        action="append",
        help="Sample case as INSTRUMENT:YYYY-MM-DD:LABEL. May be repeated. Defaults to Wangji screenshot samples.",
    )
    parser.add_argument("--end-time", default=None, help="Optional provider end date override.")
    parser.add_argument("--output", default=str(root / "reports/wangji_factor2_explanations.csv"))
    args = parser.parse_args()

    cases = [parse_sample_case(item) for item in args.sample] if args.sample else list(DEFAULT_SAMPLE_CASES)
    instruments = sorted({instrument for instrument, _, _ in cases})
    provider_configs = [
        Path(path)
        for path in (
            args.provider_config
            or [
                root / "configs/provider_current.yaml",
                root / "configs/provider_csi300_current.yaml",
            ]
        )
    ]
    csv_dirs = [
        Path(path)
        for path in (
            args.sample_csv_dir
            or [
                root / "data/tushare/wangji_samples",
                root / "data/tushare/source_csi500_full",
                root / "data/tushare/source_csi300_full",
            ]
        )
    ]
    provider_events = [fetch_csv_events(csv_dirs, instruments)] + [
        fetch_provider_events(path, instruments, args.end_time) for path in provider_configs
    ]
    explanations = build_explanation_rows(cases, provider_events)
    write_eval_report(explanations, args.output)
    print(explanations.to_string(index=False))
    print(f"wrote: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
