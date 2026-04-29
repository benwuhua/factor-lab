from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .combo_spec import ComboMember, ComboSpec, FUNDAMENTAL_SOURCE, MARKET_SOURCES
from .config import ProjectConfig
from .factor_eval import compute_quantile_return_summary
from .qlib_bootstrap import init_qlib


def evaluate_combo_member_diagnostics(
    frame: pd.DataFrame,
    spec: ComboSpec,
    *,
    horizons: Iterable[int] = (20,),
    quantiles: int = 5,
) -> pd.DataFrame:
    if frame.empty:
        return _empty_diagnostics(horizons)
    data = _ensure_factor_index(frame)
    rows: list[dict[str, object]] = []
    for member in spec.members:
        if not member.active or member.name not in data.columns:
            continue
        row: dict[str, object] = {
            "factor": member.name,
            "family": member.family or member.name,
            "suggested_role": "combo_recent_formal",
            "concerns": f"recent_formal_raw_ic_ls; source={member.source}; direction={member.direction}; weight={member.weight:g}",
        }
        for horizon in horizons:
            metrics = _evaluate_one_horizon(data, member, int(horizon), quantiles)
            row[f"neutral_rank_ic_h{int(horizon)}"] = metrics["rank_ic_mean"]
            row[f"neutral_long_short_h{int(horizon)}"] = metrics["long_short_mean_return"]
            row[f"observations_h{int(horizon)}"] = metrics["observations"]
        rows.append(row)
    return pd.DataFrame(rows)


def fetch_combo_member_frame(
    config: ProjectConfig,
    spec: ComboSpec,
    *,
    root: str | Path,
    initialize: bool = True,
) -> pd.DataFrame:
    if initialize:
        init_qlib(config)
    from qlib.data import D

    instruments = D.instruments(config.market)
    active_members = [member for member in spec.members if member.active]
    fields = ["$close"]
    names = ["close"]
    for member in active_members:
        if member.source in MARKET_SOURCES:
            fields.append(member.expression or member.name)
            names.append(member.name)
    frame = D.features(
        instruments,
        fields,
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    frame.columns = names
    frame = frame.dropna(subset=["close"])
    return attach_fundamental_combo_members(root, frame, spec)


def attach_fundamental_combo_members(root: str | Path, frame: pd.DataFrame, spec: ComboSpec) -> pd.DataFrame:
    members = [member for member in spec.members if member.active and member.source == FUNDAMENTAL_SOURCE]
    if not members or frame.empty:
        return frame
    path = spec.fundamental_path if spec.fundamental_path.is_absolute() else Path(root) / spec.fundamental_path
    fundamentals = pd.read_csv(path) if path.exists() else pd.DataFrame()
    if fundamentals.empty:
        output = frame.copy()
        for member in members:
            output[member.name] = pd.NA
        return output

    base = _ensure_factor_index(frame).reset_index()
    base["datetime"] = pd.to_datetime(base["datetime"], errors="coerce")
    pieces = []
    for instrument, group in base.sort_values("datetime").groupby("instrument", sort=False):
        source = fundamentals[fundamentals["instrument"].astype(str) == str(instrument)].copy()
        if source.empty:
            filled = group.copy()
        else:
            source["available_at"] = pd.to_datetime(source.get("available_at", source.get("announce_date", "")), errors="coerce")
            source = source.dropna(subset=["available_at"]).sort_values("available_at")
            filled = pd.merge_asof(
                group.sort_values("datetime"),
                source.sort_values("available_at"),
                left_on="datetime",
                right_on="available_at",
                by="instrument",
                direction="backward",
                suffixes=("", "_fundamental"),
            )
        pieces.append(filled)
    merged = pd.concat(pieces, ignore_index=True) if pieces else base
    for member in members:
        merged[member.name] = _score_fundamental_member(merged, member)
    return merged.set_index(["datetime", "instrument"]).sort_index()


def _evaluate_one_horizon(frame: pd.DataFrame, member: ComboMember, horizon: int, quantiles: int) -> dict[str, float | int]:
    scored = frame[[member.name, "close"]].copy()
    scored["signal"] = pd.to_numeric(scored[member.name], errors="coerce") * member.direction
    close = scored["close"].groupby(level="instrument")
    scored["future_ret"] = close.shift(-horizon) / scored["close"] - 1.0
    scored = scored.dropna(subset=["signal", "future_ret"])
    daily_rank_ic = scored.groupby(level="datetime").apply(
        _daily_rank_ic
    )
    quantile_summary = compute_quantile_return_summary(scored, "signal", "future_ret", quantiles)
    return {
        "rank_ic_mean": float(daily_rank_ic.mean()) if not daily_rank_ic.empty else float("nan"),
        "long_short_mean_return": float(quantile_summary.get("long_short_mean_return", float("nan"))),
        "observations": int(len(scored)),
    }


def _daily_rank_ic(daily: pd.DataFrame) -> float:
    if len(daily) < 2:
        return float("nan")
    if daily["signal"].nunique(dropna=True) < 2 or daily["future_ret"].nunique(dropna=True) < 2:
        return float("nan")
    value = daily["signal"].corr(daily["future_ret"], method="spearman")
    return float(value) if pd.notna(value) else float("nan")


def _score_fundamental_member(frame: pd.DataFrame, member: ComboMember) -> pd.Series:
    if member.components:
        score = pd.Series(0.0, index=frame.index)
        used = False
        for component in member.components:
            field = str(component.get("field", ""))
            if field not in frame.columns:
                continue
            values = pd.to_numeric(frame[field], errors="coerce") * int(component.get("direction", 1))
            score = score.add(_daily_zscore(values, frame["datetime"]) * float(component.get("weight", 1.0)), fill_value=0.0)
            used = True
        return score if used else pd.Series(pd.NA, index=frame.index)
    if member.name in frame.columns:
        return pd.to_numeric(frame[member.name], errors="coerce")
    return pd.Series(pd.NA, index=frame.index)


def _daily_zscore(values: pd.Series, dates: pd.Series) -> pd.Series:
    def zscore(group: pd.Series) -> pd.Series:
        std = group.std(skipna=True)
        if not std or pd.isna(std):
            return pd.Series(0.0, index=group.index)
        return ((group - group.mean(skipna=True)) / std).fillna(0.0)

    return values.groupby(pd.to_datetime(dates).dt.strftime("%Y-%m-%d"), group_keys=False).apply(zscore)


def _ensure_factor_index(frame: pd.DataFrame) -> pd.DataFrame:
    if isinstance(frame.index, pd.MultiIndex):
        names = list(frame.index.names)
        if "datetime" in names and "instrument" in names:
            return frame.sort_index()
    data = frame.copy()
    if "datetime" not in data.columns and "date" in data.columns:
        data["datetime"] = data["date"]
    if "datetime" not in data.columns or "instrument" not in data.columns:
        raise ValueError("combo diagnostics frame requires datetime/date and instrument")
    data["datetime"] = pd.to_datetime(data["datetime"], errors="coerce")
    return data.dropna(subset=["datetime", "instrument"]).set_index(["datetime", "instrument"]).sort_index()


def _empty_diagnostics(horizons: Iterable[int]) -> pd.DataFrame:
    columns = ["factor", "family", "suggested_role", "concerns"]
    for horizon in horizons:
        columns.extend([f"neutral_rank_ic_h{int(horizon)}", f"neutral_long_short_h{int(horizon)}", f"observations_h{int(horizon)}"])
    return pd.DataFrame(columns=columns)
