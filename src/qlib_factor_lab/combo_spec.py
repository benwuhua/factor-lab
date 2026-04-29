from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from .signal import SignalConfig, SignalFactor


MARKET_SOURCES = {"approved_factor", "qlib_expression", "market_ohlcv"}
FUNDAMENTAL_SOURCE = "fundamental_quality"


@dataclass(frozen=True)
class ComboMember:
    name: str
    source: str
    active: bool = True
    weight: float = 1.0
    direction: int = 1
    family: str = ""
    logic_bucket: str = ""
    expression: str = ""
    approval_status: str = "core"
    regime_profile: str = "all_weather"
    components: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class ComboSpec:
    name: str
    description: str
    members: tuple[ComboMember, ...]
    fundamental_path: Path = Path("data/fundamental_quality.csv")


def load_combo_spec(source: str | Path | dict[str, Any]) -> ComboSpec:
    if isinstance(source, dict):
        data = source
    else:
        data = yaml.safe_load(Path(source).read_text(encoding="utf-8")) or {}
    members = tuple(_combo_member(item) for item in data.get("members", []))
    if not members:
        raise ValueError("combo spec must define at least one member")
    return ComboSpec(
        name=str(data.get("name", "") or "combo_spec"),
        description=str(data.get("description", "")),
        members=members,
        fundamental_path=Path(str(data.get("fundamental_path", "data/fundamental_quality.csv"))),
    )


def signal_factors_from_combo_spec(spec: ComboSpec, approved_factors: list[SignalFactor] | None = None) -> list[SignalFactor]:
    approved = {factor.name: factor for factor in approved_factors or []}
    factors = []
    for member in spec.members:
        if not member.active:
            continue
        base = approved.get(member.name)
        expression = member.expression or (base.expression if base else member.name)
        factors.append(
            SignalFactor(
                name=member.name,
                expression=expression,
                direction=member.direction,
                family=member.family or (base.family if base else member.name),
                logic_bucket=member.logic_bucket or (base.logic_bucket if base else ""),
                approval_status=member.approval_status or (base.approval_status if base else "core"),
                regime_profile=member.regime_profile or (base.regime_profile if base else "all_weather"),
            )
        )
    return factors


def market_signal_factors_from_combo_spec(
    spec: ComboSpec,
    approved_factors: list[SignalFactor] | None = None,
) -> list[SignalFactor]:
    all_factors = signal_factors_from_combo_spec(spec, approved_factors)
    by_name = {factor.name: factor for factor in all_factors}
    return [by_name[member.name] for member in spec.members if member.active and member.source in MARKET_SOURCES]


def signal_config_for_combo_spec(config: SignalConfig, spec: ComboSpec) -> SignalConfig:
    family_weights = dict(config.family_weights)
    for member in spec.members:
        if not member.active:
            continue
        family_weights[member.family or member.name] = member.weight
    return config.__class__(
        **{
            **config.__dict__,
            "combination_mode": "family_first",
            "family_weights": family_weights,
        }
    )


def build_combo_exposures(
    root: str | Path,
    spec: ComboSpec,
    base_exposures: pd.DataFrame,
    signal_config: SignalConfig,
) -> pd.DataFrame:
    frame = base_exposures.copy()
    if frame.empty:
        return frame
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    run_date = _resolve_run_date(signal_config.run_date, frame)
    for member in spec.members:
        if not member.active:
            continue
        if member.source == FUNDAMENTAL_SOURCE:
            frame = _merge_fundamental_member(Path(root), frame, spec, member, run_date)
        elif member.name not in frame.columns:
            raise ValueError(f"combo member exposure is missing: {member.name}")
    return frame


def approved_factors_payload_from_combo_spec(spec: ComboSpec, approved_factors: list[SignalFactor] | None = None) -> dict[str, Any]:
    return {
        "generated_from_combo_spec": spec.name,
        "approved_factors": [
            {
                "name": factor.name,
                "expression": factor.expression,
                "direction": factor.direction,
                "family": factor.family,
                "logic_bucket": factor.logic_bucket,
                "approval_status": factor.approval_status,
                "regime_profile": factor.regime_profile,
            }
            for factor in signal_factors_from_combo_spec(spec, approved_factors)
        ],
    }


def factor_diagnostics_from_combo_spec(spec: ComboSpec, existing: pd.DataFrame | None = None) -> pd.DataFrame:
    existing_by_factor = {}
    if existing is not None and not existing.empty and "factor" in existing.columns:
        existing_by_factor = {
            str(row["factor"]): row
            for _, row in existing.iterrows()
        }
    rows = []
    for member in spec.members:
        if not member.active:
            continue
        matched = existing_by_factor.get(member.name)
        member_context = f"source={member.source}; direction={member.direction}; weight={member.weight:g}"
        existing_concerns = _value_from_row(matched, "concerns")
        concerns = member_context if not existing_concerns else f"{member_context}; {existing_concerns}"
        rows.append(
            {
                "factor": member.name,
                "family": member.family or _value_from_row(matched, "family") or member.name,
                "suggested_role": _value_from_row(matched, "suggested_role") or f"combo_member:{spec.name}",
                "neutral_rank_ic_h20": _value_from_row(matched, "neutral_rank_ic_h20"),
                "neutral_long_short_h20": _value_from_row(matched, "neutral_long_short_h20"),
                "concerns": concerns,
            }
        )
    return pd.DataFrame(rows)


def _combo_member(raw: dict[str, Any]) -> ComboMember:
    if "name" not in raw:
        raise ValueError("combo member is missing name")
    return ComboMember(
        name=str(raw["name"]),
        source=str(raw.get("source", "approved_factor")),
        active=bool(raw.get("active", True)),
        weight=float(raw.get("weight", 1.0)),
        direction=int(raw.get("direction", 1)),
        family=str(raw.get("family", "")),
        logic_bucket=str(raw.get("logic_bucket", "")),
        expression=str(raw.get("expression", "")),
        approval_status=str(raw.get("approval_status", "core")),
        regime_profile=str(raw.get("regime_profile", "all_weather")),
        components=tuple(dict(item) for item in raw.get("components", [])),
    )


def _merge_fundamental_member(
    root: Path,
    frame: pd.DataFrame,
    spec: ComboSpec,
    member: ComboMember,
    run_date: str,
) -> pd.DataFrame:
    path = spec.fundamental_path if spec.fundamental_path.is_absolute() else root / spec.fundamental_path
    fundamentals = pd.read_csv(path) if path.exists() else pd.DataFrame()
    if fundamentals.empty:
        frame[member.name] = pd.NA
        return frame
    latest = _latest_fundamentals(fundamentals, run_date)
    if latest.empty:
        frame[member.name] = pd.NA
        return frame
    fields = [str(component.get("field")) for component in member.components if component.get("field")]
    if not fields:
        fields = [member.name] if member.name in latest.columns else []
    keep = ["instrument", *[field for field in fields if field in latest.columns]]
    merged = frame.merge(latest.loc[:, keep], on="instrument", how="left")
    if member.components:
        score = pd.Series(0.0, index=merged.index)
        used = False
        for component in member.components:
            field = str(component.get("field", ""))
            if field not in merged.columns:
                continue
            values = pd.to_numeric(merged[field], errors="coerce") * int(component.get("direction", 1))
            score = score.add(_zscore(values) * float(component.get("weight", 1.0)), fill_value=0.0)
            used = True
        merged[member.name] = score if used else pd.NA
    elif member.name in merged.columns:
        merged[member.name] = pd.to_numeric(merged[member.name], errors="coerce")
    else:
        merged[member.name] = pd.NA
    return merged


def _latest_fundamentals(fundamentals: pd.DataFrame, run_date: str) -> pd.DataFrame:
    data = fundamentals.copy()
    if "available_at" not in data.columns:
        data["available_at"] = data.get("announce_date", "")
    data["available_at"] = pd.to_datetime(data["available_at"], errors="coerce")
    data = data.dropna(subset=["instrument", "available_at"])
    data = data[data["available_at"] <= pd.Timestamp(run_date)]
    if data.empty:
        return data
    return data.sort_values("available_at").groupby("instrument", as_index=False).tail(1)


def _resolve_run_date(run_date: str, frame: pd.DataFrame) -> str:
    if str(run_date).lower() == "latest":
        return str(pd.to_datetime(frame["date"]).max().date())
    return str(pd.Timestamp(run_date).date())


def _zscore(series: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    std = values.std(skipna=True)
    if not std or pd.isna(std):
        return pd.Series(0.0, index=series.index)
    return ((values - values.mean(skipna=True)) / std).fillna(0.0)


def _value_from_row(row: pd.Series | None, column: str) -> Any:
    if row is None or column not in row.index:
        return ""
    value = row[column]
    if pd.isna(value):
        return ""
    return value
