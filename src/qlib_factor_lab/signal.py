from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import ProjectConfig, load_yaml
from .qlib_bootstrap import init_qlib


@dataclass(frozen=True)
class SignalFactor:
    name: str
    expression: str
    direction: int
    family: str
    approval_status: str
    regime_profile: str


@dataclass(frozen=True)
class SignalConfig:
    approved_factors_path: Path
    provider_config: Path
    run_date: str
    active_regime: str
    status_weights: dict[str, float]
    regime_weights: dict[str, dict[str, float]]
    rule_weight: float
    model_weight: float
    signals_output_path: Path
    summary_output_path: Path


def load_signal_config(path: str | Path) -> SignalConfig:
    data = load_yaml(path)
    weights = data.get("weights", {})
    ensemble = weights.get("ensemble", {})
    output = data.get("output", {})
    return SignalConfig(
        approved_factors_path=Path(data.get("approved_factors_path", "reports/approved_factors.yaml")),
        provider_config=Path(data.get("provider_config", "configs/provider_current.yaml")),
        run_date=str(data.get("run_date", "latest")),
        active_regime=str(data.get("active_regime", "sideways")),
        status_weights={str(k): float(v) for k, v in weights.get("approval_status", {}).items()},
        regime_weights={
            str(profile): {str(regime): float(value) for regime, value in mapping.items()}
            for profile, mapping in weights.get("regime", {}).items()
        },
        rule_weight=float(ensemble.get("rule_score", 1.0)),
        model_weight=float(ensemble.get("model_score", 0.0)),
        signals_output_path=Path(output.get("signals", "reports/signals_latest.csv")),
        summary_output_path=Path(output.get("summary", "reports/signal_summary_latest.md")),
    )


def load_approved_signal_factors(path: str | Path) -> list[SignalFactor]:
    data = load_yaml(path)
    factors = []
    for raw in data.get("approved_factors", []):
        factors.append(
            SignalFactor(
                name=str(raw["name"]),
                expression=str(raw["expression"]),
                direction=int(raw.get("direction", 1)),
                family=str(raw.get("family", "")),
                approval_status=str(raw.get("approval_status", "")),
                regime_profile=str(raw.get("regime_profile", "all_weather")),
            )
        )
    return factors


def build_daily_signal(
    exposures: pd.DataFrame,
    factors: list[SignalFactor],
    config: SignalConfig,
) -> pd.DataFrame:
    required = {"date", "instrument"}
    missing = required - set(exposures.columns)
    if missing:
        raise ValueError(f"exposures are missing columns: {sorted(missing)}")

    frame = exposures.copy()
    if "tradable" not in frame.columns:
        frame["tradable"] = True
    frame["tradable"] = frame["tradable"].map(_bool_value)
    for column in ["suspended", "limit_up", "limit_down", "buy_blocked", "sell_blocked"]:
        if column not in frame.columns:
            frame[column] = False
        frame[column] = frame[column].map(_bool_value)
    frame["active_regime"] = config.active_regime
    frame["model_score"] = frame["model_score"].astype(float) if "model_score" in frame.columns else 0.0

    contribution_cols: list[str] = []
    active_factor_count = 0
    for factor in factors:
        if factor.name not in frame.columns:
            frame[f"{factor.name}_contribution"] = 0.0
            contribution_cols.append(f"{factor.name}_contribution")
            continue
        multiplier = factor_weight(factor, config)
        if multiplier != 0:
            active_factor_count += 1
        contribution_col = f"{factor.name}_contribution"
        frame[contribution_col] = _zscore(frame[factor.name].astype(float) * factor.direction) * multiplier
        contribution_cols.append(contribution_col)

    if contribution_cols:
        frame["rule_score"] = frame[contribution_cols].sum(axis=1)
    else:
        frame["rule_score"] = 0.0
    frame["ensemble_score"] = frame["rule_score"] * config.rule_weight + frame["model_score"] * config.model_weight
    top = frame.apply(lambda row: _top_contributions(row, factors, contribution_cols), axis=1, result_type="expand")
    top.columns = ["top_factor_1", "top_factor_1_contribution", "top_factor_2", "top_factor_2_contribution"]
    frame = pd.concat([frame, top], axis=1)
    frame["risk_flags"] = frame.apply(lambda row: _risk_flags(row, active_factor_count), axis=1)

    output_cols = [
        "date",
        "instrument",
        "tradable",
        *_passthrough_columns(frame),
        "rule_score",
        "model_score",
        "ensemble_score",
        "active_regime",
        "top_factor_1",
        "top_factor_1_contribution",
        "top_factor_2",
        "top_factor_2_contribution",
        "risk_flags",
        *contribution_cols,
    ]
    return frame.loc[:, output_cols].sort_values("ensemble_score", ascending=False).reset_index(drop=True)


def fetch_daily_factor_exposures(
    project_config: ProjectConfig,
    factors: list[SignalFactor],
    run_date: str,
) -> pd.DataFrame:
    if not factors:
        raise ValueError("at least one approved factor is required to build daily signal exposures")

    init_qlib(project_config)
    effective_run_date = resolve_run_date(project_config, run_date)

    from qlib.data import D

    fields = [factor.expression for factor in factors]
    names = [factor.name for factor in factors]
    fields.append("Mean($amount,20)")
    names.append("amount_20d")
    fields.append("$close")
    names.append("last_price")
    frame = D.features(
        D.instruments(project_config.market),
        fields,
        start_time=effective_run_date,
        end_time=effective_run_date,
        freq=project_config.freq,
    )
    frame.columns = names
    frame = frame.reset_index()
    if "datetime" in frame.columns:
        frame = frame.rename(columns={"datetime": "date"})
    if "date" not in frame.columns or "instrument" not in frame.columns:
        raise ValueError("qlib feature frame must include date/datetime and instrument columns")
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    factor_names = [factor.name for factor in factors]
    frame["tradable"] = frame[factor_names].notna().all(axis=1)
    frame = frame.dropna(subset=factor_names, how="all")
    if frame.empty:
        raise ValueError(f"no factor exposures were available for {effective_run_date}")
    return frame.loc[:, ["date", "instrument", "tradable", *names]].reset_index(drop=True)


def resolve_run_date(project_config: ProjectConfig, run_date: str) -> str:
    if run_date != "latest":
        return run_date

    from qlib.data import D

    calendar = D.calendar(start_time=project_config.start_time, end_time=project_config.end_time, freq=project_config.freq)
    if len(calendar) == 0:
        return project_config.end_time
    return pd.Timestamp(calendar[-1]).strftime("%Y-%m-%d")


def factor_weight(factor: SignalFactor, config: SignalConfig) -> float:
    status_weight = config.status_weights.get(factor.approval_status, 0.0)
    regime_weight = config.regime_weights.get(factor.regime_profile, {}).get(config.active_regime, 1.0)
    return status_weight * regime_weight


def write_daily_signal(signal: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    signal.to_csv(output, index=False)
    return output


def write_signal_summary(
    signal: pd.DataFrame,
    factors: list[SignalFactor],
    config: SignalConfig,
    output_path: str | Path,
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    tradable = signal[signal["tradable"]]
    lines = [
        "# Daily Signal Summary",
        "",
        f"- run_date: {config.run_date}",
        f"- active_regime: {config.active_regime}",
        f"- instruments: {len(signal)}",
        f"- tradable: {len(tradable)}",
        f"- approved_factors: {len(factors)}",
        "",
        "## Top Signals",
        "",
        "| instrument | ensemble_score | rule_score | top_factor_1 | top_factor_1_contribution | risk_flags |",
        "|---|---:|---:|---|---:|---|",
    ]
    for _, row in signal.head(20).iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["instrument"]),
                    _format_float(row["ensemble_score"]),
                    _format_float(row["rule_score"]),
                    str(row["top_factor_1"]),
                    _format_float(row["top_factor_1_contribution"]),
                    str(row["risk_flags"]),
                ]
            )
            + " |"
        )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def _zscore(series: pd.Series) -> pd.Series:
    mean = series.mean(skipna=True)
    std = series.std(skipna=True)
    if not std or not math.isfinite(float(std)):
        return pd.Series(0.0, index=series.index)
    return ((series - mean) / std).fillna(0.0)


def _top_contributions(row: pd.Series, factors: list[SignalFactor], contribution_cols: list[str]) -> tuple[str, float, str, float]:
    items = []
    for factor, col in zip(factors, contribution_cols):
        value = float(row[col])
        if value != 0 and math.isfinite(value):
            items.append((factor.name, value))
    items.sort(key=lambda item: abs(item[1]), reverse=True)
    first = items[0] if items else ("", 0.0)
    second = items[1] if len(items) > 1 else ("", 0.0)
    return first[0], first[1], second[0], second[1]


def _risk_flags(row: pd.Series, active_factor_count: int) -> str:
    flags = []
    if not _bool_value(row["tradable"]):
        flags.append("not_tradable")
    if active_factor_count == 0:
        flags.append("regime_gated")
    return ";".join(flags)


def _passthrough_columns(frame: pd.DataFrame) -> list[str]:
    candidates = [
        "amount_20d",
        "last_price",
        "amount",
        "volume_20d",
        "volume",
        "suspended",
        "limit_up",
        "limit_down",
        "buy_blocked",
        "sell_blocked",
    ]
    return [column for column in candidates if column in frame.columns]


def _bool_value(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "nan"}
    return bool(value)


def _format_float(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if not math.isfinite(number):
        return "nan"
    return f"{number:.6g}"
