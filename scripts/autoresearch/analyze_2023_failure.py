#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import ProjectConfig, load_project_config
from qlib_factor_lab.market_regime import compute_equal_weight_market_regime
from qlib_factor_lab.neutralization import add_size_proxy, neutralize_signal
from qlib_factor_lab.qlib_bootstrap import init_qlib


COMPONENTS = {
    "top_combo_high_mean60_vol_div": "0-Corr($high/Mean($high,20),$volume/Mean($volume,20),20)+(Mean($high,60)-$close)/$close",
    "div_high_volume_20": "0-Corr($high/Mean($high,20),$volume/Mean($volume,20),20)",
    "div_close_volume_20": "0-Corr($close/Mean($close,20),$volume/Mean($volume,20),20)",
    "div_high_amount_20": "0-Corr($high/Mean($high,20),$amount/Mean($amount,20),20)",
    "high_mean60_discount": "(Mean($high,60)-$close)/$close",
    "open_pressure_20_60": "Mean(($open - $close) / $open, 20) + Mean(($open - $close) / $open, 60)",
    "open_pressure_amount_weighted_20_60": "Mean((($open-$close)/$open)*($amount/Mean($amount,20)),20)+Mean(($open-$close)/$open,60)",
}


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Analyze why promoted autoresearch factors failed in 2023.")
    parser.add_argument(
        "--provider-config",
        action="append",
        default=[],
        help="Provider config to analyze. Repeat for csi500/csi300.",
    )
    parser.add_argument("--output-dir", default="reports/autoresearch/failure_2023_analysis")
    parser.add_argument("--start-time", default="2022-01-01")
    parser.add_argument("--end-time", default="2024-12-31")
    parser.add_argument("--horizon", type=int, default=20)
    args = parser.parse_args()

    providers = args.provider_config or ["configs/provider_current.yaml", "configs/provider_csi300_current.yaml"]
    output_dir = root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    component_rows: list[dict[str, object]] = []
    month_rows: list[dict[str, object]] = []
    regime_rows: list[dict[str, object]] = []
    market_rows: list[dict[str, object]] = []
    error_rows: list[dict[str, object]] = []

    for provider in providers:
        config = load_project_config(root / provider)
        try:
            init_qlib(config)
            component, month, regime, market = analyze_provider(
                config=config,
                start_time=args.start_time,
                end_time=args.end_time,
                horizon=args.horizon,
            )
        except Exception as exc:
            error_rows.append({"market": config.market, "error": str(exc)})
            continue
        component_rows.extend(component)
        month_rows.extend(month)
        regime_rows.extend(regime)
        market_rows.extend(market)

    component_frame = pd.DataFrame(component_rows)
    month_frame = pd.DataFrame(month_rows)
    regime_frame = pd.DataFrame(regime_rows)
    market_frame = pd.DataFrame(market_rows)
    error_frame = pd.DataFrame(error_rows)

    component_frame.to_csv(output_dir / "component_overall_2023.tsv", sep="\t", index=False)
    month_frame.to_csv(output_dir / "component_by_month_2023.tsv", sep="\t", index=False)
    regime_frame.to_csv(output_dir / "component_by_regime_2023.tsv", sep="\t", index=False)
    market_frame.to_csv(output_dir / "market_by_month_2023.tsv", sep="\t", index=False)
    error_frame.to_csv(output_dir / "errors.tsv", sep="\t", index=False)
    write_summary(output_dir, component_frame, month_frame, regime_frame, market_frame, error_frame)

    print(f"wrote: {output_dir}")
    print(f"component_rows: {len(component_rows)}")
    print(f"month_rows: {len(month_rows)}")
    print(f"regime_rows: {len(regime_rows)}")
    print(f"errors: {len(error_rows)}")
    return 0 if not error_rows else 1


def analyze_provider(
    config: ProjectConfig,
    start_time: str,
    end_time: str,
    horizon: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    from qlib.data import D

    instruments = D.instruments(config.market)
    fields = list(COMPONENTS.values()) + ["$close", "$volume"]
    names = list(COMPONENTS.keys()) + ["close", "volume"]
    frame = D.features(
        instruments,
        fields,
        start_time=start_time,
        end_time=end_time,
        freq=config.freq,
    )
    frame.columns = names
    frame = frame.dropna(subset=["close", "volume"]).copy()
    regime = compute_equal_weight_market_regime(frame[["close"]])
    frame = attach_regime(frame, regime)

    close = frame["close"].groupby(level="instrument")
    frame["future_ret"] = close.shift(-horizon) / frame["close"] - 1.0
    frame["year"] = frame.index.get_level_values("datetime").year
    frame["month"] = frame.index.get_level_values("datetime").to_period("M").astype(str)

    component_rows: list[dict[str, object]] = []
    month_rows: list[dict[str, object]] = []
    regime_rows: list[dict[str, object]] = []
    target = frame[frame["year"] == 2023].copy()
    for component in COMPONENTS:
        prepared = prepare_signal_frame(target, component)
        daily = daily_stats(prepared)
        component_rows.append(summary_row(config.market, component, daily, "overall", "2023"))

        for month, subset in prepared.groupby("month"):
            month_daily = daily_stats(subset)
            if not month_daily.empty:
                month_rows.append(summary_row(config.market, component, month_daily, "month", month))

        for market_regime, subset in prepared.groupby("market_regime"):
            regime_daily = daily_stats(subset)
            if not regime_daily.empty:
                regime_rows.append(summary_row(config.market, component, regime_daily, "regime", str(market_regime)))

    return component_rows, month_rows, regime_rows, market_month_rows(config.market, target)


def attach_regime(frame: pd.DataFrame, regime: pd.DataFrame) -> pd.DataFrame:
    lookup = regime[["market_regime", "market_ret", "trend_return"]].copy()
    lookup.index = pd.to_datetime(lookup.index)
    merged = frame.reset_index().merge(lookup, left_on="datetime", right_index=True, how="left")
    merged["market_regime"] = merged["market_regime"].fillna("unknown")
    return merged.set_index(["datetime", "instrument"]).sort_index()


def prepare_signal_frame(frame: pd.DataFrame, component: str) -> pd.DataFrame:
    data = frame.dropna(subset=[component, "future_ret", "close", "volume"]).copy()
    data["signal"] = data[component]
    data = add_size_proxy(data)
    return neutralize_signal(data, exposure_cols=["size_proxy"])


def daily_stats(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for date, daily in frame.groupby(level="datetime"):
        if len(daily) < 10:
            continue
        rows.append(
            {
                "datetime": pd.Timestamp(date),
                "raw_rank_ic": daily["signal"].corr(daily["future_ret"], method="spearman"),
                "neutral_rank_ic": daily["signal_neutral"].corr(daily["future_ret"], method="spearman"),
                "size_corr": daily["signal"].corr(daily["size_proxy"], method="spearman"),
                "observations": int(len(daily)),
            }
        )
    return pd.DataFrame(rows)


def summary_row(market: str, component: str, daily: pd.DataFrame, segment_type: str, segment: str) -> dict[str, object]:
    raw = daily["raw_rank_ic"].dropna() if "raw_rank_ic" in daily else pd.Series(dtype=float)
    neutral = daily["neutral_rank_ic"].dropna() if "neutral_rank_ic" in daily else pd.Series(dtype=float)
    size_corr = daily["size_corr"].dropna() if "size_corr" in daily else pd.Series(dtype=float)
    return {
        "market": market,
        "component": component,
        "segment_type": segment_type,
        "segment": segment,
        "days": int(len(daily)),
        "raw_rank_ic_mean": _mean(raw),
        "neutral_rank_ic_mean": _mean(neutral),
        "neutral_positive_rate": float((neutral > 0).mean()) if not neutral.empty else float("nan"),
        "size_corr_mean": _mean(size_corr),
        "raw_minus_neutral": _mean(raw) - _mean(neutral) if not raw.empty and not neutral.empty else float("nan"),
        "mean_observations": _mean(daily["observations"]) if "observations" in daily else float("nan"),
    }


def market_month_rows(market: str, frame: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    daily = frame.reset_index().drop_duplicates("datetime").sort_values("datetime")
    for month, subset in daily.groupby("month"):
        regime_counts = subset["market_regime"].value_counts(normalize=True).to_dict()
        rows.append(
            {
                "market": market,
                "month": month,
                "days": int(len(subset)),
                "equal_weight_market_ret": float((1.0 + subset["market_ret"].fillna(0.0)).prod() - 1.0),
                "down_share": float(regime_counts.get("down", 0.0)),
                "sideways_share": float(regime_counts.get("sideways", 0.0)),
                "up_share": float(regime_counts.get("up", 0.0)),
            }
        )
    return rows


def write_summary(
    output_dir: Path,
    component: pd.DataFrame,
    month: pd.DataFrame,
    regime: pd.DataFrame,
    market: pd.DataFrame,
    errors: pd.DataFrame,
) -> None:
    lines = [
        "# 2023 Failure Analysis",
        "",
        f"- generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"- errors: {0 if errors.empty else len(errors)}",
        "",
        "## Component Summary",
        "",
    ]
    if component.empty:
        lines.append("_No component rows._")
    else:
        ordered = component.sort_values(["market", "neutral_rank_ic_mean"], ascending=[True, False])
        lines.extend(
            markdown_table(
                ordered,
                [
                    "market",
                    "component",
                    "raw_rank_ic_mean",
                    "neutral_rank_ic_mean",
                    "neutral_positive_rate",
                    "size_corr_mean",
                    "raw_minus_neutral",
                ],
            )
        )
    lines.extend(["", "## Worst Months For Top Combo", ""])
    if month.empty:
        lines.append("_No month rows._")
    else:
        top = month[month["component"] == "top_combo_high_mean60_vol_div"].sort_values(
            ["market", "neutral_rank_ic_mean"]
        )
        lines.extend(
            markdown_table(
                top,
                ["market", "segment", "neutral_rank_ic_mean", "raw_rank_ic_mean", "neutral_positive_rate", "size_corr_mean"],
            )
        )
    lines.extend(["", "## Regime Split", ""])
    if regime.empty:
        lines.append("_No regime rows._")
    else:
        lines.extend(
            markdown_table(
                regime.sort_values(["market", "component", "segment"]),
                ["market", "component", "segment", "neutral_rank_ic_mean", "raw_rank_ic_mean", "neutral_positive_rate"],
                limit=80,
            )
        )
    lines.extend(["", "## Market Months", ""])
    if market.empty:
        lines.append("_No market rows._")
    else:
        lines.extend(
            markdown_table(
                market.sort_values(["market", "equal_weight_market_ret"]),
                ["market", "month", "equal_weight_market_ret", "down_share", "sideways_share", "up_share"],
            )
        )
    if not errors.empty:
        lines.extend(["", "## Errors", ""])
        lines.extend(markdown_table(errors, list(errors.columns)))
    (output_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def markdown_table(frame: pd.DataFrame, columns: list[str], limit: int = 60) -> list[str]:
    if frame.empty:
        return ["_No rows._"]
    visible = frame.loc[:, columns].head(limit)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in visible.iterrows():
        cells = []
        for column in columns:
            value = row[column]
            if isinstance(value, float):
                cells.append(f"{value:.5f}" if math.isfinite(value) else "nan")
            elif column in {"component"}:
                cells.append(f"`{value}`")
            else:
                cells.append(str(value))
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def _mean(series: pd.Series) -> float:
    if series.empty:
        return float("nan")
    return float(series.mean())


if __name__ == "__main__":
    raise SystemExit(main())
