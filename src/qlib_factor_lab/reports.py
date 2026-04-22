from __future__ import annotations

from pathlib import Path

import pandas as pd


def _format_percent(value: float | int | str) -> str:
    return f"{float(value) * 100:.2f}%"


def _format_number(value: float | int | str) -> str:
    return f"{float(value):.2f}"


def ensure_report_dir(path: str | Path = "reports") -> Path:
    report_dir = Path(path)
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


def render_event_summary_markdown(
    summary: pd.DataFrame,
    name: str,
    factor: str = "",
    universe: str = "",
    provider_config: str = "",
    command: str = "",
    data_range: str = "",
) -> str:
    if summary.empty:
        raise ValueError("event summary is empty")
    focus = _select_event_focus_row(summary)
    bucket = str(focus["bucket"])
    horizon = int(focus["horizon"])
    trade_count = int(focus["trade_count"])
    metric_note = f"horizon={horizon}, trades={trade_count}"

    return "\n".join(
        [
            f"# {name}",
            "",
            "## Report",
            "",
            f"- Name: {name}",
            "- Date:",
            "- Author:",
            f"- Related factor(s): {factor}",
            f"- Universe: {universe}",
            f"- Provider config: {provider_config}",
            f"- Data range: {data_range}",
            f"- Holding horizon(s): {', '.join(str(item) for item in sorted(summary['horizon'].unique()))}",
            "- Command:",
            "",
            "```bash",
            command,
            "```",
            "",
            "## Key Result",
            "",
            "| Metric | Value | Notes |",
            "|---|---:|---|",
            f"| Focus bucket | {bucket} | {metric_note} |",
            f"| p95-p100 mean return | {_format_percent(focus['mean_return'])} | {metric_note} |",
            f"| p95-p100 median return | {_format_percent(focus['median_return'])} |  |",
            f"| p95-p100 win rate | {_format_percent(focus['win_rate'])} |  |",
            f"| p95-p100 payoff ratio | {_format_number(focus['payoff_ratio'])} |  |",
            f"| p95-p100 MFE mean | {_format_percent(focus['mfe_mean'])} |  |",
            f"| p95-p100 MAE mean | {_format_percent(focus['mae_mean'])} |  |",
            "",
            "## Interpretation",
            "",
            "- What the factor appears to capture:",
            "- When it works:",
            "- When it fails:",
            "- Whether it is a feature-style factor, an event trigger, or a filter:",
            "- Whether it should be used alone, combined, or discarded:",
            "",
            "## Stability Checks",
            "",
            "| Check | Status | Notes |",
            "|---|---|---|",
            "| 5-day horizon |  |  |",
            "| 20-day horizon |  |  |",
            "| Yearly split |  |  |",
            "| Market-regime split |  |  |",
            "| CSI300 / CSI500 comparison |  |  |",
            "| Direction sanity check |  |  |",
            "",
            "## Caveats",
            "",
            "- Data quality:",
            "- Survivorship or universe bias:",
            "- Transaction costs:",
            "- Capacity / liquidity:",
            "- Next validation step:",
            "",
        ]
    )


def write_event_summary_markdown(
    summary: pd.DataFrame,
    output_path: str | Path,
    name: str,
    factor: str = "",
    universe: str = "",
    provider_config: str = "",
    command: str = "",
    data_range: str = "",
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        render_event_summary_markdown(
            summary,
            name=name,
            factor=factor,
            universe=universe,
            provider_config=provider_config,
            command=command,
            data_range=data_range,
        ),
        encoding="utf-8",
    )
    return output


def _select_event_focus_row(summary: pd.DataFrame) -> pd.Series:
    required = {
        "bucket",
        "horizon",
        "trade_count",
        "mean_return",
        "median_return",
        "win_rate",
        "payoff_ratio",
        "mfe_mean",
        "mae_mean",
    }
    missing = required.difference(summary.columns)
    if missing:
        raise ValueError(f"event summary is missing columns: {sorted(missing)}")
    p95 = summary[summary["bucket"] == "p95_p100"].copy()
    if p95.empty:
        p95 = summary.copy()
    h20 = p95[p95["horizon"] == 20]
    if not h20.empty:
        return h20.iloc[0]
    return p95.sort_values("horizon", ascending=False).iloc[0]


def plot_quantile_returns(frame: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    quantile_cols = [col for col in frame.columns if col.startswith("q") and col.endswith("_mean_return")]
    if not quantile_cols:
        raise ValueError("frame does not contain quantile return columns")

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 4.5))
    row = frame.iloc[0]
    values = [row[col] for col in quantile_cols]
    labels = [col.replace("_mean_return", "").upper() for col in quantile_cols]
    ax.bar(labels, values, color="#2F6FED")
    title_bits = []
    if "factor" in row:
        title_bits.append(str(row["factor"]))
    if "horizon" in row:
        title_bits.append(f"horizon={row['horizon']}")
    ax.set_title(" | ".join(title_bits) or "Quantile Mean Returns")
    ax.set_ylabel("Mean future return")
    ax.axhline(0, color="#333333", linewidth=0.8)
    fig.tight_layout()
    fig.savefig(output, dpi=160)
    plt.close(fig)
    return output
