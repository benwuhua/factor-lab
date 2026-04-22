from __future__ import annotations

from pathlib import Path

import pandas as pd


def ensure_report_dir(path: str | Path = "reports") -> Path:
    report_dir = Path(path)
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir


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
