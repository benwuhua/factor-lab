from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_mfe_mae_distribution_plot(trades: pd.DataFrame, output: str | Path) -> None:
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plot_data = trades.copy()
    fig, ax = plt.subplots(figsize=(11, 6))
    if plot_data.empty:
        ax.text(0.5, 0.5, "No Wangji event trades", ha="center", va="center", fontsize=14)
        ax.set_axis_off()
        fig.savefig(output_path, dpi=160, bbox_inches="tight")
        plt.close(fig)
        return

    plot_data["label"] = plot_data["factor"].astype(str) + " h" + plot_data["horizon"].astype(str)
    labels = sorted(plot_data["label"].unique())
    mfe_series = [plot_data.loc[plot_data["label"] == label, "mfe"].dropna() * 100 for label in labels]
    mae_series = [plot_data.loc[plot_data["label"] == label, "mae"].dropna() * 100 for label in labels]
    positions = list(range(1, len(labels) + 1))

    mfe = ax.boxplot(
        mfe_series,
        positions=[pos - 0.18 for pos in positions],
        widths=0.28,
        patch_artist=True,
        showfliers=False,
        medianprops={"color": "#111827", "linewidth": 1.2},
    )
    mae = ax.boxplot(
        mae_series,
        positions=[pos + 0.18 for pos in positions],
        widths=0.28,
        patch_artist=True,
        showfliers=False,
        medianprops={"color": "#111827", "linewidth": 1.2},
    )
    for patch in mfe["boxes"]:
        patch.set_facecolor("#86efac")
        patch.set_edgecolor("#166534")
    for patch in mae["boxes"]:
        patch.set_facecolor("#fca5a5")
        patch.set_edgecolor("#991b1b")

    ax.axhline(0, color="#6b7280", linewidth=0.9, linestyle="--")
    ax.set_title("Wangji Independent Events: MFE / MAE Distribution", fontsize=14, pad=14)
    ax.set_ylabel("Return path excursion (%)")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, rotation=35, ha="right")
    ax.grid(axis="y", color="#e5e7eb", linewidth=0.8)
    ax.legend([mfe["boxes"][0], mae["boxes"][0]], ["MFE", "MAE"], loc="upper left")
    fig.tight_layout()
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
