from __future__ import annotations

from pathlib import Path

import pandas as pd


def apply_fills_to_positions(
    current_positions: pd.DataFrame | None,
    fills: pd.DataFrame,
    min_weight: float = 1e-8,
) -> pd.DataFrame:
    weights = {}
    if current_positions is not None and not current_positions.empty:
        weights.update(
            {
                str(row["instrument"]): float(row["current_weight"])
                for _, row in current_positions.iterrows()
            }
        )
    for _, fill in fills.iterrows():
        if str(fill.get("status", "")) not in {"filled", "partial"}:
            continue
        instrument = str(fill["instrument"])
        weights[instrument] = weights.get(instrument, 0.0) + float(fill["fill_delta_weight"])

    rows = [
        {"instrument": instrument, "current_weight": weight}
        for instrument, weight in sorted(weights.items())
        if abs(weight) > min_weight
    ]
    return pd.DataFrame(rows, columns=["instrument", "current_weight"])


def write_positions_state(positions: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    positions.to_csv(output, index=False)
    return output
