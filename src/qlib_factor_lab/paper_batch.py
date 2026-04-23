from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_yaml
from .orders import OrderConfig, build_order_suggestions, write_orders
from .paper_broker import PaperFillConfig, simulate_paper_fills, write_fills
from .reconcile import ReconcileConfig, reconcile_positions, write_reconciliation_report
from .state import apply_fills_to_positions, write_positions_state


@dataclass(frozen=True)
class PaperBatchConfig:
    run_root: Path = Path("runs/paper_batch")
    max_days: int = 30
    max_reconciliation_failures: int = 0
    summary_csv_path: Path = Path("runs/paper_batch_summary.csv")
    summary_md_path: Path = Path("runs/paper_batch_summary.md")


@dataclass(frozen=True)
class PaperBatchResult:
    metrics: pd.DataFrame
    summary: dict[str, Any]
    final_positions: pd.DataFrame


def load_paper_batch_config(path: str | Path) -> PaperBatchConfig:
    data = load_yaml(path)
    raw = data.get("batch", data)
    return PaperBatchConfig(
        run_root=Path(raw.get("run_root", "runs/paper_batch")),
        max_days=int(raw.get("max_days", 30)),
        max_reconciliation_failures=int(raw.get("max_reconciliation_failures", 0)),
        summary_csv_path=Path(raw.get("summary_csv", "runs/paper_batch_summary.csv")),
        summary_md_path=Path(raw.get("summary_md", "runs/paper_batch_summary.md")),
    )


def run_paper_batch(
    target_paths: list[str | Path],
    initial_positions: pd.DataFrame,
    order_config: OrderConfig,
    fill_config: PaperFillConfig,
    reconcile_config: ReconcileConfig,
    batch_config: PaperBatchConfig = PaperBatchConfig(),
) -> PaperBatchResult:
    paths = sorted(Path(path) for path in target_paths)[: batch_config.max_days]
    current = initial_positions.copy()
    metrics: list[dict[str, Any]] = []
    batch_config.run_root.mkdir(parents=True, exist_ok=True)
    for path in paths:
        target = pd.read_csv(path)
        run_date = _run_date(target, path)
        run_dir = batch_config.run_root / run_date.replace("-", "")
        run_dir.mkdir(parents=True, exist_ok=True)

        orders = build_order_suggestions(target, current, order_config)
        fills = simulate_paper_fills(orders, fill_config)
        expected = apply_fills_to_positions(current, fills)
        actual = expected.copy()
        reconcile_report = reconcile_positions(expected, actual, reconcile_config)

        orders_path = write_orders(orders, run_dir / "orders.csv")
        fills_path = write_fills(fills, run_dir / "fills.csv")
        positions_path = write_positions_state(expected, run_dir / "positions_expected.csv")
        reconciliation_path = write_reconciliation_report(reconcile_report, run_dir / "reconciliation.md")
        manifest_path = _write_manifest(
            run_dir / "manifest.json",
            {
                "run_date": run_date,
                "target_portfolio": str(path),
                "orders": str(orders_path),
                "fills": str(fills_path),
                "positions_expected": str(positions_path),
                "reconciliation": str(reconciliation_path),
            },
        )
        metrics.append(
            {
                "run_date": run_date,
                "order_count": int(len(orders)),
                "fill_count": int((fills["status"].isin(["filled", "partial"])).sum()) if "status" in fills else 0,
                "turnover": _abs_sum(orders, "delta_weight"),
                "filled_turnover": _abs_sum(fills, "fill_delta_weight"),
                "transaction_cost": _sum(fills, "transaction_cost"),
                "position_count": int(len(expected)),
                "gross_exposure": _sum(expected, "current_weight"),
                "target_drift": _target_drift(target, expected),
                "reconciliation_passed": bool(reconcile_report.passed),
                "manifest": str(manifest_path),
            }
        )
        current = expected

    metrics_frame = pd.DataFrame(metrics)
    summary = summarize_paper_batch(metrics_frame, current)
    return PaperBatchResult(metrics=metrics_frame, summary=summary, final_positions=current)


def summarize_paper_batch(metrics: pd.DataFrame, final_positions: pd.DataFrame) -> dict[str, Any]:
    if metrics.empty:
        return {
            "days": 0,
            "total_turnover": 0.0,
            "average_turnover": 0.0,
            "max_target_drift": 0.0,
            "reconciliation_failures": 0,
            "total_transaction_cost": 0.0,
            "final_positions": int(len(final_positions)),
        }
    failures = int((~metrics["reconciliation_passed"].astype(bool)).sum())
    return {
        "days": int(len(metrics)),
        "total_turnover": float(metrics["turnover"].sum()),
        "average_turnover": float(metrics["turnover"].mean()),
        "max_target_drift": float(metrics["target_drift"].max()),
        "reconciliation_failures": failures,
        "total_transaction_cost": float(metrics["transaction_cost"].sum()),
        "final_positions": int(len(final_positions)),
    }


def write_paper_batch_outputs(result: PaperBatchResult, config: PaperBatchConfig) -> tuple[Path, Path]:
    config.summary_csv_path.parent.mkdir(parents=True, exist_ok=True)
    config.summary_md_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([result.summary]).to_csv(config.summary_csv_path, index=False)
    lines = [
        "# Paper Batch Summary",
        "",
        f"- days: {result.summary['days']}",
        f"- average_turnover: {result.summary['average_turnover']:.6g}",
        f"- max_target_drift: {result.summary['max_target_drift']:.6g}",
        f"- reconciliation_failures: {result.summary['reconciliation_failures']}",
        f"- total_transaction_cost: {result.summary['total_transaction_cost']:.6g}",
        f"- final_positions: {result.summary['final_positions']}",
        "",
        "## Daily Metrics",
        "",
        "| run_date | order_count | turnover | target_drift | transaction_cost | reconciliation_passed |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for _, row in result.metrics.iterrows():
        lines.append(
            f"| {row['run_date']} | {int(row['order_count'])} | {float(row['turnover']):.6g} | "
            f"{float(row['target_drift']):.6g} | {float(row['transaction_cost']):.6g} | "
            f"{bool(row['reconciliation_passed'])} |"
        )
    config.summary_md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config.summary_csv_path, config.summary_md_path


def _run_date(target: pd.DataFrame, path: Path) -> str:
    if "date" in target.columns and not target.empty:
        return str(target["date"].max())
    stem_digits = "".join(ch for ch in path.stem if ch.isdigit())
    if len(stem_digits) >= 8:
        value = stem_digits[-8:]
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return path.stem


def _abs_sum(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(frame[column].abs().sum())


def _sum(frame: pd.DataFrame, column: str) -> float:
    if frame.empty or column not in frame.columns:
        return 0.0
    return float(frame[column].sum())


def _target_drift(target: pd.DataFrame, expected: pd.DataFrame) -> float:
    target_weights = _weights(target, "target_weight")
    expected_weights = _weights(expected, "current_weight")
    instruments = set(target_weights) | set(expected_weights)
    return float(sum(abs(target_weights.get(instrument, 0.0) - expected_weights.get(instrument, 0.0)) for instrument in instruments))


def _weights(frame: pd.DataFrame, column: str) -> dict[str, float]:
    if frame.empty or column not in frame.columns:
        return {}
    return {str(row["instrument"]): float(row[column]) for _, row in frame.iterrows()}


def _write_manifest(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
