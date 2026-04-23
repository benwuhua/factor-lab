from __future__ import annotations

from pathlib import Path

import pandas as pd


def build_manual_order_ticket(orders: pd.DataFrame, fills: pd.DataFrame | None = None) -> pd.DataFrame:
    frame = orders.copy()
    if fills is not None and not fills.empty:
        fill_cols = ["instrument"]
        for column in ["status", "reject_reason", "execution_price", "fill_shares", "transaction_cost"]:
            if column in fills.columns:
                fill_cols.append(column)
        frame = frame.merge(fills[fill_cols], on="instrument", how="left", suffixes=("", "_fill"))
    if "status_fill" in frame.columns and "status" not in frame.columns:
        frame = frame.rename(columns={"status_fill": "fill_status"})
    elif "status_y" in frame.columns:
        frame = frame.rename(columns={"status_y": "fill_status"})
    elif "status" in frame.columns:
        frame = frame.rename(columns={"status": "order_status"})
    if "status_fill" in frame.columns:
        frame = frame.rename(columns={"status_fill": "fill_status"})
    if "fill_status" not in frame.columns:
        fill_status = frame["status"] if "status" in frame.columns else ""
        frame["fill_status"] = fill_status
    if "reject_reason" not in frame.columns:
        frame["reject_reason"] = ""

    frame["action"] = frame.apply(_ticket_action, axis=1)
    frame["review_reason"] = frame.apply(_review_reason, axis=1)
    output_cols = [
        "date",
        "instrument",
        "action",
        "side",
        "order_shares",
        "price",
        "order_value",
        "fill_status",
        "review_reason",
    ]
    for column in output_cols:
        if column not in frame.columns:
            frame[column] = ""
    return frame.loc[:, output_cols].reset_index(drop=True)


def write_manual_order_ticket(ticket: pd.DataFrame, csv_path: str | Path, markdown_path: str | Path) -> tuple[Path, Path]:
    csv_output = Path(csv_path)
    md_output = Path(markdown_path)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    md_output.parent.mkdir(parents=True, exist_ok=True)
    ticket.to_csv(csv_output, index=False)
    lines = [
        "# Manual Order Ticket",
        "",
        f"- orders: {len(ticket)}",
        f"- review_required: {int((ticket['action'] == 'REVIEW').sum()) if 'action' in ticket else 0}",
        "",
        "| instrument | action | side | order_shares | price | order_value | review_reason |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    for _, row in ticket.iterrows():
        lines.append(
            f"| {row['instrument']} | {row['action']} | {row['side']} | {row['order_shares']} | "
            f"{row['price']} | {row['order_value']} | {row['review_reason']} |"
        )
    md_output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_output, md_output


def _ticket_action(row: pd.Series) -> str:
    if _clean_reason(row.get("reject_reason", "")):
        return "REVIEW"
    side = str(row.get("side", "")).upper()
    return side if side in {"BUY", "SELL"} else "REVIEW"


def _review_reason(row: pd.Series) -> str:
    reason = _clean_reason(row.get("reject_reason", ""))
    if reason:
        return reason
    if _ticket_action(row) == "REVIEW":
        return "unknown_action"
    return ""


def _clean_reason(value: object) -> str:
    if pd.isna(value):
        return ""
    reason = str(value).strip()
    return "" if reason.lower() in {"", "nan", "none"} else reason
