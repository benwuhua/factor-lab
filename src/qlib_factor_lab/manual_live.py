from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ManualTicketConfig:
    available_cash: float | None = None
    banned_instruments: tuple[str, ...] = ()
    max_order_value: float | None = None
    allow_sells: bool = True


def build_manual_order_ticket(
    orders: pd.DataFrame,
    fills: pd.DataFrame | None = None,
    pretrade_config: ManualTicketConfig | None = None,
) -> pd.DataFrame:
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

    config = pretrade_config or ManualTicketConfig()
    frame["pretrade_reason"] = frame.apply(lambda row: _pretrade_reason(row, config), axis=1)
    frame["pretrade_status"] = frame["pretrade_reason"].map(lambda reason: "review" if reason else "pass")
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
        "pretrade_status",
        "pretrade_reason",
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
        f"- pretrade_review_required: {int((ticket['pretrade_status'] == 'review').sum()) if 'pretrade_status' in ticket else 0}",
        "",
        "| instrument | action | side | order_shares | price | order_value | pretrade_status | pretrade_reason | review_reason |",
        "|---|---|---|---:|---:|---:|---|---|---|",
    ]
    for _, row in ticket.iterrows():
        lines.append(
            f"| {row['instrument']} | {row['action']} | {row['side']} | {row['order_shares']} | "
            f"{row['price']} | {row['order_value']} | {row['pretrade_status']} | "
            f"{row['pretrade_reason']} | {row['review_reason']} |"
        )
    md_output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_output, md_output


def _ticket_action(row: pd.Series) -> str:
    if _clean_reason(row.get("reject_reason", "")):
        return "REVIEW"
    if _clean_reason(row.get("pretrade_reason", "")):
        return "REVIEW"
    side = str(row.get("side", "")).upper()
    return side if side in {"BUY", "SELL"} else "REVIEW"


def _review_reason(row: pd.Series) -> str:
    reason = _clean_reason(row.get("reject_reason", ""))
    if reason:
        return reason
    reason = _clean_reason(row.get("pretrade_reason", ""))
    if reason:
        return reason
    if _ticket_action(row) == "REVIEW":
        return "unknown_action"
    return ""


def _pretrade_reason(row: pd.Series, config: ManualTicketConfig) -> str:
    reasons: list[str] = []
    side = str(row.get("side", "")).upper()
    instrument = str(row.get("instrument", ""))
    order_value = _float_value(row.get("order_value", 0.0))
    if instrument in set(config.banned_instruments):
        reasons.append("banned_instrument")
    if side == "BUY" and config.available_cash is not None and order_value > config.available_cash:
        reasons.append("insufficient_cash")
    if config.max_order_value is not None and order_value > config.max_order_value:
        reasons.append("above_max_order_value")
    if side == "SELL" and not config.allow_sells:
        reasons.append("sell_not_allowed")
    return ";".join(reasons)


def _float_value(value: object) -> float:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return 0.0 if pd.isna(number) else float(number)


def _clean_reason(value: object) -> str:
    if pd.isna(value):
        return ""
    reason = str(value).strip()
    return "" if reason.lower() in {"", "nan", "none"} else reason
