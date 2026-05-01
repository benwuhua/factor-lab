from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def compute_paper_replay_returns(
    target_paths: list[str | Path],
    close: pd.DataFrame,
    *,
    paper_run_root: str | Path | None = None,
    total_equity: float = 1_000_000.0,
) -> pd.DataFrame:
    prices = _normalize_close(close)
    price_dates = sorted(prices["date"].unique().tolist())
    if not price_dates:
        return _empty_daily_frame()
    target_frames = [_read_target(path) for path in sorted(Path(path) for path in target_paths)]
    rows: list[dict[str, Any]] = []
    for target in target_frames:
        if target.empty:
            continue
        run_date = str(target["date"].max())
        next_date = _next_date(price_dates, run_date)
        if next_date is None:
            continue
        current_prices = prices[prices["date"].eq(run_date)].set_index("instrument")["close"]
        next_prices = prices[prices["date"].eq(next_date)].set_index("instrument")["close"]
        target = target.copy()
        target["target_weight"] = pd.to_numeric(target["target_weight"], errors="coerce").fillna(0.0)
        target["close"] = target["instrument"].map(current_prices)
        target["next_close"] = target["instrument"].map(next_prices)
        target["instrument_return"] = target["next_close"] / target["close"] - 1.0
        priced = target.dropna(subset=["instrument_return"])
        gross_return = float((priced["target_weight"] * priced["instrument_return"]).sum()) if not priced.empty else 0.0
        transaction_cost = _transaction_cost(paper_run_root, run_date)
        transaction_cost_return = transaction_cost / total_equity if total_equity > 0 else 0.0
        market_return = _market_equal_weight_return(current_prices, next_prices)
        rows.append(
            {
                "date": run_date,
                "next_date": next_date,
                "gross_return": gross_return,
                "transaction_cost": transaction_cost,
                "transaction_cost_return": transaction_cost_return,
                "net_return": gross_return - transaction_cost_return,
                "market_equal_weight_return": market_return,
                "excess_return": gross_return - transaction_cost_return - market_return,
                "position_count": int(len(target)),
                "priced_position_count": int(len(priced)),
                "missing_price_count": int(len(target) - len(priced)),
                "gross_exposure": float(target["target_weight"].sum()),
                "turnover": _turnover(paper_run_root, run_date),
            }
        )
    if not rows:
        return _empty_daily_frame()
    daily = pd.DataFrame(rows)
    daily["nav"] = (1.0 + daily["net_return"]).cumprod()
    daily["benchmark_nav"] = (1.0 + daily["market_equal_weight_return"]).cumprod()
    daily["excess_nav"] = (1.0 + daily["excess_return"]).cumprod()
    return daily


def summarize_paper_replay_returns(daily: pd.DataFrame) -> dict[str, Any]:
    if daily.empty:
        return {
            "periods": 0,
            "start_date": "",
            "end_date": "",
            "total_return": 0.0,
            "annualized_return": 0.0,
            "annualized_vol": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "benchmark_total_return": 0.0,
            "excess_total_return": 0.0,
            "average_turnover": 0.0,
            "total_transaction_cost": 0.0,
            "average_positions": 0.0,
            "average_gross_exposure": 0.0,
        }
    net = pd.to_numeric(daily["net_return"], errors="coerce").fillna(0.0)
    benchmark = pd.to_numeric(daily["market_equal_weight_return"], errors="coerce").fillna(0.0)
    excess = (
        pd.to_numeric(daily["excess_return"], errors="coerce").fillna(0.0)
        if "excess_return" in daily.columns
        else net - benchmark
    )
    nav = (1.0 + net).cumprod()
    benchmark_nav = (1.0 + benchmark).cumprod()
    excess_nav = (1.0 + excess).cumprod()
    return {
        "periods": int(len(daily)),
        "start_date": str(daily["date"].iloc[0]),
        "end_date": str(daily["next_date"].iloc[-1]),
        "total_return": float(nav.iloc[-1] - 1.0),
        "annualized_return": _annualized_return(nav.iloc[-1], len(daily)),
        "annualized_vol": float(net.std(ddof=0) * (252**0.5)),
        "max_drawdown": _max_drawdown(nav),
        "win_rate": float((net > 0).mean()),
        "benchmark_total_return": float(benchmark_nav.iloc[-1] - 1.0),
        "excess_total_return": float(excess_nav.iloc[-1] - 1.0),
        "average_turnover": _mean(daily, "turnover"),
        "total_transaction_cost": _sum(daily, "transaction_cost"),
        "average_positions": _mean(daily, "position_count"),
        "average_gross_exposure": _mean(daily, "gross_exposure"),
    }


def summarize_paper_replay_monthly_returns(daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return pd.DataFrame(
            columns=[
                "month",
                "net_return",
                "gross_return",
                "market_equal_weight_return",
                "excess_return",
                "transaction_cost_return",
                "average_turnover",
            ]
        )
    frame = daily.copy()
    if "excess_return" not in frame.columns:
        frame["excess_return"] = (
            pd.to_numeric(frame["net_return"], errors="coerce").fillna(0.0)
            - pd.to_numeric(frame["market_equal_weight_return"], errors="coerce").fillna(0.0)
        )
    frame["month"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m")
    rows: list[dict[str, Any]] = []
    for month, group in frame.groupby("month", sort=True):
        rows.append(
            {
                "month": month,
                "net_return": _compound(group["net_return"]),
                "gross_return": _compound(group["gross_return"]),
                "market_equal_weight_return": _compound(group["market_equal_weight_return"]),
                "excess_return": _compound(group["excess_return"]),
                "transaction_cost_return": float(group["transaction_cost_return"].sum()),
                "average_turnover": _mean(group, "turnover"),
            }
        )
    return pd.DataFrame(rows)


def write_paper_replay_report(
    daily: pd.DataFrame,
    summary: dict[str, Any],
    monthly: pd.DataFrame,
    output_path: str | Path,
    *,
    title: str = "Paper Replay Performance",
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    daily_frame = daily.copy()
    if "excess_return" not in daily_frame.columns and {"net_return", "market_equal_weight_return"} <= set(daily_frame.columns):
        daily_frame["excess_return"] = (
            pd.to_numeric(daily_frame["net_return"], errors="coerce").fillna(0.0)
            - pd.to_numeric(daily_frame["market_equal_weight_return"], errors="coerce").fillna(0.0)
        )
    lines = [
        f"# {title}",
        "",
        "## Summary",
        "",
        "| metric | value |",
        "|---|---:|",
    ]
    for key, value in summary.items():
        lines.append(f"| {key} | {_format_value(value)} |")
    lines.extend(["", "## Monthly", "", "| month | net_return | gross_return | market_equal_weight_return | excess_return | cost_return | avg_turnover |", "|---|---:|---:|---:|---:|---:|---:|"])
    for _, row in monthly.iterrows():
        lines.append(
            f"| {row['month']} | {_pct(row['net_return'])} | {_pct(row['gross_return'])} | "
            f"{_pct(row['market_equal_weight_return'])} | {_pct(row['excess_return'])} | "
            f"{_pct(row['transaction_cost_return'])} | {_pct(row['average_turnover'])} |"
        )
    lines.extend(["", "## Daily Sample", "", "| date | next_date | net_return | benchmark | excess | turnover | positions |", "|---|---|---:|---:|---:|---:|---:|"])
    for _, row in daily_frame.tail(20).iterrows():
        lines.append(
            f"| {row['date']} | {row['next_date']} | {_pct(row['net_return'])} | "
            f"{_pct(row['market_equal_weight_return'])} | {_pct(row['excess_return'])} | "
            f"{_pct(row['turnover'])} | {int(row['position_count'])} |"
        )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def _normalize_close(close: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "instrument", "close"}
    missing = required - set(close.columns)
    if missing:
        raise ValueError(f"close frame is missing columns: {sorted(missing)}")
    frame = close.loc[:, ["date", "instrument", "close"]].copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["instrument"] = frame["instrument"].astype(str)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame.dropna(subset=["close"])


def _read_target(path: Path) -> pd.DataFrame:
    target = pd.read_csv(path)
    required = {"date", "instrument", "target_weight"}
    missing = required - set(target.columns)
    if missing:
        raise ValueError(f"{path} is missing columns: {sorted(missing)}")
    output = target.loc[:, ["date", "instrument", "target_weight"]].copy()
    output["date"] = pd.to_datetime(output["date"]).dt.strftime("%Y-%m-%d")
    output["instrument"] = output["instrument"].astype(str)
    return output


def _next_date(price_dates: list[str], run_date: str) -> str | None:
    for value in price_dates:
        if value > run_date:
            return value
    return None


def _transaction_cost(paper_run_root: str | Path | None, run_date: str) -> float:
    fills = _fills(paper_run_root, run_date)
    if fills.empty or "transaction_cost" not in fills.columns:
        return 0.0
    return float(pd.to_numeric(fills["transaction_cost"], errors="coerce").fillna(0.0).sum())


def _turnover(paper_run_root: str | Path | None, run_date: str) -> float:
    fills = _fills(paper_run_root, run_date)
    if fills.empty or "fill_delta_weight" not in fills.columns:
        return 0.0
    return float(pd.to_numeric(fills["fill_delta_weight"], errors="coerce").fillna(0.0).abs().sum())


def _fills(paper_run_root: str | Path | None, run_date: str) -> pd.DataFrame:
    if paper_run_root is None:
        return pd.DataFrame()
    path = Path(paper_run_root) / run_date.replace("-", "") / "fills.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _market_equal_weight_return(current_prices: pd.Series, next_prices: pd.Series) -> float:
    instruments = current_prices.index.intersection(next_prices.index)
    if len(instruments) == 0:
        return 0.0
    returns = next_prices.loc[instruments] / current_prices.loc[instruments] - 1.0
    return float(returns.dropna().mean()) if not returns.dropna().empty else 0.0


def _annualized_return(final_nav: float, periods: int) -> float:
    if periods <= 0 or final_nav <= 0:
        return 0.0
    return float(final_nav ** (252.0 / periods) - 1.0)


def _max_drawdown(nav: pd.Series) -> float:
    if nav.empty:
        return 0.0
    drawdown = nav / nav.cummax() - 1.0
    return float(drawdown.min())


def _compound(values: pd.Series) -> float:
    series = pd.to_numeric(values, errors="coerce").fillna(0.0)
    return float((1.0 + series).prod() - 1.0)


def _mean(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns or frame.empty:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").fillna(0.0).mean())


def _sum(frame: pd.DataFrame, column: str) -> float:
    if column not in frame.columns or frame.empty:
        return 0.0
    return float(pd.to_numeric(frame[column], errors="coerce").fillna(0.0).sum())


def _format_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _pct(value: Any) -> str:
    try:
        return f"{float(value):.2%}"
    except (TypeError, ValueError):
        return str(value)


def _empty_daily_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "next_date",
            "gross_return",
            "transaction_cost",
            "transaction_cost_return",
            "net_return",
            "market_equal_weight_return",
            "excess_return",
            "position_count",
            "priced_position_count",
            "missing_price_count",
            "gross_exposure",
            "turnover",
            "nav",
            "benchmark_nav",
            "excess_nav",
        ]
    )
