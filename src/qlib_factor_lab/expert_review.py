from __future__ import annotations

import pandas as pd


def build_expert_review_packet(
    target_portfolio: pd.DataFrame,
    factor_diagnostics: pd.DataFrame | None = None,
    *,
    run_date: str = "",
    max_positions: int = 20,
) -> str:
    lines = [
        "# Expert Portfolio Review Packet",
        "",
        f"- run_date: {run_date}",
        f"- positions: {len(target_portfolio)}",
        f"- gross_target_weight: {_format_float(target_portfolio.get('target_weight', pd.Series(dtype=float)).sum())}",
        "",
        "## Portfolio Candidates",
        "",
        "| rank | instrument | weight | score | explanation | risk_flags |",
        "|---:|---|---:|---:|---|---|",
    ]
    portfolio = target_portfolio.sort_values("rank") if "rank" in target_portfolio.columns else target_portfolio.copy()
    for _, row in portfolio.head(max_positions).iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("rank", "")),
                    str(row.get("instrument", "")),
                    _format_float(row.get("target_weight")),
                    _format_float(row.get("ensemble_score")),
                    _text(row.get("selection_explanation", "")),
                    _text(row.get("risk_flags", "")),
                ]
            )
            + " |"
        )
    lines.extend(["", "## Factor Diagnostics", ""])
    if factor_diagnostics is None or factor_diagnostics.empty:
        lines.append("- No factor diagnostics were supplied.")
    else:
        lines.extend(
            [
                "| factor | family | role | neutral_rank_ic_h20 | neutral_ls_h20 | concerns |",
                "|---|---|---|---:|---:|---|",
            ]
        )
        for _, row in factor_diagnostics.iterrows():
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("factor", "")),
                        _text(row.get("family", "")),
                        _text(row.get("suggested_role", "")),
                        _format_float(row.get("neutral_rank_ic_h20")),
                        _format_float(row.get("neutral_long_short_h20")),
                        _text(row.get("concerns", "")),
                    ]
                )
                + " |"
            )
    lines.extend(
        [
            "",
            "## Questions For Expert LLM",
            "",
            "请站在 A 股量化投研总监的角度评价这个组合，只做研究复核，不预测收益。",
            "",
            "1. 这个组合是否被单一因子族或单一交易逻辑过度支配？",
            "2. 哪些股票看起来像因子误伤，需要人工看图或基本面复核？",
            "3. 哪些风险最值得在下单前拦截：流动性、涨跌停、行业集中、拥挤交易、市场状态冲突？",
            "4. 给出 `pass` / `caution` / `reject` 的研究复核结论，并列出原因。",
        ]
    )
    return "\n".join(lines) + "\n"


def _format_float(value) -> str:
    try:
        if pd.isna(value):
            return ""
        return f"{float(value):.6g}"
    except (TypeError, ValueError):
        return ""


def _text(value) -> str:
    if pd.isna(value):
        return ""
    return str(value)
