from __future__ import annotations

import subprocess
import re
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


EXPERT_REVIEW_FLAG = "expert_review_caution_scaled"
EXPERT_MANUAL_REVIEW_FLAG = "expert_manual_review_required"


@dataclass(frozen=True)
class ExpertReviewRunConfig:
    enabled: bool = False
    command: list[str] | None = None
    timeout_sec: int = 300
    required: bool = False
    caution_action: str = "scale"
    caution_weight_multiplier: float = 0.5


@dataclass(frozen=True)
class ExpertReviewResult:
    status: str
    decision: str
    output: str
    error: str = ""

    def to_manifest(self) -> dict[str, str]:
        return {
            "status": self.status,
            "decision": self.decision,
            "error": self.error,
        }


def load_expert_review_run_config(data: dict[str, Any]) -> ExpertReviewRunConfig:
    raw = data.get("expert_review", {}) if isinstance(data, dict) else {}
    command = raw.get("command")
    if isinstance(command, str):
        command = command.split()
    return ExpertReviewRunConfig(
        enabled=bool(raw.get("enabled", False)),
        command=[str(part) for part in command] if command else None,
        timeout_sec=int(raw.get("timeout_sec", 300)),
        required=bool(raw.get("required", False)),
        caution_action=str(raw.get("caution_action", "scale")),
        caution_weight_multiplier=float(raw.get("caution_weight_multiplier", 0.5)),
    )


def run_expert_review_command(
    packet: str,
    config: ExpertReviewRunConfig = ExpertReviewRunConfig(),
    *,
    cwd: str | Path | None = None,
) -> ExpertReviewResult:
    if not config.enabled:
        return ExpertReviewResult(status="not_run", decision="not_run", output="Expert review command is disabled.")
    if not config.command:
        return ExpertReviewResult(status="not_run", decision="not_run", output="Expert review command is not configured.")
    try:
        completed = subprocess.run(
            config.command,
            input=packet,
            capture_output=True,
            text=True,
            cwd=cwd,
            timeout=config.timeout_sec,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return ExpertReviewResult(
            status="timeout",
            decision="unknown",
            output=exc.stdout or "",
            error=f"expert review timed out after {config.timeout_sec}s",
        )
    output = completed.stdout.strip()
    error = completed.stderr.strip()
    if completed.returncode != 0:
        return ExpertReviewResult(status="failed", decision="unknown", output=output, error=error or f"exit_code={completed.returncode}")
    return ExpertReviewResult(status="completed", decision=parse_expert_review_decision(output), output=output, error="")


def apply_expert_review_portfolio_gate(
    portfolio: pd.DataFrame,
    *,
    decision: str,
    review_status: str = "completed",
    review_required: bool = False,
    caution_action: str = "scale",
    caution_weight_multiplier: float = 0.5,
    review_output: str = "",
) -> tuple[pd.DataFrame, dict[str, str]]:
    normalized_decision = str(decision or "unknown").lower()
    normalized_status = str(review_status or "unknown").lower()
    normalized_action = str(caution_action or "scale").lower()
    manual_items = parse_expert_review_manual_items(review_output)
    hard_manual = manual_items.get("hard_manual_review", [])
    if review_required and (normalized_status != "completed" or normalized_decision == "unknown"):
        return portfolio.iloc[0:0].copy(), {
            "status": "blocked",
            "action": "block",
            "decision": normalized_decision,
            "detail": f"required expert review did not complete with an actionable decision: {normalized_status}",
        }
    if normalized_decision == "reject":
        return portfolio.iloc[0:0].copy(), {
            "status": "blocked",
            "action": "block",
            "decision": normalized_decision,
            "detail": "expert review rejected the portfolio",
        }
    if hard_manual and normalized_decision in {"pass", "caution", "unknown"}:
        gated = portfolio.copy()
        if normalized_decision == "caution" and "target_weight" in gated.columns:
            gated["target_weight"] = gated["target_weight"].astype(float) * float(caution_weight_multiplier)
        gated = _append_risk_flag_for_instruments(gated, EXPERT_MANUAL_REVIEW_FLAG, set(hard_manual))
        return gated, {
            "status": "manual_confirmation_required",
            "action": "require_manual_confirmation",
            "decision": normalized_decision,
            "detail": f"expert hard manual review required: {', '.join(hard_manual)}",
        }
    if normalized_decision == "caution" and normalized_action in {"manual_confirmation", "require_confirmation"}:
        return portfolio.copy(), {
            "status": "manual_confirmation_required",
            "action": "require_manual_confirmation",
            "decision": normalized_decision,
            "detail": "expert review requested manual confirmation before execution",
        }
    if normalized_decision == "caution":
        gated = portfolio.copy()
        if "target_weight" in gated.columns:
            gated["target_weight"] = gated["target_weight"].astype(float) * float(caution_weight_multiplier)
        gated = _append_risk_flag(gated, EXPERT_REVIEW_FLAG)
        return gated, {
            "status": "scaled",
            "action": "scale",
            "decision": normalized_decision,
            "detail": f"target weights scaled by {float(caution_weight_multiplier):.6g}",
        }
    return portfolio.copy(), {
        "status": "pass",
        "action": "none",
        "decision": normalized_decision,
        "detail": "",
    }


def parse_expert_review_decision(text: str) -> str:
    lower = text.lower()
    explicit = re.search(
        r"(?:research_review_status|review_status|decision|研究复核结论|复核结论|结论)\s*[：:]\s*`?(pass|caution|reject)`?",
        lower,
    )
    if explicit:
        return explicit.group(1)
    for value in ["reject", "caution", "pass"]:
        if f"`{value}`" in lower:
            return value
    return "unknown"


def parse_expert_review_manual_items(text: str) -> dict[str, list[str]]:
    return {
        "hard_manual_review": _section_instruments(
            text,
            [
                "硬人工复核",
                "硬性人工复核",
                "阻断或人工复核",
                "人工复核后再决定",
                "硬性复核",
            ],
            stop_markers=["流动性复核", "普通下单前检查", "其余标的", "参考公开来源"],
        ),
        "liquidity_review": _section_instruments(
            text,
            ["流动性复核", "流动性"],
            stop_markers=["下单前", "参考公开来源", "硬人工复核", "硬性人工复核"],
        ),
    }


def write_expert_review_result(result: ExpertReviewResult, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Expert Review Result",
        "",
        f"- status: {result.status}",
        f"- decision: {result.decision}",
        f"- error: {result.error}",
        "",
        "## Output",
        "",
        result.output.strip() if result.output else "",
    ]
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output


def build_expert_review_packet(
    target_portfolio: pd.DataFrame,
    factor_diagnostics: pd.DataFrame | None = None,
    *,
    run_date: str = "",
    max_positions: int = 20,
    stock_cards: list[dict[str, Any]] | None = None,
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
    lines.extend(["", "## Pre-Trade Review Context", ""])
    if target_portfolio.empty:
        lines.append("- No portfolio candidates were supplied.")
    else:
        lines.extend(
            [
                "| rank | instrument | industry | amount_20d | turnover_20d | tradable | suspended | limit_up | limit_down | buy_blocked | sell_blocked | abnormal_event | announcement_flag |",
                "|---:|---|---|---:|---:|---|---|---|---|---|---|---|---|",
            ]
        )
        for _, row in portfolio.head(max_positions).iterrows():
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("rank", "")),
                        str(row.get("instrument", "")),
                        _text(row.get("industry", "")),
                        _format_float(row.get("amount_20d")),
                        _format_float(row.get("turnover_20d")),
                        _text(row.get("tradable", "")),
                        _text(row.get("suspended", "")),
                        _text(row.get("limit_up", "")),
                        _text(row.get("limit_down", "")),
                        _text(row.get("buy_blocked", "")),
                        _text(row.get("sell_blocked", "")),
                        _text(row.get("abnormal_event", "")),
                        _text(row.get("announcement_flag", "")),
                    ]
                )
                + " |"
            )
        lines.extend(["", "### Pre-Trade Checks To Consider", ""])
        lines.extend(
            [
                "- industry: review whether weights are concentrated in one industry or theme.",
                "- liquidity: review amount_20d, turnover_20d, and whether order size is realistic.",
                "- price limits: review limit_up, limit_down, suspended, buy/sell blocked flags before execution.",
                "- announcements/events: review abnormal_event and announcement_flag before orders.",
            ]
        )
    lines.extend(["", "## Event Risk Context", ""])
    if target_portfolio.empty:
        lines.append("- No portfolio candidates were supplied.")
    else:
        lines.extend(
            [
                "| rank | instrument | industry_sw | event_count | event_blocked | active_event_types | event_risk_summary | event_source_urls |",
                "|---:|---|---|---:|---|---|---|---|",
            ]
        )
        for _, row in portfolio.head(max_positions).iterrows():
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("rank", "")),
                        str(row.get("instrument", "")),
                        _text(row.get("industry_sw", "")),
                        _format_float(row.get("event_count")),
                        _text(row.get("event_blocked", "")),
                        _text(row.get("active_event_types", "")),
                        _text(row.get("event_risk_summary", "")),
                        _text(row.get("event_source_urls", "")),
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
    lines.extend(["", "## Stock Research Cards", ""])
    if not stock_cards:
        lines.append("- No stock cards were supplied.")
    else:
        lines.extend(
            [
                "| instrument | decision | score | top_factor | event_count | severity | gate_reason | evidence |",
                "|---|---|---:|---|---:|---|---|---|",
            ]
        )
        for card in stock_cards[:max_positions]:
            signal = card.get("current_signal", {}) if isinstance(card, dict) else {}
            evidence = card.get("evidence", {}) if isinstance(card, dict) else {}
            audit = card.get("audit", {}) if isinstance(card, dict) else {}
            review = card.get("review_questions", {}) if isinstance(card, dict) else {}
            lines.append(
                "| "
                + " | ".join(
                    [
                        _text(card.get("instrument", "")),
                        _text(audit.get("review_decision", "")),
                        _format_float(signal.get("ensemble_score")),
                        _text(signal.get("top_factor_1", "")),
                        _format_float(evidence.get("event_count")),
                        _text(evidence.get("max_event_severity", "")),
                        _text(review.get("gate_reason", "")),
                        _text(evidence.get("event_risk_summary", "")),
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
            "5. 哪些候选需要因为公告、监管、减持、解禁、ST/退市、诉讼或异常波动被阻断或人工复核？",
        ]
    )
    return "\n".join(lines) + "\n"


def load_stock_cards_jsonl(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    cards_path = Path(path)
    if not cards_path.exists():
        return []
    cards = []
    for line in cards_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        cards.append(json.loads(line))
    return cards


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


def _append_risk_flag(frame: pd.DataFrame, flag: str) -> pd.DataFrame:
    output = frame.copy()
    if "risk_flags" not in output.columns:
        output["risk_flags"] = ""
    output["risk_flags"] = output["risk_flags"].apply(lambda value: _join_flag(value, flag))
    return output


def _append_risk_flag_for_instruments(frame: pd.DataFrame, flag: str, instruments: set[str]) -> pd.DataFrame:
    output = frame.copy()
    if "risk_flags" not in output.columns:
        output["risk_flags"] = ""
    if "instrument" not in output.columns:
        return output
    mask = output["instrument"].astype(str).str.upper().isin({item.upper() for item in instruments})
    output.loc[mask, "risk_flags"] = output.loc[mask, "risk_flags"].apply(lambda value: _join_flag(value, flag))
    return output


def _join_flag(value, flag: str) -> str:
    existing = "" if pd.isna(value) else str(value).strip()
    if not existing:
        return flag
    flags = [part.strip() for part in existing.split(";") if part.strip()]
    if flag not in flags:
        flags.append(flag)
    return ";".join(flags)


def _section_instruments(text: str, markers: list[str], *, stop_markers: list[str]) -> list[str]:
    lines = str(text or "").splitlines()
    collecting = False
    collected: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not collecting and any(marker in stripped for marker in markers):
            collecting = True
            collected.extend(_instrument_codes(stripped))
            continue
        if not collecting:
            continue
        if not stripped.startswith(("-", "*")) and any(marker in stripped for marker in stop_markers):
            break
        if stripped.startswith("**") and collected:
            break
        collected.extend(_instrument_codes(stripped))
    return _dedupe_preserve_order(collected)


def _instrument_codes(text: str) -> list[str]:
    pattern = re.compile(r"\b(?:SH|SZ)?(?:60|68|69|00|30|301|002|003)\d{4}\b", re.IGNORECASE)
    output = []
    for match in pattern.findall(str(text or "")):
        code = match.upper()
        if code.startswith(("SH", "SZ")):
            output.append(code)
        elif code.startswith(("60", "68", "69")):
            output.append(f"SH{code}")
        else:
            output.append(f"SZ{code}")
    return output


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen = set()
    output = []
    for value in values:
        if value not in seen:
            seen.add(value)
            output.append(value)
    return output
