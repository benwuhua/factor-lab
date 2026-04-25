from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import re

import pandas as pd
import yaml

from .exposure_attribution import build_exposure_attribution, load_factor_family_map
from .risk import RiskConfig, check_portfolio_risk


AUTORESEARCH_LEDGER = Path("reports/autoresearch/expression_results.tsv")
APPROVED_FACTORS = Path("reports/approved_factors.yaml")
RISK_CONFIG = Path("configs/risk.yaml")
AUTORESEARCH_REVIEW_ANALYSIS = Path("reports/autoresearch")


@dataclass(frozen=True)
class PortfolioGateExplanation:
    decision: str
    checks: pd.DataFrame
    industry: pd.DataFrame
    family: pd.DataFrame
    style: pd.DataFrame


@dataclass(frozen=True)
class WorkbenchSnapshot:
    approved_factor_count: int
    latest_target_portfolio: Path | None
    latest_run_dir: Path | None
    autoresearch_status_counts: dict[str, int]
    freshness: list[dict[str, Any]]


def load_autoresearch_queue(root: str | Path = ".", ledger_path: str | Path = AUTORESEARCH_LEDGER) -> pd.DataFrame:
    path = _resolve(Path(root), ledger_path)
    columns = [
        "timestamp",
        "candidate_name",
        "status",
        "primary_metric",
        "neutral_rank_ic_mean_h20",
        "complexity_score",
        "decision_reason",
        "artifact_dir",
    ]
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(path, sep="\t")
    if "candidate_name" not in frame.columns and "candidate" in frame.columns:
        frame = frame.rename(columns={"candidate": "candidate_name"})
    for column in columns:
        if column not in frame.columns:
            frame[column] = "" if column not in {"primary_metric", "neutral_rank_ic_mean_h20", "complexity_score"} else float("nan")
    for column in ["primary_metric", "neutral_rank_ic_mean_h20", "complexity_score"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["_timestamp_sort"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame = frame.sort_values("_timestamp_sort", ascending=False, na_position="last")
    return frame.drop(columns=["_timestamp_sort"]).reset_index(drop=True)


def summarize_autoresearch_queue(queue: pd.DataFrame) -> dict[str, int]:
    counts = {key: 0 for key in ["review", "discard_candidate", "crash"]}
    if queue.empty or "status" not in queue.columns:
        return counts
    raw = queue["status"].fillna("").astype(str).value_counts().to_dict()
    for key in counts:
        counts[key] = int(raw.get(key, 0))
    return counts


def build_autoresearch_progress(
    *,
    queue: pd.DataFrame | None = None,
    task_runs: list[dict[str, Any]] | None = None,
    recent_limit: int = 5,
) -> dict[str, Any]:
    queue_frame = queue.copy() if queue is not None else pd.DataFrame()
    summary = summarize_autoresearch_queue(queue_frame)
    loop_tasks = [
        row
        for row in (task_runs or [])
        if str(row.get("task_id", "")) in {"autoresearch-codex-loop", "autoresearch-review"}
    ]
    latest_task = loop_tasks[0] if loop_tasks else {}
    recent_candidates = _recent_autoresearch_candidates(queue_frame, recent_limit)
    latest_candidate = recent_candidates[0] if recent_candidates else {}
    loop_status = str(latest_task.get("status", "idle") or "idle")
    return {
        "loop_status": loop_status,
        "loop_task_id": latest_task.get("task_id", "n/a"),
        "loop_run_dir": latest_task.get("run_dir", ""),
        "loop_created_at": latest_task.get("created_at", ""),
        "is_active": loop_status in {"queued", "running"},
        "candidate_count": int(len(queue_frame)),
        "review_count": summary.get("review", 0),
        "discard_count": summary.get("discard_candidate", 0),
        "crash_count": summary.get("crash", 0),
        "latest_candidate": latest_candidate.get("candidate_name", "n/a"),
        "latest_candidate_status": latest_candidate.get("status", "n/a"),
        "latest_primary_metric": latest_candidate.get("primary_metric", float("nan")),
        "recent_candidates": recent_candidates,
    }


def _recent_autoresearch_candidates(queue: pd.DataFrame, limit: int) -> list[dict[str, Any]]:
    if queue.empty:
        return []
    frame = queue.copy()
    if "timestamp" in frame.columns:
        frame["_timestamp_sort"] = pd.to_datetime(frame["timestamp"], errors="coerce")
        frame = frame.sort_values("_timestamp_sort", ascending=False, na_position="last")
    columns = [
        column
        for column in [
            "timestamp",
            "candidate_name",
            "status",
            "primary_metric",
            "neutral_rank_ic_mean_h20",
            "complexity_score",
            "artifact_dir",
        ]
        if column in frame.columns
    ]
    return frame.loc[:, columns].head(limit).to_dict(orient="records")


def load_workbench_snapshot(root: str | Path = ".") -> WorkbenchSnapshot:
    root_path = Path(root)
    queue = load_autoresearch_queue(root_path)
    return WorkbenchSnapshot(
        approved_factor_count=count_approved_factors(root_path),
        latest_target_portfolio=find_latest_target_portfolio(root_path),
        latest_run_dir=find_latest_run_dir(root_path),
        autoresearch_status_counts=summarize_autoresearch_queue(queue),
        freshness=build_workbench_freshness(root_path),
    )


def count_approved_factors(root: str | Path = ".", approved_path: str | Path = APPROVED_FACTORS) -> int:
    path = _resolve(Path(root), approved_path)
    if not path.exists():
        return 0
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return len(data.get("approved_factors") or [])


def find_latest_target_portfolio(root: str | Path = ".") -> Path | None:
    root_path = Path(root)
    candidates = list((root_path / "reports").glob("target_portfolio_*.csv"))
    candidates.extend((root_path / "runs").glob("*/target_portfolio.csv"))
    return _latest_path(candidates)


def find_latest_run_dir(root: str | Path = ".") -> Path | None:
    root_path = Path(root)
    candidates = [
        path
        for path in (root_path / "runs").glob("*")
        if path.is_dir() and path.name != "workbench_tasks"
    ]
    return _latest_path(candidates)


def load_factor_family_map_safe(root: str | Path = ".", approved_path: str | Path = APPROVED_FACTORS) -> dict[str, str]:
    path = _resolve(Path(root), approved_path)
    if not path.exists():
        return {}
    return load_factor_family_map(path)


def build_portfolio_gate_explanation(
    portfolio: pd.DataFrame,
    *,
    risk_config: dict[str, Any] | RiskConfig | None = None,
    factor_family_map: dict[str, str] | None = None,
    signal: pd.DataFrame | None = None,
) -> PortfolioGateExplanation:
    config = _risk_config(risk_config or {})
    signal_frame = signal if signal is not None else _signal_for_portfolio(portfolio)
    risk_report = check_portfolio_risk(
        portfolio,
        signal_frame,
        config,
        factor_family_map=factor_family_map or {},
    )
    attribution = build_exposure_attribution(portfolio, family_map=factor_family_map or {})
    checks = risk_report.to_frame()
    decision = classify_gate_decision(checks)
    return PortfolioGateExplanation(
        decision=decision,
        checks=checks,
        industry=attribution.industry,
        family=attribution.family,
        style=attribution.style,
    )


def load_portfolio_gate_explanation(root: str | Path = ".") -> PortfolioGateExplanation:
    root_path = Path(root)
    latest = find_latest_target_portfolio(root_path)
    portfolio = pd.read_csv(latest) if latest is not None and latest.exists() else pd.DataFrame()
    risk_config = load_risk_config_dict(root_path)
    return build_portfolio_gate_explanation(
        portfolio,
        risk_config=risk_config,
        factor_family_map=load_factor_family_map_safe(root_path),
    )


def load_risk_config_dict(root: str | Path = ".", path: str | Path = RISK_CONFIG) -> dict[str, Any]:
    resolved = _resolve(Path(root), path)
    if not resolved.exists():
        return {}
    data = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    return dict(data.get("risk", data))


def classify_gate_decision(checks: pd.DataFrame) -> str:
    if checks.empty or "status" not in checks.columns:
        return "pass"
    failed = set(checks.loc[checks["status"] == "fail", "check"].astype(str))
    if not failed:
        return "pass"
    caution_checks = {
        "max_industry_weight",
        "min_factor_family_count",
        "max_factor_family_concentration",
    }
    if failed and failed <= caution_checks:
        return "caution"
    return "reject"


def build_gate_review_items(checks: pd.DataFrame) -> pd.DataFrame:
    columns = ["check", "decision_level", "value", "limit", "review_focus"]
    if checks.empty or "status" not in checks.columns:
        return pd.DataFrame(columns=columns)
    failed = checks.loc[checks["status"] == "fail"].copy()
    if failed.empty:
        return pd.DataFrame(columns=columns)
    failed["decision_level"] = failed.apply(lambda row: classify_gate_decision(pd.DataFrame([row])), axis=1)
    failed["review_focus"] = failed["check"].astype(str).map(_review_focus_for_check).fillna("人工复核该约束是否合理。")
    keep = [column for column in columns if column in failed.columns]
    return failed.loc[:, keep].reset_index(drop=True)


def build_workbench_freshness(root: str | Path = ".", *, now: pd.Timestamp | None = None) -> list[dict[str, Any]]:
    root_path = Path(root)
    current = pd.Timestamp.now() if now is None else pd.Timestamp(now)
    artifacts = [
        ("autoresearch_ledger", root_path / AUTORESEARCH_LEDGER, 36.0, "Nightly 研究队列"),
        ("approved_factors", root_path / APPROVED_FACTORS, 24 * 14.0, "可用因子清单"),
        ("target_portfolio", find_latest_target_portfolio(root_path), 36.0, "目标组合"),
        ("latest_run", find_latest_run_dir(root_path), 36.0, "最近纸面执行包"),
    ]
    return [
        _freshness_row(name, path, max_age_hours=max_age_hours, label=label, now=current)
        for name, path, max_age_hours, label in artifacts
    ]


def get_candidate_artifacts(root: str | Path, artifact_dir: str | Path | None) -> dict[str, Any]:
    root_path = Path(root)
    artifact_path = _resolve(root_path, artifact_dir or "")
    summary_path = artifact_path / "summary.txt"
    candidate_path = artifact_path / "candidate.yaml"
    return {
        "artifact_dir": artifact_path,
        "summary": summary_path.read_text(encoding="utf-8") if summary_path.exists() else "",
        "candidate": candidate_path.read_text(encoding="utf-8") if candidate_path.exists() else "",
    }


def build_pretrade_review(portfolio: pd.DataFrame, *, min_amount_20d: float = 100_000_000) -> pd.DataFrame:
    rows = [
        _pretrade_row(
            "liquidity_floor",
            "caution",
            _instruments_where(portfolio, "amount_20d", lambda value: _number(value) < min_amount_20d),
            f"20日成交额低于 {min_amount_20d:.0f}",
        ),
        _pretrade_row(
            "limit_or_suspended",
            "reject",
            _any_flagged_instruments(portfolio, ["limit_up", "limit_down", "suspended", "buy_blocked"]),
            "涨跌停、停牌或买入阻断",
        ),
        _pretrade_row(
            "event_blocked",
            "reject",
            _any_flagged_instruments(portfolio, ["event_blocked"]),
            "公告/监管事件自动阻断",
        ),
        _pretrade_row(
            "announcement_watch",
            "caution",
            _announcement_watch_instruments(portfolio),
            "公告、异动、问询、财报窗口人工复核",
        ),
        _pretrade_row(
            "hard_risk_flags",
            "reject",
            _risk_flag_instruments(portfolio, {"not_tradable", "limit_locked", "st_or_delisting"}),
            "硬性风险标记",
        ),
    ]
    return pd.DataFrame(rows, columns=["check", "status", "count", "detail", "review_focus"])


def build_research_evidence_summary(portfolio: pd.DataFrame) -> dict[str, Any]:
    frame = portfolio.copy() if portfolio is not None else pd.DataFrame()
    detail_columns = [
        "instrument",
        "event_count",
        "event_blocked",
        "max_event_severity",
        "active_event_types",
        "event_risk_summary",
        "event_source_urls",
        "announcement_flag",
        "security_master_missing",
        "risk_flags",
    ]
    for column in detail_columns:
        if column not in frame.columns:
            frame[column] = pd.NA

    event_watch_mask = frame.apply(
        lambda row: _number(row.get("event_count")) > 0
        or not _blank(row.get("event_risk_summary"))
        or not _blank(row.get("active_event_types")),
        axis=1,
    )
    event_block_mask = frame["event_blocked"].map(_truthy)
    master_missing_mask = frame["security_master_missing"].map(_truthy)
    announcement_mask = frame["announcement_flag"].map(_truthy)
    source_urls = _count_unique_split_values(frame["event_source_urls"])

    detail_mask = event_watch_mask | event_block_mask | master_missing_mask | announcement_mask
    detail = frame.loc[detail_mask, detail_columns].reset_index(drop=True)
    event_types = _event_type_counts(frame["active_event_types"])

    return {
        "cards": {
            "positions": int(len(frame)),
            "event_watch": int(event_watch_mask.sum()),
            "event_block": int(event_block_mask.sum()),
            "announcement_watch": int(announcement_mask.sum()),
            "master_missing": int(master_missing_mask.sum()),
            "source_urls": int(source_urls),
        },
        "event_types": event_types,
        "detail": detail,
    }


def build_execution_gate_card(
    gate_decision: str,
    pretrade_review: pd.DataFrame,
    expert_review: dict[str, Any] | None,
) -> dict[str, Any]:
    reasons = []
    gate_status = _normalized_decision(gate_decision)
    expert_status = _normalized_decision((expert_review or {}).get("decision", "missing"))
    pretrade_statuses = (
        pretrade_review["status"].fillna("").astype(str).map(_normalized_decision).tolist()
        if not pretrade_review.empty and "status" in pretrade_review.columns
        else []
    )

    if gate_status == "reject":
        reasons.append("portfolio_gate: reject")
    if expert_status == "reject":
        reasons.append("expert_review: reject")
    reasons.extend(_pretrade_reasons(pretrade_review, "reject"))
    if reasons:
        return {
            "decision": "reject",
            "action": "block_paper_execution",
            "headline": "阻断纸面执行",
            "reasons": reasons,
        }

    caution_reasons = []
    if gate_status == "caution":
        caution_reasons.append("portfolio_gate: caution")
    if expert_status in {"caution", "missing", "review"}:
        caution_reasons.append(f"expert_review: {expert_status}")
    caution_reasons.extend(_pretrade_reasons(pretrade_review, "caution"))
    if "caution" in pretrade_statuses or caution_reasons:
        return {
            "decision": "caution",
            "action": "require_manual_confirmation",
            "headline": "需要人工确认后进入纸面执行",
            "reasons": caution_reasons,
        }

    return {
        "decision": "pass",
        "action": "allow_paper_execution",
        "headline": "允许进入纸面执行",
        "reasons": [],
    }


def load_execution_gate_card(root: str | Path = ".") -> dict[str, Any]:
    root_path = Path(root)
    latest = find_latest_target_portfolio(root_path)
    portfolio = pd.read_csv(latest) if latest is not None and latest.exists() else pd.DataFrame()
    gate = build_portfolio_gate_explanation(
        portfolio,
        risk_config=load_risk_config_dict(root_path),
        factor_family_map=load_factor_family_map_safe(root_path),
    )
    latest_run = find_latest_run_dir(root_path)
    expert_path = latest_run / "expert_review_result.md" if latest_run is not None else None
    expert = parse_expert_review_result(expert_path) if expert_path is not None else parse_expert_review_result("")
    return build_execution_gate_card(gate.decision, build_pretrade_review(portfolio), expert)


def parse_expert_review_result(source: str | Path | None) -> dict[str, Any]:
    text = _expert_text(source)
    metadata = _expert_metadata(text)
    output = _section_after(text, "## Output")
    return {
        "status": metadata.get("status", "missing" if not text.strip() else ""),
        "decision": _normalized_decision(metadata.get("decision", "")),
        "error": metadata.get("error", ""),
        "summary": _expert_summary(output),
        "watchlist": _expert_watchlist(output),
        "risk_notes": _expert_risk_notes(output),
        "raw_output": output.strip(),
    }


def build_research_pipeline_status(root: str | Path = ".") -> pd.DataFrame:
    root_path = Path(root)
    queue = load_autoresearch_queue(root_path)
    latest_run = find_latest_run_dir(root_path)
    target_path = find_latest_target_portfolio(root_path)
    target = pd.read_csv(target_path) if target_path is not None and target_path.exists() else pd.DataFrame()
    gate = build_portfolio_gate_explanation(
        target,
        risk_config=load_risk_config_dict(root_path),
        factor_family_map=load_factor_family_map_safe(root_path),
    )
    expert_path = latest_run / "expert_review_result.md" if latest_run is not None else None
    paper_path = latest_run / "orders.csv" if latest_run is not None else None
    rows = [
        {
            "stage": "autoresearch",
            "status": _autoresearch_stage_status(queue),
            "detail": _autoresearch_stage_detail(queue),
            "path": str(root_path / AUTORESEARCH_LEDGER),
        },
        {
            "stage": "expert_review",
            "status": parse_expert_review_result(expert_path)["decision"] if expert_path is not None else "missing",
            "detail": "LLM 专家复核结果",
            "path": str(expert_path or ""),
        },
        {
            "stage": "portfolio_gate",
            "status": gate.decision,
            "detail": "组合风险、暴露和事件门禁",
            "path": str(target_path or ""),
        },
        {
            "stage": "paper_bundle",
            "status": "ready" if paper_path is not None and paper_path.exists() else "missing",
            "detail": "纸面订单包",
            "path": str(paper_path or ""),
        },
    ]
    return pd.DataFrame(rows)


def get_candidate_diagnostics(root: str | Path, candidate_name: str, artifact_dir: str | Path | None) -> dict[str, pd.DataFrame]:
    root_path = Path(root)
    artifact_path = _resolve(root_path, artifact_dir or "")
    raw_eval = _read_csv_if_exists(artifact_path / "raw_eval.csv")
    neutral_eval = _read_csv_if_exists(artifact_path / "neutralized_eval.csv")
    return {
        "eval": _candidate_eval_frame(raw_eval, neutral_eval),
        "yearly": _candidate_analysis_frame(root_path, "stability_by_year.tsv", candidate_name),
        "redundancy": _candidate_analysis_frame(root_path, "dedup_clusters.tsv", candidate_name),
    }


def _risk_config(raw: dict[str, Any] | RiskConfig) -> RiskConfig:
    if isinstance(raw, RiskConfig):
        return raw
    return RiskConfig(
        max_single_weight=float(raw.get("max_single_weight", 0.1)),
        min_positions=int(raw.get("min_positions", 10)),
        min_signal_coverage=float(raw.get("min_signal_coverage", 0.2)),
        max_turnover=float(raw["max_turnover"]) if raw.get("max_turnover") is not None else None,
        max_industry_weight=float(raw["max_industry_weight"]) if raw.get("max_industry_weight") is not None else None,
        min_factor_family_count=int(raw["min_factor_family_count"]) if raw.get("min_factor_family_count") is not None else None,
        max_factor_family_concentration=(
            float(raw["max_factor_family_concentration"])
            if raw.get("max_factor_family_concentration") is not None
            else None
        ),
    )


def _signal_for_portfolio(portfolio: pd.DataFrame) -> pd.DataFrame:
    if portfolio.empty:
        return pd.DataFrame(columns=["eligible"])
    signal = portfolio.copy()
    signal["eligible"] = True
    return signal


def _latest_path(paths: list[Path]) -> Path | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda path: (path.stat().st_mtime, str(path)))


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root / candidate


def _freshness_row(
    artifact: str,
    path: Path | None,
    *,
    max_age_hours: float,
    label: str,
    now: pd.Timestamp,
) -> dict[str, Any]:
    if path is None or not path.exists():
        return {
            "artifact": artifact,
            "label": label,
            "status": "missing",
            "age_hours": None,
            "path": "",
        }
    modified = pd.Timestamp(path.stat().st_mtime, unit="s")
    age_hours = round(max((now - modified).total_seconds(), 0) / 3600, 1)
    status = "ready" if age_hours <= max_age_hours else "stale"
    return {
        "artifact": artifact,
        "label": label,
        "status": status,
        "age_hours": age_hours,
        "path": str(path),
    }


def _review_focus_for_check(check: str) -> str:
    focus = {
        "max_industry_weight": "降低行业集中，或要求人工确认该行业主题暴露是有意为之。",
        "min_factor_family_count": "增加不同因子家族的来源，避免组合只押单一量价逻辑。",
        "max_factor_family_concentration": "降低主导因子家族权重，检查组合是否只是单因子变体。",
        "max_single_weight": "降低单票权重，避免个股流动性和事件风险放大。",
        "min_positions": "增加持仓数量，避免样本太窄导致组合不可解释。",
        "min_signal_coverage": "检查信号覆盖率，避免用缺失数据生成组合。",
        "max_turnover": "降低换手或拉长调仓周期，检查交易成本敏感性。",
    }
    return focus.get(check, "人工复核该约束是否合理。")


def _pretrade_row(check: str, fail_status: str, instruments: list[str], focus: str) -> dict[str, Any]:
    return {
        "check": check,
        "status": fail_status if instruments else "pass",
        "count": len(instruments),
        "detail": "; ".join(instruments[:12]),
        "review_focus": focus,
    }


def _instruments_where(portfolio: pd.DataFrame, column: str, predicate) -> list[str]:
    if portfolio.empty or column not in portfolio.columns:
        return []
    output = []
    for _, row in portfolio.iterrows():
        if predicate(row.get(column)):
            output.append(str(row.get("instrument", "")))
    return [item for item in output if item]


def _any_flagged_instruments(portfolio: pd.DataFrame, columns: list[str]) -> list[str]:
    if portfolio.empty:
        return []
    output = []
    for _, row in portfolio.iterrows():
        if any(_truthy(row.get(column)) for column in columns if column in portfolio.columns):
            output.append(str(row.get("instrument", "")))
    return [item for item in output if item]


def _announcement_watch_instruments(portfolio: pd.DataFrame) -> list[str]:
    if portfolio.empty:
        return []
    output = []
    for _, row in portfolio.iterrows():
        has_event_count = _number(row.get("event_count")) > 0
        has_summary = not _blank(row.get("event_risk_summary"))
        has_types = not _blank(row.get("active_event_types"))
        if has_event_count or has_summary or has_types:
            output.append(str(row.get("instrument", "")))
    return [item for item in output if item]


def _risk_flag_instruments(portfolio: pd.DataFrame, hard_flags: set[str]) -> list[str]:
    if portfolio.empty or "risk_flags" not in portfolio.columns:
        return []
    output = []
    for _, row in portfolio.iterrows():
        flags = {item.strip() for item in str(row.get("risk_flags", "")).split(";") if item.strip()}
        flags |= {item.strip() for item in str(row.get("risk_flags", "")).split(",") if item.strip()}
        if flags & hard_flags:
            output.append(str(row.get("instrument", "")))
    return [item for item in output if item]


def _event_type_counts(values: pd.Series) -> pd.DataFrame:
    counts: dict[str, int] = {}
    for value in values:
        for item in _split_semicolon_values(value):
            counts[item] = counts.get(item, 0) + 1
    rows = [{"event_type": key, "count": value} for key, value in sorted(counts.items(), key=lambda item: (-item[1], item[0]))]
    return pd.DataFrame(rows, columns=["event_type", "count"])


def _count_unique_split_values(values: pd.Series) -> int:
    items = set()
    for value in values:
        items.update(_split_semicolon_values(value))
    return len(items)


def _split_semicolon_values(value: Any) -> list[str]:
    if _blank(value):
        return []
    return [item.strip() for item in str(value).replace(",", ";").split(";") if item.strip()]


def _autoresearch_stage_status(queue: pd.DataFrame) -> str:
    summary = summarize_autoresearch_queue(queue)
    if summary["review"] > 0:
        return "review"
    if summary["crash"] > 0:
        return "crash"
    if queue.empty:
        return "missing"
    return "done"


def _autoresearch_stage_detail(queue: pd.DataFrame) -> str:
    summary = summarize_autoresearch_queue(queue)
    return f"review={summary['review']}; discard={summary['discard_candidate']}; crash={summary['crash']}"


def _expert_decision(path: Path | None) -> str:
    if path is None or not path.exists():
        return "missing"
    return parse_expert_review_result(path)["decision"] or "review"


def _candidate_eval_frame(raw_eval: pd.DataFrame, neutral_eval: pd.DataFrame) -> pd.DataFrame:
    raw = _eval_subset(raw_eval, "raw")
    neutral = _eval_subset(neutral_eval, "neutralized")
    if raw.empty:
        return neutral
    if neutral.empty:
        return raw
    return raw.merge(neutral, on=["factor", "horizon"], how="outer")


def _eval_subset(frame: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    keep = ["factor", "horizon", "rank_ic_mean", "ic_mean", "long_short_mean_return", "top_quantile_turnover"]
    output = frame.loc[:, [column for column in keep if column in frame.columns]].copy()
    return output.rename(
        columns={
            "rank_ic_mean": f"{prefix}_rank_ic_mean",
            "ic_mean": f"{prefix}_ic_mean",
            "long_short_mean_return": f"{prefix}_long_short_mean_return",
            "top_quantile_turnover": f"{prefix}_top_quantile_turnover",
        }
    )


def _candidate_analysis_frame(root: Path, filename: str, candidate_name: str) -> pd.DataFrame:
    frames = []
    for path in sorted((root / AUTORESEARCH_REVIEW_ANALYSIS).glob(f"review_analysis_*/{filename}")):
        frame = _read_csv_if_exists(path, sep="\t")
        if frame.empty or "candidate_name" not in frame.columns:
            continue
        frames.append(frame.loc[frame["candidate_name"].astype(str) == str(candidate_name)].copy())
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_csv_if_exists(path: Path, *, sep: str = ",") -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, sep=sep)


def _truthy(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _number(value: Any) -> float:
    try:
        if pd.isna(value):
            return float("nan")
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _blank(value: Any) -> bool:
    return pd.isna(value) or str(value).strip() == ""


def _pretrade_reasons(pretrade_review: pd.DataFrame, status: str) -> list[str]:
    if pretrade_review.empty or "status" not in pretrade_review.columns:
        return []
    output = []
    matched = pretrade_review.loc[pretrade_review["status"].astype(str).map(_normalized_decision) == status]
    for _, row in matched.iterrows():
        detail = str(row.get("detail", "")).strip()
        reason = str(row.get("check", "")).strip()
        output.append(f"{reason}: {detail}" if detail else reason)
    return output


def _normalized_decision(value: Any) -> str:
    text = str(value or "").strip().lower().replace("`", "")
    if text in {"approve", "approved", "ok", "ready"}:
        return "pass"
    if text in {"reject", "caution", "pass", "review", "missing"}:
        return text
    return text or "missing"


def _expert_text(source: str | Path | None) -> str:
    if source is None:
        return ""
    if isinstance(source, Path):
        return source.read_text(encoding="utf-8") if source.exists() else ""
    text = str(source)
    possible_path = Path(text)
    if "\n" not in text and possible_path.exists():
        return possible_path.read_text(encoding="utf-8")
    return text


def _expert_metadata(text: str) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for line in text.splitlines():
        match = re.match(r"^\s*-\s*([A-Za-z_]+)\s*[:：]\s*(.*)\s*$", line)
        if match:
            metadata[match.group(1).strip().lower()] = match.group(2).strip().strip("`")
    return metadata


def _section_after(text: str, marker: str) -> str:
    if marker not in text:
        return text
    return text.split(marker, 1)[1]


def _expert_summary(output: str) -> str:
    for line in output.splitlines():
        cleaned = _clean_markdown(line)
        if cleaned:
            return cleaned
    return ""


def _expert_watchlist(output: str) -> list[str]:
    seen = set()
    instruments = []
    for match in re.finditer(r"\b(?:SH|SZ|BJ)\d{6}\b", output):
        instrument = match.group(0)
        if instrument not in seen:
            instruments.append(instrument)
            seen.add(instrument)
    return instruments


def _expert_risk_notes(output: str) -> str:
    lines = output.splitlines()
    collecting = False
    notes = []
    for line in lines:
        cleaned = _clean_markdown(line)
        if "下单前最值得拦截" in cleaned:
            collecting = True
            continue
        if collecting and (line.strip().startswith("**") or line.strip().startswith("##")):
            break
        if collecting and cleaned:
            notes.append(cleaned)
    return " ".join(notes).strip()


def _clean_markdown(line: str) -> str:
    cleaned = line.strip().replace("`", "")
    cleaned = re.sub(r"^\s*[-*]\s+", "", cleaned)
    cleaned = cleaned.replace("**", "")
    return cleaned.strip()
