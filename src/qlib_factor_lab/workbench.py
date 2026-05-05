from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import re
import json

import pandas as pd
import yaml

from .exposure_attribution import build_exposure_attribution, load_factor_family_map, load_factor_logic_map
from .company_events import COMPANY_EVENT_COLUMNS, load_company_events, load_event_risk_config
from .combo_spec import load_combo_spec
from .evidence_library import summarize_announcement_evidence
from .expert_review import parse_expert_review_manual_items
from .risk import RiskConfig, check_portfolio_risk


AUTORESEARCH_LEDGER = Path("reports/autoresearch/expression_results.tsv")
APPROVED_FACTORS = Path("reports/approved_factors.yaml")
RISK_CONFIG = Path("configs/risk.yaml")
EVENT_RISK_CONFIG = Path("configs/event_risk.yaml")
AUTORESEARCH_REVIEW_ANALYSIS = Path("reports/autoresearch")
COMBO_SPEC_DIR = Path("configs/combo_specs")
FUNDAMENTAL_QUALITY = Path("data/fundamental_quality.csv")
DATA_GOVERNANCE_REPORT_GLOB = "data_governance_*.csv"
LIQUIDITY_MICROSTRUCTURE = Path("data/liquidity_microstructure.csv")
EMOTION_ATMOSPHERE = Path("data/emotion_atmosphere.csv")
SECURITY_MASTER_HISTORY = Path("data/security_master_history.csv")
DIVIDENDS = Path("data/cninfo_dividends.csv")
COMPANY_EVENTS = Path("data/company_events.csv")
ANNOUNCEMENT_EVIDENCE = Path("data/announcement_evidence.csv")

OFFENSIVE_FACTOR_FAMILIES = {"momentum", "volume_confirm", "quiet_breakout", "growth_improvement", "event_catalyst", "theme"}
DEFENSIVE_FACTOR_FAMILIES = {"value", "dividend", "gap_risk", "cashflow_quality", "fundamental_quality", "low_vol"}
FACTOR_DATA_FIELDS = [
    {"field": "roe", "lane": "quality", "use_case": "质量防雷", "required_for": "balanced"},
    {"field": "gross_margin", "lane": "quality", "use_case": "质量/盈利能力", "required_for": "balanced"},
    {"field": "debt_ratio", "lane": "quality", "use_case": "低杠杆风险过滤", "required_for": "balanced"},
    {"field": "ep", "lane": "value", "use_case": "价值安全边际", "required_for": "defensive"},
    {"field": "cfp", "lane": "value", "use_case": "现金流价值", "required_for": "defensive"},
    {"field": "dividend_yield", "lane": "dividend", "use_case": "红利防御", "required_for": "defensive"},
    {"field": "revenue_growth_yoy", "lane": "growth", "use_case": "进攻/盈利改善", "required_for": "offensive"},
    {"field": "net_profit_growth_yoy", "lane": "growth", "use_case": "进攻/盈利改善", "required_for": "offensive"},
    {"field": "operating_cashflow_to_net_profit", "lane": "cashflow", "use_case": "质量/现金流确认", "required_for": "balanced"},
    {"field": "financial_disclosure_recency_30d", "lane": "event", "use_case": "财报披露催化", "required_for": "balanced/offensive"},
]


@dataclass(frozen=True)
class PortfolioGateExplanation:
    decision: str
    checks: pd.DataFrame
    industry: pd.DataFrame
    family: pd.DataFrame
    logic: pd.DataFrame
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


def find_latest_research_portfolio(root: str | Path = ".") -> Path | None:
    root_path = Path(root)
    candidates = list((root_path / "reports").glob("research_portfolio_*.csv"))
    candidates.extend((root_path / "runs").glob("*/research_portfolio.csv"))
    return _latest_path(candidates)


def find_latest_execution_portfolio(root: str | Path = ".") -> Path | None:
    root_path = Path(root)
    candidates = list((root_path / "reports").glob("execution_portfolio_*.csv"))
    candidates.extend((root_path / "runs").glob("*/execution_portfolio.csv"))
    return _latest_path(candidates)


def find_latest_stock_cards(root: str | Path = ".") -> Path | None:
    root_path = Path(root)
    candidates = list((root_path / "reports").glob("stock_cards_*.jsonl"))
    candidates.extend((root_path / "runs").glob("*/stock_cards.jsonl"))
    return _latest_path(candidates)


def load_stock_cards(root: str | Path = ".", path: str | Path | None = None) -> list[dict[str, Any]]:
    cards_path = _resolve(Path(root), path) if path is not None else find_latest_stock_cards(root)
    if cards_path is None or not cards_path.exists():
        return []
    cards = []
    for line in cards_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        cards.append(json.loads(line))
    return cards


def find_latest_multilane_report(root: str | Path = ".") -> Path | None:
    root_path = Path(root)
    candidates = list((root_path / "reports/autoresearch").glob("multilane*.md"))
    return _latest_path(candidates)


def load_multilane_report(root: str | Path = ".", path: str | Path | None = None) -> pd.DataFrame:
    report_path = _resolve(Path(root), path) if path is not None else find_latest_multilane_report(root)
    columns = ["lane", "activation_status", "run_status", "candidate", "primary_metric", "artifact_dir", "detail"]
    if report_path is None or not report_path.exists():
        return pd.DataFrame(columns=columns)
    json_path = report_path.with_suffix(".json")
    if json_path.exists():
        frame = pd.read_json(json_path)
    else:
        frame = _parse_multilane_markdown(report_path)
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    frame["primary_metric"] = pd.to_numeric(frame["primary_metric"], errors="coerce")
    frame["source_path"] = str(report_path)
    return frame.loc[:, columns + ["source_path"]].reset_index(drop=True)


def summarize_multilane_report(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "lanes": 0,
            "completed": 0,
            "review": 0,
            "unsupported": 0,
            "crash": 0,
            "best_lane": "n/a",
            "best_primary_metric": float("nan"),
        }
    status = frame["run_status"].fillna("").astype(str)
    detail = frame["detail"].fillna("").astype(str)
    metrics = pd.to_numeric(frame["primary_metric"], errors="coerce")
    candidate = frame["candidate"].fillna("").astype(str).str.strip() if "candidate" in frame.columns else pd.Series("", index=frame.index)
    comparable_metrics = metrics.where(candidate != "")
    best_idx = comparable_metrics.idxmax() if comparable_metrics.notna().any() else None
    return {
        "lanes": int(len(frame)),
        "completed": int((status == "completed").sum()),
        "review": int((detail == "review").sum()),
        "unsupported": int((status == "unsupported").sum()),
        "crash": int((status == "crash").sum()),
        "best_lane": str(frame.loc[best_idx, "lane"]) if best_idx is not None else "n/a",
        "best_primary_metric": float(comparable_metrics.loc[best_idx]) if best_idx is not None else float("nan"),
    }


def build_multilane_queue(frame: pd.DataFrame | None) -> dict[str, Any]:
    source = frame.copy() if frame is not None else pd.DataFrame()
    columns = [
        "lane",
        "lane_state",
        "health",
        "run_status",
        "candidate",
        "primary_metric",
        "detail",
        "next_action",
        "artifact_dir",
        "source_path",
    ]
    if source.empty:
        return {
            "cards": {
                "lanes": 0,
                "active": 0,
                "shadow": 0,
                "unsupported": 0,
                "review": 0,
                "blocked": 0,
            },
            "rows": pd.DataFrame(columns=columns),
        }

    for column in ["lane", "activation_status", "run_status", "candidate", "detail", "artifact_dir", "source_path"]:
        if column not in source.columns:
            source[column] = ""
    if "primary_metric" not in source.columns:
        source["primary_metric"] = float("nan")
    source["primary_metric"] = pd.to_numeric(source["primary_metric"], errors="coerce")
    source["lane_state"] = source.apply(_multilane_lane_state, axis=1)
    source["health"] = source.apply(_multilane_lane_health, axis=1)
    source["next_action"] = source.apply(_multilane_next_action, axis=1)

    health = source["health"].fillna("").astype(str)
    lane_state = source["lane_state"].fillna("").astype(str)
    cards = {
        "lanes": int(len(source)),
        "active": int((lane_state == "active").sum()),
        "shadow": int((lane_state == "shadow").sum()),
        "unsupported": int((health == "unsupported").sum()),
        "review": int((health == "review").sum()),
        "blocked": int(health.isin({"unsupported", "crash", "stalled"}).sum()),
    }
    sort_order = {"review": 0, "unsupported": 1, "crash": 2, "stalled": 3, "explored": 4, "healthy": 5, "shadow": 6}
    source["_health_sort"] = health.map(sort_order).fillna(9)
    source = source.sort_values(["_health_sort", "lane"], na_position="last")
    return {"cards": cards, "rows": source.loc[:, columns].reset_index(drop=True)}


def _parse_multilane_markdown(path: Path) -> pd.DataFrame:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| ") or "---" in line or " lane " in line:
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) < 6:
            continue
        rows.append(
            {
                "lane": parts[0],
                "activation_status": parts[1],
                "run_status": parts[2],
                "candidate": parts[3],
                "primary_metric": parts[4],
                "detail": parts[5],
            }
        )
    return pd.DataFrame(rows)


def _multilane_lane_state(row: pd.Series) -> str:
    activation = str(row.get("activation_status", "") or "").strip().lower()
    run_status = str(row.get("run_status", "") or "").strip().lower()
    if activation == "shadow" or run_status == "shadow_skipped":
        return "shadow"
    if activation == "active":
        return "active"
    return activation or "unknown"


def _multilane_lane_health(row: pd.Series) -> str:
    lane_state = _multilane_lane_state(row)
    run_status = str(row.get("run_status", "") or "").strip().lower()
    detail = str(row.get("detail", "") or "").strip().lower()
    candidate = str(row.get("candidate", "") or "").strip()
    if lane_state == "shadow":
        return "shadow"
    if run_status == "unsupported":
        return "unsupported"
    if run_status == "crash":
        return "crash"
    if run_status in {"queued", "running"}:
        return "stalled"
    if run_status == "completed" and detail == "review" and not candidate:
        return "allocator_review"
    if run_status == "completed" and detail == "review":
        return "review"
    if run_status == "completed" and detail == "discard_candidate":
        return "explored"
    if run_status == "completed":
        return "healthy"
    return run_status or "missing"


def _multilane_next_action(row: pd.Series) -> str:
    health = _multilane_lane_health(row)
    detail = str(row.get("detail", "") or "")
    if health == "review":
        return "打开 artifact，复核候选并决定是否进入 approved 因子筛选。"
    if health == "allocator_review":
        return "查看 regime artifact，确认当前市场状态只作为 family 权重/启停 overlay。"
    if health == "unsupported":
        return "补 runner 或把 lane 调成 shadow，避免把未覆盖方向误读为空进展。"
    if health == "shadow":
        return "启用 lane config 后重跑 multilane，或保持 shadow 作为观察车道。"
    if health == "crash":
        return "查看 artifact/log，修复失败后只重跑该 lane。"
    if health == "stalled":
        return "等待后台任务完成；超时后检查 workbench task manifest。"
    if health == "explored":
        return "记录失败模式，换表达式族或参数窗口继续探索。"
    if health == "healthy":
        return "比较指标与稳定性，决定是否纳入候选池。"
    return detail or "运行 make autoresearch-multilane 刷新多车道状态。"


def summarize_stock_cards(cards: list[dict[str, Any]]) -> dict[str, int]:
    decisions = [
        _normalized_decision(((card.get("audit") or {}).get("review_decision", "")))
        for card in cards
        if isinstance(card, dict)
    ]
    event_watch = 0
    for card in cards:
        evidence = (card.get("evidence") or {}) if isinstance(card, dict) else {}
        if _number(evidence.get("event_count")) > 0 or not _blank(evidence.get("event_risk_summary")):
            event_watch += 1
    return {
        "cards": len(cards),
        "pass": decisions.count("pass"),
        "caution": decisions.count("caution"),
        "reject": decisions.count("reject"),
        "event_watch": event_watch,
    }


def build_stock_card_announcement_evidence_summary(cards: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    card_count = 0
    positive = 0
    risk = 0
    neutral = 0
    chunks = 0
    events = 0
    for card in cards:
        if not isinstance(card, dict):
            continue
        rolling = ((card.get("announcement_evidence") or {}).get("rolling_evidence") or {})
        if not isinstance(rolling, dict) or int(float(rolling.get("chunks") or 0)) <= 0:
            continue
        card_count += 1
        chunks += int(float(rolling.get("chunks") or 0))
        events += int(float(rolling.get("events") or 0))
        polarity = rolling.get("polarity_counts") if isinstance(rolling.get("polarity_counts"), dict) else {}
        positive += int(float(polarity.get("positive") or 0))
        risk += int(float(polarity.get("risk") or 0))
        neutral += int(float(polarity.get("neutral") or 0))
        for item in rolling.get("items") or []:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "instrument": card.get("instrument", ""),
                    "name": card.get("name", ""),
                    "available_at": item.get("available_at", ""),
                    "event_type": item.get("event_type", ""),
                    "severity": item.get("severity", ""),
                    "title": item.get("title", ""),
                    "source_url": item.get("source_url", ""),
                }
            )
    detail = pd.DataFrame(
        rows,
        columns=["instrument", "name", "available_at", "event_type", "severity", "title", "source_url"],
    )
    if not detail.empty:
        detail["_available_at_sort"] = pd.to_datetime(detail["available_at"], errors="coerce")
        detail = (
            detail.sort_values(["_available_at_sort", "severity"], ascending=[False, True])
            .drop(columns=["_available_at_sort"])
            .reset_index(drop=True)
        )
    return {
        "cards": {
            "positions_with_evidence": card_count,
            "chunks": chunks,
            "events": events,
            "positive": positive,
            "risk": risk,
            "neutral": neutral,
        },
        "event_types": _value_counts_frame(detail, "event_type"),
        "severity": _value_counts_frame(detail, "severity"),
        "detail": detail,
    }


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


def load_factor_logic_map_safe(root: str | Path = ".", approved_path: str | Path = APPROVED_FACTORS) -> dict[str, str]:
    path = _resolve(Path(root), approved_path)
    if not path.exists():
        return {}
    return load_factor_logic_map(path)


def build_portfolio_gate_explanation(
    portfolio: pd.DataFrame,
    *,
    risk_config: dict[str, Any] | RiskConfig | None = None,
    factor_family_map: dict[str, str] | None = None,
    factor_logic_map: dict[str, str] | None = None,
    signal: pd.DataFrame | None = None,
    tushare_coverage: dict[str, Any] | None = None,
    min_tushare_domain_instruments: int = 1,
) -> PortfolioGateExplanation:
    config = _risk_config(risk_config or {})
    signal_frame = signal if signal is not None else _signal_for_portfolio(portfolio)
    risk_report = check_portfolio_risk(
        portfolio,
        signal_frame,
        config,
        factor_family_map=factor_family_map or {},
        factor_logic_map=factor_logic_map or {},
    )
    attribution = build_exposure_attribution(
        portfolio,
        family_map=factor_family_map or {},
        logic_map=factor_logic_map or {},
    )
    checks = risk_report.to_frame()
    gate_min_instruments = (
        int(min_tushare_domain_instruments)
        if min_tushare_domain_instruments != 1
        else int(getattr(config, "min_tushare_domain_instruments", 1))
    )
    data_checks = build_tushare_data_gate_checks(tushare_coverage, min_instruments=gate_min_instruments)
    if not data_checks.empty:
        checks = pd.concat([checks, data_checks], ignore_index=True)
    decision = classify_gate_decision(checks)
    return PortfolioGateExplanation(
        decision=decision,
        checks=checks,
        industry=attribution.industry,
        family=attribution.family,
        logic=attribution.logic,
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
        factor_logic_map=load_factor_logic_map_safe(root_path),
        tushare_coverage=build_tushare_data_coverage(root_path),
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
    cautioned = set(checks.loc[checks["status"] == "caution", "check"].astype(str))
    if not failed and not cautioned:
        return "pass"
    caution_checks = {
        "max_industry_weight",
        "min_factor_family_count",
        "max_factor_family_concentration",
        "min_factor_logic_count",
        "max_factor_logic_concentration",
        "tushare_dividend_coverage",
        "tushare_disclosure_evidence_coverage",
    }
    if not failed and cautioned:
        return "caution"
    if failed and failed <= caution_checks:
        return "caution"
    return "reject"


def build_gate_review_items(checks: pd.DataFrame) -> pd.DataFrame:
    columns = ["check", "decision_level", "value", "limit", "review_focus"]
    if checks.empty or "status" not in checks.columns:
        return pd.DataFrame(columns=columns)
    failed = checks.loc[checks["status"].isin(["fail", "caution"])].copy()
    if failed.empty:
        return pd.DataFrame(columns=columns)
    failed["decision_level"] = failed.apply(lambda row: classify_gate_decision(pd.DataFrame([row])), axis=1)
    failed["review_focus"] = failed["check"].astype(str).map(_review_focus_for_check).fillna("人工复核该约束是否合理。")
    keep = [column for column in columns if column in failed.columns]
    return failed.loc[:, keep].reset_index(drop=True)


def build_portfolio_gate_trend(root: str | Path = ".", *, limit: int = 20) -> pd.DataFrame:
    root_path = Path(root)
    rows = []
    for run_dir in sorted((root_path / "runs").glob("*")):
        target = run_dir / "target_portfolio.csv"
        if not run_dir.is_dir() or not target.exists():
            continue
        portfolio = pd.read_csv(target)
        gate = build_portfolio_gate_explanation(
            portfolio,
            risk_config=load_risk_config_dict(root_path),
            factor_family_map=load_factor_family_map_safe(root_path),
            factor_logic_map=load_factor_logic_map_safe(root_path),
        )
        rows.append(
            {
                "run_date": run_dir.name,
                "decision": gate.decision,
                "positions": int(len(portfolio)),
                "industry_coverage": _portfolio_industry_coverage(portfolio),
                "event_coverage": _portfolio_event_coverage(portfolio),
                "factor_family_concentration": _portfolio_family_concentration(gate.family),
                "factor_logic_concentration": _portfolio_family_concentration(
                    gate.logic.rename(columns={"logic_bucket": "family"})
                ),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "run_date",
                "decision",
                "positions",
                "industry_coverage",
                "event_coverage",
                "factor_family_concentration",
                "factor_logic_concentration",
            ]
        )
    return pd.DataFrame(rows).tail(limit).reset_index(drop=True)


def build_portfolio_layer_comparison(root: str | Path = ".") -> dict[str, Any]:
    root_path = Path(root)
    run_dir = find_latest_run_dir(root_path)
    research_path = _run_artifact_or_latest(run_dir, "research_portfolio.csv", find_latest_research_portfolio(root_path))
    execution_path = _run_artifact_or_latest(run_dir, "execution_portfolio.csv", find_latest_execution_portfolio(root_path))
    target_path = _run_artifact_or_latest(run_dir, "target_portfolio.csv", find_latest_target_portfolio(root_path))
    if execution_path is None:
        execution_path = target_path

    research = _read_csv_if_exists(research_path) if research_path is not None else pd.DataFrame()
    execution = _read_csv_if_exists(execution_path) if execution_path is not None else pd.DataFrame()
    detail = _portfolio_layer_detail(research, execution)
    research_weight = _gross_weight(research)
    execution_weight = _gross_weight(execution)
    status = "missing"
    if research_path is not None and execution_path is not None and execution_path != target_path:
        status = "separated"
    elif execution_path is not None:
        status = "legacy_target_only"

    return {
        "status": status,
        "run_dir": str(run_dir) if run_dir is not None else "",
        "paths": {
            "research": str(research_path or ""),
            "execution": str(execution_path or ""),
            "target": str(target_path or ""),
        },
        "cards": {
            "research_positions": int(len(research)),
            "execution_positions": int(len(execution)),
            "research_gross_weight": research_weight,
            "execution_gross_weight": execution_weight,
            "weight_delta": execution_weight - research_weight,
            "removed_positions": int((detail["action"] == "removed").sum()) if not detail.empty else 0,
            "scaled_positions": int((detail["action"] == "scaled").sum()) if not detail.empty else 0,
        },
        "detail": detail,
    }


def build_execution_performance_attribution(
    root: str | Path = ".",
    *,
    intraday_path: str | Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root)
    path = _resolve(root_path, intraday_path) if intraday_path is not None else _latest_intraday_portfolio_path(root_path)
    performance = _read_csv_if_exists(path) if path is not None else pd.DataFrame()
    layers = build_portfolio_layer_comparison(root_path)
    execution_path = Path(layers["paths"]["execution"]) if layers["paths"].get("execution") else None
    execution = _read_csv_if_exists(execution_path) if execution_path is not None else pd.DataFrame()
    if performance.empty:
        return _empty_execution_performance_attribution(path)

    frame = performance.copy()
    if not execution.empty:
        event_columns = [
            column
            for column in [
                "instrument",
                "event_count",
                "event_blocked",
                "active_event_types",
                "event_risk_summary",
                "announcement_flag",
                "risk_flags",
            ]
            if column in execution.columns
        ]
        if "instrument" in event_columns:
            frame = frame.merge(execution.loc[:, event_columns], on="instrument", how="left", suffixes=("", "_execution"))
    frame["target_weight"] = pd.to_numeric(frame.get("target_weight", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    frame["pct_today"] = pd.to_numeric(frame.get("pct_today", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
    frame["weighted_return_pct"] = pd.to_numeric(
        frame.get("weighted_return_pct", frame["target_weight"] * frame["pct_today"]),
        errors="coerce",
    ).fillna(0.0)
    frame["industry"] = _first_nonblank_column(frame, ["industry_sw", "industry", "industry_csrc"], default="unknown")
    frame["factor"] = _first_nonblank_column(frame, ["top_factor_1", "factor", "family"], default="unknown")
    existing_event_bucket = _first_nonblank_column(frame, ["event_bucket"], default="")
    computed_event_bucket = frame.apply(_performance_event_bucket, axis=1)
    frame["event_bucket"] = existing_event_bucket.where(existing_event_bucket.astype(str).str.strip().ne(""), computed_event_bucket)
    contributors = frame.sort_values("weighted_return_pct", ascending=True).reset_index(drop=True)
    keep = [
        column
        for column in [
            "instrument",
            "display_name",
            "industry",
            "factor",
            "event_bucket",
            "target_weight",
            "pct_today",
            "weighted_return_pct",
            "direction",
        ]
        if column in contributors.columns
    ]
    return {
        "path": str(path or ""),
        "summary": {
            "positions": int(len(frame)),
            "weighted_return_pct": float(frame["weighted_return_pct"].sum()),
            "up_count": int((frame.get("direction", pd.Series(dtype=str)).astype(str) == "up").sum())
            if "direction" in frame.columns
            else int((frame["pct_today"] > 0).sum()),
            "down_count": int((frame.get("direction", pd.Series(dtype=str)).astype(str) == "down").sum())
            if "direction" in frame.columns
            else int((frame["pct_today"] < 0).sum()),
            "quote_time": str(frame["quote_time"].dropna().astype(str).max()) if "quote_time" in frame.columns and frame["quote_time"].notna().any() else "",
        },
        "industry": _performance_group(frame, "industry", "industry"),
        "factor": _performance_group(frame, "factor", "factor"),
        "event": _performance_group(frame, "event_bucket", "event_bucket"),
        "contributors": contributors.loc[:, keep] if keep else contributors,
    }


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


def build_event_evidence_library(
    root: str | Path = ".",
    event_risk_config_path: str | Path = EVENT_RISK_CONFIG,
    announcement_evidence_path: str | Path = "data/announcement_evidence.csv",
) -> dict[str, Any]:
    root_path = Path(root)
    config = load_event_risk_config(_resolve(root_path, event_risk_config_path))
    events_path = _resolve_optional(root_path, config.events_path)
    events = load_company_events(events_path)
    evidence_path = _resolve(root_path, announcement_evidence_path)
    detail = _event_evidence_detail(events)
    return {
        "cards": {
            "events": int(len(events)),
            "instruments": int(events["instrument"].dropna().astype(str).nunique()) if "instrument" in events.columns else 0,
            "block_events": int((events.get("severity", pd.Series(dtype=str)).fillna("").astype(str) == "block").sum()),
            "source_urls": _count_unique_split_values(events.get("source_url", pd.Series(dtype=str))),
        },
        "event_types": _value_counts_frame(events, "event_type"),
        "severity": _value_counts_frame(events, "severity"),
        "detail": detail,
        "events_path": str(events_path) if events_path is not None else "",
        "announcement_evidence": summarize_announcement_evidence(evidence_path),
        "announcement_evidence_path": str(evidence_path),
    }


def build_research_context_health(
    root: str | Path = ".",
    security_master_path: str | Path = "data/security_master.csv",
    company_events_path: str | Path = "data/company_events.csv",
) -> dict[str, Any]:
    root_path = Path(root)
    master = _read_csv_or_empty(_resolve(root_path, security_master_path))
    events = _read_csv_or_empty(_resolve(root_path, company_events_path))
    master_instruments = set(_nonblank_strings(master.get("instrument", pd.Series(dtype=str))))
    event_instruments = set(_nonblank_strings(events.get("instrument", pd.Series(dtype=str))))
    source_urls = _nonblank_strings(events.get("source_url", pd.Series(dtype=str)))
    event_dates = pd.to_datetime(events.get("event_date", pd.Series(dtype=str)), errors="coerce")
    universe_labels = _research_universe_labels(master)
    required_universes = {"csi300", "csi500"}
    latest_event_date = ""
    if not event_dates.dropna().empty:
        latest_event_date = event_dates.dropna().max().strftime("%Y-%m-%d")

    return {
        "cards": {
            "master_instruments": int(len(master_instruments)),
            "event_instruments": int(len(event_instruments)),
            "master_universe_coverage_pct": _pct(len(universe_labels & required_universes), len(required_universes)),
            "event_coverage_pct": _pct(len(event_instruments), len(master_instruments)),
            "source_url_coverage_pct": _pct(len(source_urls), len(events)),
            "latest_event_date": latest_event_date,
        },
        "master_path": str(_resolve(root_path, security_master_path)),
        "events_path": str(_resolve(root_path, company_events_path)),
    }


def build_factor_data_gap_summary(
    root: str | Path = ".",
    fundamental_path: str | Path = FUNDAMENTAL_QUALITY,
    *,
    ready_threshold: float = 0.50,
    caution_threshold: float = 0.10,
) -> pd.DataFrame:
    root_path = Path(root)
    path = _resolve(root_path, fundamental_path)
    fundamentals = _read_csv_or_empty(path)
    denominator = max(len(fundamentals), 1)
    rows = []
    for spec in FACTOR_DATA_FIELDS:
        field = str(spec["field"])
        if field not in fundamentals.columns:
            non_null = 0
            unique_instruments = 0
        else:
            valid = pd.to_numeric(fundamentals[field], errors="coerce").notna()
            non_null = int(valid.sum())
            unique_instruments = (
                int(fundamentals.loc[valid, "instrument"].dropna().astype(str).nunique())
                if "instrument" in fundamentals.columns
                else 0
            )
        coverage = non_null / denominator
        rows.append(
            {
                "field": field,
                "lane": spec["lane"],
                "use_case": spec["use_case"],
                "required_for": spec["required_for"],
                "non_null_rows": non_null,
                "coverage_pct": coverage,
                "unique_instruments": unique_instruments,
                "status": _coverage_status(coverage, ready_threshold, caution_threshold),
                "platform_impact": _factor_data_gap_impact(field, coverage),
            }
        )
    return pd.DataFrame(rows)


def build_tushare_data_coverage(
    root: str | Path = ".",
    *,
    security_master_history_path: str | Path = SECURITY_MASTER_HISTORY,
    dividends_path: str | Path = DIVIDENDS,
    company_events_path: str | Path = COMPANY_EVENTS,
    announcement_evidence_path: str | Path = ANNOUNCEMENT_EVIDENCE,
) -> dict[str, Any]:
    root_path = Path(root)
    master_history = _read_csv_or_empty(_resolve(root_path, security_master_history_path))
    dividends = _read_csv_or_empty(_resolve(root_path, dividends_path))
    events = _read_csv_or_empty(_resolve(root_path, company_events_path))
    evidence = _read_csv_or_empty(_resolve(root_path, announcement_evidence_path))

    pit = _source_subset(master_history, "tushare_pit")
    tushare_dividends = _source_subset(dividends, "tushare_dividend")
    disclosure_events = _event_source_subset(events, "financial_report_disclosure", "tushare_disclosure_date")
    disclosure_evidence = _event_source_subset(evidence, "financial_report_disclosure", "tushare_disclosure_date")

    rows = pd.DataFrame(
        [
            _vendor_domain_row("PIT 主数据", "tushare_pit", pit, ["as_of_date", "valid_from", "list_date", "trade_date", "date"]),
            _vendor_domain_row(
                "分红派息",
                "tushare_dividend",
                tushare_dividends,
                ["available_at", "announce_date", "record_date", "ex_date", "ann_date", "end_date"],
            ),
            _vendor_domain_row("财报披露事件", "tushare_disclosure_date", disclosure_events, ["event_date", "publish_time", "ann_date"]),
            _vendor_domain_row("公告证据", "tushare_disclosure_date", disclosure_evidence, ["publish_time", "event_date", "ann_date"]),
        ]
    )
    return {
        "cards": {
            "pit_rows": int(len(pit)),
            "pit_instruments": _instrument_count(pit),
            "tushare_dividend_rows": int(len(tushare_dividends)),
            "tushare_dividend_instruments": _instrument_count(tushare_dividends),
            "financial_disclosure_events": int(len(disclosure_events)),
            "financial_disclosure_instruments": _instrument_count(disclosure_events),
            "financial_disclosure_evidence": int(len(disclosure_evidence)),
            "financial_disclosure_evidence_instruments": _instrument_count(disclosure_evidence),
        },
        "rows": rows,
        "paths": {
            "security_master_history": str(_resolve(root_path, security_master_history_path)),
            "dividends": str(_resolve(root_path, dividends_path)),
            "company_events": str(_resolve(root_path, company_events_path)),
            "announcement_evidence": str(_resolve(root_path, announcement_evidence_path)),
        },
    }


def build_tushare_data_gate_checks(
    coverage: dict[str, Any] | None,
    *,
    min_instruments: int = 1,
) -> pd.DataFrame:
    columns = ["check", "status", "value", "threshold", "detail"]
    if not coverage:
        return pd.DataFrame(columns=columns)
    rows = coverage.get("rows")
    if not isinstance(rows, pd.DataFrame) or rows.empty:
        return pd.DataFrame(columns=columns)
    by_domain = {str(row["domain"]): row for _, row in rows.iterrows() if "domain" in row}
    specs = [
        ("PIT 主数据", "tushare_pit_coverage", "mandatory"),
        ("财报披露事件", "tushare_disclosure_event_coverage", "mandatory"),
        ("分红派息", "tushare_dividend_coverage", "optional"),
        ("公告证据", "tushare_disclosure_evidence_coverage", "optional"),
    ]
    output = []
    for domain, check, level in specs:
        row = by_domain.get(domain)
        instruments_value = _number(row.get("instruments")) if row is not None else 0
        instruments = int(instruments_value) if pd.notna(instruments_value) else 0
        status = str(row.get("status", "")) if row is not None else "missing"
        passed = status == "active" and instruments >= int(min_instruments)
        output_status = "pass" if passed else ("fail" if level == "mandatory" else "caution")
        output.append(
            {
                "check": check,
                "status": output_status,
                "value": instruments,
                "threshold": int(min_instruments),
                "detail": f"{level}; domain={domain}; source={row.get('source', '') if row is not None else ''}; status={status}",
            }
        )
    return pd.DataFrame(output, columns=columns)


def build_data_domain_health(
    root: str | Path = ".",
    *,
    governance_report_path: str | Path | None = None,
    liquidity_path: str | Path = LIQUIDITY_MICROSTRUCTURE,
    emotion_path: str | Path = EMOTION_ATMOSPHERE,
) -> dict[str, Any]:
    root_path = Path(root)
    governance_path = _resolve(root_path, governance_report_path) if governance_report_path is not None else _latest_path(
        list((root_path / "reports").glob(DATA_GOVERNANCE_REPORT_GLOB))
    )
    governance = _read_csv_or_empty(governance_path) if governance_path is not None else pd.DataFrame()
    rows = _data_domain_rows(governance, root_path, liquidity_path, emotion_path)
    liquidity = _read_csv_or_empty(_resolve(root_path, liquidity_path))
    emotion = _read_csv_or_empty(_resolve(root_path, emotion_path))
    activation = rows.get("activation_status", pd.Series(dtype=str)).fillna("").astype(str) if not rows.empty else pd.Series(dtype=str)
    return {
        "cards": {
            "domains": int(len(rows)),
            "active": int((activation == "active").sum()),
            "shadow": int((activation == "shadow").sum()),
            "failed": int((rows.get("status", pd.Series(dtype=str)).fillna("").astype(str) == "fail").sum()) if not rows.empty else 0,
            "liquidity_rows": int(len(liquidity)),
            "emotion_rows": int(len(emotion)),
        },
        "rows": rows,
        "liquidity": _liquidity_domain_metrics(liquidity),
        "emotion": _emotion_domain_metrics(emotion),
        "governance_path": str(governance_path) if governance_path is not None else "",
    }


def _data_domain_rows(
    governance: pd.DataFrame,
    root: Path,
    liquidity_path: str | Path,
    emotion_path: str | Path,
) -> pd.DataFrame:
    columns = [
        "domain",
        "activation_lane",
        "status",
        "activation_status",
        "coverage_pct",
        "pit_pct",
        "freshness_status",
        "rows",
        "latest_date",
        "detail",
    ]
    if governance.empty:
        return pd.DataFrame(columns=columns)
    frame = governance.copy()
    for column in ["coverage_ratio", "pit_field_completeness"]:
        frame[column] = pd.to_numeric(frame.get(column, pd.Series(dtype=float)), errors="coerce")
    frame["coverage_pct"] = (frame["coverage_ratio"] * 100).round(3)
    frame["pit_pct"] = (frame["pit_field_completeness"] * 100).round(3)
    latest_dates = {
        "liquidity_microstructure": _latest_date_from_csv(_resolve(root, liquidity_path), "date"),
        "emotion_atmosphere": _latest_date_from_csv(_resolve(root, emotion_path), "trade_date"),
    }
    frame["latest_date"] = frame["domain"].map(latest_dates).fillna("")
    for column in columns:
        if column not in frame.columns:
            frame[column] = pd.NA
    return frame.loc[:, columns].reset_index(drop=True)


def _liquidity_domain_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"rows": 0, "buy_blocked": 0, "suspended": 0, "median_amount_20d": float("nan")}
    return {
        "rows": int(len(frame)),
        "buy_blocked": int(_truthy_series(frame.get("buy_blocked", pd.Series(dtype=object))).sum()),
        "suspended": int(_truthy_series(frame.get("suspended", pd.Series(dtype=object))).sum()),
        "median_amount_20d": _number(pd.to_numeric(frame.get("amount_20d", pd.Series(dtype=float)), errors="coerce").median()),
    }


def _emotion_domain_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "rows": 0,
            "mean_emotion_score": float("nan"),
            "mean_instrument_emotion_score": float("nan"),
            "latest_limit_up_count": 0,
            "latest_up_ratio": float("nan"),
        }
    latest = ""
    if "trade_date" in frame.columns:
        dates = pd.to_datetime(frame["trade_date"], errors="coerce")
        if dates.notna().any():
            latest = dates.max().strftime("%Y-%m-%d")
    latest_frame = frame[frame["trade_date"].astype(str) == latest] if latest and "trade_date" in frame.columns else frame
    limit_up_count = pd.to_numeric(latest_frame.get("limit_up_count", pd.Series(dtype=float)), errors="coerce").max()
    return {
        "rows": int(len(frame)),
        "mean_emotion_score": _number(pd.to_numeric(latest_frame.get("emotion_score", pd.Series(dtype=float)), errors="coerce").mean()),
        "mean_instrument_emotion_score": _number(
            pd.to_numeric(latest_frame.get("instrument_emotion_score", pd.Series(dtype=float)), errors="coerce").mean()
        ),
        "latest_limit_up_count": int(limit_up_count) if pd.notna(limit_up_count) else 0,
        "latest_up_ratio": _number(pd.to_numeric(latest_frame.get("up_ratio", pd.Series(dtype=float)), errors="coerce").mean()),
    }


def _latest_date_from_csv(path: Path, column: str) -> str:
    frame = _read_csv_or_empty(path)
    if frame.empty or column not in frame.columns:
        return ""
    dates = pd.to_datetime(frame[column], errors="coerce").dropna()
    return dates.max().strftime("%Y-%m-%d") if not dates.empty else ""


def _source_subset(frame: pd.DataFrame, source: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    if "source" not in frame.columns:
        return frame.iloc[0:0].copy()
    return frame.loc[frame["source"].fillna("").astype(str) == source].copy()


def _event_source_subset(frame: pd.DataFrame, event_type: str, source: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    if "source" not in frame.columns or "event_type" not in frame.columns:
        return frame.iloc[0:0].copy()
    source_mask = frame.get("source", pd.Series(dtype=str)).fillna("").astype(str) == source
    event_mask = frame.get("event_type", pd.Series(dtype=str)).fillna("").astype(str) == event_type
    return frame.loc[source_mask & event_mask].copy()


def _vendor_domain_row(domain: str, source: str, frame: pd.DataFrame, date_columns: list[str]) -> dict[str, Any]:
    rows = int(len(frame))
    instruments = _instrument_count(frame)
    return {
        "domain": domain,
        "source": source,
        "status": "active" if rows > 0 else "missing",
        "rows": rows,
        "instruments": instruments,
        "latest_date": _latest_date_from_frame(frame, date_columns),
        "coverage_note": f"{instruments} instruments / {rows} rows" if rows > 0 else "no vendor rows",
    }


def _latest_date_from_frame(frame: pd.DataFrame, columns: list[str]) -> str:
    if frame.empty:
        return ""
    dates: list[pd.Series] = []
    for column in columns:
        if column in frame.columns:
            parsed = pd.to_datetime(frame[column], errors="coerce").dropna()
            if not parsed.empty:
                dates.append(parsed)
    if not dates:
        return ""
    merged = pd.concat(dates, ignore_index=True)
    return merged.max().strftime("%Y-%m-%d") if not merged.empty else ""


def _instrument_count(frame: pd.DataFrame) -> int:
    if frame.empty or "instrument" not in frame.columns:
        return 0
    return int(frame["instrument"].dropna().astype(str).nunique())


def _truthy_series(values: pd.Series) -> pd.Series:
    return values.map(_truthy).fillna(False).astype(bool)


def build_combo_profile_summary(
    root: str | Path = ".",
    combo_spec_dir: str | Path = COMBO_SPEC_DIR,
) -> pd.DataFrame:
    root_path = Path(root)
    spec_dir = _resolve(root_path, combo_spec_dir)
    columns = [
        "name",
        "posture",
        "members",
        "active_members",
        "offensive_weight",
        "defensive_weight",
        "largest_family",
        "largest_family_weight",
        "data_blockers",
        "path",
    ]
    if not spec_dir.exists():
        return pd.DataFrame(columns=columns)

    data_gaps = build_factor_data_gap_summary(root_path)
    blocker_fields = set(data_gaps.loc[data_gaps["status"] == "blocked", "field"].astype(str)) if not data_gaps.empty else set()
    rows = []
    for path in sorted(spec_dir.glob("*.yaml")):
        try:
            spec = load_combo_spec(path)
        except Exception as exc:
            rows.append(
                {
                    "name": path.stem,
                    "posture": "invalid",
                    "members": 0,
                    "active_members": 0,
                    "offensive_weight": 0.0,
                    "defensive_weight": 0.0,
                    "largest_family": "n/a",
                    "largest_family_weight": 0.0,
                    "data_blockers": str(exc),
                    "path": str(path),
                }
            )
            continue
        active = [member for member in spec.members if member.active]
        family_weights: dict[str, float] = {}
        blocked = set()
        for member in active:
            family = member.family or member.name
            family_weights[family] = family_weights.get(family, 0.0) + float(member.weight)
            for component in member.components:
                field = str(component.get("field", ""))
                if field in blocker_fields:
                    blocked.add(field)
        offensive_weight = sum(weight for family, weight in family_weights.items() if family in OFFENSIVE_FACTOR_FAMILIES)
        defensive_weight = sum(weight for family, weight in family_weights.items() if family in DEFENSIVE_FACTOR_FAMILIES)
        largest_family, largest_weight = _largest_family_weight(family_weights)
        rows.append(
            {
                "name": spec.name,
                "posture": _combo_posture(offensive_weight, defensive_weight),
                "members": int(len(spec.members)),
                "active_members": int(len(active)),
                "offensive_weight": offensive_weight,
                "defensive_weight": defensive_weight,
                "largest_family": largest_family,
                "largest_family_weight": largest_weight,
                "data_blockers": ", ".join(sorted(blocked)),
                "path": str(path),
            }
        )
    return pd.DataFrame(rows, columns=columns)


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
        factor_logic_map=load_factor_logic_map_safe(root_path),
    )
    latest_run = find_latest_run_dir(root_path)
    expert_path = latest_run / "expert_review_result.md" if latest_run is not None else None
    expert = parse_expert_review_result(expert_path) if expert_path is not None else parse_expert_review_result("")
    return build_execution_gate_card(gate.decision, build_pretrade_review(portfolio), expert)


def parse_expert_review_result(source: str | Path | None) -> dict[str, Any]:
    text = _expert_text(source)
    metadata = _expert_metadata(text)
    output = _section_after(text, "## Output")
    manual_items = parse_expert_review_manual_items(output)
    structured_reasons = _expert_structured_reasons(output)
    return {
        "status": metadata.get("status", "missing" if not text.strip() else ""),
        "decision": _normalized_decision(metadata.get("decision", "")),
        "error": metadata.get("error", ""),
        "summary": _expert_summary(output),
        "watchlist": _expert_watchlist(output),
        "manual_items": manual_items,
        "hard_manual_review": manual_items.get("hard_manual_review", []),
        "liquidity_review": manual_items.get("liquidity_review", []),
        "risk_notes": _expert_risk_notes(output),
        "structured_reasons": structured_reasons,
        "reject_reasons": structured_reasons,
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
        factor_logic_map=load_factor_logic_map_safe(root_path),
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
        min_factor_logic_count=int(raw["min_factor_logic_count"]) if raw.get("min_factor_logic_count") is not None else None,
        max_factor_logic_concentration=(
            float(raw["max_factor_logic_concentration"])
            if raw.get("max_factor_logic_concentration") is not None
            else None
        ),
        enable_vendor_data_gate=bool(raw.get("enable_vendor_data_gate", False)),
        min_tushare_domain_instruments=int(raw.get("min_tushare_domain_instruments", 1)),
    )


def _signal_for_portfolio(portfolio: pd.DataFrame) -> pd.DataFrame:
    if portfolio.empty:
        return pd.DataFrame(columns=["eligible"])
    signal = portfolio.copy()
    signal["eligible"] = True
    return signal


def _run_artifact_or_latest(run_dir: Path | None, filename: str, latest: Path | None) -> Path | None:
    if run_dir is not None:
        candidate = run_dir / filename
        if candidate.exists():
            return candidate
    return latest


def _gross_weight(portfolio: pd.DataFrame) -> float:
    if portfolio.empty or "target_weight" not in portfolio.columns:
        return 0.0
    return float(pd.to_numeric(portfolio["target_weight"], errors="coerce").fillna(0.0).sum())


def _portfolio_layer_detail(research: pd.DataFrame, execution: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "instrument",
        "research_rank",
        "execution_rank",
        "research_weight",
        "execution_weight",
        "weight_delta",
        "action",
        "ensemble_score",
        "risk_flags",
    ]
    if research.empty and execution.empty:
        return pd.DataFrame(columns=columns)
    left = _portfolio_layer_subset(research, "research")
    right = _portfolio_layer_subset(execution, "execution")
    detail = left.merge(right, on="instrument", how="outer")
    for column in ["research_weight", "execution_weight"]:
        if column not in detail.columns:
            detail[column] = 0.0
        detail[column] = pd.to_numeric(detail[column], errors="coerce").fillna(0.0)
    detail["weight_delta"] = detail["execution_weight"] - detail["research_weight"]
    detail["action"] = detail.apply(_portfolio_layer_action, axis=1)
    if "execution_ensemble_score" in detail.columns:
        detail["ensemble_score"] = detail["execution_ensemble_score"]
    elif "research_ensemble_score" in detail.columns:
        detail["ensemble_score"] = detail["research_ensemble_score"]
    else:
        detail["ensemble_score"] = pd.NA
    if "execution_risk_flags" in detail.columns:
        detail["risk_flags"] = detail["execution_risk_flags"].fillna("")
    else:
        detail["risk_flags"] = ""
    for column in columns:
        if column not in detail.columns:
            detail[column] = pd.NA
    return detail.loc[:, columns].sort_values(["action", "instrument"]).reset_index(drop=True)


def _portfolio_layer_subset(portfolio: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if portfolio.empty or "instrument" not in portfolio.columns:
        return pd.DataFrame(columns=["instrument"])
    frame = portfolio.copy()
    frame["instrument"] = frame["instrument"].astype(str)
    rename = {}
    if "rank" in frame.columns:
        rename["rank"] = f"{prefix}_rank"
    if "target_weight" in frame.columns:
        rename["target_weight"] = f"{prefix}_weight"
    if "ensemble_score" in frame.columns:
        rename["ensemble_score"] = f"{prefix}_ensemble_score"
    if "risk_flags" in frame.columns:
        rename["risk_flags"] = f"{prefix}_risk_flags"
    keep = ["instrument", *rename.keys()]
    return frame.loc[:, keep].rename(columns=rename)


def _portfolio_layer_action(row: pd.Series) -> str:
    research_weight = float(row.get("research_weight", 0.0) or 0.0)
    execution_weight = float(row.get("execution_weight", 0.0) or 0.0)
    if research_weight > 0 and execution_weight <= 0:
        return "removed"
    if research_weight <= 0 and execution_weight > 0:
        return "added"
    if abs(execution_weight - research_weight) > 1e-9:
        return "scaled"
    return "unchanged"


def _latest_intraday_portfolio_path(root: Path) -> Path | None:
    formal = list((root / "reports").glob("portfolio_intraday_performance_*.csv"))
    formal.extend((root / "runs").glob("*/portfolio_intraday_performance.csv"))
    latest_formal = _latest_path(formal)
    if latest_formal is not None:
        return latest_formal
    legacy = list((root / "reports").glob("portfolio_top*_intraday_*.csv"))
    legacy.extend((root / "runs").glob("*/portfolio_top*_intraday_*.csv"))
    return _latest_path(legacy)


def _empty_execution_performance_attribution(path: Path | None) -> dict[str, Any]:
    group_columns = ["count", "target_weight", "pct_today", "weighted_return_pct"]
    return {
        "path": str(path or ""),
        "summary": {"positions": 0, "weighted_return_pct": 0.0, "up_count": 0, "down_count": 0, "quote_time": ""},
        "industry": pd.DataFrame(columns=["industry", *group_columns]),
        "factor": pd.DataFrame(columns=["factor", *group_columns]),
        "event": pd.DataFrame(columns=["event_bucket", *group_columns]),
        "contributors": pd.DataFrame(
            columns=[
                "instrument",
                "display_name",
                "industry",
                "factor",
                "event_bucket",
                "target_weight",
                "pct_today",
                "weighted_return_pct",
                "direction",
            ]
        ),
    }


def _first_nonblank_column(frame: pd.DataFrame, columns: list[str], *, default: str) -> pd.Series:
    output = pd.Series(default, index=frame.index, dtype="object")
    filled = pd.Series(False, index=frame.index)
    for column in columns:
        if column not in frame.columns:
            continue
        values = frame[column].fillna("").astype(str).str.strip()
        mask = (~filled) & values.ne("")
        output.loc[mask] = values.loc[mask]
        filled = filled | mask
    return output


def _performance_event_bucket(row: pd.Series) -> str:
    if _truthy(row.get("event_blocked")):
        return "event_block"
    if _number(row.get("event_count")) > 0 or not _blank(row.get("event_risk_summary")) or not _blank(row.get("active_event_types")):
        return "event_watch"
    if _truthy(row.get("announcement_flag")):
        return "announcement_watch"
    return "no_event"


def _performance_group(frame: pd.DataFrame, group_column: str, label: str) -> pd.DataFrame:
    columns = [label, "count", "target_weight", "pct_today", "weighted_return_pct"]
    if frame.empty or group_column not in frame.columns:
        return pd.DataFrame(columns=columns)
    grouped = (
        frame.groupby(group_column, dropna=False)
        .agg(
            count=("instrument", "count"),
            target_weight=("target_weight", "sum"),
            pct_today=("pct_today", "mean"),
            weighted_return_pct=("weighted_return_pct", "sum"),
        )
        .reset_index()
        .rename(columns={group_column: label})
        .sort_values("weighted_return_pct", ascending=True)
        .reset_index(drop=True)
    )
    return grouped.loc[:, columns]


def _latest_path(paths: list[Path]) -> Path | None:
    existing = [path for path in paths if path.exists()]
    if not existing:
        return None
    return max(existing, key=lambda path: (path.stat().st_mtime, str(path)))


def _resolve(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else root / candidate


def _resolve_optional(root: Path, path: str | Path | None) -> Path | None:
    if path is None:
        return None
    return _resolve(root, path)


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
        "min_factor_logic_count": "补充趋势、流动性、风险结构、情绪等非同类逻辑，避免只靠反转修复。",
        "max_factor_logic_concentration": "降低主导交易逻辑暴露，避免多个不同名字的因子其实押同一件事。",
        "max_single_weight": "降低单票权重，避免个股流动性和事件风险放大。",
        "min_positions": "增加持仓数量，避免样本太窄导致组合不可解释。",
        "min_signal_coverage": "检查信号覆盖率，避免用缺失数据生成组合。",
        "max_turnover": "降低换手或拉长调仓周期，检查交易成本敏感性。",
    }
    return focus.get(check, "人工复核该约束是否合理。")


def _portfolio_industry_coverage(portfolio: pd.DataFrame) -> float:
    if portfolio.empty:
        return 0.0
    columns = [column for column in ["industry", "industry_sw", "industry_csrc"] if column in portfolio.columns]
    if not columns:
        return 0.0
    covered = pd.Series(False, index=portfolio.index)
    for column in columns:
        covered = covered | portfolio[column].fillna("").astype(str).str.strip().ne("")
    return float(covered.mean())


def _portfolio_event_coverage(portfolio: pd.DataFrame) -> float:
    if portfolio.empty:
        return 0.0
    covered = pd.Series(False, index=portfolio.index)
    if "event_count" in portfolio.columns:
        covered = covered | (pd.to_numeric(portfolio["event_count"], errors="coerce").fillna(0) > 0)
    for column in ["active_event_types", "event_risk_summary", "event_source_urls", "announcement_flag"]:
        if column in portfolio.columns:
            covered = covered | portfolio[column].fillna("").astype(str).str.strip().ne("")
    return float(covered.mean())


def _portfolio_family_concentration(family: pd.DataFrame) -> float:
    if family.empty or "abs_weighted_contribution" not in family.columns:
        return 0.0
    total = float(pd.to_numeric(family["abs_weighted_contribution"], errors="coerce").fillna(0.0).sum())
    if total <= 0:
        return 0.0
    return float(pd.to_numeric(family["abs_weighted_contribution"], errors="coerce").fillna(0.0).max() / total)


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


def _value_counts_frame(frame: pd.DataFrame, column: str) -> pd.DataFrame:
    if frame.empty or column not in frame.columns:
        return pd.DataFrame(columns=[column, "count"])
    values = frame[column].fillna("").astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return pd.DataFrame(columns=[column, "count"])
    counts = values.value_counts().rename_axis(column).reset_index(name="count")
    return counts


def _event_evidence_detail(events: pd.DataFrame) -> pd.DataFrame:
    frame = events.copy()
    for column in COMPANY_EVENT_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    columns = [
        "event_date",
        "instrument",
        "event_type",
        "severity",
        "title",
        "summary",
        "source",
        "source_url",
        "active_until",
    ]
    frame["_event_date_sort"] = pd.to_datetime(frame["event_date"], errors="coerce")
    return frame.sort_values("_event_date_sort", ascending=False, na_position="last").loc[:, columns].reset_index(drop=True)


def _count_unique_split_values(values: pd.Series) -> int:
    items = set()
    for value in values:
        items.update(_split_semicolon_values(value))
    return len(items)


def _read_csv_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def _nonblank_strings(values: pd.Series) -> list[str]:
    if values is None or values.empty:
        return []
    text = values.fillna("").astype(str).str.strip()
    return text[text != ""].tolist()


def _research_universe_labels(master: pd.DataFrame) -> set[str]:
    labels: set[str] = set()
    if master.empty or "research_universes" not in master.columns:
        return labels
    for value in master["research_universes"]:
        labels.update(item.strip() for item in re.split(r"[,;]", str(value)) if item.strip())
    return labels


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator) * 100.0, 1)


def _coverage_status(coverage: float, ready_threshold: float, caution_threshold: float) -> str:
    if coverage >= ready_threshold:
        return "ready"
    if coverage >= caution_threshold:
        return "caution"
    return "blocked"


def _factor_data_gap_impact(field: str, coverage: float) -> str:
    if coverage >= 0.50:
        return "可用于正式因子或门禁复核。"
    if field in {"revenue_growth_yoy", "net_profit_growth_yoy"}:
        return "进攻型盈利改善 lane 会退化，不能把增长因子当作有效驱动。"
    if field == "operating_cashflow_to_net_profit":
        return "现金流质量确认不足，质量因子只能先做弱约束。"
    if field in {"ep", "cfp", "dividend_yield"}:
        return "价值/红利覆盖不足时，防御组合需要降权或人工确认。"
    return "覆盖不足，需要补数据或保持 shadow/watch 状态。"


def _largest_family_weight(family_weights: dict[str, float]) -> tuple[str, float]:
    if not family_weights:
        return "n/a", 0.0
    family = max(family_weights, key=lambda key: family_weights[key])
    return family, float(family_weights[family])


def _combo_posture(offensive_weight: float, defensive_weight: float) -> str:
    if offensive_weight >= 0.45 and offensive_weight > defensive_weight:
        return "offensive"
    if defensive_weight >= 0.45 and defensive_weight >= offensive_weight:
        return "defensive"
    return "balanced"


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
        return source.read_text(encoding="utf-8") if source.exists() and source.is_file() else ""
    text = str(source)
    if not text.strip():
        return ""
    possible_path = Path(text)
    if "\n" not in text and possible_path.exists() and possible_path.is_file():
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


def _expert_structured_reasons(output: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for line in output.splitlines():
        cleaned = _clean_markdown(line)
        if not cleaned:
            continue
        category = _expert_reason_category(cleaned)
        severity = _expert_reason_severity(cleaned)
        if category == "other" and severity == "review":
            continue
        key = (category, cleaned)
        if key in seen:
            continue
        rows.append(
            {
                "severity": severity,
                "category": category,
                "instruments": _expert_watchlist(cleaned),
                "detail": cleaned,
            }
        )
        seen.add(key)
    return rows


def _expert_reason_category(text: str) -> str:
    lowered = text.lower()
    if any(keyword in text for keyword in ["事件", "公告", "问询", "监管", "处罚", "纪律", "减持", "诉讼", "解禁"]) or "block" in lowered:
        return "event_risk"
    if any(keyword in text for keyword in ["流动性", "成交额", "容量", "换手"]) or "liquidity" in lowered:
        return "liquidity"
    if any(keyword in text for keyword in ["因子", "family", "逻辑单一"]) or "factor" in lowered:
        return "factor_concentration"
    if any(keyword in text for keyword in ["行业", "板块", "主题暴露"]):
        return "industry_concentration"
    if any(keyword in text for keyword in ["涨跌停", "停牌", "休市", "交易日", "不可成交"]):
        return "tradability"
    if any(keyword in text for keyword in ["数据", "缺失", "覆盖", "诊断"]):
        return "data_quality"
    if any(keyword in text for keyword in ["人工", "复核", "看图"]) or "manual" in lowered:
        return "manual_review"
    return "other"


def _expert_reason_severity(text: str) -> str:
    lowered = text.lower()
    if any(keyword in lowered for keyword in ["reject", "block"]) or any(
        keyword in text for keyword in ["阻断", "硬拦截", "拦截", "不建议进入", "不建议直接"]
    ):
        return "reject"
    if any(keyword in lowered for keyword in ["caution", "manual", "watch"]) or any(
        keyword in text for keyword in ["复核", "降仓", "观察", "偏低", "过高", "过度", "集中"]
    ):
        return "caution"
    return "review"


def _clean_markdown(line: str) -> str:
    cleaned = line.strip().replace("`", "")
    cleaned = re.sub(r"^\s*[-*]\s+", "", cleaned)
    cleaned = re.sub(r"^\s*\d+[.、]\s*", "", cleaned)
    cleaned = re.sub(r"^#+\s*", "", cleaned)
    cleaned = cleaned.replace("**", "")
    return cleaned.strip()
