from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from .exposure_attribution import build_exposure_attribution, load_factor_family_map
from .risk import RiskConfig, check_portfolio_risk


AUTORESEARCH_LEDGER = Path("reports/autoresearch/expression_results.tsv")
APPROVED_FACTORS = Path("reports/approved_factors.yaml")
RISK_CONFIG = Path("configs/risk.yaml")


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
    candidates = [path for path in (root_path / "runs").glob("*") if path.is_dir()]
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
