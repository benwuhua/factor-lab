from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from .config import load_yaml


@dataclass(frozen=True)
class DataDomainConfig:
    name: str
    path: Path | None = None
    required_fields: tuple[str, ...] = ()
    pit_fields: tuple[str, ...] = ()
    min_coverage_ratio: float = 0.0
    activation_lane: str = ""
    activation_if_missing: str = "shadow"
    activation_if_failed: str = "shadow"
    freshness_date_column: str = ""
    max_age_days: int | None = None


@dataclass(frozen=True)
class DataGovernanceConfig:
    expected_universe_path: Path | None = None
    domains: tuple[DataDomainConfig, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class DataGovernanceReport:
    rows: tuple[dict[str, Any], ...]

    @property
    def passed(self) -> bool:
        return all(row["status"] == "pass" for row in self.rows)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            self.rows,
            columns=[
                "domain",
                "status",
                "activation_status",
                "coverage_ratio",
                "pit_field_completeness",
                "freshness_status",
                "rows",
                "detail",
            ],
        )


def load_data_governance_config(path: str | Path) -> DataGovernanceConfig:
    data = load_yaml(path)
    raw = data.get("data_governance", data)
    domains = []
    for name, domain in (raw.get("domains") or {}).items():
        domains.append(
            DataDomainConfig(
                name=str(name),
                path=_optional_path(domain.get("path")),
                required_fields=tuple(str(item) for item in domain.get("required_fields", ())),
                pit_fields=tuple(str(item) for item in domain.get("pit_fields", ())),
                min_coverage_ratio=float(domain.get("min_coverage_ratio", raw.get("min_coverage_ratio", 0.0))),
                activation_lane=str(domain.get("activation_lane", name)),
                activation_if_missing=str(domain.get("activation_if_missing", raw.get("activation_if_missing", "shadow"))),
                activation_if_failed=str(domain.get("activation_if_failed", raw.get("activation_if_failed", "shadow"))),
                freshness_date_column=str(domain.get("freshness_date_column", "")),
                max_age_days=int(domain["max_age_days"]) if domain.get("max_age_days") is not None else None,
            )
        )
    return DataGovernanceConfig(
        expected_universe_path=_optional_path(raw.get("expected_universe_path")),
        domains=tuple(domains),
    )


def build_data_governance_report(
    config: DataGovernanceConfig,
    *,
    project_root: str | Path = ".",
    as_of_date: str | None = None,
) -> DataGovernanceReport:
    root = Path(project_root)
    expected_instruments = _load_expected_instruments(_resolve_optional(root, config.expected_universe_path))
    rows = [
        _evaluate_domain(
            domain,
            root=root,
            expected_instruments=expected_instruments,
            as_of_date=as_of_date,
        )
        for domain in config.domains
    ]
    return DataGovernanceReport(tuple(rows))


def write_data_governance_report(report: DataGovernanceReport, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame = report.to_frame()
    lines = [
        "# Data Governance Report",
        "",
        f"- status: {'pass' if report.passed else 'fail'}",
        "",
        "| domain | status | activation | coverage | pit_completeness | freshness | rows | detail |",
        "|---|---|---|---:|---:|---|---:|---|",
    ]
    for _, row in frame.iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["domain"]),
                    str(row["status"]),
                    str(row["activation_status"]),
                    _format_ratio(row["coverage_ratio"]),
                    _format_ratio(row["pit_field_completeness"]),
                    str(row["freshness_status"]),
                    str(row["rows"]),
                    str(row["detail"]),
                ]
            )
            + " |"
        )
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    frame.to_csv(output.with_suffix(".csv"), index=False)
    return output


def _evaluate_domain(
    domain: DataDomainConfig,
    *,
    root: Path,
    expected_instruments: set[str],
    as_of_date: str | None,
) -> dict[str, Any]:
    path = _resolve_optional(root, domain.path, as_of_date=as_of_date)
    if path is None or not path.exists():
        return _row(domain, "missing", domain.activation_if_missing, 0.0, 0.0, "missing", 0, "file missing")

    frame = pd.read_csv(path)
    missing_required = [column for column in domain.required_fields if column not in frame.columns]
    coverage = _coverage_ratio(frame, expected_instruments)
    pit_completeness = _pit_completeness(frame, domain.pit_fields)
    freshness_status = _freshness_status(frame, domain, as_of_date)
    failures = []
    if frame.empty:
        failures.append("empty")
    if missing_required:
        failures.append(f"missing_required={missing_required}")
    if coverage < domain.min_coverage_ratio:
        failures.append(f"coverage_below_{domain.min_coverage_ratio:g}")
    if domain.pit_fields and pit_completeness < 1.0:
        failures.append("pit_incomplete")
    if freshness_status == "missing":
        failures.append("freshness_missing")
    if freshness_status == "stale":
        failures.append("stale")

    status = "fail" if failures else "pass"
    activation = "active" if status == "pass" else domain.activation_if_failed
    return _row(
        domain,
        status,
        activation,
        coverage,
        pit_completeness,
        freshness_status,
        len(frame),
        "; ".join(failures),
    )


def _coverage_ratio(frame: pd.DataFrame, expected_instruments: set[str]) -> float:
    if frame.empty:
        return 0.0
    if "instrument" not in frame.columns or not expected_instruments:
        return 1.0
    covered = set(frame["instrument"].dropna().astype(str))
    return min(1.0, len(covered & expected_instruments) / len(expected_instruments))


def _pit_completeness(frame: pd.DataFrame, pit_fields: tuple[str, ...]) -> float:
    if not pit_fields:
        return 1.0
    if frame.empty:
        return 0.0
    values = []
    for field_name in pit_fields:
        if field_name not in frame.columns:
            values.append(0.0)
            continue
        values.append(float(frame[field_name].notna().mean()))
    return sum(values) / len(values)


def _freshness_status(frame: pd.DataFrame, domain: DataDomainConfig, as_of_date: str | None) -> str:
    if not domain.freshness_date_column or domain.max_age_days is None or as_of_date is None:
        return "not_checked"
    if domain.freshness_date_column not in frame.columns or frame.empty:
        return "missing"
    latest = pd.to_datetime(frame[domain.freshness_date_column], errors="coerce").max()
    current = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(latest) or pd.isna(current):
        return "missing"
    return "pass" if (current - latest).days <= domain.max_age_days else "stale"


def _load_expected_instruments(path: Path | None) -> set[str]:
    if path is None or not path.exists():
        return set()
    frame = pd.read_csv(path)
    if "instrument" not in frame.columns:
        return set()
    return set(frame["instrument"].dropna().astype(str))


def _row(
    domain: DataDomainConfig,
    status: str,
    activation_status: str,
    coverage_ratio: float,
    pit_field_completeness: float,
    freshness_status: str,
    rows: int,
    detail: str,
) -> dict[str, Any]:
    return {
        "domain": domain.name,
        "status": status,
        "activation_status": activation_status,
        "coverage_ratio": float(coverage_ratio),
        "pit_field_completeness": float(pit_field_completeness),
        "freshness_status": freshness_status,
        "rows": int(rows),
        "detail": detail,
    }


def _resolve_optional(root: Path, path: Path | None, as_of_date: str | None = None) -> Path | None:
    if path is None:
        return None
    materialized = _materialize(path, as_of_date)
    return materialized if materialized.is_absolute() else root / materialized


def _optional_path(value: Any) -> Path | None:
    if value is None or str(value).strip() == "":
        return None
    return Path(str(value))


def _materialize(path: Path, as_of_date: str | None) -> Path:
    if as_of_date is None:
        return path
    yyyymmdd = str(as_of_date).replace("-", "")
    return Path(str(path).format(as_of_date=as_of_date, run_yyyymmdd=yyyymmdd))


def _format_ratio(value: Any) -> str:
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return ""
