from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


PASS_REQUIRED_ARTIFACTS = (
    "signals",
    "target_portfolio",
    "orders",
    "fills",
    "run_summary",
)


@dataclass(frozen=True)
class ReplayReport:
    run_dir: Path
    run_date: str
    manifest_path: Path
    manifest_status: str
    rows: tuple[dict[str, Any], ...]
    generated_at: str

    @property
    def passed(self) -> bool:
        return all(row["status"] == "pass" for row in self.rows)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=["check", "status", "detail"])

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_dir": str(self.run_dir),
            "run_date": self.run_date,
            "manifest_path": str(self.manifest_path),
            "manifest_status": self.manifest_status,
            "status": "pass" if self.passed else "fail",
            "generated_at": self.generated_at,
            "checks": list(self.rows),
        }


def replay_daily_run(run_dir: str | Path) -> ReplayReport:
    run_path = Path(run_dir).expanduser().resolve()
    manifest_path = run_path / "manifest.json"
    rows: list[dict[str, Any]] = []
    manifest: dict[str, Any] = {}

    if not manifest_path.exists():
        rows.append(_row("manifest_exists", "fail", f"missing: {manifest_path}"))
        return ReplayReport(
            run_dir=run_path,
            run_date="",
            manifest_path=manifest_path,
            manifest_status="",
            rows=tuple(rows),
            generated_at=_now(),
        )

    rows.append(_row("manifest_exists", "pass", str(manifest_path)))
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows.append(_row("manifest_json", "pass", "valid json"))
    except json.JSONDecodeError as exc:
        rows.append(_row("manifest_json", "fail", str(exc)))
        return ReplayReport(run_path, "", manifest_path, "", tuple(rows), _now())

    run_date = str(manifest.get("run_date", ""))
    manifest_status = str(manifest.get("status", ""))
    artifacts = manifest.get("artifacts", {}) or {}
    if not isinstance(artifacts, dict):
        rows.append(_row("artifacts_mapping", "fail", "manifest.artifacts must be a mapping"))
        artifacts = {}
    else:
        rows.append(_row("artifacts_mapping", "pass", f"{len(artifacts)} artifacts"))

    for key in _required_artifacts(manifest_status):
        if key not in artifacts:
            rows.append(_row(f"required_artifact:{key}", "fail", "missing from manifest.artifacts"))
        else:
            rows.append(_row(f"required_artifact:{key}", "pass", str(artifacts[key])))

    for key, raw_path in sorted(artifacts.items()):
        path = _resolve_artifact_path(run_path, raw_path)
        if path.exists():
            rows.append(_row(f"artifact:{key}", "pass", str(path)))
        else:
            rows.append(_row(f"artifact:{key}", "fail", f"missing file: {path}"))

    _check_run_summary(run_path, manifest, artifacts, rows)
    return ReplayReport(run_path, run_date, manifest_path, manifest_status, tuple(rows), _now())


def write_replay_report(report: ReplayReport, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Replay Report",
        "",
        f"- run_dir: {report.run_dir}",
        f"- run_date: {report.run_date}",
        f"- manifest_status: {report.manifest_status}",
        f"- status: {'pass' if report.passed else 'fail'}",
        f"- generated_at: {report.generated_at}",
        "",
        "| check | status | detail |",
        "|---|---|---|",
    ]
    for row in report.rows:
        lines.append(f"| {row['check']} | {row['status']} | {_table_cell(row['detail'])} |")
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    output.with_suffix(".json").write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output


def _required_artifacts(manifest_status: str) -> tuple[str, ...]:
    if manifest_status == "pass":
        return PASS_REQUIRED_ARTIFACTS
    return ()


def _check_run_summary(run_dir: Path, manifest: dict[str, Any], artifacts: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    raw_path = artifacts.get("run_summary")
    if not raw_path:
        return
    path = _resolve_artifact_path(run_dir, raw_path)
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8", errors="ignore")
    expected_status = str(manifest.get("status", ""))
    if expected_status and f"status: {expected_status}" not in text:
        rows.append(_row("run_summary_status", "fail", f"run_summary does not mention status: {expected_status}"))
    else:
        rows.append(_row("run_summary_status", "pass", expected_status))
    if "risk_passed" in manifest:
        expected_risk = str(bool(manifest.get("risk_passed")))
        if f"risk_passed: {expected_risk}" not in text:
            rows.append(_row("run_summary_risk_passed", "fail", f"run_summary does not mention risk_passed: {expected_risk}"))
        else:
            rows.append(_row("run_summary_risk_passed", "pass", expected_risk))


def _resolve_artifact_path(run_dir: Path, raw_path: Any) -> Path:
    path = Path(str(raw_path)).expanduser()
    if path.is_absolute():
        return path
    candidate = run_dir / path
    if candidate.exists():
        return candidate
    return run_dir.parent.parent / path


def _row(check: str, status: str, detail: str) -> dict[str, Any]:
    return {"check": check, "status": status, "detail": detail}


def _table_cell(value: Any) -> str:
    return " ".join(str(value or "").split()).replace("|", "\\|")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")
