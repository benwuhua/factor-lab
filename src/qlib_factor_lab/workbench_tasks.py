from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
import os
import subprocess
import sys
from typing import Any


@dataclass(frozen=True)
class WorkbenchTask:
    task_id: str
    label: str
    command: tuple[str, ...]
    description: str


@dataclass(frozen=True)
class WorkbenchTaskRecord:
    task_id: str
    run_dir: Path
    manifest_path: Path
    log_path: Path


WORKBENCH_TASKS: dict[str, WorkbenchTask] = {
    "check-env": WorkbenchTask("check-env", "检查 provider 环境", ("make", "check-env"), "检查 Qlib provider、calendar 和因子注册表。"),
    "data-governance": WorkbenchTask("data-governance", "检查数据治理", ("make", "data-governance"), "检查数据域覆盖率、PIT 字段和 lane 激活状态。"),
    "daily-signal": WorkbenchTask("daily-signal", "生成当日信号", ("make", "daily-signal"), "从 approved 因子生成当日解释型信号。"),
    "check-data-quality": WorkbenchTask("check-data-quality", "检查信号质量", ("make", "check-data-quality"), "对当日信号覆盖率和缺失情况做质量门禁。"),
    "select-factors": WorkbenchTask("select-factors", "生成 approved 因子", ("make", "select-factors"), "根据治理配置刷新 approved_factors.yaml。"),
    "autoresearch-codex-loop": WorkbenchTask("autoresearch-codex-loop", "启动自动挖掘", ("make", "autoresearch-codex-loop"), "启动受控候选表达式研究循环。"),
    "autoresearch-multilane": WorkbenchTask("autoresearch-multilane", "多车道挖掘", ("make", "autoresearch-multilane"), "按 lane_space 运行 active lane，并记录 shadow/unsupported lane 状态。"),
    "autoresearch-review": WorkbenchTask("autoresearch-review", "复核自动挖掘", ("make", "autoresearch-review"), "汇总 nightly ledger、稳定性和重复簇。"),
    "research-context": WorkbenchTask("research-context", "刷新证据库", ("make", "research-context"), "刷新 data/security_master.csv 和 data/company_events.csv。"),
    "target-portfolio": WorkbenchTask("target-portfolio", "生成目标组合", ("make", "target-portfolio"), "把当日信号转换成 target_portfolio。"),
    "stock-cards": WorkbenchTask("stock-cards", "生成股票卡片", ("make", "stock-cards"), "把 target_portfolio 转换成可复核 JSONL 股票卡片。"),
    "exposure-attribution": WorkbenchTask("exposure-attribution", "暴露归因", ("make", "exposure-attribution"), "解释行业、因子族和风格暴露。"),
    "paper-orders": WorkbenchTask("paper-orders", "生成纸面订单", ("make", "paper-orders"), "把目标组合转换为纸面订单和模拟成交。"),
    "reconcile-account": WorkbenchTask("reconcile-account", "账户对账", ("make", "reconcile-account"), "对 expected 和 actual positions 做对账。"),
    "paper-batch": WorkbenchTask("paper-batch", "滚动纸面批测", ("make", "paper-batch"), "对历史 target portfolio 做滚动纸面执行复盘。"),
}

ALLOWED_TASK_ENV_OVERRIDES = {
    "RUN_DATE",
    "RESEARCH_CONTEXT_AS_OF",
    "RESEARCH_CONTEXT_NOTICE_START",
    "RESEARCH_CONTEXT_NOTICE_END",
    "RESEARCH_CONTEXT_UNIVERSES",
}


def launch_workbench_task(
    root: str | Path,
    task_id: str,
    env_overrides: dict[str, str] | None = None,
) -> WorkbenchTaskRecord:
    root_path = Path(root)
    task = WORKBENCH_TASKS[task_id]
    run_dir = _new_run_dir(root_path, task_id)
    run_dir.mkdir(parents=True, exist_ok=False)
    manifest_path = task_manifest_path(run_dir)
    log_path = run_dir / "task.log"
    _write_manifest(
        manifest_path,
        {
            "task_id": task.task_id,
            "label": task.label,
            "command": list(task.command),
            "status": "queued",
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "started_at": "",
            "finished_at": "",
            "returncode": None,
            "env_overrides": _sanitize_env_overrides(env_overrides),
            "log_path": str(log_path),
        },
    )
    runner = root_path / "scripts/run_workbench_task.py"
    subprocess.Popen(
        [
            sys.executable,
            str(runner),
            "--task-id",
            task_id,
            "--run-dir",
            str(run_dir),
            "--root",
            str(root_path),
        ],
        cwd=root_path,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return WorkbenchTaskRecord(task_id=task_id, run_dir=run_dir, manifest_path=manifest_path, log_path=log_path)


def rerun_workbench_task(root: str | Path, run_dir: str | Path) -> WorkbenchTaskRecord:
    manifest = _read_manifest(task_manifest_path(Path(run_dir)))
    task_id = str(manifest.get("task_id", ""))
    if task_id not in WORKBENCH_TASKS:
        raise KeyError(task_id)
    return launch_workbench_task(root, task_id, env_overrides=manifest.get("env_overrides", {}))


def run_workbench_task(root: str | Path, task_id: str, run_dir: str | Path) -> int:
    root_path = Path(root)
    task = WORKBENCH_TASKS[task_id]
    run_path = Path(run_dir)
    manifest_path = task_manifest_path(run_path)
    log_path = run_path / "task.log"
    manifest = _read_manifest(manifest_path)
    manifest.update({"status": "running", "started_at": datetime.now().isoformat(timespec="seconds")})
    _write_manifest(manifest_path, manifest)
    process_env = os.environ.copy()
    process_env.update(_sanitize_env_overrides(manifest.get("env_overrides", {})))
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.run(task.command, cwd=root_path, stdout=log, stderr=subprocess.STDOUT, text=True, env=process_env)
    manifest.update(
        {
            "status": "succeeded" if process.returncode == 0 else "failed",
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "returncode": process.returncode,
        }
    )
    _write_manifest(manifest_path, manifest)
    return int(process.returncode)


def latest_workbench_task_runs(root: str | Path, limit: int = 8) -> list[dict[str, Any]]:
    task_root = Path(root) / "runs/workbench_tasks"
    if not task_root.exists():
        return []
    manifests = sorted(task_root.glob("*/manifest.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    rows = []
    for path in manifests[:limit]:
        data = _read_manifest(path)
        data["run_dir"] = str(path.parent)
        data["log_tail"] = tail_workbench_task_log(path.parent)
        rows.append(data)
    return rows


def summarize_workbench_task_runs(rows: list[dict[str, Any]]) -> dict[str, int]:
    summary = {status: 0 for status in ["queued", "running", "succeeded", "failed"]}
    for row in rows:
        status = str(row.get("status", "") or "")
        if status in summary:
            summary[status] += 1
    return summary


def tail_workbench_task_log(run_dir: str | Path, lines: int = 8) -> str:
    log_path = Path(run_dir) / "task.log"
    if not log_path.exists():
        return ""
    content = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(content[-lines:])


def load_workbench_task_detail(run_dir: str | Path) -> dict[str, Any]:
    run_path = Path(run_dir)
    log_path = run_path / "task.log"
    log = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
    return {
        "run_dir": str(run_path),
        "manifest": _read_manifest(task_manifest_path(run_path)),
        "log": log,
        "log_line_count": len(log.splitlines()),
    }


def task_manifest_path(run_dir: str | Path) -> Path:
    return Path(run_dir) / "manifest.json"


def _new_run_dir(root: Path, task_id: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return root / "runs/workbench_tasks" / f"{timestamp}_{task_id}"


def _read_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _write_manifest(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sanitize_env_overrides(env_overrides: dict[str, Any] | None) -> dict[str, str]:
    if not env_overrides:
        return {}
    sanitized: dict[str, str] = {}
    for key, value in env_overrides.items():
        if key in ALLOWED_TASK_ENV_OVERRIDES and value is not None:
            sanitized[key] = str(value)
    return sanitized
