from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
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
    "daily-signal": WorkbenchTask("daily-signal", "生成当日信号", ("make", "daily-signal"), "从 approved 因子生成当日解释型信号。"),
    "check-data-quality": WorkbenchTask("check-data-quality", "检查信号质量", ("make", "check-data-quality"), "对当日信号覆盖率和缺失情况做质量门禁。"),
    "select-factors": WorkbenchTask("select-factors", "生成 approved 因子", ("make", "select-factors"), "根据治理配置刷新 approved_factors.yaml。"),
    "autoresearch-codex-loop": WorkbenchTask("autoresearch-codex-loop", "启动自动挖掘", ("make", "autoresearch-codex-loop"), "启动受控候选表达式研究循环。"),
    "autoresearch-review": WorkbenchTask("autoresearch-review", "复核自动挖掘", ("make", "autoresearch-review"), "汇总 nightly ledger、稳定性和重复簇。"),
    "target-portfolio": WorkbenchTask("target-portfolio", "生成目标组合", ("make", "target-portfolio"), "把当日信号转换成 target_portfolio。"),
    "exposure-attribution": WorkbenchTask("exposure-attribution", "暴露归因", ("make", "exposure-attribution"), "解释行业、因子族和风格暴露。"),
    "paper-orders": WorkbenchTask("paper-orders", "生成纸面订单", ("make", "paper-orders"), "把目标组合转换为纸面订单和模拟成交。"),
    "reconcile-account": WorkbenchTask("reconcile-account", "账户对账", ("make", "reconcile-account"), "对 expected 和 actual positions 做对账。"),
    "paper-batch": WorkbenchTask("paper-batch", "滚动纸面批测", ("make", "paper-batch"), "对历史 target portfolio 做滚动纸面执行复盘。"),
}


def launch_workbench_task(root: str | Path, task_id: str) -> WorkbenchTaskRecord:
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


def run_workbench_task(root: str | Path, task_id: str, run_dir: str | Path) -> int:
    root_path = Path(root)
    task = WORKBENCH_TASKS[task_id]
    run_path = Path(run_dir)
    manifest_path = task_manifest_path(run_path)
    log_path = run_path / "task.log"
    manifest = _read_manifest(manifest_path)
    manifest.update({"status": "running", "started_at": datetime.now().isoformat(timespec="seconds")})
    _write_manifest(manifest_path, manifest)
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.run(task.command, cwd=root_path, stdout=log, stderr=subprocess.STDOUT, text=True)
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


def task_manifest_path(run_dir: str | Path) -> Path:
    return Path(run_dir) / "manifest.json"


def _new_run_dir(root: Path, task_id: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return root / "runs/workbench_tasks" / f"{timestamp}_{task_id}"


def _read_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _write_manifest(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
