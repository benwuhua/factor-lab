from __future__ import annotations

import json
import math
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from qlib_factor_lab.autoresearch.codex_loop import parse_until_deadline, resolve_max_iterations
from qlib_factor_lab.autoresearch.multilane import MultiLaneReport, run_multilane_autoresearch


@dataclass(frozen=True)
class MultiLaneLoopResult:
    iterations_started: int
    crash_count: int
    log_dir: Path
    stop_reason: str


MultiLaneRunner = Callable[..., MultiLaneReport]


def run_multilane_loop(
    *,
    project_root: str | Path,
    lane_space_path: str | Path,
    contract_path: str | Path,
    expression_space_path: str | Path,
    expression_candidate_path: str | Path,
    mining_config_path: str | Path,
    provider_config_path: str | Path,
    output_root: str | Path = "reports/autoresearch/multilane_loop",
    data_governance_report_path: str | Path | None = None,
    include_shadow: bool = False,
    max_workers: int = 4,
    start_time: str | None = None,
    end_time: str | None = None,
    deadline: datetime | None = None,
    max_hours: float | None = None,
    max_iterations: int | None = None,
    max_crashes: int = 5,
    sleep_sec: float = 60.0,
    runner: MultiLaneRunner = run_multilane_autoresearch,
) -> MultiLaneLoopResult:
    root = Path(project_root)
    tzinfo = deadline.tzinfo if deadline is not None else ZoneInfo("Asia/Shanghai")
    started = datetime.now(tzinfo)
    final_deadline = _min_datetime(deadline, started + timedelta(hours=max_hours) if max_hours is not None else None)
    output_base = _resolve(root, output_root)
    log_dir = output_base / started.strftime("%Y%m%dT%H%M%S")
    log_dir.mkdir(parents=True, exist_ok=True)
    output_base.mkdir(parents=True, exist_ok=True)
    (output_base / "latest_log_dir.txt").write_text(str(log_dir) + "\n", encoding="utf-8")

    iterations: list[dict[str, Any]] = []
    iterations_started = 0
    crash_count = 0
    crash_budget = max(1, int(max_crashes))
    stop_reason = "max_iterations"
    iteration = 0
    _write_loop_summary(
        log_dir,
        iterations=iterations,
        iterations_started=iterations_started,
        crash_count=crash_count,
        stop_reason="running",
    )

    while True:
        if final_deadline is not None and datetime.now(final_deadline.tzinfo) >= final_deadline:
            stop_reason = "deadline"
            break
        if max_iterations is not None and iteration >= max_iterations:
            stop_reason = "max_iterations"
            break

        iteration += 1
        iterations_started += 1
        iteration_output = log_dir / f"multilane_iteration_{iteration:03d}.md"
        iteration_started_at = datetime.now(tzinfo)
        iteration_row: dict[str, Any] = {
            "iteration": iteration,
            "started_at": iteration_started_at.isoformat(timespec="seconds"),
            "output_path": str(iteration_output),
            "status": "completed",
            "lane_crashes": 0,
            "lanes": [],
        }

        try:
            report = runner(
                lane_space_path=lane_space_path,
                project_root=root,
                contract_path=contract_path,
                expression_space_path=expression_space_path,
                expression_candidate_path=expression_candidate_path,
                mining_config_path=mining_config_path,
                provider_config_path=provider_config_path,
                output_path=iteration_output,
                data_governance_report_path=data_governance_report_path,
                include_shadow=include_shadow,
                max_workers=max_workers,
                start_time=start_time,
                end_time=end_time,
            )
            frame = report.to_frame()
            lanes = _json_safe(frame.to_dict(orient="records"))
            lane_crashes = int((frame["run_status"] == "crash").sum()) if "run_status" in frame else 0
            if lane_crashes:
                crash_count += lane_crashes
                iteration_row["status"] = "lane_crash"
            iteration_row["lane_crashes"] = lane_crashes
            iteration_row["lanes"] = lanes
        except Exception as exc:
            crash_count += 1
            iteration_row["status"] = "crash"
            iteration_row["error"] = str(exc)
            (log_dir / f"iteration_{iteration:03d}_error.txt").write_text(
                "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
                encoding="utf-8",
            )

        iteration_row["finished_at"] = datetime.now(tzinfo).isoformat(timespec="seconds")
        iteration_row["crash_count_after_iteration"] = crash_count
        iterations.append(iteration_row)
        _write_loop_summary(
            log_dir,
            iterations=iterations,
            iterations_started=iterations_started,
            crash_count=crash_count,
            stop_reason="running",
        )

        if crash_count >= crash_budget:
            stop_reason = "max_crashes"
            break
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    _write_loop_summary(
        log_dir,
        iterations=iterations,
        iterations_started=iterations_started,
        crash_count=crash_count,
        stop_reason=stop_reason,
    )
    return MultiLaneLoopResult(
        iterations_started=iterations_started,
        crash_count=crash_count,
        log_dir=log_dir,
        stop_reason=stop_reason,
    )


def parse_multilane_deadline(value: str | None, timezone: str = "Asia/Shanghai") -> datetime | None:
    return parse_until_deadline(value, timezone=timezone)


def resolve_multilane_max_iterations(
    max_iterations: int | None,
    *,
    has_deadline: bool,
    max_hours: float | None,
) -> int | None:
    return resolve_max_iterations(
        max_iterations=max_iterations,
        has_deadline=has_deadline,
        max_hours=max_hours,
        safe_default=1,
    )


def _write_loop_summary(
    log_dir: Path,
    *,
    iterations: list[dict[str, Any]],
    iterations_started: int,
    crash_count: int,
    stop_reason: str,
) -> None:
    payload = {
        "iterations_started": iterations_started,
        "crash_count": crash_count,
        "stop_reason": stop_reason,
        "iterations": iterations,
    }
    (log_dir / "summary.json").write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        f"iterations_started: {iterations_started}",
        f"crash_count: {crash_count}",
        f"stop_reason: {stop_reason}",
        "",
        "| iteration | status | lane_crashes | output |",
        "|---:|---|---:|---|",
    ]
    for item in iterations:
        lines.append(
            f"| {item['iteration']} | {item['status']} | {item.get('lane_crashes', 0)} | {item.get('output_path', '')} |"
        )
    (log_dir / "summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _resolve(root: Path, path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else root / value


def _min_datetime(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)
