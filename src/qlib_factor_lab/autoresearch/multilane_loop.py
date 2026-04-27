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


DEFAULT_ROTATION_FACTOR_NAMES: dict[str, list[str]] = {
    "pattern_event": ["wangji-factor1", "wangji-reversal20-combo", "quiet_breakout_20", "quiet_breakout_60"],
    "emotion_atmosphere": [
        "arbr_26",
        "breadth_proxy_20",
        "davol_5",
        "davol_10",
        "davol_20",
        "heat_cooling_5_20",
        "limit_pressure_5",
        "turnover_mean_5",
        "turnover_mean_20",
    ],
    "liquidity_microstructure": [
        "amount_mean_5",
        "amount_mean_20",
        "amount_mean_60",
        "amihud_illiq_10",
        "amihud_illiq_20",
        "amihud_illiq_60",
        "turnover_mean_5",
        "turnover_mean_20",
        "turnover_mean_60",
        "turnover_volatility_20",
        "vosc_12_26",
    ],
    "risk_structure": [
        "max_drawdown_20",
        "max_drawdown_60",
        "downside_vol_20",
        "downside_vol_60",
        "gap_risk_20",
        "intraday_excursion_20",
    ],
}


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
    expression_candidate_glob: str | None = "configs/autoresearch/candidates/*.yaml",
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
    lane_factor_batch_size: int = 2,
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
    expression_candidates = _expression_candidate_paths(root, expression_candidate_path, expression_candidate_glob)
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
        expression_candidate_for_iteration = _rotate_items(expression_candidates, iteration, 1)[0]
        lane_factor_overrides = _lane_factor_name_overrides(iteration, lane_factor_batch_size)
        rotation = {
            "expression_candidate": Path(expression_candidate_for_iteration).name,
            "lane_factor_name_overrides": lane_factor_overrides,
        }
        iteration_row: dict[str, Any] = {
            "iteration": iteration,
            "started_at": iteration_started_at.isoformat(timespec="seconds"),
            "output_path": str(iteration_output),
            "status": "completed",
            "lane_crashes": 0,
            "lanes": [],
            "rotation": rotation,
        }

        try:
            report = runner(
                lane_space_path=lane_space_path,
                project_root=root,
                contract_path=contract_path,
                expression_space_path=expression_space_path,
                expression_candidate_path=expression_candidate_for_iteration,
                mining_config_path=mining_config_path,
                provider_config_path=provider_config_path,
                output_path=iteration_output,
                data_governance_report_path=data_governance_report_path,
                include_shadow=include_shadow,
                max_workers=max_workers,
                start_time=start_time,
                end_time=end_time,
                lane_factor_name_overrides=lane_factor_overrides,
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


def _expression_candidate_paths(root: Path, fallback: str | Path, glob_pattern: str | None) -> list[str]:
    fallback_path = _resolve(root, fallback)
    candidates: list[Path] = []
    if glob_pattern:
        candidates = sorted(_resolve(root, glob_pattern).parent.glob(Path(glob_pattern).name))
    if fallback_path not in candidates:
        candidates.insert(0, fallback_path)
    existing = [path for path in candidates if path.exists()]
    paths = existing or [fallback_path]
    return [str(path) for path in paths]


def _lane_factor_name_overrides(iteration: int, batch_size: int) -> dict[str, list[str]]:
    size = max(1, int(batch_size))
    return {
        lane: _rotate_items(names, iteration, size)
        for lane, names in DEFAULT_ROTATION_FACTOR_NAMES.items()
        if names
    }


def _rotate_items(items: list[Any], iteration: int, batch_size: int) -> list[Any]:
    if not items:
        raise ValueError("cannot rotate an empty item list")
    size = max(1, int(batch_size))
    start = ((max(1, int(iteration)) - 1) * size) % len(items)
    return [items[(start + offset) % len(items)] for offset in range(min(size, len(items)))]


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
