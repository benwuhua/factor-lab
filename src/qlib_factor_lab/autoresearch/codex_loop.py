from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from qlib_factor_lab.config import load_yaml


DEFAULT_ALLOWED_FAMILIES = ["reversal", "volatility", "turnover", "price_position", "divergence"]


@dataclass(frozen=True)
class LoopResult:
    iterations_started: int
    crash_count: int
    log_dir: Path
    stop_reason: str


def parse_until_deadline(value: str | None, now: datetime | None = None, timezone: str = "Asia/Shanghai") -> datetime | None:
    if not value:
        return None
    tz = ZoneInfo(timezone)
    current = now.astimezone(tz) if now is not None else datetime.now(tz)
    stripped = value.strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(stripped, fmt).replace(tzinfo=tz)
        except ValueError:
            pass
    try:
        hour, minute = _parse_hhmm(stripped)
    except ValueError as exc:
        raise ValueError("until must be HH:MM or YYYY-MM-DD HH:MM") from exc
    deadline = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if deadline <= current:
        deadline += timedelta(days=1)
    return deadline


def resolve_max_iterations(
    max_iterations: int | None,
    has_deadline: bool,
    max_hours: float | None,
    safe_default: int = 30,
) -> int | None:
    if max_iterations is not None:
        return None if max_iterations <= 0 else max_iterations
    if has_deadline or max_hours is not None:
        return None
    return safe_default


def build_codex_command(
    root: Path,
    prompt: str,
    model: str,
    sandbox: str = "workspace-write",
    full_auto: bool = False,
) -> list[str]:
    command = ["codex", "exec", "-C", str(root), "--sandbox", sandbox, "-m", model]
    if full_auto:
        command.append("--full-auto")
    command.append(prompt)
    return command


def build_candidate_prompt(
    iteration: int,
    candidate_file: str,
    ledger_text: str,
    allowed_families: Iterable[str] = DEFAULT_ALLOWED_FAMILIES,
    target_family: str | None = None,
) -> str:
    families = ", ".join(allowed_families)
    ledger_block = ledger_text.strip() or "暂无 ledger 记录。"
    family_rule = (
        f"- 本轮强制家族：{target_family}\n"
        f"- 候选 YAML 的 family 字段必须严格写成：family: {target_family}\n"
        "- 不要把同一结构只换 open/high/low/close 或窗口数字当作新类型\n"
        if target_family
        else ""
    )
    return f"""你现在做第 {iteration} 轮 expression autoresearch。

严格遵守 configs/autoresearch/program_expression.md。

本轮 Codex 只负责提出并写入一个候选因子 YAML。外层 runner 会负责 git commit、make autoresearch-expression 和 make autoresearch-ledger。

只允许修改：
{candidate_file}

不要修改：
src/
scripts/
configs/autoresearch/contracts/
configs/provider_current.yaml
Makefile
README.md
docs/

不要运行 make autoresearch-expression。
不要运行 make autoresearch-ledger。
不要 git commit。

候选要求：
- 每轮只试一个因子
- 低复杂度、可解释
- 优先家族：{families}
{family_rule}- 尽量探索和最近 5 轮不同的结构，除非 ledger 显示该方向明显领先
- 避免重复 ledger 里已经 discard_candidate 或 crash 的思路
- 表达式必须符合 configs/autoresearch/expression_space.yaml

当前 ledger 摘要：
```text
{ledger_block}
```

请只完成一件事：更新 {candidate_file}。
"""


def find_disallowed_changes(
    changed_files: Iterable[str],
    candidate_file: str,
    allowed_prefixes: Iterable[str] = ("reports/autoresearch/",),
) -> list[str]:
    allowed = {candidate_file}
    prefixes = tuple(allowed_prefixes)
    disallowed = []
    for file_path in changed_files:
        if file_path in allowed:
            continue
        if prefixes and file_path.startswith(prefixes):
            continue
        disallowed.append(file_path)
    return disallowed


def select_target_family(iteration: int, allowed_families: Iterable[str]) -> str:
    families = [family for family in allowed_families if str(family).strip()]
    if not families:
        raise ValueError("allowed_families must contain at least one family")
    return families[(iteration - 1) % len(families)]


def validate_candidate_family(candidate_path: str | Path, expected_family: str) -> str:
    try:
        candidate = load_yaml(candidate_path)
    except Exception as exc:
        return f"failed to read candidate YAML: {exc}"
    actual = str(candidate.get("family", "")).strip()
    expected = str(expected_family).strip()
    if actual != expected:
        return f"expected family {expected}, got {actual or '<missing>'}"
    return ""


def is_protected_branch(branch: str) -> bool:
    return branch in {"main", "master"}


def run_codex_autoloop(
    root: Path,
    max_iterations: int | None,
    max_crashes: int,
    deadline: datetime | None,
    max_hours: float | None,
    sleep_sec: float,
    model: str,
    sandbox: str,
    candidate_file: str,
    ledger_path: str,
    allowed_families: list[str],
    full_auto: bool = False,
    allow_protected_branch: bool = False,
) -> LoopResult:
    started = datetime.now(deadline.tzinfo if deadline else ZoneInfo("Asia/Shanghai"))
    hour_deadline = started + timedelta(hours=max_hours) if max_hours is not None else None
    final_deadline = _min_datetime(deadline, hour_deadline)
    log_dir = root / "reports/autoresearch/codex_loop" / started.strftime("%Y%m%dT%H%M%S")
    log_dir.mkdir(parents=True, exist_ok=True)
    _ensure_safe_branch(root, allow_protected_branch)
    _ensure_codex_logged_in(root, log_dir)

    iterations_started = 0
    crash_count = 0
    stop_reason = "max_iterations"
    iteration = 0
    while True:
        if final_deadline is not None and datetime.now(final_deadline.tzinfo) >= final_deadline:
            stop_reason = "deadline"
            break
        if max_iterations is not None and iteration >= max_iterations:
            stop_reason = "max_iterations"
            break
        _assert_clean_start(root)
        iteration += 1
        iterations_started += 1
        iteration_dir = log_dir / f"iteration_{iteration:03d}"
        iteration_dir.mkdir(parents=True, exist_ok=True)
        ledger_text = _ledger_context(root / ledger_path)
        target_family = select_target_family(iteration, allowed_families)
        prompt = build_candidate_prompt(iteration, candidate_file, ledger_text, allowed_families, target_family=target_family)
        (iteration_dir / "prompt.txt").write_text(prompt, encoding="utf-8")

        codex_result = _run_capture(
            build_codex_command(root, prompt, model=model, sandbox=sandbox, full_auto=full_auto),
            root,
            iteration_dir / "codex_stdout.txt",
            iteration_dir / "codex_stderr.txt",
        )
        if codex_result.returncode != 0:
            crash_count += 1
            if crash_count >= max_crashes:
                stop_reason = "max_crashes"
                break
            time.sleep(sleep_sec)
            continue

        changed_files = _git_changed_files(root)
        disallowed = find_disallowed_changes(changed_files, candidate_file)
        if disallowed:
            (iteration_dir / "disallowed_changes.txt").write_text("\n".join(disallowed) + "\n", encoding="utf-8")
            stop_reason = "disallowed_changes"
            break
        if candidate_file not in changed_files:
            (iteration_dir / "no_candidate_change.txt").write_text("Codex did not update the candidate file.\n", encoding="utf-8")
            stop_reason = "no_candidate_change"
            break
        family_error = validate_candidate_family(root / candidate_file, target_family)
        if family_error:
            (iteration_dir / "candidate_family_mismatch.txt").write_text(family_error + "\n", encoding="utf-8")
            _run_capture(
                ["git", "restore", "--", candidate_file],
                root,
                iteration_dir / "candidate_restore_stdout.txt",
                iteration_dir / "candidate_restore_stderr.txt",
            )
            crash_count += 1
            if crash_count >= max_crashes:
                stop_reason = "max_crashes"
                break
            time.sleep(sleep_sec)
            continue

        _run_checked(["git", "add", candidate_file], root, iteration_dir / "git_add.txt")
        _run_checked(
            ["git", "commit", "-m", f"try autoresearch candidate iteration {iteration:03d}"],
            root,
            iteration_dir / "git_commit.txt",
        )
        oracle_result = _run_capture(
            ["make", "autoresearch-expression"],
            root,
            iteration_dir / "oracle_stdout.txt",
            iteration_dir / "oracle_stderr.txt",
        )
        if oracle_result.returncode != 0 or "status: crash" in (iteration_dir / "oracle_stdout.txt").read_text(encoding="utf-8"):
            crash_count += 1
        _run_capture(
            ["make", "autoresearch-ledger"],
            root,
            iteration_dir / "ledger_stdout.txt",
            iteration_dir / "ledger_stderr.txt",
        )
        if crash_count >= max_crashes:
            stop_reason = "max_crashes"
            break
        if sleep_sec > 0:
            time.sleep(sleep_sec)

    (log_dir / "summary.txt").write_text(
        "\n".join(
            [
                f"iterations_started: {iterations_started}",
                f"crash_count: {crash_count}",
                f"stop_reason: {stop_reason}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return LoopResult(
        iterations_started=iterations_started,
        crash_count=crash_count,
        log_dir=log_dir,
        stop_reason=stop_reason,
    )


def _parse_hhmm(value: str) -> tuple[int, int]:
    parts = value.split(":")
    if len(parts) != 2:
        raise ValueError("not HH:MM")
    hour = int(parts[0])
    minute = int(parts[1])
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError("invalid HH:MM")
    return hour, minute


def _min_datetime(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _ensure_codex_logged_in(root: Path, log_dir: Path) -> None:
    result = _run_capture(["codex", "login", "status"], root, log_dir / "codex_login_stdout.txt", log_dir / "codex_login_stderr.txt")
    if result.returncode != 0:
        raise RuntimeError("codex login status failed; run `codex login --device-auth` first")


def _ensure_safe_branch(root: Path, allow_protected_branch: bool) -> None:
    result = subprocess.run(["git", "branch", "--show-current"], cwd=root, capture_output=True, text=True, check=True)
    branch = result.stdout.strip()
    if is_protected_branch(branch) and not allow_protected_branch:
        raise RuntimeError(
            f"refusing to run Codex autoloop on protected branch {branch}; "
            "create an experiment branch or pass --allow-protected-branch"
        )


def _assert_clean_start(root: Path) -> None:
    changed = _git_changed_files(root)
    if changed:
        raise RuntimeError(f"working tree must be clean before each iteration: {changed}")


def _git_changed_files(root: Path) -> list[str]:
    result = subprocess.run(["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True, check=True)
    files: list[str] = []
    for line in result.stdout.splitlines():
        if not line:
            continue
        files.append(line[3:].strip())
    return files


def _ledger_context(path: Path, max_lines: int = 80) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return ""
    header, rows = lines[0], lines[1:]
    if not rows:
        return header
    return "\n".join([header, *rows[-max_lines:]])


def _run_capture(command: list[str], cwd: Path, stdout_path: Path, stderr_path: Path) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    stdout_path.write_text(result.stdout, encoding="utf-8")
    stderr_path.write_text(result.stderr, encoding="utf-8")
    return result


def _run_checked(command: list[str], cwd: Path, output_path: Path) -> None:
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=True)
    output_path.write_text(result.stdout + result.stderr, encoding="utf-8")
