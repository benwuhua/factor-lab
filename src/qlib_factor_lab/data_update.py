from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DailyDataUpdateConfig:
    project_root: Path
    as_of_date: str
    market_data_provider: str = "tushare"
    skip_market_data: bool = False
    skip_research_context: bool = False
    fetch_fundamentals: bool = False
    fundamental_provider: str = "tushare"
    derive_valuation_fields: bool = False
    fetch_cninfo_dividends: bool = False
    fundamental_source: Path | None = None
    limit: int | None = None
    offset: int = 0
    delay: float = 0.2


@dataclass(frozen=True)
class DataUpdateStep:
    name: str
    command: tuple[str, ...]


def build_daily_data_update_plan(config: DailyDataUpdateConfig) -> list[DataUpdateStep]:
    root = Path(config.project_root).expanduser().resolve()
    python_bin = sys.executable
    yyyymmdd = _yyyymmdd(config.as_of_date)
    steps: list[DataUpdateStep] = []

    if not config.skip_market_data:
        market_provider = str(config.market_data_provider or "akshare").strip().lower()
        if market_provider not in {"akshare", "tushare"}:
            raise ValueError(f"unsupported market_data_provider: {config.market_data_provider}")
        market_script = "scripts/build_tushare_qlib_data.py" if market_provider == "tushare" else "scripts/build_akshare_qlib_data.py"
        market_source_root = "data/tushare" if market_provider == "tushare" else "data/akshare"
        steps.extend(
            [
                DataUpdateStep(
                    "market_data_csi500",
                    (
                        python_bin,
                        market_script,
                        "--universe",
                        "csi500",
                        "--end",
                        yyyymmdd,
                        "--source-dir",
                        f"{market_source_root}/source_csi500",
                        "--qlib-dir",
                        "data/qlib/cn_data_current",
                        "--provider-config",
                        "configs/provider_current.yaml",
                    )
                    + _limit_args(config.limit)
                    + ("--delay", str(config.delay)),
                ),
                DataUpdateStep(
                    "market_data_csi300",
                    (
                        python_bin,
                        market_script,
                        "--universe",
                        "csi300",
                        "--end",
                        yyyymmdd,
                        "--source-dir",
                        f"{market_source_root}/source_csi300",
                        "--qlib-dir",
                        "data/qlib/cn_data_csi300_current",
                        "--provider-config",
                        "configs/provider_csi300_current.yaml",
                    )
                    + _limit_args(config.limit)
                    + ("--delay", str(config.delay)),
                ),
            ]
        )

    if not config.skip_research_context:
        steps.append(
            DataUpdateStep(
                "research_context",
                (
                    python_bin,
                    "scripts/build_research_context_data.py",
                    "--as-of-date",
                    config.as_of_date,
                    "--notice-start",
                    config.as_of_date,
                    "--notice-end",
                    config.as_of_date,
                    "--universes",
                    "csi300",
                    "csi500",
                )
                + _limit_args(config.limit)
                + ("--delay", str(config.delay)),
            )
        )

    research_domain_command = (
        python_bin,
        "scripts/build_research_data_domains.py",
        "--as-of-date",
        config.as_of_date,
    )
    if config.fetch_fundamentals:
        research_domain_command += (
            "--fetch-fundamentals",
            "--fundamental-provider",
            str(config.fundamental_provider),
            "--delay",
            str(config.delay),
        )
        research_domain_command += _limit_args(config.limit)
    if config.derive_valuation_fields:
        research_domain_command += ("--derive-valuation-fields",)
    if config.fetch_cninfo_dividends:
        research_domain_command += ("--fetch-cninfo-dividends",)
    if config.fetch_fundamentals or config.fetch_cninfo_dividends:
        research_domain_command += _offset_args(config.offset)
    if config.fundamental_source is not None:
        research_domain_command += ("--fundamental-source", str(config.fundamental_source))
    steps.append(DataUpdateStep("research_data_domains", research_domain_command))

    steps.append(
        DataUpdateStep(
            "data_governance",
            (
                python_bin,
                "scripts/check_data_governance.py",
                "--config",
                "configs/data_governance.yaml",
                "--as-of-date",
                config.as_of_date,
                "--output",
                f"reports/data_governance_{yyyymmdd}.md",
            ),
        )
    )
    return steps


def run_daily_data_update(config: DailyDataUpdateConfig, *, dry_run: bool = False) -> list[tuple[DataUpdateStep, str, int, str]]:
    rows: list[tuple[DataUpdateStep, str, int, str]] = []
    for step in build_daily_data_update_plan(config):
        if dry_run:
            rows.append((step, "dry_run", 0, ""))
            continue
        completed = subprocess.run(
            step.command,
            cwd=Path(config.project_root).expanduser().resolve(),
            text=True,
            capture_output=True,
            check=False,
        )
        output = "\n".join(part for part in [completed.stdout.strip(), completed.stderr.strip()] if part)
        status = "pass" if completed.returncode == 0 else "fail"
        rows.append((step, status, completed.returncode, output[-4000:]))
        if completed.returncode != 0:
            break
    return rows


def write_update_manifest(
    output_path: str | Path,
    *,
    as_of_date: str,
    rows: list[tuple[DataUpdateStep, str, int, str]],
) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Daily Data Update",
        "",
        f"- as_of_date: {as_of_date}",
        f"- status: {'pass' if rows and all(row[1] in {'pass', 'dry_run'} for row in rows) else 'fail'}",
        "",
        "| step | status | return_code | command | detail |",
        "|---|---|---:|---|---|",
    ]
    for step, status, return_code, detail in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    step.name,
                    status,
                    str(return_code),
                    " ".join(step.command).replace("|", "\\|"),
                    _table_cell(detail),
                ]
            )
            + " |"
        )
    output.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return output


def _limit_args(limit: int | None) -> tuple[str, ...]:
    if limit is None:
        return ()
    return ("--limit", str(limit))


def _offset_args(offset: int) -> tuple[str, ...]:
    if int(offset) <= 0:
        return ()
    return ("--offset", str(int(offset)))


def _yyyymmdd(value: str) -> str:
    text = str(value).replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"date must be YYYYMMDD or YYYY-MM-DD: {value}")
    return text


def _table_cell(value: str) -> str:
    cleaned = " ".join(str(value or "").split())
    if len(cleaned) > 240:
        cleaned = cleaned[:237] + "..."
    return cleaned.replace("|", "\\|")
