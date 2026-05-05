from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from os import environ
from pathlib import Path


@dataclass(frozen=True)
class DailyDataUpdateConfig:
    project_root: Path
    as_of_date: str
    market_data_provider: str = "tushare"
    force_market_start: str | None = None
    skip_market_data: bool = False
    skip_research_context: bool = False
    fetch_fundamentals: bool = False
    fundamental_provider: str = "tushare"
    derive_valuation_fields: bool = False
    fetch_cninfo_dividends: bool = False
    fundamental_source: Path | None = None
    security_master_history_source: Path | None = None
    env_file: Path | None = None
    rqdata_security_master_history: bool = False
    rqdata_start_date: str | None = None
    rqdata_output: Path = Path("data/vendor/security_master_history_rqdata.csv")
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
                    + _force_start_args(config.force_market_start)
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
                    + _force_start_args(config.force_market_start)
                    + _limit_args(config.limit)
                    + ("--delay", str(config.delay)),
                ),
            ]
        )

    steps.extend(
        [
            DataUpdateStep(
                "liquidity_microstructure_csi500",
                (
                    python_bin,
                    "scripts/build_liquidity_microstructure.py",
                    "--provider-config",
                    "configs/provider_current.yaml",
                    "--start-date",
                    config.as_of_date,
                    "--end-date",
                    config.as_of_date,
                    "--output",
                    "data/liquidity_microstructure.csv",
                    "--merge-existing",
                ),
            ),
            DataUpdateStep(
                "liquidity_microstructure_csi300",
                (
                    python_bin,
                    "scripts/build_liquidity_microstructure.py",
                    "--provider-config",
                    "configs/provider_csi300_current.yaml",
                    "--start-date",
                    config.as_of_date,
                    "--end-date",
                    config.as_of_date,
                    "--output",
                    "data/liquidity_microstructure.csv",
                    "--merge-existing",
                ),
            ),
            DataUpdateStep(
                "emotion_atmosphere",
                (
                    python_bin,
                    "scripts/build_emotion_atmosphere.py",
                    "--liquidity-csv",
                    "data/liquidity_microstructure.csv",
                    "--start-date",
                    config.as_of_date,
                    "--end-date",
                    config.as_of_date,
                    "--output",
                    "data/emotion_atmosphere.csv",
                    "--merge-existing",
                ),
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
                    "--merge-existing-events",
                )
                + _limit_args(config.limit)
                + ("--delay", str(config.delay)),
            )
        )

    security_master_history_source = config.security_master_history_source
    if config.rqdata_security_master_history:
        security_master_history_source = config.rqdata_output
        steps.append(
            DataUpdateStep(
                "rqdata_security_master_history",
                (
                    python_bin,
                    "scripts/build_rqdata_vendor_data.py",
                    "--instruments",
                )
                + _security_master_instruments_args(root)
                + (
                    "--start-date",
                    str(config.rqdata_start_date or "2015-01-01"),
                    "--end-date",
                    config.as_of_date,
                    "--as-of-date",
                    config.as_of_date,
                    "--env-file",
                    str(config.env_file or ".env"),
                    "--output",
                    str(config.rqdata_output),
                ),
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
    if security_master_history_source is not None:
        research_domain_command += ("--security-master-history-source", str(security_master_history_source))
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
    env = load_env_file(config.env_file, env=dict(environ)) if config.env_file is not None else None
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
            env=env,
        )
        output = "\n".join(part for part in [completed.stdout.strip(), completed.stderr.strip()] if part)
        status = "pass" if completed.returncode == 0 else "fail"
        rows.append((step, status, completed.returncode, output[-4000:]))
        if completed.returncode != 0:
            break
    return rows


def load_env_file(path: str | Path | None, *, env: dict[str, str] | None = None) -> dict[str, str]:
    loaded = dict(environ if env is None else env)
    if path is None:
        return loaded
    env_path = Path(path).expanduser()
    if not env_path.exists():
        return loaded
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in loaded:
            continue
        loaded[key] = _unquote_env_value(value.strip())
    return loaded


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


def _force_start_args(force_start: str | None) -> tuple[str, ...]:
    if not force_start:
        return ()
    return ("--force-start", str(force_start))


def _security_master_instruments_args(root: Path) -> tuple[str, ...]:
    path = root / "data/security_master.csv"
    if not path.exists():
        return ()
    try:
        import pandas as pd

        frame = pd.read_csv(path, usecols=lambda column: column == "instrument")
    except Exception:
        return ()
    if "instrument" not in frame.columns:
        return ()
    symbols = frame["instrument"].dropna().astype(str).str.upper().drop_duplicates().tolist()
    return tuple(symbols)


def _unquote_env_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


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
