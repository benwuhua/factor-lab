from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from .config import ProjectConfig


def _date(value: str) -> str:
    return str(value)


def _cap_to_available_calendar(config: ProjectConfig, buffer_sessions: int = 1) -> str:
    calendar_path = config.provider_uri / "calendars" / "day.txt"
    if not calendar_path.exists():
        return _date(config.end_time)
    dates = [line.strip() for line in calendar_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not dates:
        return _date(config.end_time)
    requested = pd.Timestamp(config.end_time)
    available = [date for date in dates if pd.Timestamp(date) <= requested]
    if not available:
        return _date(config.end_time)
    index = max(0, len(available) - 1 - buffer_sessions)
    return available[index]


def _choose_segments(config: ProjectConfig, test_end: str) -> tuple[str, str, str, str, str, str]:
    train_start = _date(config.start_time)
    if pd.Timestamp(config.end_time) >= pd.Timestamp("2024-01-01"):
        return train_start, "2021-12-31", "2022-01-01", "2023-12-31", "2024-01-01", test_end
    return train_start, "2016-12-31", "2017-01-01", "2018-12-31", "2019-01-01", test_end


def _read_instrument_symbols(path: Path) -> list[str]:
    if not path.exists():
        return []
    symbols: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.strip().split()
        if parts:
            symbols.append(parts[0])
    return symbols


def _benchmark_for_backtest(config: ProjectConfig) -> str | list[str] | None:
    code = str(config.benchmark).lower()
    feature_dir = config.provider_uri / "features" / code
    if (feature_dir / "close.day.bin").exists():
        return config.benchmark

    instrument_dir = config.provider_uri / "instruments"
    symbols = _read_instrument_symbols(instrument_dir / f"{config.market}.txt")
    if symbols:
        return symbols
    symbols = _read_instrument_symbols(instrument_dir / "all.txt")
    return symbols or None


def render_lgb_workflow_config(config: ProjectConfig, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    test_end = _cap_to_available_calendar(config)
    train_start, train_end, valid_start, valid_end, test_start, test_end = _choose_segments(config, test_end)
    backtest_benchmark = _benchmark_for_backtest(config)
    data_handler_config = {
        "start_time": train_start,
        "end_time": test_end,
        "fit_start_time": train_start,
        "fit_end_time": train_end,
        "instruments": config.market,
    }
    workflow = {
        "qlib_init": {
            "provider_uri": str(config.provider_uri),
            "region": config.region,
        },
        "market": config.market,
        "benchmark": config.benchmark,
        "data_handler_config": data_handler_config,
        "port_analysis_config": {
            "strategy": {
                "class": "TopkDropoutStrategy",
                "module_path": "qlib.contrib.strategy",
                "kwargs": {"signal": "<PRED>", "topk": 50, "n_drop": 5},
            },
            "backtest": {
                "start_time": test_start,
                "end_time": test_end,
                "account": 100000000,
                "benchmark": backtest_benchmark,
                "exchange_kwargs": {
                    "limit_threshold": 0.095,
                    "deal_price": "close",
                    "open_cost": 0.0005,
                    "close_cost": 0.0015,
                    "min_cost": 5,
                },
            },
        },
        "task": {
            "model": {
                "class": "LGBModel",
                "module_path": "qlib.contrib.model.gbdt",
                "kwargs": {
                    "loss": "mse",
                    "colsample_bytree": 0.9,
                    "learning_rate": 0.1,
                    "subsample": 0.9,
                    "lambda_l1": 205.6999,
                    "lambda_l2": 580.9768,
                    "max_depth": 8,
                    "num_leaves": 210,
                    "num_threads": 4,
                },
            },
            "dataset": {
                "class": "DatasetH",
                "module_path": "qlib.data.dataset",
                "kwargs": {
                    "handler": {
                        "class": "Alpha158",
                        "module_path": "qlib.contrib.data.handler",
                        "kwargs": data_handler_config,
                    },
                    "segments": {
                        "train": [train_start, train_end],
                        "valid": [valid_start, valid_end],
                        "test": [test_start, test_end],
                    },
                },
            },
            "record": [
                {
                    "class": "SignalRecord",
                    "module_path": "qlib.workflow.record_temp",
                    "kwargs": {"model": "<MODEL>", "dataset": "<DATASET>"},
                },
                {
                    "class": "SigAnaRecord",
                    "module_path": "qlib.workflow.record_temp",
                    "kwargs": {"ana_long_short": False, "ann_scaler": 252},
                },
                {
                    "class": "PortAnaRecord",
                    "module_path": "qlib.workflow.record_temp",
                    "kwargs": {"config": "<PORT_ANALYSIS_CONFIG>"},
                },
            ],
        },
    }
    workflow["task"]["record"][2]["kwargs"]["config"] = workflow["port_analysis_config"]
    output.write_text(yaml.safe_dump(workflow, sort_keys=False), encoding="utf-8")
    return output


def build_qrun_command(config_path: str | Path, qrun_bin: str = "qrun") -> list[str]:
    return [qrun_bin, str(config_path)]
