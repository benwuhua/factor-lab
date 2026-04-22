from __future__ import annotations

import datetime as dt
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml


AKSHARE_COLUMNS = {
    "日期": "date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "振幅": "amplitude",
    "涨跌幅": "change",
    "涨跌额": "change_amount",
    "换手率": "turnover",
}


@dataclass(frozen=True)
class UniverseSpec:
    name: str
    benchmark: str
    symbols: list[str]


def today_for_daily_data(now: dt.date | None = None) -> str:
    today = now or dt.date.today()
    # Use the previous calendar day by default. It avoids mixing incomplete current-day bars.
    return (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")


def qlib_symbol_from_code(code: str) -> str:
    pure = "".join(ch for ch in str(code).strip() if ch.isdigit()).zfill(6)
    if len(pure) != 6 or not pure.isdigit():
        raise ValueError(f"invalid A-share code: {code}")
    if pure.startswith(("5", "6", "9")):
        return f"SH{pure}"
    return f"SZ{pure}"


def akshare_code_from_qlib(symbol: str) -> str:
    text = str(symbol).strip().upper()
    if text.startswith(("SH", "SZ")):
        text = text[2:]
    if len(text) != 6 or not text.isdigit():
        raise ValueError(f"invalid qlib symbol: {symbol}")
    return text


def prefixed_akshare_symbol(symbol: str) -> str:
    text = str(symbol).strip().upper()
    if text.startswith("SH"):
        return f"sh{text[2:]}"
    if text.startswith("SZ"):
        return f"sz{text[2:]}"
    return prefixed_akshare_symbol(qlib_symbol_from_code(text))


def normalize_akshare_history(raw: pd.DataFrame, code: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    volume_is_hands = "成交量" in raw.columns
    frame = raw.rename(columns=AKSHARE_COLUMNS).copy()
    required = ["date", "open", "close", "high", "low", "volume", "amount"]
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"AkShare history is missing columns: {missing}")

    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame["symbol"] = qlib_symbol_from_code(code)
    for col in ["open", "close", "high", "low", "volume", "amount", "change", "turnover"]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    if volume_is_hands:
        frame["volume"] = frame["volume"] * 100.0
    frame["vwap"] = frame["amount"] / frame["volume"].replace(0, pd.NA)
    frame["vwap"] = frame["vwap"].fillna(frame["close"])
    # AkShare qfq bars are already adjusted. Qlib's exchange layer expects a factor field, so use 1.0.
    frame["factor"] = 1.0
    columns = ["date", "symbol", "open", "close", "high", "low", "volume", "amount", "vwap", "factor"]
    optional = [col for col in ["change", "turnover"] if col in frame.columns]
    frame = frame[columns + optional].dropna(subset=["date", "open", "close", "high", "low", "volume"])
    return frame.sort_values("date").drop_duplicates("date")


def write_symbol_csv(frame: pd.DataFrame, output_dir: str | Path, symbol: str) -> Path:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    path = output / f"{symbol.lower()}.csv"
    frame.to_csv(path, index=False)
    return path


def build_dump_bin_command(
    dump_bin_path: str | Path,
    source_dir: str | Path,
    qlib_dir: str | Path,
    python_bin: str = sys.executable,
    max_workers: int = 4,
) -> list[str]:
    return [
        python_bin,
        str(dump_bin_path),
        "dump_all",
        "--data_path",
        str(source_dir),
        "--qlib_dir",
        str(qlib_dir),
        "--freq",
        "day",
        "--exclude_fields",
        "date,symbol",
        "--file_suffix",
        ".csv",
        "--max_workers",
        str(max_workers),
    ]


def write_provider_config(
    path: str | Path,
    qlib_dir: str | Path,
    market: str,
    benchmark: str,
    end_time: str,
    start_time: str = "2015-01-01",
) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "provider_uri": str(Path(qlib_dir).expanduser()),
        "region": "cn",
        "market": market,
        "benchmark": benchmark,
        "freq": "day",
        "start_time": start_time,
        "end_time": end_time,
    }
    output.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return output


def write_instrument_alias(qlib_dir: str | Path, alias: str) -> Path:
    qlib_path = Path(qlib_dir)
    source = qlib_path / "instruments" / "all.txt"
    target = qlib_path / "instruments" / f"{alias}.txt"
    if not source.exists():
        raise FileNotFoundError(f"missing Qlib all instrument file: {source}")
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    return target


def _read_old_instrument_symbols(path: Path) -> list[str]:
    if not path.exists():
        return []
    rows = pd.read_csv(path, sep="\t", header=None)
    if rows.empty:
        return []
    return sorted(set(rows.iloc[:, 0].astype(str).str.upper()))


def load_symbols_from_existing_qlib(qlib_dir: str | Path, market: str) -> list[str]:
    path = Path(qlib_dir) / "instruments" / f"{market}.txt"
    return _read_old_instrument_symbols(path)


def _extract_symbol_column(frame: pd.DataFrame) -> pd.Series:
    for col in ["品种代码", "代码", "成分券代码", "证券代码", "symbol"]:
        if col in frame.columns:
            return frame[col].astype(str)
    raise ValueError(f"cannot find symbol column in AkShare frame columns: {list(frame.columns)}")


def _get_akshare():
    try:
        import akshare as ak  # type: ignore
    except ImportError as exc:
        raise RuntimeError("akshare is not installed. Run `python -m pip install akshare`.") from exc
    return ak


def fetch_universe_symbols(universe: str, fallback_qlib_dir: str | Path | None = None) -> UniverseSpec:
    universe = universe.lower()
    benchmark = {"csi300": "SH000300", "csi500": "SH000905", "csi800": "SH000906", "all": "SH000985"}.get(
        universe,
        "SH000905",
    )
    try:
        ak = _get_akshare()
        if universe == "csi300":
            raw = ak.index_stock_cons_csindex(symbol="000300")
            symbols = [qlib_symbol_from_code(code) for code in _extract_symbol_column(raw)]
        elif universe == "csi500":
            raw = ak.index_stock_cons_csindex(symbol="000905")
            symbols = [qlib_symbol_from_code(code) for code in _extract_symbol_column(raw)]
        elif universe == "csi800":
            raw300 = ak.index_stock_cons_csindex(symbol="000300")
            raw500 = ak.index_stock_cons_csindex(symbol="000905")
            symbols = [qlib_symbol_from_code(code) for code in pd.concat([_extract_symbol_column(raw300), _extract_symbol_column(raw500)])]
        elif universe == "all":
            raw = ak.stock_info_a_code_name()
            symbols = [qlib_symbol_from_code(code) for code in _extract_symbol_column(raw)]
        else:
            raise ValueError(f"unsupported universe: {universe}")
        symbols = sorted(set(symbols))
        if symbols:
            return UniverseSpec(f"{universe}_current", benchmark, symbols)
    except Exception:
        if fallback_qlib_dir is None:
            raise

    if fallback_qlib_dir is None:
        raise RuntimeError(f"failed to fetch universe from AkShare: {universe}")
    fallback_market = "all" if universe == "all" else universe.replace("csi800", "csi500")
    symbols = load_symbols_from_existing_qlib(fallback_qlib_dir, fallback_market)
    if universe == "csi800":
        symbols = sorted(set(symbols) | set(load_symbols_from_existing_qlib(fallback_qlib_dir, "csi300")))
    if not symbols:
        raise RuntimeError(f"failed to fetch universe and no fallback symbols found for {universe}")
    return UniverseSpec(f"{universe}_current", benchmark, symbols)


def download_history_csvs(
    symbols: Iterable[str],
    output_dir: str | Path,
    start: str,
    end: str,
    adjust: str = "qfq",
    delay: float = 0.2,
    limit: int | None = None,
    retries: int = 3,
    source: str = "sina",
) -> list[Path]:
    ak = _get_akshare()
    output_paths: list[Path] = []
    selected = list(symbols)
    if limit is not None:
        selected = selected[:limit]
    for i, symbol in enumerate(selected, start=1):
        code = akshare_code_from_qlib(symbol)
        last_error: Exception | None = None
        raw = None
        for attempt in range(1, retries + 1):
            try:
                if source == "em":
                    raw = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end, adjust=adjust)
                elif source == "sina":
                    raw = ak.stock_zh_a_daily(
                        symbol=prefixed_akshare_symbol(symbol),
                        start_date=start,
                        end_date=end,
                        adjust=adjust,
                    )
                elif source == "tx":
                    raw = ak.stock_zh_a_hist_tx(
                        symbol=prefixed_akshare_symbol(symbol),
                        start_date=start,
                        end_date=end,
                        adjust=adjust,
                    )
                    if "amount" in raw.columns and "volume" not in raw.columns:
                        raw = raw.rename(columns={"amount": "volume"})
                        raw["amount"] = pd.to_numeric(raw["close"], errors="coerce") * pd.to_numeric(
                            raw["volume"], errors="coerce"
                        )
                else:
                    raise ValueError(f"unsupported AkShare history source: {source}")
                break
            except Exception as exc:  # AkShare upstream endpoints can intermittently disconnect.
                last_error = exc
                if attempt < retries and delay > 0:
                    time.sleep(delay * attempt)
        if raw is None:
            print(f"skip failed history: {symbol} ({last_error})")
            continue
        frame = normalize_akshare_history(raw, code)
        if frame.empty:
            print(f"skip empty history: {symbol}")
            continue
        path = write_symbol_csv(frame, output_dir, qlib_symbol_from_code(code))
        output_paths.append(path)
        print(f"[{i}/{len(selected)}] wrote {path.name}: {frame['date'].min()} -> {frame['date'].max()} rows={len(frame)}")
        if delay > 0:
            time.sleep(delay)
    return output_paths


def dump_csvs_to_qlib(
    source_dir: str | Path,
    qlib_dir: str | Path,
    dump_bin_path: str | Path,
    python_bin: str = sys.executable,
    max_workers: int = 4,
) -> None:
    qlib_path = Path(qlib_dir)
    if qlib_path.exists():
        shutil.rmtree(qlib_path)
    command = build_dump_bin_command(dump_bin_path, source_dir, qlib_path, python_bin=python_bin, max_workers=max_workers)
    print("+", " ".join(command))
    subprocess.run(command, check=True)
