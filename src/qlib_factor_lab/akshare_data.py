from __future__ import annotations

import datetime as dt
import hashlib
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

SECURITY_MASTER_COLUMNS = [
    "instrument",
    "name",
    "exchange",
    "board",
    "industry_sw",
    "industry_csrc",
    "is_st",
    "listing_date",
    "delisting_date",
    "valid_from",
    "valid_to",
]

COMPANY_EVENT_COLUMNS = [
    "event_id",
    "instrument",
    "event_type",
    "event_date",
    "source",
    "source_url",
    "title",
    "severity",
    "summary",
    "evidence",
    "active_until",
]

FIXED_RESEARCH_UNIVERSES = ("csi300", "csi500")


@dataclass(frozen=True)
class UniverseSpec:
    name: str
    benchmark: str
    symbols: list[str]


def validate_research_universe(universe: str) -> str:
    normalized = str(universe).strip().lower()
    if normalized not in FIXED_RESEARCH_UNIVERSES:
        raise ValueError("factor-lab data layer only supports csi300 and csi500")
    return normalized


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


def normalize_security_master_snapshot(raw: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=SECURITY_MASTER_COLUMNS)
    code_col = _first_present(raw, ["代码", "品种代码", "证券代码", "symbol", "code"])
    name_col = _first_present(raw, ["名称", "股票简称", "证券简称", "name"])
    rows = []
    for _, row in raw.iterrows():
        code = str(row.get(code_col, "")).strip()
        if not code:
            continue
        instrument = qlib_symbol_from_code(code)
        name = str(row.get(name_col, "")).strip()
        rows.append(
            {
                "instrument": instrument,
                "name": name,
                "exchange": _exchange_from_instrument(instrument),
                "board": _board_from_code(code),
                "industry_sw": _optional_text(row, ["申万行业", "行业", "industry_sw", "所属行业"]),
                "industry_csrc": _optional_text(row, ["证监会行业", "industry_csrc"]),
                "is_st": _is_st_name(name),
                "listing_date": _optional_date(row, ["上市日期", "上市时间", "listing_date"]),
                "delisting_date": _optional_date(row, ["退市日期", "delisting_date"]),
                "valid_from": as_of_date,
                "valid_to": "",
            }
        )
    return pd.DataFrame(rows, columns=SECURITY_MASTER_COLUMNS).drop_duplicates("instrument", keep="last")


def normalize_akshare_notices(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)
    code_col = _first_present(raw, ["代码", "品种代码", "证券代码", "symbol", "code"])
    title_col = _first_present(raw, ["公告标题", "标题", "title"])
    date_col = _first_present(raw, ["公告日期", "发布日期", "date", "公告时间"])
    type_col = _first_present(raw, ["公告类型", "类型", "category"], required=False)
    url_col = _first_present(raw, ["网址", "公告链接", "url", "source_url"], required=False)
    rows = []
    for _, row in raw.iterrows():
        code = str(row.get(code_col, "")).strip()
        title = str(row.get(title_col, "")).strip()
        if not code or not title:
            continue
        instrument = qlib_symbol_from_code(code)
        event_date = pd.to_datetime(row.get(date_col), errors="coerce")
        event_date_text = "" if pd.isna(event_date) else str(event_date.date())
        event_type, severity = classify_notice_event(title, str(row.get(type_col, "") if type_col else ""))
        source_url = str(row.get(url_col, "") if url_col else "").strip()
        event_key = "|".join([instrument, event_date_text, title, source_url])
        rows.append(
            {
                "event_id": hashlib.sha1(event_key.encode("utf-8")).hexdigest()[:16],
                "instrument": instrument,
                "event_type": event_type,
                "event_date": event_date_text,
                "source": "akshare_notice",
                "source_url": source_url,
                "title": title,
                "severity": severity,
                "summary": title,
                "evidence": title,
                "active_until": "",
            }
        )
    return pd.DataFrame(rows, columns=COMPANY_EVENT_COLUMNS)


def filter_frame_to_universes(frame: pd.DataFrame, universe_symbols: dict[str, Iterable[str]]) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame.copy()
    if "instrument" not in frame.columns:
        raise ValueError("frame must include instrument column for universe filtering")
    membership: dict[str, list[str]] = {}
    for universe, symbols in universe_symbols.items():
        normalized = validate_research_universe(universe)
        for symbol in symbols:
            membership.setdefault(str(symbol).upper(), []).append(normalized)
    allowed = set(membership)
    output = frame[frame["instrument"].astype(str).str.upper().isin(allowed)].copy()
    output["research_universes"] = output["instrument"].astype(str).str.upper().map(
        lambda instrument: ",".join(sorted(set(membership.get(instrument, []))))
    )
    return output.reset_index(drop=True)


def load_universe_symbols_csv(path: str | Path) -> dict[str, list[str]]:
    frame = pd.read_csv(path)
    if "universe" not in frame.columns or "instrument" not in frame.columns:
        raise ValueError("universe symbols CSV must include universe and instrument columns")
    universes: dict[str, list[str]] = {}
    for universe, group in frame.groupby("universe"):
        normalized = validate_research_universe(str(universe))
        universes[normalized] = sorted(set(group["instrument"].astype(str).str.upper()))
    return universes


def classify_notice_event(title: str, category: str = "") -> tuple[str, str]:
    text = f"{title} {category}"
    if any(word in text for word in ["纪律处分", "处罚", "行政处罚"]):
        return "disciplinary_action", "block"
    if any(word in text for word in ["退市", "终止上市"]):
        return "delisting_risk", "block"
    if "停牌" in text:
        return "trading_suspension", "block"
    if any(word in text for word in ["ST", "风险警示", "退市风险警示"]):
        return "st_status", "block"
    if any(word in text for word in ["问询函", "关注函", "监管函", "监管工作函"]):
        return "regulatory_inquiry", "risk"
    if "减持" in text:
        return "shareholder_reduction", "risk"
    if any(word in text for word in ["解禁", "限售股上市流通", "限售股份上市流通"]):
        return "large_unlock", "watch"
    if any(word in text for word in ["业绩预告", "预亏", "业绩下修", "向下修正"]):
        return "performance_warning_down", "risk"
    if any(word in text for word in ["诉讼", "仲裁"]):
        return "lawsuit", "risk"
    if "担保" in text:
        return "guarantee", "watch"
    if "质押" in text:
        return "pledge_risk", "watch"
    if any(word in text for word in ["异动", "异常波动"]):
        return "abnormal_volatility", "watch"
    return "announcement", "info"


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
    universe = validate_research_universe(universe)
    benchmark = {"csi300": "SH000300", "csi500": "SH000905"}[universe]
    try:
        ak = _get_akshare()
        if universe == "csi300":
            raw = ak.index_stock_cons_csindex(symbol="000300")
            symbols = [qlib_symbol_from_code(code) for code in _extract_symbol_column(raw)]
        elif universe == "csi500":
            raw = ak.index_stock_cons_csindex(symbol="000905")
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
    symbols = load_symbols_from_existing_qlib(fallback_qlib_dir, universe)
    if not symbols:
        raise RuntimeError(f"failed to fetch universe and no fallback symbols found for {universe}")
    return UniverseSpec(f"{universe}_current", benchmark, symbols)


def fetch_security_master_snapshot(as_of_date: str, limit: int | None = None) -> pd.DataFrame:
    ak = _get_akshare()
    raw = ak.stock_info_a_code_name()
    if limit is not None:
        raw = raw.head(limit)
    return normalize_security_master_snapshot(raw, as_of_date=as_of_date)


def fetch_company_notices(start_date: str, end_date: str, delay: float = 0.2) -> pd.DataFrame:
    ak = _get_akshare()
    frames = []
    for day in pd.date_range(start=start_date, end=end_date, freq="D"):
        date_text = day.strftime("%Y%m%d")
        try:
            raw = ak.stock_notice_report(symbol="全部", date=date_text)
        except TypeError:
            raw = ak.stock_notice_report(date=date_text)
        if raw is not None and not raw.empty:
            frames.append(raw)
        if delay > 0:
            time.sleep(delay)
    if not frames:
        return pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)
    return normalize_akshare_notices(pd.concat(frames, ignore_index=True))


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


def _first_present(frame: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    for column in candidates:
        if column in frame.columns:
            return column
    if required:
        raise ValueError(f"cannot find any of columns {candidates} in frame columns: {list(frame.columns)}")
    return None


def _exchange_from_instrument(instrument: str) -> str:
    if str(instrument).startswith("SH"):
        return "SSE"
    if str(instrument).startswith("SZ"):
        return "SZSE"
    return ""


def _board_from_code(code: str) -> str:
    pure = "".join(ch for ch in str(code).strip() if ch.isdigit()).zfill(6)
    if pure.startswith(("688", "689")):
        return "STAR"
    if pure.startswith(("300", "301")):
        return "ChiNext"
    if pure.startswith(("8", "4", "920")):
        return "BSE"
    return "main"


def _is_st_name(name: str) -> bool:
    upper = str(name).upper()
    return "ST" in upper or "退" in upper


def _optional_text(row: pd.Series, candidates: list[str]) -> str:
    for column in candidates:
        if column in row.index and not pd.isna(row[column]):
            return str(row[column]).strip()
    return ""


def _optional_date(row: pd.Series, candidates: list[str]) -> str:
    text = _optional_text(row, candidates)
    if not text:
        return ""
    value = pd.to_datetime(text, errors="coerce")
    return "" if pd.isna(value) else str(value.date())
