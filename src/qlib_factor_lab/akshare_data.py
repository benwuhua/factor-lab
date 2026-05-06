from __future__ import annotations

import datetime as dt
import hashlib
import shutil
import struct
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

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

INDUSTRY_OVERRIDE_COLUMNS = [
    "证券代码",
    "证券简称",
    "行业中类",
    "行业大类",
    "行业门类",
    "行业来源",
    "更新截止",
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


def today_for_daily_data(now: dt.date | dt.datetime | None = None) -> str:
    shanghai = ZoneInfo("Asia/Shanghai")
    if now is None:
        current = dt.datetime.now(shanghai)
    elif isinstance(now, dt.datetime):
        current = now.replace(tzinfo=shanghai) if now.tzinfo is None else now.astimezone(shanghai)
    else:
        current = dt.datetime.combine(now, dt.time(23, 59), tzinfo=shanghai)
    market_close = dt.time(15, 0)
    target = current.date() if current.time() >= market_close else current.date() - dt.timedelta(days=1)
    return target.strftime("%Y-%m-%d")


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


def enrich_security_master_industries(master: pd.DataFrame, industries: pd.DataFrame) -> pd.DataFrame:
    """Fill missing industry fields from an offline/alternative industry table."""
    if master is None or master.empty or industries is None or industries.empty:
        return master.copy()

    output = master.copy()
    for column in SECURITY_MASTER_COLUMNS:
        if column not in output.columns:
            output[column] = ""
    code_col = _first_present(
        industries,
        ["instrument", "证券代码", "代码", "品种代码", "symbol", "code"],
        required=False,
    )
    if code_col is None:
        return output

    sw_col = _first_present(
        industries,
        ["申万行业", "行业中类", "行业次类", "行业", "industry_sw", "所属行业"],
        required=False,
    )
    csrc_col = _first_present(
        industries,
        ["证监会行业", "行业大类", "行业门类", "industry_csrc", "门类"],
        required=False,
    )
    if sw_col is None and csrc_col is None:
        return output

    sw_by_instrument: dict[str, str] = {}
    csrc_by_instrument: dict[str, str] = {}
    for _, row in industries.iterrows():
        instrument = _industry_row_instrument(row.get(code_col, ""))
        if not instrument:
            continue
        if sw_col is not None:
            sw_value = _clean_text(row.get(sw_col, ""))
            if sw_value:
                sw_by_instrument[instrument] = sw_value
        if csrc_col is not None:
            csrc_value = _clean_text(row.get(csrc_col, ""))
            if csrc_value:
                csrc_by_instrument[instrument] = csrc_value

    instruments = output["instrument"].astype(str).str.upper()
    if "industry_sw" not in output.columns:
        output["industry_sw"] = ""
    if "industry_csrc" not in output.columns:
        output["industry_csrc"] = ""

    sw_blank = output["industry_sw"].map(_is_blank_text)
    csrc_blank = output["industry_csrc"].map(_is_blank_text)
    output.loc[sw_blank, "industry_sw"] = instruments.loc[sw_blank].map(sw_by_instrument).fillna(
        output.loc[sw_blank, "industry_sw"]
    )
    output.loc[csrc_blank, "industry_csrc"] = instruments.loc[csrc_blank].map(csrc_by_instrument).fillna(
        output.loc[csrc_blank, "industry_csrc"]
    )
    return output.loc[:, [column for column in SECURITY_MASTER_COLUMNS if column in output.columns]]


def normalize_cninfo_industry_override(raw: pd.DataFrame, code: str, as_of_date: str) -> dict[str, str]:
    if raw is None or raw.empty:
        return {
            "证券代码": akshare_code_from_qlib(code),
            "证券简称": "",
            "行业中类": "",
            "行业大类": "",
            "行业门类": "",
            "行业来源": "cninfo_stock_industry_change",
            "更新截止": _yyyymmdd(as_of_date),
        }

    sw = _first_not_none(_pick_latest_industry(raw, "申银万国"), _pick_latest_industry(raw, "中证"), _pick_latest_industry(raw))
    official = _first_not_none(
        _pick_latest_industry(raw, "中国上市公司协会"),
        _pick_latest_industry(raw, "巨潮"),
        _pick_latest_industry(raw),
    )
    return {
        "证券代码": akshare_code_from_qlib(code),
        "证券简称": _industry_cell(sw, "新证券简称") or _industry_cell(official, "新证券简称"),
        "行业中类": _industry_cell(sw, "行业中类") or _industry_cell(sw, "行业大类"),
        "行业大类": _industry_cell(official, "行业大类") or _industry_cell(sw, "行业大类"),
        "行业门类": _industry_cell(official, "行业门类") or _industry_cell(sw, "行业门类"),
        "行业来源": "cninfo_stock_industry_change",
        "更新截止": _yyyymmdd(as_of_date),
    }


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
        active_until = _notice_active_until(event_date, event_type, severity)
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
                "active_until": active_until,
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
    if any(word in text for word in ["增持", "持股计划增持"]):
        return "shareholder_increase", "watch"
    if any(word in text for word in ["回购", "股份回购"]):
        return "buyback", "watch"
    if any(word in text for word in ["股东户数", "股东人数", "股东总户数"]):
        return "holder_count_change", "info"
    if any(word in text for word in ["解禁", "限售股上市流通", "限售股份上市流通"]):
        return "large_unlock", "watch"
    if any(word in text for word in ["预增", "预盈", "扭亏为盈", "业绩上修", "向上修正"]):
        return "performance_warning_up", "watch"
    if any(word in text for word in ["减亏", "亏损收窄", "亏损同比收窄", "亏损减少", "亏损幅度收窄"]):
        return "performance_warning_repair", "info"
    if any(word in text for word in ["预亏", "首亏", "续亏", "预减", "业绩下修", "向下修正", "由盈转亏"]):
        return "performance_warning_down", "risk"
    if "业绩预告" in text:
        return "performance_warning_neutral", "info"
    if any(word in text for word in ["诉讼", "仲裁"]):
        return "lawsuit", "risk"
    if "担保" in text:
        return "guarantee", "watch"
    if any(word in text for word in ["解除质押", "质押解除"]):
        return "pledge_release", "info"
    if "质押" in text:
        return "pledge_risk", "watch"
    if any(word in text for word in ["权益变动", "股本变动", "股份变动"]):
        return "capital_structure_change", "watch"
    if any(word in text for word in ["异动", "异常波动"]):
        return "abnormal_volatility", "watch"
    return "announcement", "info"


def _notice_active_until(event_date: pd.Timestamp, event_type: str, severity: str) -> str:
    if pd.isna(event_date):
        return ""
    if severity == "block" or event_type in {"disciplinary_action", "delisting_risk", "trading_suspension", "st_status"}:
        days = 90
    elif severity == "risk":
        days = 60
    elif severity == "watch":
        days = 45
    else:
        days = 30
    return str((event_date.date() + dt.timedelta(days=days)))


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
    mode: str = "dump_all",
) -> list[str]:
    if mode not in {"dump_all", "dump_update"}:
        raise ValueError("mode must be dump_all or dump_update")
    return [
        python_bin,
        str(dump_bin_path),
        mode,
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


def read_latest_qlib_calendar_date(qlib_dir: str | Path, freq: str = "day") -> str | None:
    calendar_path = Path(qlib_dir).expanduser() / "calendars" / f"{freq}.txt"
    if not calendar_path.exists():
        return None
    lines = [line.strip() for line in calendar_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return None
    return lines[-1]


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


def fetch_security_industry_overrides(
    symbols: Iterable[str],
    as_of_date: str,
    *,
    start_date: str = "20000101",
    delay: float = 0.1,
    limit: int | None = None,
    retries: int = 2,
) -> pd.DataFrame:
    ak = _get_akshare()
    selected = list(symbols)
    if limit is not None:
        selected = selected[:limit]
    end_date = _yyyymmdd(as_of_date)
    rows = []
    for i, symbol in enumerate(selected, start=1):
        code = akshare_code_from_qlib(symbol)
        raw = None
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                raw = ak.stock_industry_change_cninfo(symbol=code, start_date=start_date, end_date=end_date)
                break
            except Exception as exc:
                last_error = exc
                if attempt < retries and delay > 0:
                    time.sleep(delay * attempt)
        if raw is None:
            print(f"skip failed industry: {symbol} ({last_error})")
            continue
        rows.append(normalize_cninfo_industry_override(raw, code, as_of_date))
        print(f"[{i}/{len(selected)}] industry {symbol}")
        if delay > 0:
            time.sleep(delay)
    return pd.DataFrame(rows, columns=INDUSTRY_OVERRIDE_COLUMNS)


def fetch_company_notices(start_date: str, end_date: str, delay: float = 0.2) -> pd.DataFrame:
    ak = _get_akshare()
    frames = []
    for day in pd.date_range(start=start_date, end=end_date, freq="D"):
        date_text = day.strftime("%Y%m%d")
        try:
            raw = ak.stock_notice_report(symbol="全部", date=date_text)
        except TypeError as exc:
            try:
                raw = ak.stock_notice_report(date=date_text)
            except Exception as fallback_exc:
                print(f"skip failed notices: {date_text} ({fallback_exc}; fallback after {exc})")
                raw = None
        except Exception as exc:
            print(f"skip failed notices: {date_text} ({exc})")
            raw = None
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
    update: bool = False,
    update_existing_fields_only: bool = False,
) -> None:
    qlib_path = Path(qlib_dir)
    if qlib_path.exists() and not update:
        shutil.rmtree(qlib_path)
    effective_source_dir = Path(source_dir)
    if update and update_existing_fields_only:
        effective_source_dir, dropped = filter_source_csvs_to_existing_qlib_fields(source_dir, qlib_path)
        if dropped:
            print(f"drop new incremental Qlib fields without historical backfill: {','.join(sorted(dropped))}")
        _rewind_instrument_end_dates_to_feature_bins(qlib_path, effective_source_dir)
        if not any(effective_source_dir.glob("*.csv")):
            print(f"skip Qlib dump_update: no source rows need appending in {effective_source_dir}")
            return
    command = build_dump_bin_command(
        dump_bin_path,
        effective_source_dir,
        qlib_path,
        python_bin=python_bin,
        max_workers=max_workers,
        mode="dump_update" if update else "dump_all",
    )
    print("+", " ".join(command))
    subprocess.run(command, check=True)


def filter_source_csvs_to_existing_qlib_fields(source_dir: str | Path, qlib_dir: str | Path) -> tuple[Path, set[str]]:
    source_path = Path(source_dir).expanduser().resolve()
    qlib_path = Path(qlib_dir).expanduser()
    existing_fields = _existing_qlib_feature_fields(qlib_path)
    if not existing_fields:
        return source_path, set()
    calendar = _read_qlib_calendar(qlib_path)
    calendar_index = {date: idx for idx, date in enumerate(calendar)}
    output_dir = source_path.with_name(f"{source_path.name}_existing_fields")
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    keep = {"date", "symbol"} | existing_fields
    dropped: set[str] = set()
    for path in source_path.glob("*.csv"):
        frame = pd.read_csv(path)
        drop_columns = [column for column in frame.columns if column not in keep]
        dropped.update(drop_columns)
        frame = frame[[column for column in frame.columns if column in keep]]
        frame = _pad_incremental_frame_to_qlib_calendar(frame, qlib_path, calendar, calendar_index)
        if not frame.empty:
            frame.to_csv(output_dir / path.name, index=False)
    return output_dir, dropped


def _existing_qlib_feature_fields(qlib_dir: str | Path) -> set[str]:
    feature_root = Path(qlib_dir).expanduser() / "features"
    if not feature_root.exists():
        return set()
    fields: set[str] = set()
    for path in feature_root.glob("*/*.day.bin"):
        fields.add(path.name.removesuffix(".day.bin"))
    return fields


def _read_qlib_calendar(qlib_dir: str | Path) -> list[str]:
    path = Path(qlib_dir).expanduser() / "calendars" / "day.txt"
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _pad_incremental_frame_to_qlib_calendar(
    frame: pd.DataFrame,
    qlib_dir: Path,
    calendar: list[str],
    calendar_index: dict[str, int],
) -> pd.DataFrame:
    if frame.empty or not calendar or "date" not in frame.columns or "symbol" not in frame.columns:
        return frame
    symbol = str(frame["symbol"].dropna().astype(str).iloc[0]).lower()
    current_end = _existing_symbol_feature_end_index(qlib_dir, symbol)
    if current_end is None:
        return frame
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    source_indexes = [calendar_index[date] for date in frame["date"].tolist() if date in calendar_index]
    if not source_indexes:
        return frame
    max_source_index = max(source_indexes)
    if current_end >= max_source_index:
        return pd.DataFrame(columns=frame.columns)
    pad_dates = calendar[current_end + 1 : max_source_index + 1]
    skeleton = pd.DataFrame({"date": pad_dates, "symbol": str(frame["symbol"].dropna().astype(str).iloc[0])})
    return skeleton.merge(frame, on=["date", "symbol"], how="left")


def _existing_symbol_feature_end_index(qlib_dir: Path, symbol: str) -> int | None:
    feature_dir = qlib_dir / "features" / symbol.lower()
    if not feature_dir.exists():
        return None
    for path in sorted(feature_dir.glob("*.day.bin")):
        size = path.stat().st_size
        if size < 8 or size % 4:
            continue
        with path.open("rb") as handle:
            start = int(struct.unpack("<f", handle.read(4))[0])
        value_count = size // 4 - 1
        return start + value_count - 1
    return None


def _rewind_instrument_end_dates_to_feature_bins(qlib_dir: Path, source_dir: Path) -> None:
    calendar = _read_qlib_calendar(qlib_dir)
    if not calendar:
        return
    source_symbols = {path.stem.upper() for path in source_dir.glob("*.csv")}
    if not source_symbols:
        return
    instruments_dir = qlib_dir / "instruments"
    if not instruments_dir.exists():
        return
    for path in instruments_dir.glob("*.txt"):
        try:
            frame = pd.read_csv(path, sep="\t", header=None, names=["instrument", "start", "end"], dtype=str)
        except Exception:
            continue
        if frame.empty:
            continue
        changed = False
        for idx, row in frame.iterrows():
            symbol = str(row["instrument"]).upper()
            if symbol not in source_symbols:
                continue
            end_index = _existing_symbol_feature_end_index(qlib_dir, symbol.lower())
            if end_index is None or end_index >= len(calendar):
                continue
            feature_end = calendar[end_index]
            if str(row["end"]) > feature_end:
                frame.loc[idx, "end"] = feature_end
                changed = True
        if changed:
            frame.to_csv(path, sep="\t", header=False, index=False)


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


def _industry_row_instrument(value) -> str:
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL"}:
        return ""
    if text.startswith(("SH", "SZ")) and len(text) >= 8:
        return text[:8]
    try:
        return qlib_symbol_from_code(text)
    except ValueError:
        return ""


def _clean_text(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def _is_blank_text(value) -> bool:
    return _clean_text(value) == ""


def _pick_latest_industry(frame: pd.DataFrame, standard_contains: str | None = None) -> pd.Series | None:
    data = frame.copy()
    if standard_contains and "分类标准" in data.columns:
        data = data[data["分类标准"].astype(str).str.contains(standard_contains, na=False)]
    if data.empty:
        return None
    data = data.copy()
    if "变更日期" in data.columns:
        data["变更日期"] = pd.to_datetime(data["变更日期"], errors="coerce")
        data = data.sort_values("变更日期")
    return data.iloc[-1]


def _industry_cell(row: pd.Series | None, column: str) -> str:
    if row is None or column not in row.index or pd.isna(row[column]):
        return ""
    return str(row[column]).strip()


def _first_not_none(*values):
    for value in values:
        if value is not None:
            return value
    return None


def _yyyymmdd(value: str) -> str:
    return pd.Timestamp(value).strftime("%Y%m%d")


def _optional_date(row: pd.Series, candidates: list[str]) -> str:
    text = _optional_text(row, candidates)
    if not text:
        return ""
    value = pd.to_datetime(text, errors="coerce")
    return "" if pd.isna(value) else str(value.date())
