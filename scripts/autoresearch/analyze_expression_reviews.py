#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import add_src_to_path, project_root

add_src_to_path()

from qlib_factor_lab.config import ProjectConfig, load_project_config
from qlib_factor_lab.factor_registry import FactorDef
from qlib_factor_lab.market_regime import compute_equal_weight_market_regime
from qlib_factor_lab.neutralization import add_size_proxy, neutralize_signal
from qlib_factor_lab.qlib_bootstrap import init_qlib


@dataclass(frozen=True)
class ReviewCandidate:
    name: str
    expression: str
    direction: int
    family: str
    description: str
    commit: str
    artifact_dir: str
    primary_metric: float
    secondary_metric: float
    guard_metric: float
    complexity_score: float
    rank_ic_h5: float
    rank_ic_h20: float
    neutral_rank_ic_h5: float
    neutral_rank_ic_h20: float


def main() -> int:
    root = project_root()
    parser = argparse.ArgumentParser(description="Deduplicate expression autoresearch reviews and re-check stability.")
    parser.add_argument("--ledger", default="reports/autoresearch/expression_results.tsv")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument(
        "--provider-config",
        action="append",
        default=[],
        help="Provider config to evaluate. Repeat for csi500/csi300.",
    )
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--horizons", nargs="+", type=int, default=[5, 20])
    args = parser.parse_args()

    provider_configs = args.provider_config or ["configs/provider_current.yaml", "configs/provider_csi300_current.yaml"]
    stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_dir = root / (args.output_dir or f"reports/autoresearch/review_analysis_{stamp}")
    output_dir.mkdir(parents=True, exist_ok=True)

    candidates = load_review_candidates(root / args.ledger)
    clusters = cluster_candidates(candidates)
    selected = select_representatives(clusters, args.top)

    write_clusters(clusters, output_dir / "dedup_clusters.tsv")
    write_selected(selected, output_dir / "selected_candidates.tsv")

    overall_rows: list[dict[str, object]] = []
    year_rows: list[dict[str, object]] = []
    regime_rows: list[dict[str, object]] = []
    error_rows: list[dict[str, object]] = []
    for provider_path in provider_configs:
        config = load_project_config(root / provider_path)
        init_qlib(config)
        for candidate in selected:
            try:
                daily = evaluate_daily_rank_ic(config, candidate, tuple(args.horizons))
            except Exception as exc:
                error_rows.append(
                    {
                        "market": config.market,
                        "candidate_name": candidate.name,
                        "commit": candidate.commit,
                        "error": str(exc),
                    }
                )
                continue
            overall_rows.extend(summarize_overall(config.market, candidate, daily))
            year_rows.extend(summarize_by_year(config.market, candidate, daily))
            regime_rows.extend(summarize_by_regime(config.market, candidate, daily))

    pd.DataFrame(overall_rows).to_csv(output_dir / "stability_overall.tsv", sep="\t", index=False)
    pd.DataFrame(year_rows).to_csv(output_dir / "stability_by_year.tsv", sep="\t", index=False)
    pd.DataFrame(regime_rows).to_csv(output_dir / "stability_by_regime.tsv", sep="\t", index=False)
    pd.DataFrame(error_rows).to_csv(output_dir / "stability_errors.tsv", sep="\t", index=False)
    write_summary(output_dir, candidates, clusters, selected, overall_rows, year_rows, regime_rows, error_rows)
    print(f"wrote: {output_dir}")
    print(f"reviews: {len(candidates)}")
    print(f"clusters: {len(clusters)}")
    print(f"selected: {len(selected)}")
    print(f"errors: {len(error_rows)}")
    return 0


def load_review_candidates(ledger_path: Path) -> list[ReviewCandidate]:
    rows = list(csv.DictReader(ledger_path.open("r", encoding="utf-8"), delimiter="\t"))
    reviews = [row for row in rows if row.get("status") == "review"]
    candidates: list[ReviewCandidate] = []
    seen: set[str] = set()
    for row in reviews:
        name = row["candidate_name"]
        if name in seen:
            continue
        seen.add(name)
        artifact_dir = Path(row["artifact_dir"])
        candidate_path = artifact_dir / "candidate.yaml"
        if not candidate_path.exists():
            continue
        raw = yaml.safe_load(candidate_path.read_text(encoding="utf-8")) or {}
        candidates.append(
            ReviewCandidate(
                name=name,
                expression=str(raw["expression"]),
                direction=int(raw.get("direction", 1)),
                family=str(raw.get("family", "expression")),
                description=str(raw.get("description", "")),
                commit=row["commit"],
                artifact_dir=row["artifact_dir"],
                primary_metric=_float(row.get("primary_metric")),
                secondary_metric=_float(row.get("secondary_metric")),
                guard_metric=_float(row.get("guard_metric")),
                complexity_score=_float(row.get("complexity_score")),
                rank_ic_h5=_float(row.get("rank_ic_mean_h5")),
                rank_ic_h20=_float(row.get("rank_ic_mean_h20")),
                neutral_rank_ic_h5=_float(row.get("neutral_rank_ic_mean_h5")),
                neutral_rank_ic_h20=_float(row.get("neutral_rank_ic_mean_h20")),
            )
        )
    return candidates


def cluster_candidates(candidates: list[ReviewCandidate], threshold: float = 0.48) -> list[list[ReviewCandidate]]:
    ordered = sorted(candidates, key=_selection_score, reverse=True)
    clusters: list[list[ReviewCandidate]] = []
    cluster_tokens: list[set[str]] = []
    for candidate in ordered:
        tokens = _candidate_tokens(candidate)
        best_index = None
        best_similarity = 0.0
        for index, existing in enumerate(cluster_tokens):
            similarity = _jaccard(tokens, existing)
            if similarity > best_similarity:
                best_similarity = similarity
                best_index = index
        if best_index is None or best_similarity < threshold:
            clusters.append([candidate])
            cluster_tokens.append(tokens)
        else:
            clusters[best_index].append(candidate)
            cluster_tokens[best_index] |= tokens
    for cluster in clusters:
        cluster.sort(key=_selection_score, reverse=True)
    clusters.sort(key=lambda cluster: _selection_score(cluster[0]), reverse=True)
    return clusters


def select_representatives(clusters: list[list[ReviewCandidate]], top: int) -> list[ReviewCandidate]:
    return [cluster[0] for cluster in clusters[:top]]


def evaluate_daily_rank_ic(config: ProjectConfig, candidate: ReviewCandidate, horizons: tuple[int, ...]) -> pd.DataFrame:
    from qlib.data import D

    factor = FactorDef(
        name=candidate.name,
        expression=candidate.expression,
        direction=candidate.direction,
        category=candidate.family,
        description=candidate.description,
    )
    instruments = D.instruments(config.market)
    frame = D.features(
        instruments,
        [factor.expression, "$close", "$volume"],
        start_time=config.start_time,
        end_time=config.end_time,
        freq=config.freq,
    )
    frame.columns = ["factor", "close", "volume"]
    frame = frame.dropna(subset=["factor", "close"]).copy()
    frame["signal"] = frame["factor"] * factor.direction
    frame = add_size_proxy(frame)
    frame = neutralize_signal(frame, exposure_cols=["size_proxy"])
    regime = compute_equal_weight_market_regime(frame[["close"]])

    rows: list[dict[str, object]] = []
    close = frame["close"].groupby(level="instrument")
    for horizon in horizons:
        scored = frame.copy()
        scored["future_ret"] = close.shift(-horizon) / scored["close"] - 1.0
        scored = scored.dropna(subset=["signal", "signal_neutral", "future_ret"])
        for date, daily in scored.groupby(level="datetime"):
            if len(daily) < 10:
                continue
            rows.append(
                {
                    "datetime": pd.Timestamp(date),
                    "horizon": horizon,
                    "rank_ic": daily["signal"].corr(daily["future_ret"], method="spearman"),
                    "neutral_rank_ic": daily["signal_neutral"].corr(daily["future_ret"], method="spearman"),
                    "observations": int(len(daily)),
                }
            )
    daily_result = pd.DataFrame(rows)
    if daily_result.empty:
        return daily_result
    regime_lookup = regime[["market_regime"]].copy()
    regime_lookup.index = pd.to_datetime(regime_lookup.index)
    daily_result = daily_result.merge(regime_lookup, left_on="datetime", right_index=True, how="left")
    daily_result["market_regime"] = daily_result["market_regime"].fillna("unknown")
    daily_result["year"] = daily_result["datetime"].dt.year
    return daily_result


def summarize_overall(market: str, candidate: ReviewCandidate, daily: pd.DataFrame) -> list[dict[str, object]]:
    rows = []
    for horizon, subset in daily.groupby("horizon"):
        rows.append(_summary_row(market, candidate, subset, int(horizon), "overall", "all"))
    return rows


def summarize_by_year(market: str, candidate: ReviewCandidate, daily: pd.DataFrame) -> list[dict[str, object]]:
    rows = []
    focus = daily[daily["horizon"] == 20]
    for year, subset in focus.groupby("year"):
        rows.append(_summary_row(market, candidate, subset, 20, "year", str(int(year))))
    return rows


def summarize_by_regime(market: str, candidate: ReviewCandidate, daily: pd.DataFrame) -> list[dict[str, object]]:
    rows = []
    focus = daily[daily["horizon"] == 20]
    for regime, subset in focus.groupby("market_regime"):
        rows.append(_summary_row(market, candidate, subset, 20, "regime", str(regime)))
    return rows


def _summary_row(
    market: str,
    candidate: ReviewCandidate,
    subset: pd.DataFrame,
    horizon: int,
    segment_type: str,
    segment: str,
) -> dict[str, object]:
    neutral = subset["neutral_rank_ic"].dropna()
    raw = subset["rank_ic"].dropna()
    return {
        "market": market,
        "candidate_name": candidate.name,
        "commit": candidate.commit,
        "horizon": horizon,
        "segment_type": segment_type,
        "segment": segment,
        "days": int(len(subset)),
        "rank_ic_mean": float(raw.mean()) if not raw.empty else float("nan"),
        "neutral_rank_ic_mean": float(neutral.mean()) if not neutral.empty else float("nan"),
        "neutral_rank_ic_std": float(neutral.std()) if len(neutral) > 1 else float("nan"),
        "neutral_rank_icir": float(neutral.mean() / neutral.std()) if len(neutral) > 1 and neutral.std() else float("nan"),
        "neutral_positive_rate": float((neutral > 0).mean()) if not neutral.empty else float("nan"),
        "mean_observations": float(subset["observations"].mean()) if not subset.empty else float("nan"),
    }


def write_clusters(clusters: list[list[ReviewCandidate]], output_path: Path) -> None:
    rows = []
    for index, cluster in enumerate(clusters, start=1):
        representative = cluster[0]
        for member_index, candidate in enumerate(cluster, start=1):
            rows.append(
                {
                    "cluster_id": f"C{index:03d}",
                    "member_rank": member_index,
                    "is_representative": member_index == 1,
                    "cluster_size": len(cluster),
                    "candidate_name": candidate.name,
                    "commit": candidate.commit,
                    "primary_metric": candidate.primary_metric,
                    "secondary_metric": candidate.secondary_metric,
                    "guard_metric": candidate.guard_metric,
                    "complexity_score": candidate.complexity_score,
                    "representative": representative.name,
                    "family": candidate.family,
                    "expression": candidate.expression,
                }
            )
    pd.DataFrame(rows).to_csv(output_path, sep="\t", index=False)


def write_selected(candidates: list[ReviewCandidate], output_path: Path) -> None:
    rows = []
    for rank, candidate in enumerate(candidates, start=1):
        rows.append(
            {
                "rank": rank,
                "candidate_name": candidate.name,
                "commit": candidate.commit,
                "primary_metric": candidate.primary_metric,
                "secondary_metric": candidate.secondary_metric,
                "guard_metric": candidate.guard_metric,
                "complexity_score": candidate.complexity_score,
                "family": candidate.family,
                "expression": candidate.expression,
                "description": candidate.description,
            }
        )
    pd.DataFrame(rows).to_csv(output_path, sep="\t", index=False)


def write_summary(
    output_dir: Path,
    candidates: list[ReviewCandidate],
    clusters: list[list[ReviewCandidate]],
    selected: list[ReviewCandidate],
    overall_rows: list[dict[str, object]],
    year_rows: list[dict[str, object]],
    regime_rows: list[dict[str, object]],
    error_rows: list[dict[str, object]],
) -> None:
    overall = pd.DataFrame(overall_rows)
    year = pd.DataFrame(year_rows)
    regime = pd.DataFrame(regime_rows)
    lines = [
        "# Autoresearch Expression Review Analysis",
        "",
        f"- generated_at: {datetime.now().isoformat(timespec='seconds')}",
        f"- review_candidates: {len(candidates)}",
        f"- dedup_clusters: {len(clusters)}",
        f"- selected_representatives: {len(selected)}",
        f"- stability_errors: {len(error_rows)}",
        "",
        "## Selected Representatives",
        "",
        "| rank | candidate | primary | secondary | guard | complexity | commit |",
        "|---:|---|---:|---:|---:|---:|---|",
    ]
    for rank, candidate in enumerate(selected, start=1):
        lines.append(
            f"| {rank} | `{candidate.name}` | {candidate.primary_metric:.5f} | "
            f"{candidate.secondary_metric:.5f} | {candidate.guard_metric:.5f} | "
            f"{candidate.complexity_score:.3f} | `{candidate.commit}` |"
        )
    if not overall.empty:
        lines.extend(["", "## Overall Stability", ""])
        focus = overall[overall["horizon"] == 20].sort_values(
            ["market", "neutral_rank_ic_mean"], ascending=[True, False]
        )
        lines.extend(_markdown_table(focus, ["market", "candidate_name", "neutral_rank_ic_mean", "rank_ic_mean", "neutral_positive_rate", "days"]))
    if not year.empty:
        lines.extend(["", "## Weakest Annual H20 Segment", ""])
        weakest = (
            year.sort_values("neutral_rank_ic_mean")
            .groupby(["market", "candidate_name"], as_index=False)
            .head(1)
            .sort_values(["market", "neutral_rank_ic_mean"])
        )
        lines.extend(_markdown_table(weakest, ["market", "candidate_name", "segment", "neutral_rank_ic_mean", "neutral_positive_rate", "days"]))
    if not regime.empty:
        lines.extend(["", "## Regime H20 Stability", ""])
        regime_focus = regime.sort_values(["market", "candidate_name", "segment"])
        lines.extend(_markdown_table(regime_focus, ["market", "candidate_name", "segment", "neutral_rank_ic_mean", "neutral_positive_rate", "days"]))
    if error_rows:
        lines.extend(["", "## Errors", ""])
        lines.extend(_markdown_table(pd.DataFrame(error_rows), ["market", "candidate_name", "error"]))
    (output_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _markdown_table(frame: pd.DataFrame, columns: list[str], limit: int = 80) -> list[str]:
    if frame.empty:
        return ["_No rows._"]
    visible = frame.loc[:, columns].head(limit).copy()
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in visible.iterrows():
        cells = []
        for column in columns:
            value = row[column]
            if isinstance(value, float):
                cells.append(f"{value:.5f}" if math.isfinite(value) else "nan")
            elif column == "candidate_name":
                cells.append(f"`{value}`")
            else:
                cells.append(str(value))
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def _candidate_tokens(candidate: ReviewCandidate) -> set[str]:
    raw = f"{candidate.name} {candidate.family} {candidate.expression}"
    tokens = set(re.findall(r"[A-Za-z]+|\d+", raw.lower()))
    drop = {"v", "mean", "ref", "max", "min", "corr", "abs", "close", "open", "high", "low", "volume", "amount"}
    tokens = {token for token in tokens if token not in drop and len(token) > 1}
    fields = set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", candidate.expression.lower()))
    operators = set(re.findall(r"\b([A-Z][A-Za-z0-9_]*)\s*\(", candidate.expression))
    windows = {f"w{value}" for value in re.findall(r",\s*(\d+)\s*\)", candidate.expression)}
    return tokens | fields | {operator.lower() for operator in operators} | windows


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    return len(left & right) / len(left | right)


def _selection_score(candidate: ReviewCandidate) -> float:
    penalty = 0.012 * candidate.complexity_score + 0.004 * candidate.guard_metric
    return candidate.primary_metric + 0.15 * candidate.secondary_metric - penalty


def _float(value: object) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return result if math.isfinite(result) else float("nan")


if __name__ == "__main__":
    raise SystemExit(main())
