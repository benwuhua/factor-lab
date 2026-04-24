from __future__ import annotations

from pathlib import Path

import pandas as pd


def build_single_factor_diagnostics(
    raw_eval: pd.DataFrame,
    neutral_eval: pd.DataFrame,
    metadata: pd.DataFrame | None = None,
    *,
    focus_horizon: int = 20,
) -> pd.DataFrame:
    raw = _focus(raw_eval, focus_horizon).rename(
        columns={
            "rank_ic_mean": "raw_rank_ic_h20",
            "rank_icir": "raw_rank_icir_h20",
            "long_short_mean_return": "raw_long_short_h20",
            "top_quantile_turnover": "raw_top_quantile_turnover_h20",
            "observations": "raw_observations_h20",
        }
    )
    neutral = _focus(neutral_eval, focus_horizon).rename(
        columns={
            "rank_ic_mean": "neutral_rank_ic_h20",
            "rank_icir": "neutral_rank_icir_h20",
            "long_short_mean_return": "neutral_long_short_h20",
            "top_quantile_turnover": "neutral_top_quantile_turnover_h20",
            "observations": "neutral_observations_h20",
        }
    )
    keep_raw = [
        "factor",
        "raw_rank_ic_h20",
        "raw_rank_icir_h20",
        "raw_long_short_h20",
        "raw_top_quantile_turnover_h20",
        "raw_observations_h20",
    ]
    keep_neutral = [
        "factor",
        "neutral_rank_ic_h20",
        "neutral_rank_icir_h20",
        "neutral_long_short_h20",
        "neutral_top_quantile_turnover_h20",
        "neutral_observations_h20",
    ]
    diagnostics = raw.loc[:, [column for column in keep_raw if column in raw.columns]].merge(
        neutral.loc[:, [column for column in keep_neutral if column in neutral.columns]],
        on="factor",
        how="outer",
    )
    if metadata is not None and not metadata.empty:
        diagnostics = diagnostics.merge(_metadata_frame(metadata), on="factor", how="left")
    for column, default in [("family", ""), ("approval_status", "")]:
        if column not in diagnostics:
            diagnostics[column] = default
        diagnostics[column] = diagnostics[column].fillna(default)
    diagnostics["neutral_retention_h20"] = diagnostics["neutral_rank_ic_h20"] / diagnostics["raw_rank_ic_h20"]
    role_info = diagnostics.apply(lambda row: pd.Series(_suggested_role(row), index=["suggested_role", "_role_rank"]), axis=1)
    diagnostics = pd.concat([diagnostics, role_info], axis=1)
    diagnostics["concerns"] = diagnostics.apply(_concerns, axis=1)
    diagnostics["family_representative"] = _family_representatives(diagnostics)
    return diagnostics.sort_values(
        ["_role_rank", "family_representative", "neutral_rank_ic_h20", "neutral_long_short_h20"],
        ascending=[True, False, False, False],
    ).drop(columns=["_role_rank"]).reset_index(drop=True)


def write_single_factor_diagnostics(frame: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output, index=False)
    return output


def write_single_factor_diagnostics_markdown(frame: pd.DataFrame, output_path: str | Path) -> Path:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Single Factor Diagnostics",
        "",
        "## Ranking",
        "",
        "| factor | family | role | neutral_rank_ic_h20 | neutral_ls_h20 | retention | concerns |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    for _, row in frame.iterrows():
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{row['factor']}`",
                    str(row.get("family", "")),
                    str(row.get("suggested_role", "")),
                    _format_float(row.get("neutral_rank_ic_h20")),
                    _format_float(row.get("neutral_long_short_h20")),
                    _format_float(row.get("neutral_retention_h20")),
                    str(row.get("concerns", "")),
                ]
            )
            + " |"
        )
    lines.extend(["", "## Family Representatives", ""])
    reps = frame[frame["family_representative"].fillna(False).astype(bool)]
    for _, row in reps.iterrows():
        lines.append(
            f"- `{row['family']}`: `{row['factor']}` "
            f"neutral_rank_ic_h20={_format_float(row.get('neutral_rank_ic_h20'))} "
            f"neutral_ls_h20={_format_float(row.get('neutral_long_short_h20'))}"
        )
    lines.extend(["", "## Use In Portfolio", ""])
    lines.append("- Use `core_candidate` factors as first-pass family representatives.")
    lines.append("- Keep `shadow_review` factors out of main score until their concerns clear.")
    lines.append("- Prefer family representatives over multiple near-duplicate factors.")
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output


def _focus(frame: pd.DataFrame, horizon: int) -> pd.DataFrame:
    if "horizon" not in frame.columns:
        raise ValueError("factor eval frame must include horizon")
    return frame[frame["horizon"].astype(int) == int(horizon)].copy()


def _metadata_frame(metadata: pd.DataFrame) -> pd.DataFrame:
    output = metadata.copy()
    if "factor" not in output.columns and "name" in output.columns:
        output = output.rename(columns={"name": "factor"})
    keep = ["factor", "family", "approval_status"]
    return output.loc[:, [column for column in keep if column in output.columns]].drop_duplicates("factor")


def _concerns(row: pd.Series) -> str:
    concerns = []
    if _number(row.get("neutral_long_short_h20")) < 0:
        concerns.append("negative_neutral_long_short")
    if _number(row.get("neutral_retention_h20")) < 0.55:
        concerns.append("low_neutral_retention")
    if _number(row.get("neutral_top_quantile_turnover_h20")) > 0.2:
        concerns.append("high_turnover")
    return ";".join(concerns)


def _suggested_role(row: pd.Series) -> str:
    neutral_ic = _number(row.get("neutral_rank_ic_h20"))
    neutral_icir = _number(row.get("neutral_rank_icir_h20"))
    neutral_ls = _number(row.get("neutral_long_short_h20"))
    retention = _number(row.get("neutral_retention_h20"))
    if neutral_ic >= 0.035 and neutral_icir >= 0.25 and neutral_ls > 0 and retention >= 0.55:
        return "core_candidate", 0
    if neutral_ic >= 0.025 and neutral_ls > 0:
        return "candidate_review", 1
    if neutral_ic >= 0.025:
        return "shadow_review", 2
    return "reject_review", 3


def _family_representatives(frame: pd.DataFrame) -> pd.Series:
    representative = pd.Series(False, index=frame.index)
    family_values = frame["family"].replace("", pd.NA).fillna(frame["factor"])
    sortable = frame.assign(_family_key=family_values, _bad_ls=frame["neutral_long_short_h20"].fillna(0) <= 0)
    for _, group in sortable.groupby("_family_key", sort=False):
        ranked = group.sort_values(
            ["_bad_ls", "neutral_rank_ic_h20", "neutral_long_short_h20"],
            ascending=[True, False, False],
        )
        if not ranked.empty:
            representative.loc[ranked.index[0]] = True
    return representative


def _number(value) -> float:
    try:
        if pd.isna(value):
            return float("nan")
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _format_float(value) -> str:
    number = _number(value)
    if pd.isna(number):
        return ""
    return f"{number:.6g}"
