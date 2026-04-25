# Factor Purification and Exposure Attribution Design

## Goal

Add a lightweight AlphaPurify-inspired research layer to factor-lab without taking a runtime dependency on AlphaPurify. The layer should make factor evaluation more mature by adding explicit factor purification before diagnostics and exposure attribution after portfolio construction.

## Scope

This first version is intentionally conservative:

- Support CSI300 and CSI500 research data already configured in the project.
- Add deterministic factor purification helpers for MAD winsorization, z-score standardization, rank standardization, and OLS residual neutralization.
- Add portfolio exposure attribution helpers that explain a target portfolio or signal table by factor-family, industry, and liquidity/size style exposure.
- Add a CLI report generator that can run on an existing daily signal or target portfolio CSV.

Out of scope:

- Importing AlphaPurify as a dependency.
- Porting AlphaPurify's database layer.
- Adding machine-learning neutralizers such as random forest, GBDT, ICA, or PCA.
- Replacing existing Qlib factor evaluation or trading constraint logic.

## Architecture

The implementation adds two focused modules:

- `qlib_factor_lab.factor_purification`: cross-sectional transformations over `(datetime, instrument)` frames. These helpers are small, deterministic, and reusable by evaluation/oracle scripts.
- `qlib_factor_lab.exposure_attribution`: report-oriented aggregation over selected portfolios or signal tables. It computes weighted factor-family contributions, style exposure, industry concentration, and residual/unknown exposure.

The new code does not change existing production behavior by default. It provides explicit functions and one CLI entry point so we can wire it into autoresearch or portfolio gates incrementally.

## Data Flow

```text
raw factor values
  -> purify_factor_frame(...)
  -> existing factor_eval / diagnostics

daily signal or target portfolio
  -> build_exposure_attribution(...)
  -> write CSV + markdown report
```

## Testing

Tests cover:

- MAD winsorization caps cross-sectional outliers by date.
- z-score and rank standardization are cross-sectional and stable.
- OLS neutralization removes linear exposure.
- Exposure attribution sums factor contribution columns by family.
- Industry concentration and style exposure are weight-aware.

## Safety

The implementation is additive and does not alter current daily pipeline outputs. The report can be used by portfolio gate later, but this change only creates the capability and a script-level artifact.
