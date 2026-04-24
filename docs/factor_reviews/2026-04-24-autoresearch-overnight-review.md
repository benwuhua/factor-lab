# Autoresearch Overnight Review

- review_date: 2026-04-24
- source: `reports/autoresearch/review_analysis_20260424_night`
- run_scope: 2026-04-23 overnight Codex expression autoloop
- review_goal: rank fresh candidates, deduplicate repeated ideas, and promote only factors that add distinct signal families or materially stronger robustness

## Shortlist

| rank | factor | family | csi500 neutral H20 | csi300 neutral H20 | weakest year | weakest neutral H20 | turnover | complexity | decision |
|---:|---|---|---:|---:|---|---:|---:|---:|---|
| 1 | `quiet_close_range_divergence_20_v1` | quiet range divergence | 0.03631 | 0.03289 | 2023 | -0.00828 | 0.16726 | 0.414 | promote challenger |
| 2 | `intraday_volatility_skew_20_v1` | intraday excursion volatility | 0.03370 | 0.02831 | 2023 | 0.00589 | 0.14186 | 0.392 | promote core |
| 3 | `two_sided_excursion_convergence_20_v1` | intraday excursion volatility | 0.03427 | 0.02833 | 2021 | -0.01502 | 0.17251 | 0.404 | promote challenger |
| 4 | `open_norm_selling_pressure_reversal_20_60_v1` | selling pressure reversal | 0.03618 | 0.02180 | 2026 partial | -0.01343 | 0.15175 | 0.442 | promote reserve |
| 5 | `multi_horizon_low_norm_selling_pressure_reversal_20_60_120_v1` | selling pressure reversal | 0.03553 | 0.02369 | 2026 partial | -0.01991 | 0.13294 | 0.624 | defer |
| 6 | `open_norm_amount_weighted_pressure_reversal_20_60_v1` | selling pressure reversal | 0.03207 | 0.02717 | 2023 | -0.01444 | 0.14845 | 0.576 | defer |

## Why These Four

### `quiet_close_range_divergence_20_v1`

This is the cleanest new divergence idea from the overnight set. It holds up in both CSI500 and CSI300, and unlike the earlier price-volume divergence family it does not collapse in 2023. The csi500 regime split is also balanced enough to treat it as `all_weather`:

- down: 0.04496
- sideways: 0.03600
- up: 0.03211

### `intraday_volatility_skew_20_v1`

This is the strongest new all-weather candidate. It is positive in every yearly cut we checked, including the weakest csi500 year in 2023, and it behaves well across down, sideways, and up regimes. That makes it a good `core` representative for a new intraday excursion volatility family.

### `two_sided_excursion_convergence_20_v1`

This belongs with the same intraday excursion volatility family, but it captures a different shape: synchronized two-sided movement around the open rather than skew between upside and downside volatility. It is slightly weaker in the 2021 cut, so it fits better as a `challenger` than as another core member.

### `open_norm_selling_pressure_reversal_20_60_v1`

This reversal blend is useful, but it is clearly more regime-sensitive than the other three promotions. CSI500 is strong, CSI300 is only moderate, and the weakest csi500 segment is the short 2026 partial sample. It is worth keeping as a `reserve` candidate for down/sideways conditions, not as an all-weather production core.

## Defer / Do Not Promote Yet

- `multi_horizon_low_norm_selling_pressure_reversal_20_60_120_v1`: similar idea to the promoted selling-pressure blend, but materially more complex for only marginal benefit.
- `open_norm_amount_weighted_pressure_reversal_20_60_v1`: decent, but complexity is higher and the improvement over the simpler open-normalized version is not compelling enough.
- previously approved divergence family members remain the representative set for price-volume style weak-reversal signals.

## Governance Decision

Promote four new factors into the formal candidate pool:

1. `intraday_volatility_skew_20_v1` as `core`, `all_weather`
2. `quiet_close_range_divergence_20_v1` as `challenger`, `all_weather`
3. `two_sided_excursion_convergence_20_v1` as `challenger`, `all_weather`
4. `open_norm_selling_pressure_reversal_20_60_v1` as `reserve`, `down_sideways`

Keep the earlier divergence family approvals unchanged; this overnight pass adds breadth more than it adds another representative to the same old family.
