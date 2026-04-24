# Factor Review

- generated_at: 2026-04-24T00:00:00
- approved_count: 9

## Approved Factors

| factor | status | family | regime | universes | neutral_h20_500 | neutral_h20_300 | weakest_year | redundancy |
|---|---|---|---|---|---:|---:|---|---|
| `high_mean60_discount_volume_divergence_reversal_20_60_v1` | core | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.04005 | 0.04488 | 2023 | F001 |
| `fast_high_60d_discount_volume_divergence_reversal_10_60_v1` | challenger | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.03452 | 0.04338 | 2023 | F001 |
| `high_norm_price_volume_divergence_20_v1` | challenger | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.03449 | 0.04229 | 2023 | F001 |
| `normalized_price_volume_divergence_20_v1` | reserve | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.03195 | 0.04076 | 2023 | F001 |
| `high_norm_price_amount_divergence_20_v1` | reserve | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.03003 | 0.03926 | 2023 | F001 |
| `quiet_close_range_divergence_20_v1` | challenger | quiet_range_divergence | all_weather | csi500_current,csi300_current | 0.03631 | 0.03289 | 2023 | F002 |
| `intraday_volatility_skew_20_v1` | core | intraday_excursion_volatility | all_weather | csi500_current,csi300_current | 0.03370 | 0.02831 | 2023 | F003 |
| `two_sided_excursion_convergence_20_v1` | challenger | intraday_excursion_volatility | all_weather | csi500_current,csi300_current | 0.03427 | 0.02833 | 2021 | F003 |
| `open_norm_selling_pressure_reversal_20_60_v1` | reserve | selling_pressure_reversal | down_sideways | csi500_current,csi300_current | 0.03618 | 0.02180 | 2026 | F004 |

## Redundancy Groups

- F001: `high_mean60_discount_volume_divergence_reversal_20_60_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=1.000
- F001: `fast_high_60d_discount_volume_divergence_reversal_10_60_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=0.833
- F001: `high_norm_price_volume_divergence_20_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=0.818
- F001: `normalized_price_volume_divergence_20_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=0.818
- F001: `high_norm_price_amount_divergence_20_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=0.818
- F002: `quiet_close_range_divergence_20_v1` representative=`quiet_close_range_divergence_20_v1` similarity=1.000
- F003: `intraday_volatility_skew_20_v1` representative=`intraday_volatility_skew_20_v1` similarity=1.000
- F003: `two_sided_excursion_convergence_20_v1` representative=`intraday_volatility_skew_20_v1` similarity=0.700
- F004: `open_norm_selling_pressure_reversal_20_60_v1` representative=`open_norm_selling_pressure_reversal_20_60_v1` similarity=1.000

## Review Notes

### high_mean60_discount_volume_divergence_reversal_20_60_v1

Core representative for the promoted divergence/weak-reversal family. Strongest cross-universe size-neutral H20 result, but 2023 diagnostics show up-regime weakness; use as down/sideways eligible until monitored live-like signal evidence says otherwise.

### fast_high_60d_discount_volume_divergence_reversal_10_60_v1

Faster 10/60 challenger. Strong 300 robustness, but turnover is materially higher than the core representative.

### high_norm_price_volume_divergence_20_v1

Pure high-normalized price/volume divergence challenger. Cleaner and simpler than the core combo, but 2023 was the weakest year.

### normalized_price_volume_divergence_20_v1

Close-normalized reserve variant. Useful for monitoring whether close-based divergence behaves better than high-based divergence.

### high_norm_price_amount_divergence_20_v1

Amount-based reserve variant. Keep for comparison against volume-based divergence but do not treat as an independent core signal yet.

### quiet_close_range_divergence_20_v1

Quiet accumulation style divergence candidate. Unlike the earlier price-volume divergence family, this signal stays usable through the 2023 weak patch and remains positive across down, sideways, and up regime cuts.

### intraday_volatility_skew_20_v1

Core representative for the intraday excursion volatility family. It is the cleanest new all-weather signal from the overnight batch, with positive yearly diagnostics even in its weakest csi500 year.

### two_sided_excursion_convergence_20_v1

Challenger for the intraday excursion volatility family. It adds a distinct two-sided instability signature around the open, with solid cross-universe stability but a weaker 2021 cut than the core skew variant.

### open_norm_selling_pressure_reversal_20_60_v1

Reserve representative for selling-pressure reversal. The signal is strong enough to keep, but its cross-universe robustness is weaker and its weakest csi500 cut is a short 2026 partial sample, so it should stay regime-gated.
