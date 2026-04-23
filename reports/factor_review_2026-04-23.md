# Factor Review

- generated_at: 2026-04-23T00:00:00
- approved_count: 5

## Approved Factors

| factor | status | family | regime | universes | neutral_h20_500 | neutral_h20_300 | weakest_year | redundancy |
|---|---|---|---|---|---:|---:|---|---|
| `high_mean60_discount_volume_divergence_reversal_20_60_v1` | core | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.04005 | 0.04488 | 2023 | F001 |
| `fast_high_60d_discount_volume_divergence_reversal_10_60_v1` | challenger | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.03452 | 0.04338 | 2023 | F001 |
| `high_norm_price_volume_divergence_20_v1` | challenger | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.03449 | 0.04229 | 2023 | F001 |
| `normalized_price_volume_divergence_20_v1` | reserve | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.03195 | 0.04076 | 2023 | F001 |
| `high_norm_price_amount_divergence_20_v1` | reserve | divergence_weak_reversal | down_sideways | csi500_current,csi300_current | 0.03003 | 0.03926 | 2023 | F001 |

## Redundancy Groups

- F001: `high_mean60_discount_volume_divergence_reversal_20_60_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=1.000
- F001: `fast_high_60d_discount_volume_divergence_reversal_10_60_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=0.833
- F001: `high_norm_price_volume_divergence_20_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=0.818
- F001: `normalized_price_volume_divergence_20_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=0.818
- F001: `high_norm_price_amount_divergence_20_v1` representative=`high_mean60_discount_volume_divergence_reversal_20_60_v1` similarity=0.818

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
