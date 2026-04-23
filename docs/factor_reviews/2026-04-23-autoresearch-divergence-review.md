# Autoresearch Divergence Family Review

- review_date: 2026-04-23
- source: overnight expression autoresearch plus 300/500 stability review
- family: autoresearch divergence / weak reversal
- primary_horizon: 20

## Decision

Approve the promoted `autoresearch_divergence` factors as the first alpha governance family.

The family is useful enough for production-candidate monitoring, but it is not all-weather. The approved regime profile is `down_sideways` because 2023 diagnostics showed a clear failure in up-regime / theme-led conditions.

## Core Evidence

| factor | status | csi500 neutral H20 | csi300 neutral H20 | weakest year | weakest neutral H20 | turnover profile |
|---|---|---:|---:|---|---:|---|
| `high_mean60_discount_volume_divergence_reversal_20_60_v1` | core | 0.04005 | 0.04488 | 2023 | -0.03934 | medium |
| `fast_high_60d_discount_volume_divergence_reversal_10_60_v1` | challenger | 0.03452 | 0.04338 | 2023 | -0.03528 | high |
| `high_norm_price_volume_divergence_20_v1` | challenger | 0.03449 | 0.04229 | 2023 | -0.04623 | medium |
| `normalized_price_volume_divergence_20_v1` | reserve | 0.03195 | 0.04076 | 2023 | -0.03541 | medium |
| `high_norm_price_amount_divergence_20_v1` | reserve | 0.03003 | 0.03926 | 2023 | -0.04584 | medium |

## 2023 Failure Notes

The 2023 weak year was not random noise. Component diagnostics showed two overlapping issues:

- The price-volume divergence leg reversed in 2023. `div_high_volume_20` had 2023 size-neutral H20 Rank IC around -0.046 in CSI500 and -0.045 in CSI300.
- Some reversal / selling-pressure legs had positive raw Rank IC but became negative after size-proxy neutralization, indicating dependence on size or liquidity exposure rather than clean alpha.
- The family was especially weak in up-regime segments. The top combo had roughly -0.128 size-neutral H20 Rank IC in 2023 CSI500 up-regime and roughly -0.125 in CSI300 up-regime.

## Governance Notes

- Use `high_mean60_discount_volume_divergence_reversal_20_60_v1` as the core representative.
- Keep the faster 10/60 variant as challenger because it has stronger 300 performance but higher turnover.
- Keep pure divergence variants as reserve/challenger evidence, not as independent core factors.
- Route the family through `down_sideways` regime eligibility until signal-layer monitoring proves all-weather behavior.
