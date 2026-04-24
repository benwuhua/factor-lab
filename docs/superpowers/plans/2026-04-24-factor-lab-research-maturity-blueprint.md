# Factor Lab Research Maturity Blueprint

> **For agentic workers:** REQUIRED: Use superpowers:test-driven-development for implementation changes. This blueprint defines the next stage after the first daily portfolio bundle.

**Goal:** Move factor-lab from a single style factor basket toward a multi-lane research system with single-factor gates, family-first combination, and expert review packets.

**Architecture:** Autoresearch should generate candidates in separate lanes. Single-factor diagnostics decide what can enter the approved pool. Portfolio construction should combine family scores rather than every individual factor. Expert LLM review is a research-control layer, not a return oracle.

**Tech Stack:** Python, pandas, Qlib expression factors, YAML configs, Markdown run artifacts.

---

## 1. Diagnosis

The current approved pool is too concentrated:

- 6 divergence factors
- 2 intraday volatility factors
- 1 reversal factor

This is still useful for paper trading, but it is not a mature portfolio engine. The highest risk is that several similar price-volume factors agree with each other and make the portfolio look more diversified than it really is.

## 2. Multi-Lane Autoresearch

Autoresearch should run multiple lanes instead of one broad expression search.

| lane | cadence | role | primary metrics |
|---|---|---|---|
| expression_price_volume | daily | normal cross-sectional alpha | neutral IC, ICIR, long-short |
| pattern_event | daily | Wangji-like setups and event triggers | event return, payoff, MFE/MAE |
| risk_structure | daily | downside and drawdown quality | neutral IC, downside capture |
| liquidity_microstructure | daily | volume and liquidity behavior | H5/H20 IC, slippage proxy |
| fundamental_quality | weekly | value/quality/growth once data exists | H20/H60 stability |
| regime | daily | controls factor activation, not stock selection | drawdown reduction, activation accuracy |

Config anchor: `configs/autoresearch/lane_space.yaml`.

## 3. Single-Factor Gate

Every candidate must pass single-factor diagnostics before it can influence the daily score.

Required checks:

- raw Rank IC and ICIR
- size-proxy neutralized Rank IC and ICIR
- neutralized long-short return
- neutral retention versus raw
- top quantile turnover
- family assignment
- concerns such as negative neutral long-short or high turnover

New artifacts:

- `reports/single_factor_diagnostics_*.csv`
- `reports/single_factor_diagnostics_*.md`

New script:

```bash
python scripts/summarize_single_factor_eval.py \
  --raw-eval reports/single_factor_eval_promoted_raw_20260423.csv \
  --neutral-eval reports/single_factor_eval_promoted_size_neutral_20260423.csv \
  --approved-factors reports/approved_factors.yaml \
  --output-csv reports/single_factor_diagnostics_20260423.csv \
  --output-md reports/single_factor_diagnostics_20260423.md
```

## 4. Family-First Combination

Do not sum all approved factors directly.

Recommended path:

1. Score every factor independently.
2. Pick or build one representative score per family.
3. Cap each family weight.
4. Combine family scores into final daily score.
5. Keep reserve factors in shadow mode unless promoted.

Initial family caps:

- max single family weight: 35%
- max single factor weight: 20%
- reserve factors: shadow only

This prevents F001-like clusters from overwhelming the portfolio.

## 5. Expert LLM Review

Expert LLM review should answer research-quality questions, not predict returns.

It should review:

- whether the portfolio is dominated by one factor family
- whether top stocks look like factor misfires
- liquidity and tradability concerns
- market regime mismatch
- names needing manual chart or fundamental review

New script:

```bash
python scripts/build_expert_review_packet.py \
  --target-portfolio runs/20260423/target_portfolio.csv \
  --factor-diagnostics reports/single_factor_diagnostics_20260423.csv \
  --run-date 2026-04-23 \
  --output runs/20260423/expert_review_packet.md
```

Expected LLM decision values:

- `pass`
- `caution`
- `reject`

## 6. Near-Term Execution Order

1. Generate single-factor diagnostics for current promoted factors.
2. Use the diagnostics to demote factors with negative neutral long-short to shadow.
3. Change daily scoring from factor-first to family-first.
4. Add expert review packet to the daily run bundle.
5. Only then revisit portfolio optimization.

## 7. Non-Goals

- Do not let the expert LLM override hard risk checks.
- Do not use LLM comments as proof of alpha.
- Do not optimize factor weights on one recent window.
- Do not add fundamental lanes until the data source is explicit and reproducible.
