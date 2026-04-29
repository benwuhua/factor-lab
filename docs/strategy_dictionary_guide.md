# Strategy Dictionary Guide

The strategy dictionary is an inspiration layer for factor-lab. It translates broad strategy archetypes from external references such as *151 Trading Strategies* into A-share research prompts that can be tested by the platform.

It is not evidence that a strategy works in A shares. Every proposal must pass point-in-time data checks, factor diagnostics, event review, portfolio gates, and transaction-cost-aware validation before it can influence a portfolio.

## How It Fits

```text
strategy dictionary
-> proposal script
-> autoresearch candidate spec
-> factor diagnostics / event diagnostics
-> ledger
-> combo construction
-> portfolio gate
```

The dictionary helps answer three questions:

1. Which strategy families are missing from the current research pool?
2. Is a new candidate just a duplicate of an existing factor?
3. Which lane should explore the idea: expression, fundamental, theme, pattern, or combo?

## Dictionary Fields

- `strategy_id`: stable identifier.
- `strategy_family`: grouping used for diversity and redundancy checks.
- `candidate_lane`: where autoresearch should explore it.
- `template_formula`: human-readable template, not production code.
- `required_data`: data domains needed before implementation.
- `a_share_transferability`: high, medium, or low.
- `risk_notes`: A-share-specific caveats.
- `related_factors`: existing or planned factor names.

## First Dictionary

The first dictionary lives at:

```text
configs/strategy_dictionary/151_trading_strategies_equity.yaml
```

It intentionally extracts only equity-like ideas that are plausible for A-share research:

- price momentum
- earnings momentum
- value
- low volatility
- multifactor portfolio
- residual momentum
- pairs trading
- cluster mean reversion
- moving average trend
- support/resistance channel
- sector momentum rotation
- alpha combos

## CLI Usage

Generate proposals against the current balanced combo:

```bash
.venv/bin/python scripts/autoresearch/propose_from_strategy_dictionary.py
```

Generate only expression-lane ideas:

```bash
.venv/bin/python scripts/autoresearch/propose_from_strategy_dictionary.py --lane expression --limit 5
```

Outputs:

```text
reports/strategy_dictionary_proposals.md
reports/strategy_dictionary_proposals.csv
```

## Research Rules

- Treat the dictionary as a search-space seed, not as a signal source.
- Prefer missing families before over-optimizing existing families.
- Keep weak but low-correlation families in the candidate pool until formal diagnostics reject them.
- Never let dictionary proposals bypass A-share PIT rules, liquidity rules, event risk, or portfolio gates.
- If a proposal requires data the platform does not yet govern, mark it as blocked until the data governance layer is added.

## Practical Next Candidates

The current platform already has value, dividend, quality gates, gap risk, and pattern ideas. The next useful dictionary-inspired candidates are:

1. `low_vol_120`
2. `momentum_120_skip_20`
3. `residual_momentum_120_skip_20`
4. `sector_momentum_60`
5. `industry_residual_reversal_20`

