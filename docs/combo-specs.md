# Combo Specs

Combo specs are the promotion layer between autoresearch candidates and the formal daily pipeline.

Autoresearch can still explore lanes independently, but a candidate should only move toward portfolio output after it is written as a governed combo spec under `configs/combo_specs/`. The daily pipeline then runs the same formal chain for the combo:

```text
combo spec
-> qlib/fundamental exposures
-> daily signal
-> tradability
-> target portfolio
-> stock cards
-> expert review
-> portfolio gate
-> paper orders, only if gates pass
```

## Run

```bash
.venv/bin/python scripts/run_daily_pipeline.py \
  --combo-spec configs/combo_specs/quality_gap_breakout_v1.yaml
```

Use `--exposures-csv` for fixture or offline validation. Without `--exposures-csv`, market members are fetched from the configured Qlib provider and fundamental members are joined from `data/fundamental_quality.csv` using `available_at <= run_date`.

## First Promoted Candidate

`quality_gap_breakout_v1` is intentionally conservative:

- `quality_low_leverage` uses point-in-time ROE with a small low-leverage stabilizer.
- `gap_risk_20` penalizes names with unstable overnight gaps.
- `quiet_breakout_20` keeps a modest repair/breakout component.

It is not a final production portfolio. It is a formal research candidate that now receives the same stock-card, event-risk, expert-review, and portfolio-gate treatment as the normal daily pipeline.

## Balanced Fundamental Candidate

`balanced_multifactor_v1` is the first broader multi-factor candidate pool:

- Active today: quality/low leverage, growth improvement, cashflow quality, low gap risk, quiet breakout.
- Shadow until data governance is ready: value via `ep/cfp`, dividend via `dividend_yield`.

The shadow members are deliberately present in the spec but excluded from scoring. This keeps the target architecture visible while preventing missing PIT valuation/dividend data from silently entering the daily portfolio.

Run it with:

```bash
.venv/bin/python scripts/run_daily_pipeline.py \
  --combo-spec configs/combo_specs/balanced_multifactor_v1.yaml
```

## Manual Expert Confirmation

When expert review returns `caution` or names hard manual-review items, the run is blocked unless manual confirmation is recorded.

Use this only after reading `expert_review_result.md`, `stock_cards.jsonl`, event context, and charts:

```bash
.venv/bin/python scripts/run_daily_pipeline.py \
  --combo-spec configs/combo_specs/quality_gap_breakout_v1.yaml \
  --expert-manual-confirm \
  --expert-reviewer ryan \
  --expert-confirm-reason "checked event risk, charts, and liquidity"
```

The manifest records the confirmation and the selected rows receive `expert_manual_confirmed` in `risk_flags`.
