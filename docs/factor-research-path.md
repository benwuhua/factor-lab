# Minimal Factor Research Path

This example shows the shortest repeatable path from candidate factors to an event-backtest note. It assumes the local CSI500 or CSI300 Qlib data has already been built; see `docs/data-and-artifacts.md` for data setup.

## 1. Check The Local Environment

```bash
make check-env
```

This verifies the configured provider path and Python dependencies.

## 2. Generate Candidate Factors

```bash
make candidates
```

Output:

```text
reports/factor_mining_candidates_current.csv
```

Use this file to confirm that the factor is present and that its expression, category, and direction match the intended logic.

## 3. Run A Broad CSI500 IC Screen

```bash
make mine-csi500
```

Output:

```text
reports/factor_mining_current_h5_h20.csv
```

Use this for broad triage. IC and Rank IC are useful for cross-sectional features; they are less conclusive for absolute pattern or trigger factors.

## 4. Optional Controlled Autoresearch Loop

To test one expression candidate inside a locked research contract:

```bash
make autoresearch-expression
```

Default candidate:

```text
configs/autoresearch/candidates/example_expression.yaml
```

The loop writes generated artifacts under `reports/autoresearch/` and appends `expression_results.tsv`. These outputs are local research memory and are ignored by Git.

Summarize the local ledger:

```bash
make autoresearch-ledger
```

For an overnight Codex CLI loop, create an experiment branch first because the runner commits one candidate per iteration:

```bash
git switch -c autoresearch/nightly-$(date +%Y%m%d)
tmux new -s factor-night
make autoresearch-codex-loop AUTORESEARCH_CODEX_UNTIL=08:30 AUTORESEARCH_CODEX_ITERATIONS=30
```

This uses the local `codex` ChatGPT login, not an API key. Logs are written under `reports/autoresearch/codex_loop/`.

## 5. Run An Event Backtest

```bash
make event-csi300 FACTOR=arbr_26
```

Default output paths:

```text
reports/factor_arbr_26_event_backtest_trades_csi300.csv
reports/factor_arbr_26_event_backtest_summary_csi300.csv
reports/factor_arbr_26_event_backtest_yearly_csi300.csv
```

Event backtests apply the factor's configured `direction` before percentile bucketing. The `p95_p100` bucket means the highest configured score, not necessarily the highest raw factor value.

## 6. Generate A Markdown Summary

```bash
make summarize-event \
  FACTOR=arbr_26 \
  SUMMARY=reports/factor_arbr_26_event_backtest_summary_csi300.csv \
  SUMMARY_MD=reports/factor_arbr_26_event_backtest_summary_csi300.md
```

The Markdown file is a local artifact by default. Copy the useful parts into an issue, research memo, or design review if the result is worth preserving.

## 7. Promotion Checklist

Before a factor graduates into a model workflow or live-trading design, check:

- Multiple horizons, usually 5-day and 20-day.
- Yearly split, including weak market years.
- CSI300 and CSI500 comparison when the factor is meant to generalize.
- Direction sanity: the p95 bucket should represent the configured good side.
- Transaction-cost and turnover sensitivity.
- Whether the factor works as a standalone signal, a filter, or a model feature.
