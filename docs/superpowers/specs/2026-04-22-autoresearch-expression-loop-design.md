# Autoresearch Expression Loop Design

## Goal

Build the first controlled autoresearch loop for Qlib Factor Lab. The loop should let an AI agent propose and test expression factors while keeping data, evaluation, reporting, and rollback rules fixed.

This is not a fully autonomous alpha factory yet. The first target is a repeatable expression-factor experiment loop that can run one candidate at a time, write a standard summary block, append an experiment ledger, and leave enough artifacts for human review.

## Current Project Baseline

The project already has the core research primitives needed for an expression loop:

- Candidate templates: `configs/factor_mining.yaml`
- Candidate generation: `src/qlib_factor_lab/factor_mining.py`
- Single factor evaluation: `src/qlib_factor_lab/factor_eval.py`, `scripts/eval_factor.py`
- Candidate screening: `scripts/mine_factors.py`
- Size-proxy neutralization: `src/qlib_factor_lab/neutralization.py`
- Event backtests for trigger-like factors: `src/qlib_factor_lab/event_backtest.py`, `scripts/backtest_factor_events.py`
- Markdown summary helpers: `src/qlib_factor_lab/reports.py`, `scripts/summarize_event_report.py`
- Current provider configs: `configs/provider_current.yaml`, `configs/provider_csi300_current.yaml`

The missing layer is not another factor formula. The missing layer is a controlled research harness that defines the search space, locks the evaluation contract, standardizes each run, records outcomes, and prevents failed experiments from polluting the stable project.

## Scope

In scope for the first version:

- Expression-factor autoresearch only.
- Daily A-share data.
- Initial locked universe: `csi500_current`.
- Fixed horizons: 5 and 20 trading days.
- Raw and size-proxy-neutralized evaluation.
- One candidate per loop run.
- Human-readable summary block.
- Append-only TSV ledger.
- Run artifact directory under `reports/autoresearch/runs/`.
- Dedicated branch or worktree for experiments.

Out of scope for the first version:

- Pattern/event autoresearch.
- Factor-combination autoresearch.
- Model training optimization.
- Broker/live-trading changes.
- Autonomous infinite loops.
- Agent edits to evaluation code.
- Using `git reset --hard` on `main`.

## Design Principles

1. The agent changes research specs, not system code.

   The editable surface should be YAML candidate/search files. Evaluation scripts and package modules are treated as the contract, not as the playground.

2. Each run is a small transaction.

   A run proposes one candidate, evaluates it against the locked contract, writes artifacts, appends the ledger, and then receives a keep/discard decision.

3. The oracle output must be compact.

   The agent should read one summary block instead of manually piecing together many CSV files. Detailed CSV artifacts still exist for audit.

4. A weak neutralized result is a warning.

   Raw Rank IC alone is not enough. The loop should always show size-proxy-neutralized metrics so liquidity or size exposure does not look like alpha by accident.

5. The ledger is research memory.

   Git history records the current source state. The ledger records failed, discarded, and crashed attempts so the agent does not repeat the same bad idea.

## Six Components

### 1. Search Space

The expression loop gets its own search files:

```text
configs/autoresearch/expression_space.yaml
configs/autoresearch/candidates/example_expression.yaml
```

`expression_space.yaml` defines what the agent may use:

- Fields: `open`, `high`, `low`, `close`, `volume`, `amount`, `turnover`
- Windows: `3`, `5`, `10`, `20`, `60`, `120`
- Operators: `Ref`, `Mean`, `Std`, `Sum`, `Max`, `Min`, `Abs`, `Corr`, `Greater`, `Less`, `And`, `If`
- Families: momentum, reversal, volatility, liquidity, turnover, divergence, price-position
- Complexity budget: maximum expression depth, maximum window count, and maximum number of binary operations

`candidates/example_expression.yaml` is the single-run editable candidate file. It should describe one candidate:

```yaml
name: mom_skip_60_5_v1
family: momentum
expression: "Ref($close, 5) / Ref($close, 60) - 1"
direction: 1
description: "60 day momentum skipping the most recent 5 sessions."
expected_behavior: "Higher values should indicate persistent medium-term strength."
```

The initial implementation should validate that every referenced field, operator, and window is allowed by `expression_space.yaml`.

### 2. Evaluation Contract

The contract lives in:

```text
configs/autoresearch/contracts/csi500_current_v1.yaml
```

The contract locks the parts the agent may not tune per run:

```yaml
name: csi500_current_v1
provider_config: configs/provider_current.yaml
universe: csi500_current
benchmark: SH000905
start_time: "2015-01-01"
end_time: "2026-04-20"
horizons: [5, 20]
metric: rank_ic_mean
neutralization:
  raw: true
  size_proxy: true
minimum_observations: 10000
artifact_root: reports/autoresearch/runs
ledger_path: reports/autoresearch/expression_results.tsv
```

The first version should treat these files as read-only during a run:

- `src/qlib_factor_lab/factor_eval.py`
- `src/qlib_factor_lab/factor_mining.py`
- `src/qlib_factor_lab/neutralization.py`
- `scripts/eval_factor.py`
- `scripts/mine_factors.py`
- `scripts/autoresearch/run_expression_loop.py`

Changing the contract is a separate human-reviewed decision, not an autoresearch loop action.

### 3. Oracle

The expression oracle is:

```text
scripts/autoresearch/run_expression_loop.py
```

It wraps existing project primitives:

1. Load the locked contract.
2. Load and validate the candidate YAML.
3. Convert the candidate to a `FactorDef`.
4. Run raw evaluation for horizons 5 and 20.
5. Run size-proxy-neutralized evaluation for horizons 5 and 20.
6. Compute a simple complexity score.
7. Write run artifacts.
8. Print a standard summary block.
9. Append the ledger.

The summary block should be stable and easy for an agent to parse:

```text
---
loop: expression
run_id: 20260422T193000_mom_skip_60_5_v1
candidate: mom_skip_60_5_v1
commit: abc1234
contract: csi500_current_v1
universe: csi500_current
horizons: 5,20
rank_ic_mean_h5: 0.0182
rank_ic_mean_h20: 0.0415
neutral_rank_ic_mean_h5: 0.0114
neutral_rank_ic_mean_h20: 0.0273
long_short_mean_return_h20: 0.0038
top_quantile_turnover_h20: 0.34
observations_h20: 981234
complexity_score: 0.18
primary_metric: 0.0273
guard_metric: 0.34
status: review
artifact_dir: reports/autoresearch/runs/expression_20260422T193000_mom_skip_60_5_v1
---
```

The oracle should not decide permanent truth by itself. It can mark `status: review`, `status: crash`, or a mechanical `status: discard_candidate` when the candidate fails validation or observations are too low.

### 4. Ledger

The expression ledger is:

```text
reports/autoresearch/expression_results.tsv
```

This file is generated research memory and should remain ignored by Git. Its columns:

```text
timestamp
run_id
commit
loop
contract
candidate_name
candidate_file
candidate_hash
status
decision_reason
primary_metric
secondary_metric
guard_metric
rank_ic_mean_h5
rank_ic_mean_h20
neutral_rank_ic_mean_h5
neutral_rank_ic_mean_h20
long_short_mean_return_h20
top_quantile_turnover_h20
observations_h20
complexity_score
artifact_dir
elapsed_sec
```

`status` starts as `review`, `discard_candidate`, or `crash`. A human or later review script may update it to `keep` or `discard`.

Each run writes artifacts under:

```text
reports/autoresearch/runs/expression_<run_id>/
  candidate.yaml
  raw_eval.csv
  neutralized_eval.csv
  summary.txt
  summary.json
```

The ledger keeps the compact history. The artifact directory keeps enough detail to reproduce and audit a specific run.

### 5. Revert Mechanism

The revert mechanism should be conservative:

- Run autoresearch on a dedicated branch or temporary worktree.
- Commit the candidate YAML before the oracle runs.
- Append the ledger after the oracle runs.
- Keep promising candidate commits.
- Discard weak candidate commits from the experiment branch only.
- Never run destructive reset commands on `main`.

The first implementation should document the intended manual workflow instead of hiding it inside an automatic destructive command:

```bash
git switch -c autoresearch/expression-mom-skip
git add configs/autoresearch/candidates/example_expression.yaml
git commit -m "try expression candidate mom_skip_60_5_v1"
python scripts/autoresearch/run_expression_loop.py \
  --contract configs/autoresearch/contracts/csi500_current_v1.yaml \
  --candidate configs/autoresearch/candidates/example_expression.yaml
```

If the result is weak, the user can drop the experiment branch or reset the branch explicitly. The tool should not surprise the user by rewriting the stable project history.

### 6. Loop Protocol

The agent-facing protocol lives in:

```text
configs/autoresearch/program_expression.md
```

It should contain the operating rules for future agents:

- You may edit only `configs/autoresearch/candidates/*.yaml` during a normal expression experiment.
- You may not edit evaluation code, provider configs, or contract files.
- Run only one candidate per loop.
- Commit the candidate before running the oracle.
- Read the oracle summary block first.
- Append or preserve the ledger even when the run is bad.
- Prefer discard when the neutralized score is weak.
- Prefer discard when complexity rises without a clear metric gain.
- Prefer discard when turnover is too high for a 5 or 20 day holding horizon.
- Do not repeat a discarded idea without a materially different hypothesis.

## Decision Rules

The first version should keep decision rules simple and visible:

- Primary metric: `neutral_rank_ic_mean_h20`
- Secondary metric: `rank_ic_mean_h20`
- Guard metric: `top_quantile_turnover_h20`
- Minimum observations: contract-defined, initially `10000`
- Complexity penalty: reject candidates that only improve by noise while materially increasing complexity

Suggested human review bands:

```text
strong_review:
  neutral_rank_ic_mean_h20 >= 0.030
  rank_ic_mean_h20 >= 0.035
  top_quantile_turnover_h20 <= 0.60

watchlist_review:
  neutral_rank_ic_mean_h20 >= 0.015
  rank_ic_mean_h20 >= 0.020

discard_bias:
  neutral_rank_ic_mean_h20 < 0.010
  or observations_h20 < minimum_observations
  or complexity_score > 0.70
```

These thresholds are not universal alpha laws. They are a starting review policy for this project's current CSI500 data and should be changed only through contract review.

## File List For Implementation

Create:

- `configs/autoresearch/contracts/csi500_current_v1.yaml`
  - Locked provider, universe, horizon, neutralization, artifact, and ledger settings.
- `configs/autoresearch/expression_space.yaml`
  - Allowed fields, operators, windows, families, and complexity limits.
- `configs/autoresearch/candidates/example_expression.yaml`
  - One editable candidate used by the first loop.
- `configs/autoresearch/program_expression.md`
  - Agent operating protocol for expression experiments.
- `reports/autoresearch/README.md`
  - Explains ignored generated ledgers and run artifacts.
- `src/qlib_factor_lab/autoresearch/__init__.py`
  - Package marker.
- `src/qlib_factor_lab/autoresearch/contracts.py`
  - Contract loading and validation.
- `src/qlib_factor_lab/autoresearch/expressions.py`
  - Candidate loading, allowed-space validation, and `FactorDef` conversion.
- `src/qlib_factor_lab/autoresearch/oracle.py`
  - Run evaluation, collect metrics, build summary block and JSON payload.
- `src/qlib_factor_lab/autoresearch/ledger.py`
  - Append-only TSV writer.
- `scripts/autoresearch/run_expression_loop.py`
  - CLI entry point for one expression candidate run.
- `tests/test_autoresearch_contracts.py`
  - Contract validation tests.
- `tests/test_autoresearch_expressions.py`
  - Candidate and search-space validation tests.
- `tests/test_autoresearch_ledger.py`
  - Ledger append tests.
- `tests/test_autoresearch_oracle.py`
  - Summary payload tests using small fake evaluation frames.

Modify:

- `.gitignore`
  - Ensure `reports/autoresearch/*` is ignored except `reports/autoresearch/README.md`.
- `Makefile`
  - Add `autoresearch-expression` target.
- `README.md`
  - Add a short "Autoresearch" section after Candidate Mining or Event Backtests.
- `docs/factor-research-path.md`
  - Link the autoresearch loop as an optional controlled path.

## Testing Strategy

Unit tests should not require downloaded Qlib data:

- Validate a contract with required fields.
- Reject a candidate that uses a disallowed field.
- Reject a candidate that uses a disallowed operator.
- Reject a candidate that uses a disallowed window.
- Convert a valid candidate to `FactorDef`.
- Append a ledger row and preserve TSV headers.
- Render a summary block from fake raw and neutralized metrics.

Data-dependent integration tests should be manual:

```bash
python scripts/autoresearch/run_expression_loop.py \
  --contract configs/autoresearch/contracts/csi500_current_v1.yaml \
  --candidate configs/autoresearch/candidates/example_expression.yaml
```

Expected result:

- A summary block is printed.
- A run directory is created.
- Raw and neutralized evaluation CSVs are written.
- `reports/autoresearch/expression_results.tsv` receives one row.

## Future Pattern Loop

The pattern loop should be added after the expression loop is stable. It should use the same six-component shape but call event-oriented evaluators:

- Search space: `configs/autoresearch/pattern_space.yaml`
- Contract: fixed bucket, horizon, confirmation, and minimum trade-count settings
- Oracle: `scripts/autoresearch/run_pattern_loop.py`
- Ledger: `reports/autoresearch/pattern_results.tsv`
- Protocol: `configs/autoresearch/program_pattern.md`

Important pattern metrics:

- `trade_count`
- `horizon_5_mean_return`
- `horizon_20_mean_return`
- `horizon_20_win_rate`
- `horizon_20_payoff`
- `mfe_mean`
- `mae_mean`
- `complexity_score`

This is the right home for Wangji-like event factors. It should not be mixed into the expression-loop MVP.

## Future Combo Loop

The combo loop should come last because it is easiest to overfit. It should combine already-reviewed candidates rather than invent arbitrary formulas:

- Search space: `configs/autoresearch/combo_space.yaml`
- Contract: fixed maximum factor count, standardization, redundancy, and neutralization rules
- Oracle: `scripts/autoresearch/run_combo_loop.py`
- Ledger: `reports/autoresearch/combo_results.tsv`
- Protocol: `configs/autoresearch/program_combo.md`

Important combo metrics:

- `rank_ic_mean_h20`
- `neutralized_rank_ic_mean_h20`
- `event_mean_return_p95_h20` when event gating is enabled
- `redundancy_score`
- `complexity_score`

The combo loop should require a combination to be more stable than its members, not merely better in one lucky window.

## Implementation Order

1. Add the expression contract, search-space spec, example candidate, and protocol document.
2. Add contract/candidate validation modules with unit tests.
3. Add ledger writer with unit tests.
4. Add oracle summary builder with fake-frame unit tests.
5. Add the CLI wrapper.
6. Run a local CSI500 smoke run if data is present.
7. Document the command in `README.md` and `docs/factor-research-path.md`.

This order keeps the first step useful even before the data-heavy evaluation is run, and it makes each component testable without depending on the local Qlib data directory.
