# Factor To Live Trading Design

## Goal

Build a staged research-to-live workflow for the current Qlib Factor Lab project. The workflow turns candidate factors into reviewed daily orders through explicit quality gates, simulation, risk controls, and audit logs.

This design does not make the project an automatic trading system on day one. The first production target is a human-reviewed paper/live assistant that produces target positions and order suggestions. Broker execution can be added only after the simulation and risk gates are stable.

## Current Project Baseline

The project already contains these working pieces:

- Current A-share data builder using AkShare: `scripts/build_akshare_qlib_data.py`
- Qlib provider configs: `configs/provider.yaml`, `configs/provider_current.yaml`
- Factor registry and candidate mining: `factors/registry.yaml`, `configs/factor_mining.yaml`, `src/qlib_factor_lab/factor_mining.py`
- Single and batch factor evaluation: `scripts/eval_factor.py`, `scripts/batch_eval_factors.py`
- Neutralization helpers: `src/qlib_factor_lab/neutralization.py`
- LightGBM workflow rendering and Qlib backtest: `scripts/run_lgb_workflow.py`, `src/qlib_factor_lab/model_workflow.py`

The current data and factor framework is good enough for research and paper trading. Before real trading, the project needs stronger data quality, tradability filters, position construction, execution simulation, and monitoring.

## Scope

In scope:

- Daily A-share equity strategy workflow.
- Initial universe: `csi500_current`.
- Data frequency: daily bars.
- Alpha sources: expression factors and Qlib model predictions.
- Output: daily target portfolio, order list, risk report, audit bundle.
- Execution modes: research, paper trading, manual-live, and broker-live later.

Out of scope for the first live-ready design:

- Intraday alpha.
- High frequency execution.
- Options, futures, margin financing, short selling.
- Fully unattended broker execution.
- Portfolio optimization requiring commercial risk models.

## Operating Modes

### Mode 1: Research

Purpose: discover and validate factors.

Inputs:

- Qlib provider data.
- Factor registry.
- Mining templates.
- Train, validation, and test windows.

Outputs:

- Factor candidate table.
- Single-factor IC reports.
- Factor stability report.
- Backtest artifacts.

Allowed actions:

- Read historical data.
- Generate reports.
- Run model training and backtests.

Not allowed:

- Produce real orders.
- Touch broker accounts.

### Mode 2: Paper Trading

Purpose: run the live workflow every trading day without sending real orders.

Inputs:

- Latest market data.
- Latest approved factor/model config.
- Yesterday's paper portfolio state.

Outputs:

- Target positions.
- Paper orders.
- Simulated fills.
- Daily performance and drift report.

Allowed actions:

- Generate target holdings.
- Simulate orders using conservative fill assumptions.
- Record what would have been traded.

Not allowed:

- Real broker order submission.

### Mode 3: Manual Live

Purpose: produce orders that a human reviews and enters manually.

Inputs:

- Same as paper trading.
- Real current holdings imported from broker export.

Outputs:

- Human-readable order ticket.
- Risk checklist.
- Reject list with reasons.

Allowed actions:

- Produce signed order files.
- Require human approval before execution.

Not allowed:

- API-based broker order submission.

### Mode 4: Broker Live

Purpose: submit approved orders to a broker adapter.

Entry criteria:

- At least 30 trading days of paper trading.
- Zero critical data failures.
- Manual-live workflow used successfully for at least 10 trading days.
- Slippage and rejected orders within predefined limits.
- Human kill switch tested.

Allowed actions:

- Submit orders through a broker adapter.
- Cancel orders.
- Reconcile fills and positions.

Not allowed:

- Trading if any pre-trade gate fails.

## End-To-End Workflow

```text
Data Refresh
  -> Data Quality Gate
  -> Universe And Tradability Filters
  -> Factor Calculation
  -> Factor Quality Gate
  -> Signal Combination
  -> Portfolio Construction
  -> Pre-Trade Risk Gate
  -> Order Generation
  -> Paper Or Manual Execution
  -> Reconciliation
  -> Daily Report
  -> Monitoring And Review
```

## Component Design

### 1. Data Layer

Responsibilities:

- Download or refresh market data.
- Convert raw bars into Qlib format.
- Keep raw source data separate from Qlib bin data.
- Store data build metadata.

Existing files:

- `src/qlib_factor_lab/akshare_data.py`
- `scripts/build_akshare_qlib_data.py`
- `configs/provider_current.yaml`

Future files:

- `src/qlib_factor_lab/data_quality.py`
- `scripts/check_data_quality.py`
- `reports/data_quality_YYYYMMDD.csv`

Data quality checks:

- Calendar completeness.
- Duplicate rows.
- Missing `open`, `high`, `low`, `close`, `volume`, `amount`.
- Negative prices or volume.
- `high < low`, `close > high`, `close < low`.
- Abnormal one-day returns.
- Long suspension-like gaps.
- AkShare download failures by symbol.

Gate:

- Research mode may continue with warnings.
- Paper/live modes fail closed if current trading date data is missing or if critical fields are invalid.

### 2. Universe And Tradability Layer

Responsibilities:

- Start from the configured universe.
- Remove names that should not be traded.
- Produce the final daily tradable universe.

Initial filters:

- Exclude missing current-day bar.
- Exclude zero volume or zero amount.
- Exclude price-limit locked names when order direction is blocked.
- Exclude instruments with too few historical bars.
- Exclude configured blacklist.

Future filters:

- ST and delisting-risk filter.
- Listing age filter.
- Suspension calendar.
- Min average traded amount filter.
- Max participation rate filter.

Future files:

- `configs/trading.yaml`
- `src/qlib_factor_lab/universe.py`
- `src/qlib_factor_lab/tradability.py`
- `reports/tradable_universe_YYYYMMDD.csv`

Gate:

- If tradable universe count is below a configured floor, no new buys are allowed.

### 3. Factor Research Layer

Responsibilities:

- Generate candidate factor expressions.
- Evaluate IC, RankIC, ICIR, quantile returns, and turnover.
- Rank factors by stability and economic usability.

Existing files:

- `configs/factor_mining.yaml`
- `factors/registry.yaml`
- `src/qlib_factor_lab/factor_mining.py`
- `src/qlib_factor_lab/factor_eval.py`
- `scripts/mine_factors.py`
- `scripts/eval_factor.py`

Factor approval criteria:

- Direction is normalized so higher score means more bullish.
- Sufficient observations.
- Stable RankIC across train, validation, and test.
- Quantile returns are directionally sensible.
- Turnover is not too high for the intended holding period.
- The factor has a plausible rationale.
- Correlation to existing approved factors is below the redundancy threshold.

Future files:

- `src/qlib_factor_lab/factor_selection.py`
- `scripts/select_factors.py`
- `reports/approved_factors.yaml`

Gate:

- A factor cannot enter live scoring until it is selected into `approved_factors.yaml` with direction, category, horizon, evidence path, and approval date.

### 4. Signal Layer

Responsibilities:

- Convert approved factors and model predictions into a daily stock score.
- Support both transparent score blending and ML prediction.

Signal methods:

- Rule score: z-scored approved factors with ICIR or equal weights.
- ML score: LightGBM prediction from Qlib workflow.
- Ensemble score: weighted blend of rule score and ML score.

Initial recommendation:

- Use rule score first for paper trading.
- Keep LightGBM as a parallel challenger model until it beats the rule score after costs.

Future files:

- `configs/signal.yaml`
- `src/qlib_factor_lab/signal.py`
- `scripts/build_daily_signal.py`
- `reports/signals_YYYYMMDD.csv`

Gate:

- Daily score distribution must be sane: non-empty, enough coverage, no all-constant scores, no extreme concentration from one factor.

### 5. Portfolio Construction Layer

Responsibilities:

- Transform daily scores into target weights.
- Apply position, industry, turnover, and liquidity constraints.

Initial portfolio method:

- TopK equal-weight portfolio.
- Default K: 50.
- Daily rebalance with dropout.
- Max new buys per day.
- Max single-name weight.
- Cash buffer.

Future advanced method:

- Score-weighted portfolio.
- Risk-constrained optimizer.
- Industry-neutral or benchmark-aware active weights.

Future files:

- `configs/portfolio.yaml`
- `src/qlib_factor_lab/portfolio.py`
- `scripts/build_target_portfolio.py`
- `reports/target_portfolio_YYYYMMDD.csv`

Gate:

- Target portfolio must pass concentration, turnover, and liquidity checks before order generation.

### 6. Execution And Order Layer

Responsibilities:

- Compare current holdings to target holdings.
- Generate orders.
- Apply pre-trade checks.
- Simulate or route orders depending on mode.

Order fields:

- `date`
- `instrument`
- `side`
- `target_weight`
- `current_weight`
- `target_shares`
- `current_shares`
- `order_shares`
- `reference_price`
- `estimated_notional`
- `reason`
- `risk_flags`

Initial execution assumptions:

- Paper mode uses next open or conservative VWAP proxy.
- Manual-live mode exports CSV and Markdown ticket.
- Broker-live mode is disabled by default.

Future files:

- `configs/execution.yaml`
- `src/qlib_factor_lab/orders.py`
- `src/qlib_factor_lab/paper_broker.py`
- `src/qlib_factor_lab/broker_base.py`
- `scripts/generate_orders.py`
- `reports/orders_YYYYMMDD.csv`
- `reports/order_ticket_YYYYMMDD.md`

Gate:

- No order can be generated for non-tradable instruments.
- Buy orders fail closed on limit-up or insufficient liquidity.
- Sell orders fail closed on limit-down or suspension.
- Any order above configured notional or participation rate requires manual override.

### 7. State And Reconciliation Layer

Responsibilities:

- Track holdings, cash, target portfolio, orders, fills, and rejected trades.
- Reconcile paper/live account state after each run.

Storage:

- Lightweight CSV or SQLite for first version.
- One dated artifact folder per run.

Suggested layout:

```text
runs/YYYYMMDD/
  data_quality.csv
  tradable_universe.csv
  signals.csv
  target_portfolio.csv
  orders.csv
  fills.csv
  risk_report.md
  manifest.json
```

Future files:

- `src/qlib_factor_lab/state.py`
- `src/qlib_factor_lab/reconcile.py`
- `scripts/reconcile_account.py`

Gate:

- Live mode cannot start if current holdings are missing or cannot be reconciled.

### 8. Risk Layer

Responsibilities:

- Enforce pre-trade and post-trade limits.
- Produce explanations for rejected orders.
- Provide a kill switch.

Pre-trade checks:

- Max gross exposure.
- Max single-name weight.
- Max daily turnover.
- Max number of buys and sells.
- Min average traded amount.
- Max participation rate.
- Limit-up and limit-down restrictions.
- Blacklist and manual lock list.
- Cash buffer.

Post-trade checks:

- Actual weights versus target weights.
- Unfilled order review.
- Slippage estimate.
- Drift from intended exposure.
- Drawdown and daily loss checks.

Future files:

- `configs/risk.yaml`
- `src/qlib_factor_lab/risk.py`
- `reports/risk_report_YYYYMMDD.md`

Gate:

- Any critical risk failure stops the workflow before execution.

### 9. Reporting And Monitoring Layer

Responsibilities:

- Make each run reviewable.
- Alert when behavior deviates from research assumptions.
- Track factor decay.

Daily report:

- Data freshness.
- Universe count.
- Top and bottom signals.
- Target holdings.
- Orders and rejects.
- Exposure summary.
- Paper/live PnL.
- Slippage and turnover.

Weekly report:

- Factor IC since launch.
- Hit rate.
- Turnover.
- Drawdown.
- Capacity pressure.
- Drift from backtest.

Future files:

- `src/qlib_factor_lab/live_report.py`
- `scripts/build_daily_report.py`
- `scripts/build_weekly_review.py`
- `reports/daily_report_YYYYMMDD.md`

Gate:

- Paper/live workflows must write a manifest and report even when trading is skipped.

## Configuration Design

Use small YAML files by responsibility:

```text
configs/provider_current.yaml  # data path, market, benchmark, date range
configs/factor_mining.yaml     # candidate templates
configs/signal.yaml            # approved factor weights and model blend
configs/portfolio.yaml         # TopK, dropout, max weights
configs/risk.yaml              # risk limits
configs/execution.yaml         # paper/manual/broker execution mode
configs/trading.yaml           # universe, dates, run directory
```

Recommended first live-like config:

```yaml
mode: paper
market: csi500_current
rebalance_frequency: daily
holding_count: 50
max_new_buys: 5
max_single_weight: 0.02
cash_buffer: 0.02
min_avg_amount_20d: 50000000
max_participation_rate: 0.02
allow_broker_orders: false
```

## Quality Gates

### Research Gate

Required before a factor or model can become a live candidate:

- Tests pass.
- Factor expression is Qlib-compatible.
- Evaluation report exists.
- Direction is normalized.
- RankIC and quantile evidence are saved.
- Candidate has rationale and category.

### Simulation Gate

Required before manual-live:

- Paper workflow runs for at least 30 trading days.
- No critical data failures.
- Orders are generated only from tradable names.
- Slippage assumptions are conservative.
- Daily reports are generated.
- Manual override and kill switch are documented.

### Live Gate

Required before broker-live:

- Manual-live process has been used successfully.
- Account reconciliation is reliable.
- Broker adapter has dry-run tests.
- Pre-trade risk checks are enforced.
- Emergency stop is available.
- Position and order logs are immutable enough for audit.

## Failure Handling

Fail closed in paper/live modes when:

- Market data is missing for the run date.
- Current holdings cannot be loaded.
- Tradable universe is too small.
- Signal coverage is too low.
- Risk config is missing.
- Any critical pre-trade risk check fails.
- Broker adapter returns inconsistent state.

Fail soft in research mode when:

- Some symbols fail data quality checks.
- Some factors produce sparse data.
- A candidate expression fails, as long as the error is logged and the rest of the batch can continue.

## Testing Strategy

Unit tests:

- Data quality checks.
- Tradability filters.
- Factor direction normalization.
- Factor correlation pruning.
- Signal score construction.
- Portfolio target generation.
- Order diff generation.
- Risk rule enforcement.
- Paper broker fills.
- Reconciliation.

Integration tests:

- Build a small fake Qlib-like dataset.
- Run data quality -> signal -> portfolio -> orders.
- Verify artifacts and manifest are produced.
- Verify critical failures stop execution.

Regression tests:

- A known factor report remains parseable.
- A known paper trading day produces the same order list.
- Risk checks reject known bad orders.

Manual verification:

- Run current unit tests:

```bash
python -m unittest discover -s tests
```

- Run current data environment check:

```bash
python scripts/check_env.py --provider-config configs/provider_current.yaml
```

## Rollout Plan

### Phase 1: Research Hardening

Add factor selection, direction correction, correlation pruning, and approved factor catalog.

Exit criteria:

- `approved_factors.yaml` exists.
- At least 5 approved factors with evidence.
- Factor reports are reproducible.

### Phase 2: Paper Trading MVP

Add daily signal, portfolio, order, paper fill, and daily report artifacts.

Exit criteria:

- One command can run a paper trading day.
- All artifacts are written under `runs/YYYYMMDD/`.
- Critical risk checks fail closed.

### Phase 3: Manual Live Assistant

Add broker holdings import, reconciliation, and human order ticket.

Exit criteria:

- The system compares broker holdings to target holdings.
- Manual order CSV and Markdown ticket are produced.
- Human approval is required outside the system.

### Phase 4: Broker Adapter

Add broker interface behind an explicit disabled-by-default adapter.

Exit criteria:

- Dry-run broker adapter test passes.
- Kill switch is tested.
- No automatic trading unless `allow_broker_orders: true`.

## Recommended Next Implementation Plan

The first implementation plan should cover only Phase 1 and the smallest useful slice of Phase 2:

1. Add approved factor catalog generation.
2. Add direction correction from factor mining results.
3. Add correlation pruning for candidate factors.
4. Add daily signal file builder.
5. Add TopK target portfolio builder.
6. Add order diff generator in paper mode.
7. Add risk checks for max weight, max turnover, and missing data.

This keeps the next build testable without introducing broker risk.

