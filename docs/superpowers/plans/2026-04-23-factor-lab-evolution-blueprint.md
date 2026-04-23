# Factor Lab Evolution Blueprint

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evolve Qlib Factor Lab from an upstream alpha research scaffold into a production-facing daily signal and paper-trading engine without collapsing its boundary with PersonalQuant.

**Architecture:** Keep `factor-lab` focused on upstream alpha governance: data quality, factor discovery, factor approval, daily signal generation, portfolio simulation, and paper/manual-live artifacts. Do not turn it into a dashboard-heavy end-user product. Let PersonalQuant remain the downstream decision surface for scanners, fusion ranking, cron delivery, and operator-facing dashboards. Introduce production capability in layers: approved factors -> daily signal -> target portfolio -> paper trading -> manual live -> thin broker adapter last.

**Tech Stack:** Python 3.9, pandas, numpy, PyYAML, Qlib, LightGBM, local CSV/SQLite artifacts, Git-tracked configs and docs, local ignored research outputs.

---

## 0. Current State Diagnosis

### What already exists

- Data build path:
  - `scripts/build_akshare_qlib_data.py`
  - `src/qlib_factor_lab/akshare_data.py`
- Single-factor evaluation:
  - `src/qlib_factor_lab/factor_eval.py`
  - `scripts/eval_factor.py`
- Candidate mining:
  - `src/qlib_factor_lab/factor_mining.py`
  - `configs/factor_mining.yaml`
  - `scripts/mine_factors.py`
- Event/pattern-style backtest:
  - `src/qlib_factor_lab/event_backtest.py`
  - `scripts/backtest_factor_events.py`
- Market-regime analysis:
  - `src/qlib_factor_lab/market_regime.py`
- Qlib model baseline:
  - `src/qlib_factor_lab/model_workflow.py`
  - `scripts/run_lgb_workflow.py`
- Controlled expression autoresearch:
  - `src/qlib_factor_lab/autoresearch/*`
  - `scripts/autoresearch/run_expression_loop.py`
  - `scripts/autoresearch/summarize_expression_ledger.py`
  - `scripts/autoresearch/analyze_expression_reviews.py`

### What is still missing

The repo does not yet contain real implementations for:

- data quality gates
- tradable-universe filtering
- approved factor registry distinct from the broad factor registry
- redundancy / family clustering / production promotion workflow
- daily signal construction
- portfolio construction
- pre-trade risk checks
- order generation
- paper broker / fill simulation
- holdings state and reconciliation
- manual-live tickets
- broker integration

### Real thesis of the repo today

`factor-lab` is already good enough to discover, test, and classify alpha families. It is not yet a trading system. Its strongest current identity is:

- upstream alpha discovery engine
- controlled experiment harness
- production-candidate approval bench

That identity should be preserved during expansion.

---

## 1. Product Boundary: What belongs in factor-lab vs PersonalQuant

### factor-lab should own

- local research data quality and provider sanity
- factor registry and candidate generation
- autoresearch loops
- factor approval and evidence packs
- daily signal generation from approved factors and challenger models
- paper portfolio generation and risk checks
- order suggestion artifacts for manual review
- reconciliation-grade run manifests and audit bundles

### factor-lab should not own

- dashboard-heavy operator UX
- Telegram-facing delivery logic
- scanner product UX
- fused decision surfaces combining PersonalQuant-specific scanners unless exported as explicit upstream signals
- ad hoc research note presentation logic that belongs in downstream reporting

### PersonalQuant should own

- dashboard / front-end surfaces
- Telegram and cron-facing reporting
- scanner execution for model + wangji + future parallel lanes
- fusion ranking and human review experience
- operator workflow around candidate interpretation and watchlists

### Operational contract between the repos

The handoff should be file-first and explicit. `factor-lab` should export dated artifacts such as:

- `signals_YYYYMMDD.csv`
- `approved_factors.yaml`
- `target_portfolio_YYYYMMDD.csv`
- `orders_YYYYMMDD.csv`
- `risk_report_YYYYMMDD.md`
- `manifest.json`

PersonalQuant should consume these as upstream evidence, not re-derive them implicitly.

---

## 2. Does current factor mining satisfy the research need?

### Yes, for upstream alpha discovery

The current stack is already sufficient for:

- generating candidate expressions
- evaluating IC / RankIC / quantile returns
- checking size-proxy-neutralized robustness
- separating cross-sectional factors from event-like factors
- running controlled one-candidate autoresearch loops
- keeping local experiment memory via ledger artifacts
- identifying factor families such as the current divergence / weak-reversal cluster

### No, for research-to-production closure

The current stack still cannot answer these production questions in a disciplined way:

- which factors are approved for deployment today
- which factors are redundant variants of the same idea
- which regime each factor is allowed to influence
- how approved factors combine into a daily score
- what portfolio the score implies after turnover/liquidity constraints
- what would have been traded today
- whether live-like behavior still matches the original research assumptions

### Bottom line

Current factor mining is necessary but not sufficient. The missing layer is not “more search,” but “alpha governance.”

---

## 3. North-Star Target Architecture

```text
Data Refresh
  -> Data Quality Gate
  -> Tradable Universe
  -> Approved Factor Load
  -> Daily Factor/Model Signal Build
  -> Signal Sanity Gate
  -> Portfolio Construction
  -> Pre-Trade Risk Gate
  -> Order Suggestion / Paper Fill Simulation
  -> Reconciliation
  -> Daily Audit Bundle Export
  -> Downstream Consumption by PersonalQuant
```

This architecture should be introduced in five stages.

---

## 4. Stage A (Immediate): Alpha Governance Layer

**Objective:** Turn factor-lab from a factor generator into an approval bench that decides which alpha objects are production candidates.

### Deliverables

- `src/qlib_factor_lab/factor_selection.py`
- `scripts/select_factors.py`
- `configs/factor_selection.yaml`
- `reports/approved_factors.yaml` (Git-tracked small artifact or generated local artifact with a committed reference snapshot)
- `reports/factor_review_<date>.md`

### Responsibilities

- load review candidates from registry and autoresearch ledger outputs
- deduplicate candidate families
- compute redundancy using signal correlation and/or evaluation similarity
- assign family labels and role labels
- output a production candidate list with evidence paths

### Required factor metadata

Every approved factor should carry at least:

- `name`
- `family`
- `type`: `cross_sectional` or `event_like`
- `primary_horizon`
- `supported_universes`
- `regime_profile`: `all_weather`, `down_sideways`, `up_only`, etc.
- `turnover_profile`
- `approval_status`: `core`, `challenger`, `reserve`, `regime_only`
- `evidence_paths`
- `approval_date`
- `review_notes`

### Promotion rules

Do not promote a factor using one pretty full-sample number. Require:

- neutralized H20 evidence
- yearly split including weak years
- regime split
- universe comparison when the factor claims generality
- turnover sanity for the intended use
- redundancy check against already approved factors

### Immediate recommendation for the current divergence family

Treat the current promoted divergence family as the first approved family to run through this pipeline. The family is already informative enough to exercise:

- family clustering
- core vs challenger role assignment
- regime-aware labeling
- weak-year diagnostics

---

## 5. Stage B (Next): Daily Signal Engine

**Objective:** Convert approved alpha objects into a daily, inspectable stock score that downstream systems can consume.

### Deliverables

- `configs/signal.yaml`
- `src/qlib_factor_lab/signal.py`
- `scripts/build_daily_signal.py`
- `reports/signals_YYYYMMDD.csv`
- `reports/signal_summary_YYYYMMDD.md`

### Responsibilities

- load `approved_factors.yaml`
- compute approved factor exposures for the run date
- z-score or otherwise normalize approved factors consistently
- optionally combine with model predictions from the LightGBM workflow
- support regime-aware weights
- emit per-stock contributions for explainability

### Signal output schema

Minimum columns:

- `date`
- `instrument`
- `tradable`
- `rule_score`
- `model_score`
- `ensemble_score`
- `active_regime`
- `top_factor_1`
- `top_factor_1_contribution`
- `top_factor_2`
- `top_factor_2_contribution`
- `risk_flags`

### Design rules

- start with transparent rule-score blending before making the ML model dominant
- keep the model as a parallel challenger until it beats the rule engine after practical costs
- preserve explainability: the daily score must be decomposable by factor family

### Why this stage matters

Without a daily signal layer, factor-lab can only do research notes. With it, factor-lab starts producing a daily upstream asset that PersonalQuant can consume directly.

---

## 6. Stage C: Tradability + Portfolio + Risk

**Objective:** Force alpha through the real constraints that separate research from deployable workflow.

### Deliverables

- `configs/trading.yaml`
- `configs/portfolio.yaml`
- `configs/risk.yaml`
- `src/qlib_factor_lab/data_quality.py`
- `src/qlib_factor_lab/universe.py`
- `src/qlib_factor_lab/tradability.py`
- `src/qlib_factor_lab/portfolio.py`
- `src/qlib_factor_lab/risk.py`
- `scripts/check_data_quality.py`
- `scripts/build_target_portfolio.py`

### Responsibilities

#### Data quality
- verify calendar completeness
- verify current-day bar coverage
- catch missing or invalid OHLCV/amount fields
- fail closed outside pure research mode when run-date data is broken

#### Tradable universe
- exclude suspended / missing-bar / zero-volume names
- exclude names outside minimum listing age or liquidity thresholds
- later: add ST, blacklist, price-limit lock logic

#### Portfolio construction
Start simple:
- TopK equal weight
- dropout rebalance
- max new buys per day
- max single-name weight
- cash buffer

Do not begin with a risk optimizer. The first goal is not sophistication. The first goal is a transparent live-like baseline.

#### Risk
Minimum pre-trade checks:
- max turnover
- max single-name weight
- min average amount
- max participation rate
- limit-up / limit-down blocks
- blacklist / manual lock list
- signal coverage floor

### Why this stage matters

This is where many “good factors” die. That is healthy. The purpose is to learn which alpha survives tradability and turnover reality.

---

## 7. Stage D: Paper Trading + Reconciliation

**Objective:** Make factor-lab produce a reproducible, daily paper-trading workflow before touching real broker APIs.

### Deliverables

- `configs/execution.yaml`
- `src/qlib_factor_lab/orders.py`
- `src/qlib_factor_lab/paper_broker.py`
- `src/qlib_factor_lab/state.py`
- `src/qlib_factor_lab/reconcile.py`
- `scripts/generate_orders.py`
- `scripts/reconcile_account.py`
- `runs/YYYYMMDD/manifest.json`
- `runs/YYYYMMDD/orders.csv`
- `runs/YYYYMMDD/fills.csv`
- `runs/YYYYMMDD/risk_report.md`

### Responsibilities

- compare target portfolio with current holdings state
- generate daily order suggestions
- simulate conservative fills in paper mode
- record rejects and reasons
- reconcile expected vs simulated end-of-day state
- emit one audit bundle per run

### Run-bundle layout

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

### Entry criteria for moving beyond this stage

Require at least:

- 30 trading days of stable paper runs
- no silent data failures
- reconciliation mismatches understood and bounded
- signal-to-order drift understood
- turnover and capacity within expected limits

---

## 8. Stage E: Manual Live First, Broker Adapter Last

**Objective:** Reach real trading through a human-reviewed path before enabling direct broker submission.

### Deliverables

- `src/qlib_factor_lab/manual_live.py`
- `reports/order_ticket_YYYYMMDD.md`
- `reports/order_ticket_YYYYMMDD.csv`
- later only if needed:
  - `src/qlib_factor_lab/broker_base.py`
  - `src/qlib_factor_lab/broker_<vendor>.py`

### Operating rule

The first real-money mode should be manual live, not unattended broker-live. The workflow should:

- import current holdings
- generate target diff
- produce human-readable tickets
- list rejects and risk flags
- require explicit human approval before any broker action

### Why manual live comes first

This stage reveals the true operational failure modes:

- limit-up / limit-down interference
- liquidity mismatch
- practical lot-size and partial-fill issues
- run timing issues around A-share daily operations
- whether the strategy is psychologically and operationally tolerable

Only after this should a thin broker adapter be considered.

---

## 9. Research Lanes: Keep Them Parallel, Not Collapsed

The current expression autoresearch lane is a good start, but factor-lab should evolve into parallel lanes rather than a single monolithic search loop.

### Recommended lanes

#### Lane 1: Expression factors
- existing lane
- cross-sectional formula search
- IC / neutralized IC / stability oriented

#### Lane 2: Event / pattern factors
- absolute trigger or pattern-score research
- event backtests and confirmation logic
- more appropriate for breakout / repair / setup signals

#### Lane 3: Combo / fusion lane
- searches for combination rules over already approved factors
- should not rewrite evaluation primitives
- optimizes weighting, gating, and regime routing rather than raw formulas

### Design principle

Keep contracts, ledgers, and evidence separate per lane. Do not merge unlike research objects into one giant loop. This preserves interpretability and makes promotion decisions cleaner.

---

## 10. 4-Week Suggested Build Order

### Week 1: Approval layer
- [ ] Create `configs/factor_selection.yaml`.
- [ ] Implement `src/qlib_factor_lab/factor_selection.py`.
- [ ] Add `scripts/select_factors.py`.
- [ ] Output a first `approved_factors.yaml` covering the current divergence family.
- [ ] Add tests for deduping, role assignment, and evidence validation.

### Week 2: Daily signal layer
- [ ] Create `configs/signal.yaml`.
- [ ] Implement `src/qlib_factor_lab/signal.py`.
- [ ] Add `scripts/build_daily_signal.py`.
- [ ] Emit per-stock factor contribution columns.
- [ ] Validate the first dated `signals_YYYYMMDD.csv` artifact.

### Week 3: Tradability + portfolio + risk
- [ ] Implement `data_quality.py`, `universe.py`, and `tradability.py`.
- [ ] Implement `portfolio.py` with TopK equal-weight plus dropout.
- [ ] Implement `risk.py` with fail-closed pre-trade checks.
- [ ] Add `scripts/build_target_portfolio.py`.
- [ ] Verify a full dry-run from data refresh to target portfolio.

### Week 4: Paper workflow
- [ ] Implement `orders.py`, `paper_broker.py`, `state.py`, and `reconcile.py`.
- [ ] Add `scripts/generate_orders.py` and `scripts/reconcile_account.py`.
- [ ] Emit dated run bundles under `runs/YYYYMMDD/`.
- [ ] Start a paper-trading streak and log run failures explicitly.

---

## 11. 8-Week Suggested Build Order

### Weeks 1-4
Ship the 4-week plan above.

### Weeks 5-6: Regime-aware signal governance
- [ ] Add regime-aware weights in `signal.yaml`.
- [ ] Extend factor approval metadata with regime eligibility.
- [ ] Add challenger-vs-core monitoring in signal summaries.
- [ ] Add drift monitoring between research assumptions and paper behavior.

### Weeks 7-8: Manual live preparation
- [ ] Add holdings import format and reconciliation contract.
- [ ] Add manual-live order tickets.
- [ ] Add kill-switch and hard fail-closed controls.
- [ ] Document manual-live SOP and rollback procedure.
- [ ] Run at least 10 manual-review days before any broker automation discussion.

---

## 12. What not to do next

### Do not over-invest in broader search before governance exists
More candidate generation without approval logic will inflate the registry and reduce trust.

### Do not let the model absorb every alpha family prematurely
Keep rule factors and model predictions parallel until daily signal evidence proves a clear winner.

### Do not jump straight from factor evaluation to broker execution
The missing middle is where most operational truth lives.

### Do not move dashboard/product concerns into factor-lab
That would blur the repo boundary and create duplicated product logic with PersonalQuant.

---

## 13. Acceptance Criteria For “Ready To Influence Real Money”

The repo should only be considered ready to influence real money when all of the following are true:

- approved factors exist with explicit evidence and regime labels
- a daily signal artifact is generated automatically and is explainable
- a target portfolio can be built from that signal
- pre-trade risk checks fail closed
- paper trading has run stably for at least 30 trading days
- reconciliation is reliable
- manual-live order tickets are understandable and usable
- kill switch and operator override are documented and tested

Direct broker submission is not part of this minimum definition.

---

## 14. Recommended First Concrete Milestone

If only one thing is built next, it should be:

**Approved factor governance + daily signal generation for the current divergence family.**

That milestone is the narrowest useful bridge between today’s repo and a real production path. It turns the current research win into an object that can feed portfolio simulation, PersonalQuant consumption, and later paper trading.

---

## 15. Summary

The right evolution is not “make factor-lab a whole trading app.” The right evolution is:

1. keep factor-lab as the upstream alpha engine
2. add alpha governance
3. export a daily signal product
4. add portfolio/risk/paper layers
5. reach manual live before broker live
6. let PersonalQuant remain the downstream operator-facing system

That path preserves clarity, makes the current autoresearch investment compound, and gives the repo a credible route from factor discovery to real trading influence without pretending that one good factor table is already a live system.
