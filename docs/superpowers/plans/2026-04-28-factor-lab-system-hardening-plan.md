# Factor Lab System Hardening Plan

> **For agentic workers:** REQUIRED: Use superpowers:test-driven-development for implementation changes. This plan upgrades Factor Lab from a factor research workbench into a controlled research-to-execution system.

**Goal:** Build a closed-loop A-share quant research platform covering data governance, multi-lane factor research, theme research, portfolio gates, execution simulation, audit replay, and agent-assisted operations.

**Positioning:** Factor Lab is a research and verification system. It should produce evidence, hypotheses, gates, and review packets. It must not present automated output as guaranteed return or unsupervised investment advice.

**Fixed universe:** CSI300 + CSI500. The data layer, governance checks, and daily update mechanism should assume these two research universes unless a new universe is explicitly added with the same governance contract.

---

## 1. Why This Hardening Is Needed

The current platform can already mine factors, run multi-lane autoresearch, scan themes, build target portfolios, and apply several gates. The weak points are now around system reliability:

- Too much alpha work still depends on price-volume data.
- Fundamental, shareholder, event, and evidence data are only partially available.
- Portfolio gates exist, but process replay is not yet strong enough.
- Paper/manual execution exists, but there is no clean broker abstraction for future real execution.
- The UI shows workflow state, but it is not yet a full operating console.
- Agent assistance exists, but it needs clearer boundaries and audit trails.

The hardening path should make every recommendation traceable:

```text
data snapshot
-> factor/theme evidence
-> signal
-> portfolio
-> gate decision
-> expert review
-> orders/fills
-> replay report
```

---

## 2. Design Principles

1. **Point-in-time first.** Every non-market dataset must carry `available_at`, `announce_date`, `event_date`, or an explicit lag rule.
2. **Data gates before research gates.** A lane cannot become active unless its data domain passes governance.
3. **Shadow is a first-class state.** Missing or low-coverage data should keep a lane in `shadow`, not silently disable it or let it pollute portfolios.
4. **Theme research differs from executable portfolios.** A same-theme shortlist can be useful research, but a real portfolio still needs concentration and liquidity controls.
5. **LLM review is a control layer, not an oracle.** Expert LLM output can flag concerns, require confirmation, or reduce weight, but it cannot override hard data/risk failures.
6. **Every run must replay.** A daily run without sufficient artifacts is incomplete even if it produced a target portfolio.
7. **Real execution must be adapter-gated.** Do not connect real broker APIs until paper/manual flows share the same interface and audit path.

---

## 3. Stage A: Data Layer Hardening

**Objective:** Make the data layer broad enough to support multiple research lanes without faking readiness.

### A1. Current Data Domains

| domain | current role | target state |
|---|---|---|
| market_ohlcv | Qlib daily bars for CSI300/CSI500 | active, daily incremental |
| security_master | stock names, board, industry, ST, universe | active, PIT versioning improved |
| company_events | announcement/event/risk tags | active, richer event taxonomy |
| fundamental_quality | value/quality/growth fields | active after >=70% universe coverage |
| shareholder_capital | pledge/reduction/unlock/buyback events | active event data |
| announcement_evidence | searchable announcement evidence chunks | active evidence layer |
| emotion_atmosphere | market mood and heat proxies | shadow until explicit data file exists |
| liquidity_microstructure | tradability and limit/suspension status | partial, should become daily active |

### A2. Deliverables

- `data/fundamental_quality.csv`
- `data/shareholder_capital.csv`
- `data/announcement_evidence.csv`
- `data/announcement_evidence.jsonl`
- `data/emotion_atmosphere.csv`
- `reports/data_governance_<date>.md`
- `reports/daily_data_update_<date>.md`

### A3. Daily Update Contract

The daily update command should remain the primary entry point:

```bash
make daily-data-update DAILY_DATA_AS_OF=YYYYMMDD
```

Expected order:

1. Incrementally update CSI500 bars.
2. Incrementally update CSI300 bars.
3. Refresh security master and company events.
4. Build research data domains.
5. Run data governance.
6. Write update manifest.

For preview:

```bash
make daily-data-update DAILY_DATA_AS_OF=YYYYMMDD DAILY_DATA_DRY_RUN=1
```

For fundamental-only refresh:

```bash
make research-data-domains RUN_DATE=YYYYMMDD DAILY_DATA_FETCH_FUNDAMENTALS=1
```

### A4. Acceptance Criteria

- `security_master` coverage >= 95%.
- `company_events` PIT completeness = 100%.
- `fundamental_quality` coverage >= 70% before active promotion.
- `shareholder_capital` event coverage threshold remains event-style, not 70% stock coverage.
- `announcement_evidence` contains title/summary/evidence/source URL and PIT fields.
- Governance reports correctly classify `active`, `shadow`, `missing`, and `disabled`.

---

## 4. Stage B: Multi-Lane Research Hardening

**Objective:** Ensure the platform researches different alpha families in parallel rather than repeatedly rediscovering price-volume variants.

### B1. Target Lanes

| lane | data dependency | current target |
|---|---|---|
| expression_price_volume | market_ohlcv | active |
| pattern_event | market_ohlcv | active |
| emotion_atmosphere | market_ohlcv, emotion_atmosphere | active only for market-proxy factors; data file still shadow |
| liquidity_microstructure | market_ohlcv, execution calendar | active/partial |
| risk_structure | market_ohlcv | active |
| fundamental_quality | fundamental_quality | active after data coverage passes |
| shareholder_capital | shareholder_capital | event lane after event count passes |
| theme_event | theme config + evidence + signal | research-only until portfolio gate |
| regime | index/breadth/liquidity state | allocator, not stock selector |

### B2. Lane Requirements

Each lane must have:

- a YAML search/spec space under `configs/autoresearch/`
- an oracle or evaluator
- primary metrics
- guard metrics
- artifact directory
- summary block
- ledger-compatible result
- governance activation rule

### B3. Non-Duplication Rule

Autoresearch should avoid repeatedly testing the same economic idea:

- Track factor expression hash.
- Track factor family and lane.
- Penalize near-duplicate expressions.
- Keep only family representatives for portfolio construction.
- Allow repeated testing only when data window, universe, or horizon changes intentionally.

### B4. Acceptance Criteria

- `make autoresearch-multilane` reports every configured lane.
- Unsupported lanes are explicit, not silent.
- Shadow lanes are shown in UI and reports.
- Fundamental and shareholder lanes cannot become portfolio-active without data governance passing.
- Nightly runs summarize lane-level best candidates and duplicate/skipped attempts.

---

## 5. Stage C: Portfolio Gate Hardening

**Objective:** Make portfolio acceptance explainable and enforceable.

### C1. Gate Types

| gate | purpose |
|---|---|
| data gate | checks data readiness and freshness |
| factor gate | checks single-factor quality and redundancy |
| theme gate | checks research shortlist viability within one theme |
| portfolio gate | checks executable diversification and concentration |
| expert gate | LLM/human review control |
| execution gate | checks tradability, orders, fills, and kill switches |

### C2. Decision Semantics

- `pass`: portfolio can proceed.
- `caution`: reduce weight or require manual confirmation.
- `reject`: block portfolio/order generation.
- `shadow`: research can proceed, but portfolio impact is blocked.

### C3. Required Explanations

Every caution/reject should produce:

- failed check name
- measured value
- threshold
- affected instruments
- suggested next action
- artifact link

### C4. Acceptance Criteria

- `target_portfolio.csv` includes gate-adjusted weights.
- `portfolio_gate_explanation.md` explains caution/reject.
- Expert review output is applied mechanically:
  - reject blocks
  - caution reduces weight or requires confirmation
  - pass proceeds
- Theme shortlists do not fail merely because names share the same theme.
- Executable portfolios still enforce concentration caps.

---

## 6. Stage D: Execution Layer Hardening

**Objective:** Prepare the system for safe paper/manual/real execution without coupling portfolio logic to broker-specific APIs.

### D1. Broker Adapter Interface

Create a stable adapter boundary:

```text
BrokerAdapter
- validate_orders()
- submit_orders()
- cancel_orders()
- fetch_fills()
- fetch_positions()
- fetch_cash()
- reconcile()
```

Initial adapters:

- `PaperBrokerAdapter`
- `ManualTicketBrokerAdapter`
- `DryRunBrokerAdapter`

Future adapter:

- `RealBrokerAdapter`, blocked behind explicit config and manual approval.

### D2. Order Lifecycle

Orders should move through explicit states:

```text
created
validated
submitted
partially_filled
filled
rejected
cancelled
reconciled
```

### D3. Execution Guards

Required before any real execution:

- no order if data freshness fails
- no order if portfolio gate rejects
- no order if expert gate rejects
- no buy if limit-up blocked
- no sell if T+1 blocked
- no order if liquidity below threshold
- global kill switch
- max single-order notional
- max daily turnover

### D4. Acceptance Criteria

- Paper/manual/real-dry-run share the same order schema.
- Every order has `run_id`, `order_id`, and `audit_id`.
- Reconciliation writes expected vs actual positions.
- Manual ticket output is reproducible from the run artifacts.
- Real API config defaults to disabled.

---

## 7. Stage E: Process Replay And Audit

**Objective:** Make every daily run reproducible after the fact.

### E1. Required Run Bundle

Each daily run should write:

```text
runs/<run_date>/
  manifest.json
  run_summary.md
  data_governance.md
  signal_summary.md
  signals.csv
  event_risk_snapshot.csv
  expert_review_packet.md
  expert_review_result.md
  target_portfolio.csv
  target_portfolio_summary.md
  portfolio_gate_explanation.md
  stock_cards.jsonl
  orders.csv
  fills.csv
  positions_expected.csv
  reconciliation.md
  replay_report.md
```

### E2. Replay Command

Add a replay entry point:

```bash
python scripts/replay_daily_run.py --run-dir runs/YYYYMMDD
```

Replay should verify:

- configs exist
- referenced data dates are available
- signals can be reconstructed or compared
- target portfolio can be rebuilt
- gate decision matches
- orders/fills reconcile

### E3. Acceptance Criteria

- A run can be replayed without internet access.
- Replay reports mismatch reasons, not just pass/fail.
- UI can open replay artifacts.
- Missing artifacts are treated as audit failures.

---

## 8. Stage F: Workbench UI Hardening

**Objective:** Turn the UI into an operating console, not just a dashboard.

### F1. Target Pages

| page | role |
|---|---|
| 01 Overview | system status and pipeline actions |
| 02 Data Governance | domain readiness, freshness, coverage |
| 03 Factor Research | factor diagnostics and lane performance |
| 04 Autoresearch | nightly queue, lane state, experiment ledger |
| 05 Portfolio Gate | pass/caution/reject explanation |
| 06 Expert Review | packets, LLM/human decisions, action applied |
| 07 Theme Scanner | theme shortlist and theme gate |
| 08 Evidence Library | announcement/event evidence search |
| 09 Execution | orders, fills, reconciliation |
| 10 Replay | run selection, replay report, artifact chain |

### F2. UI Actions

The UI should expose safe actions:

- dry-run daily data update
- run data governance
- run research data domain build
- start/stop autoresearch loop
- run theme scan
- run portfolio gate
- generate manual ticket
- replay a run

Actions with side effects should write task artifacts and never silently mutate data.

### F3. Acceptance Criteria

- Navigation works across all pages.
- UI tests cover navigation, data readiness, gate explanation, and replay.
- Long tasks show running/stopped/failed state.
- UI never hides caution/reject reasons.

---

## 9. Stage G: Agent Assistance Hardening

**Objective:** Keep agentic automation useful but bounded.

### G1. Allowed Agent Actions

Agents may:

- propose factor candidates
- edit search-space YAML specs
- summarize nightly results
- detect duplicate factor ideas
- generate expert review packets
- explain gate failures
- suggest next experiments

Agents should not:

- modify evaluator code during research loops
- change provider/date/universe contracts silently
- override reject decisions
- connect real broker APIs
- claim alpha from LLM commentary alone

### G2. Agent Ledger

Every agent experiment should record:

- run_id
- commit
- lane
- candidate
- data contract
- primary metric
- guard metric
- status
- reason
- artifact_dir

### G3. Acceptance Criteria

- Agent actions are visible in reports.
- Crashes and rejected experiments are logged.
- Duplicate attempts are tracked and skipped.
- Agent-generated portfolio changes must pass the same gates.

---

## 10. Recommended Execution Order

### Batch 1: Data And Replay Foundation

1. Finish full `fundamental_quality` build.
2. Add shareholder capital event enrichment.
3. Add announcement evidence search/index metadata.
4. Add replay manifest and `replay_daily_run.py`.
5. Expose data/replay state in UI.

### Batch 2: Lane Activation

1. Run data governance after full data refresh.
2. Promote `fundamental_quality` lane only if coverage passes.
3. Add shareholder capital event oracle.
4. Add duplicate-factor detection to nightly autoresearch.
5. Add lane-level summary to UI research queue.

### Batch 3: Execution Safety

1. Add broker adapter interface.
2. Move paper/manual orders behind adapter.
3. Add execution kill switch.
4. Add order lifecycle state.
5. Add execution replay and reconciliation checks.

### Batch 4: Workbench Consolidation

1. Add replay page.
2. Add evidence library page backed by `announcement_evidence`.
3. Add execution page.
4. Add action queue/task status.
5. Expand browser E2E tests.

---

## 11. Immediate Next Tasks

1. **Complete full fundamental data refresh**
   - Command already running in `tmux` session `fundamental_full_fetch`.
   - Validate row count, instrument coverage, PIT completeness.
   - Re-run data governance.

2. **Implement replay manifest**
   - Add `src/qlib_factor_lab/replay.py`.
   - Add `scripts/replay_daily_run.py`.
   - Add tests for missing artifact, matching artifact, and mismatch report.

3. **Add broker adapter abstraction**
   - Add `src/qlib_factor_lab/broker_adapter.py`.
   - Refactor paper/manual order generation behind adapter.
   - Keep real broker disabled by default.

4. **Add evidence library primitive**
   - Search `data/announcement_evidence.csv`.
   - Filter by instrument, event type, severity, and keyword.
   - Return cited source URL and event date.

5. **Update Workbench**
   - Add replay page.
   - Add execution page.
   - Add evidence search page integration.
   - Add task status for long-running update/autoresearch tasks.

---

## 12. Non-Goals

- Do not build live trading first.
- Do not let LLM output become a buy/sell instruction.
- Do not activate data lanes without governance passing.
- Do not optimize factor weights on one short recent period.
- Do not treat theme concentration as a portfolio pass condition.
- Do not add new universes until CSI300/CSI500 are stable end to end.

