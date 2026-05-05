# P1-P4 Research Platform Upgrade Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the P1-P4 investment research priorities into working, testable platform capabilities: stronger fundamental factors, richer event risk taxonomy, portfolio construction controls, and stock-card/report explanations.

**Architecture:** Keep P0 data governance as the foundation and implement P1-P4 as independent layers on top of existing modules. P1 extends research data/factor definitions, P2 normalizes event semantics, P3 tightens portfolio/risk construction, and P4 enriches explainability artifacts without changing the core data provider contract.

**Tech Stack:** Python 3.9, pandas, YAML configs, unittest, Qlib-compatible local data, Streamlit workbench consumers.

---

## File Structure

- Modify `src/qlib_factor_lab/research_data_domains.py`: add PIT-friendly derived fundamental columns and dividend stability helpers.
- Modify `src/qlib_factor_lab/tushare_data.py`: map any newly supported Tushare fields into normalized fundamental columns when the upstream data is available.
- Modify `configs/combo_specs/*.yaml`: register CSV-backed P1 factor members with clear source lineage and alpha/guardrail roles.
- Modify `configs/autoresearch/fundamental_space.yaml`: expose the P1 families to autoresearch without letting it invent unsupported accounting fields.
- Do not add CSV-backed fundamental factors to `factors/registry.yaml`; that registry is expression-only and requires a Qlib expression.
- Modify `src/qlib_factor_lab/company_events.py` and `configs/event_risk.yaml`: classify P2 events into positive catalysts, watch risks, and blocking risks.
- Modify `src/qlib_factor_lab/risk.py` and `configs/risk.yaml`: add P3 cost, liquidity capacity, industry, family, and profile-aware risk checks.
- Modify `src/qlib_factor_lab/portfolio.py` and `configs/portfolio.yaml`: support construction profiles and per-profile score/weight constraints.
- Modify `src/qlib_factor_lab/stock_cards.py` and `scripts/build_stock_cards.py`: add P4 evidence, counter-evidence, factor contribution, anomaly, and follow-up fields.
- Add/modify tests in `tests/test_research_data_domains.py`, `tests/test_tushare_data.py`, `tests/test_factor_registry.py`, `tests/test_company_events.py`, `tests/test_stage_c.py`, `tests/test_stock_cards.py`.

## Chunk 1: P1 Fundamental Factors

### Task 1: Add Derived Fundamental Quality Fields

**Files:**
- Modify: `src/qlib_factor_lab/research_data_domains.py`
- Modify: `src/qlib_factor_lab/tushare_data.py`
- Test: `tests/test_research_data_domains.py`
- Test: `tests/test_tushare_data.py`

- [ ] **Step 1: Write failing tests for expanded normalized columns**

Add tests that expect `normalize_fundamental_quality()` and Tushare normalization to preserve or derive these columns when available:

```python
expected = [
    "roic",
    "accrual_ratio",
    "gross_margin_change_yoy",
    "revenue_growth_change_yoy",
    "net_profit_growth_change_yoy",
    "cashflow_growth_change_yoy",
    "dividend_stability",
    "dividend_cashflow_coverage",
]
```

- [ ] **Step 2: Run targeted tests to verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_research_data_domains tests.test_tushare_data
```

Expected: FAIL because the new columns do not exist.

- [ ] **Step 3: Implement minimal normalization and derivation**

Rules:
- Keep all existing columns backward compatible.
- If upstream has a direct field, normalize it.
- If the field cannot be derived reliably, leave it blank/NA rather than inventing precision.
- Derive change fields per instrument by sorted `report_period`, using current minus previous available period.
- Add a helper such as `derive_fundamental_quality_fields(fundamentals, dividends=None)` and call it after dividend loading in `write_research_data_domains()`.
- Derive `dividend_stability` from historical dividends only inside that helper, after `cninfo_dividends.csv` has been loaded or fetched.
- Derive `dividend_cashflow_coverage` only when dividend and operating cashflow per share are both available.

- [ ] **Step 4: Run targeted tests to verify pass**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_research_data_domains tests.test_tushare_data
```

Expected: PASS.

### Task 2: Register P1 Factor Families

**Files:**
- Modify: `configs/combo_specs/balanced_multifactor_v1.yaml`
- Modify: `configs/combo_specs/offensive_multifactor_v1.yaml`
- Modify: `configs/autoresearch/fundamental_space.yaml`
- Test: `tests/test_combo_spec.py`
- Test: `tests/test_autoresearch_lane_spaces.py`

- [ ] **Step 1: Write failing combo/autoresearch tests**

Assert the registry exposes these groups:

```text
value: fundamental_ep, fundamental_cfp, fundamental_dividend_yield
quality: fundamental_roe, fundamental_roic, fundamental_cfo_to_ni, fundamental_low_debt, fundamental_low_accrual
growth_improvement: fundamental_revenue_growth_change, fundamental_profit_growth_change, fundamental_margin_change
dividend: fundamental_dividend_stability, fundamental_dividend_cashflow_coverage
```

- [ ] **Step 2: Implement config entries**

Use `source=fundamental_quality` semantics in combo specs and keep Qlib-expression factors separate from CSV fundamental factors. Add descriptions with `source_table`, `source_field`, and intended role in prose.

- [ ] **Step 3: Run tests**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_combo_spec tests.test_autoresearch_lane_spaces
```

Expected: PASS.

## Chunk 2: P2 Announcement And Event Semantics

### Task 3: Classify Event Taxonomy

**Files:**
- Modify: `src/qlib_factor_lab/company_events.py`
- Modify: `configs/event_risk.yaml`
- Test: `tests/test_company_events.py`

- [ ] **Step 1: Write failing tests for event class mapping**

Expected mapping:

```text
positive_catalyst: buyback, shareholder_increase, order_contract, earnings_preannouncement_up, equity_incentive
watch_risk: shareholder_reduction, large_unlock, regulatory_inquiry, pledge_risk, guarantee, lawsuit
block_risk: disciplinary_action, investigation, st_risk, delisting_risk, nonstandard_audit, major_penalty
```

- [ ] **Step 2: Implement taxonomy helper**

Add a pure helper, for example `classify_event_type(event_type: str) -> dict`, returning class, default severity, and portfolio action.

- [ ] **Step 3: Wire taxonomy into event risk snapshot**

Keep existing explicit `severity` values authoritative. If missing, fill severity from taxonomy. Include `event_classes` and `event_actions` summary columns in event risk snapshots.

- [ ] **Step 4: Run tests**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_company_events
```

Expected: PASS.

### Task 4: Expose Event Evidence To Reports

**Files:**
- Modify: `src/qlib_factor_lab/company_events.py`
- Modify: `src/qlib_factor_lab/portfolio.py`
- Modify: `src/qlib_factor_lab/stock_cards.py`
- Test: `tests/test_company_events.py`
- Test: `tests/test_stage_c.py`
- Test: `tests/test_stock_cards.py`

- [ ] **Step 1: Write failing tests for event source/evidence fields**

Stock cards should include positive catalysts separately from risk events.

- [ ] **Step 2: Implement event evidence separation**

Add fields:

```text
positive_event_types
positive_event_summary
risk_event_types
risk_event_summary
event_source_urls
```

- [ ] **Step 3: Extend portfolio passthrough**

Add the new event fields to `EXECUTION_PASSTHROUGH_COLUMNS`, otherwise cards will not receive them from target portfolio rows.

- [ ] **Step 4: Run tests**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_company_events tests.test_stage_c tests.test_stock_cards
```

Expected: PASS.

## Chunk 3: P3 Portfolio Construction And Gate

### Task 5: Add Portfolio Profiles

**Files:**
- Modify: `src/qlib_factor_lab/portfolio.py`
- Modify: `configs/portfolio.yaml`
- Test: `tests/test_stage_c.py`

- [ ] **Step 1: Write failing tests for profiles**

Profiles:

```text
defensive: quality/dividend/low-vol tolerated, stricter max single weight
balanced: default diversified profile
offensive: requires growth or momentum confirmation and caps defensive family weight
```

- [ ] **Step 2: Implement config parsing**

Add `profile` and optional `profile_constraints` to `PortfolioConfig`.

- [ ] **Step 3: Implement profile filtering**

For `offensive`, require positive contribution from configured `profile_confirmation_columns`, defaulting to:

```text
family_growth_improvement_score
family_momentum_score
family_theme_score
family_event_score
```

For `defensive`, allow quality/dividend confirmation but apply lower `max_single_weight`.

- [ ] **Step 4: Run tests**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_stage_c
```

Expected: PASS.

### Task 6: Add Cost, Liquidity, And Risk Budget Checks

**Files:**
- Modify: `src/qlib_factor_lab/risk.py`
- Modify: `configs/risk.yaml`
- Test: `tests/test_stage_c.py`

- [ ] **Step 1: Write failing tests for new checks**

Add checks for:

```text
min_amount_20d
max_position_amount_share
max_estimated_cost
max_risk_budget_per_position
```

- [ ] **Step 2: Implement risk config fields**

Keep defaults disabled unless configured. Add `portfolio_value` or `capital_base`, plus explicit cost assumptions such as `commission_bps`, `slippage_bps`, and `stamp_tax_bps`; do not infer notional from weights alone.

- [ ] **Step 3: Implement checks**

Use existing portfolio passthrough columns:

```text
amount_20d
turnover_20d
target_weight
```

If required source columns are missing and the check is configured, fail closed with clear detail.

- [ ] **Step 4: Run tests**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_stage_c tests.test_exposure_attribution
```

Expected: PASS.

## Chunk 4: P4 Stock Cards And Research Reports

### Task 7: Enrich Stock Cards

**Files:**
- Modify: `src/qlib_factor_lab/stock_cards.py`
- Modify: `scripts/build_stock_cards.py`
- Test: `tests/test_stock_cards.py`

- [ ] **Step 1: Write failing tests for P4 card sections**

Cards should include:

```text
selection_thesis
factor_contributions
counter_evidence
announcement_evidence
financial_anomalies
manual_review_actions
tracking
```

- [ ] **Step 2: Implement schema-compatible enrichment**

Do not remove existing keys. Add new keys so current UI/readers keep working.

- [ ] **Step 3: Run tests**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_stock_cards
```

Expected: PASS.

### Task 8: Add Human-Readable Candidate Report

**Files:**
- Modify: `scripts/build_stock_cards.py`
- Modify: `src/qlib_factor_lab/stock_cards.py`
- Test: `tests/test_stock_cards.py`

- [ ] **Step 1: Write failing test for Markdown report**

Given a target portfolio and gate checks, write a Markdown report with:

```text
why selected
top drivers
risks
evidence urls
manual review action
tracking placeholder
```

- [ ] **Step 2: Implement writer**

Add `write_stock_card_report(cards, output_path)`.

- [ ] **Step 3: Run tests**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_stock_cards
```

Expected: PASS.

## Final Verification

- [ ] Run focused tests:

```bash
PYTHONPATH=src .venv/bin/python -m unittest \
  tests.test_research_data_domains \
  tests.test_tushare_data \
  tests.test_combo_spec \
  tests.test_autoresearch_lane_spaces \
  tests.test_company_events \
  tests.test_stage_c \
  tests.test_stock_cards
```

- [ ] Run full suite:

```bash
make test
```

- [ ] Run token scan before commit:

```bash
python3 - <<'PY'
from pathlib import Path
needle = "replace_with_sensitive_token_to_scan"
hits = []
for path in Path(".").rglob("*"):
    if not path.is_file() or any(part in {".git", ".venv", "data", "reports"} for part in path.parts):
        continue
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        continue
    if needle in text:
        hits.append(str(path))
print("token_hits", len(hits))
for hit in hits:
    print(hit)
PY
```

- [ ] Commit:

```bash
git add docs/superpowers/plans/2026-04-30-p1-p4-research-platform-upgrade.md \
  src/qlib_factor_lab tests configs factors scripts
git commit -m "feat: add p1-p4 research platform upgrades"
```
