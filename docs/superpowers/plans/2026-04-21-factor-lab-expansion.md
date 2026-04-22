# Factor Lab Expansion Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add neutralized factor evaluation with charts, a Qlib LightGBM workflow runner, and a candidate factor mining template.

**Architecture:** Keep the current expression-factor scaffold. Add small modules for neutralization, chart reports, factor candidate generation, and workflow config rendering so each capability can be used independently from CLI scripts.

**Tech Stack:** Python, pandas, numpy, matplotlib, PyYAML, Qlib, LightGBM.

---

### Task 1: Neutralization And Charts

**Files:**
- Create: `src/qlib_factor_lab/neutralization.py`
- Modify: `src/qlib_factor_lab/factor_eval.py`
- Modify: `src/qlib_factor_lab/reports.py`
- Modify: `scripts/eval_factor.py`
- Test: `tests/test_neutralization.py`
- Test: `tests/test_reports.py`

- [ ] Write failing tests for cross-sectional residualization, industry demeaning, quantile return summary, and PNG chart output.
- [ ] Run tests and confirm they fail on missing functions.
- [ ] Implement neutralization helpers and chart functions.
- [ ] Wire CLI flags for size-proxy neutralization, industry map, and plot output.
- [ ] Run focused and full tests.

### Task 2: Factor Mining Template

**Files:**
- Create: `src/qlib_factor_lab/factor_mining.py`
- Create: `configs/factor_mining.yaml`
- Create: `scripts/mine_factors.py`
- Test: `tests/test_factor_mining.py`

- [ ] Write failing tests for deterministic candidate generation and IC-based screening order.
- [ ] Run tests and confirm they fail on missing module.
- [ ] Implement formula templates, candidate generation, and batch evaluation summary.
- [ ] Add CLI script that writes `reports/factor_mining_candidates.csv`.
- [ ] Run focused and full tests.

### Task 3: Qlib LightGBM Workflow

**Files:**
- Create: `src/qlib_factor_lab/model_workflow.py`
- Create: `configs/qlib_lgb_workflow.yaml`
- Create: `scripts/run_lgb_workflow.py`
- Test: `tests/test_model_workflow.py`

- [ ] Write failing tests for provider URI injection and workflow command construction.
- [ ] Run tests and confirm they fail on missing module.
- [ ] Implement config rendering from local provider config.
- [ ] Add runner with `--dry-run` and real `qrun` execution modes.
- [ ] Smoke-test config rendering and dry-run command.

### Task 4: Documentation And Verification

**Files:**
- Modify: `README.md`

- [ ] Document the new commands and outputs.
- [ ] Run unit tests.
- [ ] Run `scripts/check_env.py`.
- [ ] Run representative CLI smoke tests.
