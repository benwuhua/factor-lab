# Factor Purification and Exposure Attribution Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add lightweight AlphaPurify-inspired factor purification and exposure attribution to factor-lab.

**Architecture:** Add two small library modules and one CLI script. Keep existing factor evaluation and portfolio construction unchanged by default.

**Tech Stack:** Python, pandas, numpy, unittest, existing factor-lab script bootstrap.

---

### Task 1: Factor Purification

**Files:**
- Create: `src/qlib_factor_lab/factor_purification.py`
- Test: `tests/test_factor_purification.py`

- [ ] Write failing tests for MAD winsorization, z-score standardization, rank standardization, and neutralization.
- [ ] Run the targeted test and confirm it fails because the module does not exist.
- [ ] Implement minimal deterministic helpers.
- [ ] Run the targeted test and confirm it passes.

### Task 2: Exposure Attribution

**Files:**
- Create: `src/qlib_factor_lab/exposure_attribution.py`
- Create: `scripts/build_exposure_attribution.py`
- Test: `tests/test_exposure_attribution.py`

- [ ] Write failing tests for weighted factor-family contribution, industry concentration, and style exposure.
- [ ] Run the targeted test and confirm it fails because the module does not exist.
- [ ] Implement attribution helpers and markdown/CSV writers.
- [ ] Add CLI that reads a CSV and optional family map.
- [ ] Run the targeted test and confirm it passes.

### Task 3: Documentation and Verification

**Files:**
- Modify: `README.md`

- [ ] Document how to run the new exposure attribution report.
- [ ] Run targeted tests.
- [ ] Run the broader test suite if feasible.
- [ ] Commit and push to `main`.
