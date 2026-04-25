# Streamlit Workbench Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade the static Factor Lab UI mockup into a real Streamlit workbench that reads local research artifacts.

**Architecture:** Keep data loading and explanation logic in a pure Python module under `src/qlib_factor_lab/workbench.py`, then use `app/streamlit_app.py` only for presentation. The first version is read-only: it displays pipeline status, portfolio gate explanations, and autoresearch nightly queues without directly running trading or research commands.

**Tech Stack:** Python, pandas, PyYAML, Streamlit, existing factor-lab reports/artifacts.

---

## Task 1: Workbench Data Layer

**Files:**
- Create: `src/qlib_factor_lab/workbench.py`
- Test: `tests/test_workbench.py`

- [ ] Write tests for loading autoresearch ledger status summaries.
- [ ] Write tests for building portfolio gate explanations from portfolio, risk config, and approved factor family map.
- [ ] Write tests for a workbench snapshot that counts approved factors and finds latest local target portfolio.
- [ ] Implement the smallest pure-Python functions to pass.

## Task 2: Streamlit App

**Files:**
- Create: `app/streamlit_app.py`
- Create: `scripts/run_workbench.py`

- [ ] Build a three-page Streamlit app: Dashboard, Portfolio Gate, Autoresearch Queue.
- [ ] Keep the app read-only and artifact-driven.
- [ ] Add a runner script that prints a clear install hint when Streamlit is missing.

## Task 3: Project Integration

**Files:**
- Modify: `requirements.txt`
- Modify: `pyproject.toml`
- Modify: `Makefile`
- Modify: `README.md`

- [ ] Add `streamlit` dependency.
- [ ] Add `make workbench`.
- [ ] Document how to start the workbench.
- [ ] Run targeted tests and full unit tests.
