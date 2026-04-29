# Strategy Dictionary Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a strategy dictionary layer that turns external strategy archetypes into governed factor-research proposals for autoresearch and combo construction.

**Architecture:** Store strategy archetypes in a YAML dictionary, load and validate them through a small Python module, and expose a CLI that proposes unexplored candidate ideas while avoiding families already crowded in the current combo spec. The first version is read-only and advisory: it does not auto-edit factor configs.

**Tech Stack:** Python dataclasses, PyYAML, existing combo spec loader, unittest, Markdown docs.

---

## Chunk 1: Dictionary Data And Loader

### Task 1: Strategy Dictionary Loader

**Files:**
- Create: `src/qlib_factor_lab/strategy_dictionary.py`
- Create: `configs/strategy_dictionary/151_trading_strategies_equity.yaml`
- Test: `tests/test_strategy_dictionary.py`

- [ ] **Step 1: Write failing loader tests**
  - Test that the YAML loads into strategy entries.
  - Test that duplicate `strategy_id` values fail.
  - Test that entries can be filtered by candidate lane.

- [ ] **Step 2: Run tests to verify failure**
  - Run: `PYTHONPATH=src .venv/bin/python -m unittest tests.test_strategy_dictionary`
  - Expected: import failure because the module does not exist.

- [ ] **Step 3: Implement loader**
  - Add `StrategyEntry`, `load_strategy_dictionary`, and `filter_strategy_entries`.

- [ ] **Step 4: Run tests**
  - Expected: tests pass.

## Chunk 2: Proposal CLI

### Task 2: Strategy Dictionary Proposal Script

**Files:**
- Create: `scripts/autoresearch/propose_from_strategy_dictionary.py`
- Modify: `src/qlib_factor_lab/strategy_dictionary.py`
- Test: `tests/test_strategy_dictionary.py`

- [ ] **Step 1: Write failing proposal test**
  - Given a dictionary and current combo spec, propose strategies whose `strategy_family` is not already crowded.
  - Confirm the output includes `strategy_id`, `candidate_lane`, `template_formula`, and `reason`.

- [ ] **Step 2: Run tests to verify failure**
  - Expected: missing proposal function.

- [ ] **Step 3: Implement proposal function and CLI**
  - Add `propose_strategy_ideas`.
  - CLI writes Markdown and optional CSV.

- [ ] **Step 4: Run tests and a smoke CLI command**
  - Expected: tests pass and script writes a proposal report.

## Chunk 3: Documentation

### Task 3: Usage Guide

**Files:**
- Create: `docs/strategy_dictionary_guide.md`

- [ ] **Step 1: Document the operating model**
  - Explain that the dictionary is an inspiration source, not evidence.
  - Explain how autoresearch should use it.
  - Explain how combo construction should apply family constraints.

- [ ] **Step 2: Verify docs and full tests**
  - Run: `make test`
  - Expected: all tests pass.

