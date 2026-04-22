# Autoresearch Expression Loop Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first expression-factor autoresearch loop with locked contracts, candidate validation, oracle summary output, run artifacts, and an append-only ledger.

**Architecture:** Add a small `qlib_factor_lab.autoresearch` package that validates YAML specs, converts one candidate into a `FactorDef`, evaluates it through existing factor-eval primitives, writes artifacts, and appends a TSV ledger. Keep data-heavy evaluation behind a CLI while unit tests cover validation, summary assembly, and ledger behavior without Qlib data.

**Tech Stack:** Python 3.9, pandas, PyYAML, existing Qlib Factor Lab modules, `unittest`, `make`.

---

## File Structure

- `configs/autoresearch/contracts/csi500_current_v1.yaml`: locked first contract.
- `configs/autoresearch/expression_space.yaml`: allowed fields/operators/windows/families.
- `configs/autoresearch/candidates/example_expression.yaml`: editable single candidate.
- `configs/autoresearch/program_expression.md`: future-agent operating rules.
- `reports/autoresearch/README.md`: tracked placeholder for ignored run outputs.
- `src/qlib_factor_lab/autoresearch/contracts.py`: load and validate contracts.
- `src/qlib_factor_lab/autoresearch/expressions.py`: load/validate candidate and convert to `FactorDef`.
- `src/qlib_factor_lab/autoresearch/ledger.py`: append TSV rows.
- `src/qlib_factor_lab/autoresearch/oracle.py`: metrics extraction, summary rendering, run orchestration.
- `scripts/autoresearch/run_expression_loop.py`: CLI wrapper.
- `tests/test_autoresearch_contracts.py`: contract tests.
- `tests/test_autoresearch_expressions.py`: expression-space tests.
- `tests/test_autoresearch_ledger.py`: ledger tests.
- `tests/test_autoresearch_oracle.py`: summary/oracle unit tests.

## Chunk 1: Specs And Validation

### Task 1: Contract Loader

**Files:**
- Create: `tests/test_autoresearch_contracts.py`
- Create: `src/qlib_factor_lab/autoresearch/__init__.py`
- Create: `src/qlib_factor_lab/autoresearch/contracts.py`
- Create: `configs/autoresearch/contracts/csi500_current_v1.yaml`

- [ ] **Step 1: Write failing tests** for required contract fields and typed horizons.
- [ ] **Step 2: Run the contract tests and verify they fail** because the module does not exist.
- [ ] **Step 3: Implement the dataclass and loader** using existing YAML helpers.
- [ ] **Step 4: Run the contract tests and verify they pass.**

### Task 2: Candidate And Search-Space Validation

**Files:**
- Create: `tests/test_autoresearch_expressions.py`
- Create: `src/qlib_factor_lab/autoresearch/expressions.py`
- Create: `configs/autoresearch/expression_space.yaml`
- Create: `configs/autoresearch/candidates/example_expression.yaml`

- [ ] **Step 1: Write failing tests** for valid candidate conversion plus disallowed field/operator/window failures.
- [ ] **Step 2: Run the expression tests and verify they fail.**
- [ ] **Step 3: Implement search-space and candidate validation.**
- [ ] **Step 4: Run the expression tests and verify they pass.**

## Chunk 2: Ledger And Oracle

### Task 3: Ledger Writer

**Files:**
- Create: `tests/test_autoresearch_ledger.py`
- Create: `src/qlib_factor_lab/autoresearch/ledger.py`

- [ ] **Step 1: Write failing tests** for creating headers and appending rows.
- [ ] **Step 2: Run the ledger tests and verify they fail.**
- [ ] **Step 3: Implement append-only TSV writing.**
- [ ] **Step 4: Run the ledger tests and verify they pass.**

### Task 4: Oracle Summary Builder

**Files:**
- Create: `tests/test_autoresearch_oracle.py`
- Create: `src/qlib_factor_lab/autoresearch/oracle.py`

- [ ] **Step 1: Write failing tests** for extracting h5/h20 metrics and rendering a summary block.
- [ ] **Step 2: Run the oracle tests and verify they fail.**
- [ ] **Step 3: Implement summary extraction and rendering.**
- [ ] **Step 4: Run the oracle tests and verify they pass.**

## Chunk 3: CLI And Docs

### Task 5: CLI Wrapper

**Files:**
- Create: `scripts/autoresearch/run_expression_loop.py`
- Modify: `Makefile`

- [ ] **Step 1: Add a CLI smoke test or import test** that verifies the script parses and exposes `main`.
- [ ] **Step 2: Implement the CLI wrapper around the oracle.**
- [ ] **Step 3: Run targeted tests and `make help`.**

### Task 6: User-Facing Docs

**Files:**
- Modify: `.gitignore`
- Modify: `README.md`
- Modify: `docs/factor-research-path.md`
- Create: `configs/autoresearch/program_expression.md`
- Create: `reports/autoresearch/README.md`

- [ ] **Step 1: Add tracked docs and ignore generated autoresearch artifacts.**
- [ ] **Step 2: Document the command and workflow.**
- [ ] **Step 3: Run the full unit suite.**

## Final Verification

- [ ] Run `make test PYTHON=/Users/ryan/Documents/Codex/2026-04-20-https-www-joinquant-com-study/qlib-factor-lab/.venv/bin/python`.
- [ ] Run `make help`.
- [ ] Run a CLI import/help smoke check for `scripts/autoresearch/run_expression_loop.py`.
- [ ] Inspect `git status --short --branch`.
- [ ] Commit the implementation branch.
