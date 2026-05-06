# AI Semiconductor Theme Mode Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents are explicitly authorized) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a focused AI supply-chain research mode that hides the platform's data/factor complexity behind a chips, semiconductor, and memory signal workflow.

**Architecture:** Reuse the existing theme scanner as the boundary. Keep the low-level data governance and factor machinery unchanged, but add a theme universe, simplified score components, A/B/C tiers, markdown output, and a Streamlit page. Product scope is signal-only: do not expose portfolio construction, paper trading, or execution as the default workbench path.

**Tech Stack:** Python, pandas, YAML, unittest, Streamlit.

---

## File Structure

- Modify `src/qlib_factor_lab/theme_scanner.py`: add simplified component scoring, A/B/C tiering, and business-readable reasons while preserving existing columns.
- Create `configs/themes/ai_semiconductor.yaml`: focused AI industry-chain universe for chips, semiconductors, storage, advanced packaging, optical interconnect, and compute integration.
- Modify `app/streamlit_app.py`: add an `AI产业链` page that reads the latest theme scan and displays tiers, scores, and reasons.
- Modify `app/streamlit_app.py`: narrow default navigation to data, factor research, autoresearch, AI theme, and evidence. Hide portfolio gate, expert review, stock-card, and paper execution pages from the main product surface.
- Modify `tests/test_theme_scanner.py`: test component scores, tiers, reasons, markdown output, and the new config.
- Modify `tests/test_streamlit_workbench_ui.py`: verify the new page renders and is available in navigation.

## Task 1: Theme Candidate Score Contract

- [ ] **Step 1: Write failing tests**
  - Add a test where the signal frame includes quality, growth, momentum, event, and risk columns.
  - Assert output includes `sub_chain`, `theme_score`, `quality_score`, `growth_score`, `momentum_score`, `event_score`, `risk_penalty`, `total_score`, `tier`, and `reason`.
  - Assert blocked/risk names are tier C and strong clean names are tier A/B.

- [ ] **Step 2: Run targeted tests and verify failure**
  - Run `PYTHONPATH=src .venv/bin/python -m unittest tests.test_theme_scanner`.
  - Expected: fail because the new columns do not exist.

- [ ] **Step 3: Implement scoring in `theme_scanner.py`**
  - Normalize available factor-family columns into business components.
  - Compute `total_score = 0.30*theme + 0.20*quality + 0.20*growth + 0.15*momentum + 0.10*event - 0.15*risk_penalty`.
  - Derive A/B/C tiers from risk status and total score.
  - Generate a short Chinese `reason` for UI and report.

- [ ] **Step 4: Re-run targeted tests**
  - Expected: `tests.test_theme_scanner` passes.

## Task 2: AI Semiconductor Theme Universe

- [ ] **Step 1: Create config test**
  - Load `configs/themes/ai_semiconductor.yaml`.
  - Assert it contains at least chip design, foundry, semiconductor equipment/materials, memory/storage, advanced packaging/interconnect, and compute integration roles.

- [ ] **Step 2: Add focused YAML config**
  - Include only research universe metadata and known chain roles.
  - Keep language as research candidate, not investment advice.

- [ ] **Step 3: Re-run config and theme scanner tests**
  - Expected: all pass.

## Task 3: Workbench Theme Page

- [ ] **Step 1: Add UI tests**
  - Extend page render tests to include `10 AI产业链`.
  - Assert the page shows `AI产业链主题研究`, `A重点研究`, and `非投资建议`.

- [ ] **Step 2: Implement page**
  - Add page navigation entry.
  - Load latest `reports/theme_scans/ai_semiconductor_*.csv`.
  - Render KPI cards by tier and a simplified table.
  - Show a fallback command if no report exists.

- [ ] **Step 3: Re-run Streamlit UI tests**
  - Expected: Streamlit page tests pass.

## Task 3b: Signal-Only Product Surface

- [ ] **Step 1: Update UI tests**
  - Assert the sidebar exposes only dashboard, data governance, factor research, autoresearch, AI theme, and evidence.
  - Assert paper trading and portfolio gate pages are not exposed.

- [ ] **Step 2: Simplify dashboard copy**
  - Change the pipeline from data -> factor -> portfolio -> paper to data -> factor -> signal -> theme scan.
  - Change the right rail from portfolio gate to signal boundary.

- [ ] **Step 3: Update README**
  - State that the current supported output is `signals.csv` and theme signal reports.
  - Mark portfolio/paper artifacts as historical or out of current scope.

## Task 4: Run Current Scan and Verify

- [ ] **Step 1: Run theme scanner on latest signal**
  - Command: `PYTHONPATH=src .venv/bin/python scripts/run_theme_scanner.py --theme-config configs/themes/ai_semiconductor.yaml --signal-csv runs/20260430/signals.csv --top-k 30`

- [ ] **Step 2: Run verification**
  - Run targeted tests, then `make test`.
  - Run a diff secret scan for token/password patterns.

- [ ] **Step 3: Commit and push**
  - Commit message: `feat: add ai semiconductor theme mode`.
  - Push `main` to GitHub.
