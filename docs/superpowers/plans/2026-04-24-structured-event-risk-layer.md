# Structured Event Risk Layer Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a minimal point-in-time security master and structured company event risk layer, then feed it into daily portfolio gating and expert review packets.

**Architecture:** Keep this as a file-first research layer rather than a full RAG platform. Load normalized CSV inputs into small pandas modules, join event/security context into the daily signal and portfolio, add hard risk rules for severe events, and pass richer context to the expert LLM. This creates the data foundation for future announcement RAG without blocking current factor-lab usage.

**Tech Stack:** Python, pandas, YAML configs, CSV fixtures, Markdown run artifacts, unittest.

---

## File Structure

Create:

- `src/qlib_factor_lab/security_master.py`  
  Load point-in-time security metadata and join it to signal rows by `instrument` and `date`.

- `src/qlib_factor_lab/company_events.py`  
  Load normalized company events, filter active event windows, summarize event risk per instrument/date, and expose compact fields for portfolio/risk/expert review.

- `configs/event_risk.yaml`  
  Configure file paths, severity rules, lookback windows, hard-block event types, and packet display limits.

- `scripts/build_event_risk_snapshot.py`  
  Standalone CLI to build a daily event risk snapshot for inspection.

- `tests/test_security_master.py`  
  Unit tests for point-in-time metadata selection.

- `tests/test_company_events.py`  
  Unit tests for event loading, active-window filtering, severity summarization, and hard-block flags.

Modify:

- `src/qlib_factor_lab/daily_pipeline.py`  
  Load security/event context before portfolio construction, enrich signal rows, write `event_risk_snapshot.csv`, and route severe event blocks into risk/expert artifacts.

- `src/qlib_factor_lab/expert_review.py`  
  Include security master fields and event risk summaries in the packet.

- `src/qlib_factor_lab/portfolio.py`  
  Pass through event/security context columns into `target_portfolio.csv`.

- `src/qlib_factor_lab/risk.py`  
  Add hard checks for blocked event names and missing critical metadata.

- `tests/test_daily_pipeline.py`  
  Cover event enrichment, hard-block behavior, and packet visibility.

- `tests/test_expert_review.py`  
  Cover the richer event/security context in the review packet.

Do not create a database in this stage. CSV inputs are enough:

```text
data/security_master.csv
data/company_events.csv
```

## Data Contracts

### `data/security_master.csv`

Required columns:

```text
instrument,name,exchange,board,industry_sw,industry_csrc,is_st,listing_date,delisting_date,valid_from,valid_to
```

Rules:

- `instrument` must match the rest of factor-lab, e.g. `SH600000` or `SZ000001`.
- `valid_from` and `valid_to` define point-in-time validity.
- Empty `valid_to` means still valid.
- Empty `delisting_date` means not delisted.

### `data/company_events.csv`

Required columns:

```text
event_id,instrument,event_type,event_date,source,source_url,title,severity,summary,evidence,active_until
```

Rules:

- `event_date` is the public disclosure/effective date used for backtests and daily runs.
- `active_until` is optional. If empty, use config lookback by `event_type`.
- `severity` is one of `info`, `watch`, `risk`, `block`.
- `source` should preserve data credibility, e.g. `cninfo`, `sse`, `szse`, `manual`.

Initial event types:

```text
performance_warning_down
regulatory_inquiry
regulatory_warning
disciplinary_action
shareholder_reduction
large_unlock
trading_suspension
st_status
delisting_risk
lawsuit
guarantee
pledge_risk
abnormal_volatility
```

## Chunk 1: Point-In-Time Security Master

### Task 1: Load Security Master

**Files:**

- Create: `src/qlib_factor_lab/security_master.py`
- Test: `tests/test_security_master.py`

- [ ] **Step 1: Write failing tests for point-in-time metadata**

Add tests:

```python
def test_security_master_selects_row_valid_on_trade_date():
    frame = pd.DataFrame({
        "instrument": ["AAA", "AAA"],
        "name": ["Old Name", "New Name"],
        "exchange": ["SSE", "SSE"],
        "board": ["main", "main"],
        "industry_sw": ["old_industry", "new_industry"],
        "industry_csrc": ["old", "new"],
        "is_st": [False, True],
        "listing_date": ["2020-01-01", "2020-01-01"],
        "delisting_date": ["", ""],
        "valid_from": ["2020-01-01", "2026-01-01"],
        "valid_to": ["2025-12-31", ""],
    })
    signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["AAA"]})

    enriched = enrich_with_security_master(signal, frame)

    assert enriched.loc[0, "name"] == "New Name"
    assert bool(enriched.loc[0, "is_st"]) is True
```

Also test missing metadata:

```python
def test_security_master_marks_missing_metadata():
    signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["MISSING"]})
    enriched = enrich_with_security_master(signal, pd.DataFrame(columns=SECURITY_MASTER_COLUMNS))
    assert enriched.loc[0, "security_master_missing"] is True
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_security_master
```

Expected: import or function-not-found failure.

- [ ] **Step 3: Implement minimal security master module**

Implement:

```python
SECURITY_MASTER_COLUMNS = (...)

def load_security_master(path: str | Path | None) -> pd.DataFrame:
    if path is None or not Path(path).exists():
        return pd.DataFrame(columns=SECURITY_MASTER_COLUMNS)
    return pd.read_csv(path)

def enrich_with_security_master(signal: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
    # For each signal row, choose the master row where:
    # instrument matches
    # valid_from <= date
    # valid_to empty or date <= valid_to
    # pick the latest valid_from if more than one row matches.
```

Keep implementation simple; current daily runs are one date at a time, so a clear loop is acceptable.

- [ ] **Step 4: Run tests and verify pass**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_security_master
```

Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add src/qlib_factor_lab/security_master.py tests/test_security_master.py
git commit -m "feat: add point-in-time security master"
```

## Chunk 2: Structured Company Events

### Task 2: Load And Summarize Active Events

**Files:**

- Create: `src/qlib_factor_lab/company_events.py`
- Create: `configs/event_risk.yaml`
- Test: `tests/test_company_events.py`

- [ ] **Step 1: Write failing tests for active event summary**

Add tests:

```python
def test_event_summary_flags_blocking_event_within_window():
    signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["AAA"]})
    events = pd.DataFrame({
        "event_id": ["E1"],
        "instrument": ["AAA"],
        "event_type": ["disciplinary_action"],
        "event_date": ["2026-04-20"],
        "source": ["cninfo"],
        "source_url": ["https://example.com/e1"],
        "title": ["处罚公告"],
        "severity": ["block"],
        "summary": ["major penalty"],
        "evidence": ["公告原文"],
        "active_until": [""],
    })
    config = EventRiskConfig(block_event_types=("disciplinary_action",), default_lookback_days=30)

    snapshot = build_event_risk_snapshot(signal, events, config)

    assert snapshot.loc[0, "event_blocked"] is True
    assert "disciplinary_action" in snapshot.loc[0, "active_event_types"]
    assert "major penalty" in snapshot.loc[0, "event_risk_summary"]
```

Also test expired events are ignored.

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_company_events
```

Expected: import or function-not-found failure.

- [ ] **Step 3: Implement event risk module**

Implement:

```python
@dataclass(frozen=True)
class EventRiskConfig:
    events_path: Path | None = None
    security_master_path: Path | None = None
    default_lookback_days: int = 30
    event_type_lookbacks: dict[str, int] = field(default_factory=dict)
    block_event_types: tuple[str, ...] = (...)
    block_severities: tuple[str, ...] = ("block",)
    max_events_per_name: int = 3

def load_event_risk_config(path: str | Path) -> EventRiskConfig:
    ...

def load_company_events(path: str | Path | None) -> pd.DataFrame:
    ...

def build_event_risk_snapshot(signal: pd.DataFrame, events: pd.DataFrame, config: EventRiskConfig) -> pd.DataFrame:
    ...
```

Snapshot columns:

```text
date,instrument,event_count,event_blocked,max_event_severity,active_event_types,event_risk_summary,event_source_urls
```

Rules:

- An event is active if `event_date <= date <= active_until`.
- If `active_until` is empty, use `event_date + lookback_days`.
- `event_blocked` is true if any active event has severity in `block_severities` or event type in `block_event_types`.
- `event_risk_summary` should be compact: join up to `max_events_per_name` event summaries with `; `.

- [ ] **Step 4: Add default config**

Create `configs/event_risk.yaml`:

```yaml
event_risk:
  security_master_path: data/security_master.csv
  events_path: data/company_events.csv
  default_lookback_days: 30
  event_type_lookbacks:
    shareholder_reduction: 60
    large_unlock: 45
    regulatory_inquiry: 45
    disciplinary_action: 180
    delisting_risk: 180
  block_event_types:
    - disciplinary_action
    - delisting_risk
    - trading_suspension
    - st_status
  block_severities:
    - block
  max_events_per_name: 3
```

- [ ] **Step 5: Run tests and verify pass**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_company_events
```

Expected: OK.

- [ ] **Step 6: Commit**

```bash
git add src/qlib_factor_lab/company_events.py configs/event_risk.yaml tests/test_company_events.py
git commit -m "feat: add structured company event risk"
```

## Chunk 3: Event Risk Snapshot CLI

### Task 3: Build Daily Event Snapshot Script

**Files:**

- Create: `scripts/build_event_risk_snapshot.py`
- Test: `tests/test_company_events.py`

- [ ] **Step 1: Write failing CLI test**

Add a subprocess test that writes small `signals.csv`, `security_master.csv`, and `company_events.csv`, then runs:

```bash
.venv/bin/python scripts/build_event_risk_snapshot.py \
  --signals runs/20260423/signals.csv \
  --event-risk-config configs/event_risk.yaml \
  --output runs/20260423/event_risk_snapshot.csv \
  --project-root <tmp>
```

Assert output exists and includes `event_blocked`.

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_company_events
```

Expected: script missing.

- [ ] **Step 3: Implement CLI**

CLI responsibilities:

- Resolve paths relative to `--project-root`.
- Load signals.
- Load `EventRiskConfig`.
- Load company events.
- Build snapshot.
- Write CSV.
- Print `wrote: <path>`.

- [ ] **Step 4: Run test and verify pass**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_company_events
```

Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add scripts/build_event_risk_snapshot.py tests/test_company_events.py
git commit -m "feat: add event risk snapshot cli"
```

## Chunk 4: Daily Pipeline Integration

### Task 4: Enrich Signals With Security And Event Context

**Files:**

- Modify: `src/qlib_factor_lab/daily_pipeline.py`
- Modify: `src/qlib_factor_lab/portfolio.py`
- Modify: `scripts/run_daily_pipeline.py`
- Test: `tests/test_daily_pipeline.py`

- [ ] **Step 1: Add failing daily pipeline test**

In `tests/test_daily_pipeline.py`, extend fixture creation with:

```text
configs/event_risk.yaml
data/security_master.csv
data/company_events.csv
```

Add test:

```python
def test_daily_pipeline_writes_event_risk_snapshot_and_enriches_portfolio():
    ...
    assert (run_dir / "event_risk_snapshot.csv").exists()
    portfolio = pd.read_csv(run_dir / "target_portfolio.csv")
    assert "industry_sw" in portfolio.columns
    assert "event_risk_summary" in portfolio.columns
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_daily_pipeline
```

Expected: missing CLI arg or missing artifact.

- [ ] **Step 3: Add pipeline input and CLI arg**

Extend `DailyPipelineInputs`:

```python
event_risk_config_path: Path | None = None
```

Extend `scripts/run_daily_pipeline.py`:

```text
--event-risk-config configs/event_risk.yaml
```

If the config path is missing or file does not exist, skip enrichment and write no snapshot. This keeps existing fixtures simple.

- [ ] **Step 4: Enrich signal before tradability/portfolio**

Pipeline sequence should become:

```text
signal
  -> security master enrichment
  -> event risk snapshot join
  -> write signals.csv
  -> tradability filter
  -> portfolio
```

Important: write `signals.csv` after enrichment so run bundles are reproducible.

Add artifact:

```python
artifacts["event_risk_snapshot"] = str(event_risk_path)
```

- [ ] **Step 5: Pass through new columns in portfolio**

Extend `EXECUTION_PASSTHROUGH_COLUMNS` in `portfolio.py` with:

```text
name,exchange,board,industry_sw,industry_csrc,is_st,security_master_missing,
event_count,event_blocked,max_event_severity,active_event_types,event_risk_summary,event_source_urls
```

- [ ] **Step 6: Run tests and verify pass**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_daily_pipeline
```

Expected: OK.

- [ ] **Step 7: Commit**

```bash
git add src/qlib_factor_lab/daily_pipeline.py src/qlib_factor_lab/portfolio.py scripts/run_daily_pipeline.py tests/test_daily_pipeline.py
git commit -m "feat: enrich daily pipeline with event risk"
```

### Task 5: Add Hard Event Risk Gate

**Files:**

- Modify: `src/qlib_factor_lab/risk.py`
- Modify: `tests/test_daily_pipeline.py`
- Create or modify: `tests/test_risk.py` if it exists; otherwise add event risk assertions in `tests/test_daily_pipeline.py`.

- [ ] **Step 1: Write failing risk test**

Add behavior:

```python
portfolio = pd.DataFrame({
    "instrument": ["AAA"],
    "target_weight": [0.1],
    "event_blocked": [True],
    "event_risk_summary": ["disciplinary_action: penalty"],
})
report = check_portfolio_risk(portfolio, signal, RiskConfig(min_positions=1))
assert report.passed is False
assert "event_blocked_positions" in report.to_frame()["check"].tolist()
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_daily_pipeline
```

Expected: event risk check missing.

- [ ] **Step 3: Implement risk check**

In `check_portfolio_risk`, add:

```python
blocked_count = int(portfolio.get("event_blocked", pd.Series(dtype=bool)).fillna(False).astype(bool).sum())
rows.append(_row("event_blocked_positions", blocked_count == 0, blocked_count, 0, summaries))
```

Make `summaries` a short `; ` joined list of blocked instrument and summary.

- [ ] **Step 4: Run tests and verify pass**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_daily_pipeline
```

Expected: OK.

- [ ] **Step 5: Commit**

```bash
git add src/qlib_factor_lab/risk.py tests/test_daily_pipeline.py
git commit -m "feat: block portfolios with severe company events"
```

## Chunk 5: Expert Packet And Blocking Report

### Task 6: Add Event Context To Expert Review Packet

**Files:**

- Modify: `src/qlib_factor_lab/expert_review.py`
- Modify: `tests/test_expert_review.py`

- [ ] **Step 1: Write failing packet test**

Add to `_target_portfolio()` fixture:

```text
industry_sw,event_count,event_blocked,active_event_types,event_risk_summary,event_source_urls
```

Assert packet contains:

```text
Event Risk Context
active_event_types
event_risk_summary
event_source_urls
```

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_expert_review
```

Expected: missing section or fields.

- [ ] **Step 3: Implement packet section**

Add a section after `Pre-Trade Review Context`:

```markdown
## Event Risk Context

| rank | instrument | industry_sw | event_count | event_blocked | active_event_types | event_risk_summary | event_source_urls |
```

Keep rows limited by `max_positions`.

- [ ] **Step 4: Update expert questions**

Add one question:

```text
5. 哪些候选需要因为公告、监管、减持、解禁、ST/退市、诉讼或异常波动被阻断或人工复核？
```

- [ ] **Step 5: Run tests and verify pass**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_expert_review
```

Expected: OK.

- [ ] **Step 6: Commit**

```bash
git add src/qlib_factor_lab/expert_review.py tests/test_expert_review.py
git commit -m "feat: include event context in expert review"
```

### Task 7: Write Dedicated Block Report

**Files:**

- Modify: `src/qlib_factor_lab/daily_pipeline.py`
- Test: `tests/test_daily_pipeline.py`

- [ ] **Step 1: Write failing test for block report**

For risk failures caused by event risk or expert review gate, assert:

```text
runs/YYYYMMDD/block_report.md
```

exists and contains:

```text
expert_review_blocked
event_blocked_positions
```

depending on the test scenario.

- [ ] **Step 2: Run test and verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_daily_pipeline
```

Expected: block report missing.

- [ ] **Step 3: Implement helper**

Add in `daily_pipeline.py`:

```python
def _write_block_report(path: Path, *, status: str, risk_report: RiskReport | None, expert_review_gate: dict[str, str] | None) -> Path:
    ...
```

Report sections:

- status
- expert review gate
- failed risk checks
- next actions

- [ ] **Step 4: Wire block report into artifacts**

When status is `expert_review_blocked` or `risk_failed`, write:

```python
artifacts["block_report"] = str(block_report_path)
```

- [ ] **Step 5: Run tests and verify pass**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_daily_pipeline
```

Expected: OK.

- [ ] **Step 6: Commit**

```bash
git add src/qlib_factor_lab/daily_pipeline.py tests/test_daily_pipeline.py
git commit -m "feat: write daily block reports"
```

## Chunk 6: Run Bundle Verification And Docs

### Task 8: Add Minimal Example Data

**Files:**

- Create: `data/security_master.example.csv`
- Create: `data/company_events.example.csv`
- Modify: `README.md`

- [ ] **Step 1: Add tiny example CSVs**

Create 2-3 rows each. Do not include large datasets in git.

- [ ] **Step 2: Document how to copy examples**

Add README section:

```bash
cp data/security_master.example.csv data/security_master.csv
cp data/company_events.example.csv data/company_events.csv
```

- [ ] **Step 3: Commit**

```bash
git add data/security_master.example.csv data/company_events.example.csv README.md
git commit -m "docs: add event risk data examples"
```

### Task 9: End-To-End Verification

**Files:**

- No new files unless generated run artifacts are intentionally inspected. Do not commit `runs/`.

- [ ] **Step 1: Run full test suite**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests
```

Expected: all tests pass.

- [ ] **Step 2: Run daily pipeline with examples**

Run:

```bash
.venv/bin/python scripts/run_daily_pipeline.py \
  --project-root . \
  --event-risk-config configs/event_risk.yaml
```

Expected:

- `runs/YYYYMMDD/event_risk_snapshot.csv` exists if event data exists.
- `runs/YYYYMMDD/expert_review_packet.md` includes event context.
- severe events produce `risk_failed` or `expert_review_blocked`, not orders.

- [ ] **Step 3: Inspect manifest**

Run:

```bash
.venv/bin/python - <<'PY'
import json
from pathlib import Path
run = sorted(Path("runs").glob("20*"))[-1]
manifest = json.loads((run / "manifest.json").read_text())
print(json.dumps({
    "run": str(run),
    "status": manifest["status"],
    "artifacts": sorted(manifest["artifacts"]),
    "expert_review": manifest.get("expert_review"),
    "expert_review_gate": manifest.get("expert_review_gate"),
}, ensure_ascii=False, indent=2))
PY
```

- [ ] **Step 4: Commit any final docs-only cleanup**

Only if needed:

```bash
git add README.md docs/superpowers/plans/2026-04-24-structured-event-risk-layer.md
git commit -m "docs: finalize structured event risk plan"
```

## Acceptance Criteria

- Security metadata is point-in-time and does not use future rows.
- Company events are active only within configured event windows.
- Severe event names are blocked before order generation.
- `event_risk_snapshot.csv` is written into the daily run bundle.
- `target_portfolio.csv` includes relevant security/event context.
- `expert_review_packet.md` includes industry, liquidity, trading restriction, and structured event risk sections.
- `block_report.md` explains why a run stopped.
- Full tests pass with `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests`.

## Non-Goals

- Do not implement PDF parsing, crawling, embeddings, or a vector database in this plan.
- Do not make LLM output the source of truth for event severity.
- Do not use news/social media as hard facts unless manually curated into `company_events.csv`.
- Do not change factor scoring or autoresearch lanes in this plan.

