# Qlib Factor Lab

Qlib Factor Lab is a lightweight research scaffold for A-share factor work. It keeps factor definitions, data-building scripts, single-factor evaluation, event backtests, and model workflow generation in one small Python package.

The current project focuses on:

- Formula-style price, volume, turnover, volatility, reversal, and pattern factors.
- CSI500 and CSI300 local research datasets built from AkShare.
- JoinQuant factor-library migration candidates that can be expressed with local OHLCV and turnover fields.
- A factor-to-live-trading design note under `docs/superpowers/specs/`.

Generated market data, Qlib binaries, MLflow records, and backtest reports are intentionally ignored by Git. See [docs/data-and-artifacts.md](docs/data-and-artifacts.md).

For a compact command-by-command example, see [docs/factor-research-path.md](docs/factor-research-path.md).

## Project Layout

```text
configs/                 Provider, factor-mining, and model configs
docs/                    Design notes and operating docs
factors/                 Factor registry and candidate-family notes
reports/joinquant_factorlib/
                         Small JoinQuant factor-library snapshots
scripts/                 CLI commands for data, factors, events, models
src/qlib_factor_lab/     Reusable Python package
tests/                   Unit tests that do not require downloaded data
```

## Research Flow

```mermaid
flowchart TD
    A["Build or download local data"] --> B["Check provider config"]
    B --> C["Generate candidate factors"]
    C --> D["Mine IC / Rank IC"]
    D --> E{"Factor type?"}
    E -->|Cross-sectional feature| F["Batch compare and model workflow"]
    E -->|Absolute or pattern trigger| G["Event backtest by percentile bucket"]
    F --> H["Review stability by horizon, year, and market regime"]
    G --> H
    H --> I["Promote robust factors into model or live-trading design"]
    I --> J["Keep generated reports local; commit source and small references"]
```

The loop is intentionally conservative: use IC/Rank IC for broad triage, event backtests for trigger-like signals, then require horizon, yearly, and market-regime checks before a factor graduates.

## Quick Start

Create a local environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
```

Run the unit tests:

```bash
make test
```

Check the local Qlib environment after data has been downloaded or built:

```bash
make check-env
```

## Data Setup

Default provider configs:

```text
configs/provider.yaml                Official Qlib sample data
configs/provider_current.yaml        Current CSI500 AkShare/Qlib data
configs/provider_csi300_current.yaml Current CSI300 AkShare/Qlib data
```

Download official Qlib CN sample data:

```bash
python scripts/download_qlib_data.py
```

Build current CSI500 data:

```bash
python scripts/build_akshare_qlib_data.py \
  --universe csi500 \
  --start 20150101 \
  --end 20260420 \
  --history-source sina \
  --qlib-dir data/qlib/cn_data_current \
  --source-dir data/akshare/source \
  --provider-config configs/provider_current.yaml
```

Build current CSI300 data:

```bash
python scripts/build_akshare_qlib_data.py \
  --universe csi300 \
  --start 20150101 \
  --end 20260420 \
  --history-source sina \
  --qlib-dir data/qlib/cn_data_csi300_current \
  --source-dir data/akshare/source_csi300 \
  --provider-config configs/provider_csi300_current.yaml
```

AkShare free sources are good enough for local research prototypes, but production research should use a stable vendor feed. Use `--limit` for smoke tests and `--delay`/`--retries` when a source throttles requests.

## Factor Evaluation

Evaluate one registry factor:

```bash
python scripts/eval_factor.py \
  --provider-config configs/provider_current.yaml \
  --factor ret_20 \
  --output reports/factor_ret_20_current.csv
```

Run batch evaluation:

```bash
python scripts/batch_eval_factors.py \
  --provider-config configs/provider_current.yaml \
  --output reports/factor_batch_current.csv
```

Optional neutralization:

```bash
python scripts/eval_factor.py \
  --provider-config configs/provider_current.yaml \
  --factor ret_20 \
  --neutralize-size-proxy \
  --plot \
  --plot-horizon 5
```

The public Qlib CN sample data has no industry or market-cap fields. The project therefore supports:

- `--neutralize-size-proxy`: cross-sectional neutralization with `log(close * volume)` as a size/liquidity proxy.
- `--industry-map path/to/industry.csv`: optional custom industry map with `instrument,industry` columns.

## Candidate Mining

Candidate templates live in:

```text
configs/factor_mining.yaml
```

The current pool includes momentum, reversal, volatility, volume-price, liquidity, divergence, Wangji pattern, and JoinQuant-migrated turnover/emotion/technical factors.

Generate the candidate table only:

```bash
make candidates
```

Run a 5-day and 20-day CSI500 screen:

```bash
make mine-csi500
```

The result table includes IC, Rank IC, quintile mean returns, long-short return, turnover, and observation counts.

## Autoresearch

The first controlled autoresearch loop is expression-factor only. It lets an agent edit one candidate YAML while the provider, horizons, neutralization, ledger, and artifact paths stay locked by contract:

```bash
make autoresearch-expression
```

Default inputs:

```text
configs/autoresearch/contracts/csi500_current_v1.yaml
configs/autoresearch/expression_space.yaml
configs/autoresearch/candidates/example_expression.yaml
```

The loop prints a compact summary block, writes raw and size-proxy-neutralized evaluation artifacts, and appends a local ledger under `reports/autoresearch/`. Generated run outputs are ignored by Git.

## Event Backtests

Use event backtests when a factor is closer to an absolute trigger or pattern score than a pure IC feature:

```bash
make event-csi300 FACTOR=arbr_26
```

Event backtests apply the factor's configured `direction` before percentile bucketing. For example, a `direction: -1` factor treats lower raw values as higher scores, so `p95_p100` means the best configured score bucket.

Optional breakout-volume confirmation:

```bash
python scripts/backtest_factor_events.py \
  --factor wangji-factor1 \
  --provider-config configs/provider_current.yaml \
  --horizon 20 \
  --confirm-window 3 \
  --confirm-volume-ratio 1.2
```

Generate a Markdown summary from an event backtest summary CSV:

```bash
make summarize-event \
  FACTOR=arbr_26 \
  SUMMARY=reports/factor_arbr_26_event_backtest_summary_csi300.csv \
  SUMMARY_MD=reports/factor_arbr_26_event_backtest_summary_csi300.md
```

## Model Workflow

Render a Qlib Alpha158 + LightGBM workflow config without training:

```bash
python scripts/run_lgb_workflow.py \
  --provider-config configs/provider_current.yaml \
  --output configs/qlib_lgb_workflow_current.yaml \
  --dry-run
```

Run the workflow:

```bash
python scripts/run_lgb_workflow.py \
  --provider-config configs/provider_current.yaml \
  --output configs/qlib_lgb_workflow_current.yaml
```

For current data, the default split is:

```text
train: 2015-01-01 ~ 2021-12-31
valid: 2022-01-01 ~ 2023-12-31
test:  2024-01-01 ~ latest complete local trading day
```

If the benchmark index binary is missing, the workflow uses the candidate-pool stocks as an equal-weight benchmark proxy.

## CI

GitHub Actions runs:

```bash
python -m unittest discover -s tests
```

CI does not download market data or run long backtests. Those are local research steps because they depend on data availability, rate limits, and machine storage.

## Recommended Workflow

1. Build or download a local Qlib dataset.
2. Generate the candidate table from `configs/factor_mining.yaml`.
3. Use IC/Rank IC mining for broad factor triage.
4. Use event backtests for absolute or pattern-like factors.
5. Promote stable factors into model workflows or a live-trading pipeline design.
