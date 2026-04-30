# Data and Artifact Policy

This project keeps source code, configuration, tests, and small research references in Git. Large local data and generated research outputs stay on the workstation.

## Tracked

- `configs/`: provider, factor mining, and model workflow templates.
- `factors/`: hand-written factor registry and candidate-family notes.
- `scripts/`: command-line entry points for data building, factor evaluation, mining, event backtests, and model workflows.
- `src/qlib_factor_lab/`: reusable package code.
- `tests/`: unit tests that run without downloading market data.
- `docs/`: design notes and operating docs.
- `reports/joinquant_factorlib/`: small JoinQuant factor-library snapshots used to explain why some factors were migrated.

## Ignored

- `.venv/`: local Python environment.
- `data/`: downloaded Qlib packages, AkShare source CSVs, normalized CSVs, and dumped Qlib binaries.
- `mlruns/`: local Qlib and MLflow experiment records.
- `reports/*.csv`, `reports/*.parquet`, `reports/*.png`: generated factor evaluation, backtest, chart, and audit outputs.
- `configs/qlib_lgb_workflow*.yaml`: rendered model workflow files generated from provider config.

## Rebuild Local Data

Official Qlib CN sample data:

```bash
python scripts/download_qlib_data.py
```

Current CSI500 data:

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

Current CSI300 data:

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

Use `--limit` for smoke tests and `--delay`/`--retries` if the free data source throttles requests.

Daily incremental refresh for the fixed CSI300 + CSI500 research universe:

```bash
make daily-data-update DAILY_DATA_AS_OF=20260427
```

Preview the update plan without touching data:

```bash
make daily-data-update DAILY_DATA_AS_OF=20260427 DAILY_DATA_DRY_RUN=1
```

Run only the research context/data-domain refresh when the market providers are already current:

```bash
make daily-data-update DAILY_DATA_AS_OF=20260427 DAILY_DATA_SKIP_MARKET=1
```

Replay and audit a completed daily run bundle:

```bash
make replay-daily-run RUN_DATE=20260427
```

This reads `runs/<date>/manifest.json`, verifies required artifacts, checks summary consistency, and writes:

```text
runs/<date>/replay_report.md
runs/<date>/replay_report.json
```

Daily portfolio outputs are intentionally split:

- `research_portfolio.csv`: pre-review research candidates generated from signals.
- `execution_portfolio.csv`: post-review, post-gate execution candidates used by risk checks and paper orders.
- `target_portfolio.csv`: legacy alias for `execution_portfolio.csv` kept for older scripts.
- `portfolio_intraday_performance.csv`: formal intraday performance attribution for the execution portfolio.

Build the intraday attribution artifact for a completed run:

```bash
make portfolio-intraday-performance RUN_DATE=20260430
```

For offline or audited quote snapshots:

```bash
make portfolio-intraday-performance \
  RUN_DATE=20260430 \
  PORTFOLIO_INTRADAY_QUOTES=/path/to/quotes.csv
```

Execution outputs are generated through `BrokerAdapter` implementations:

- `paper`: default simulated fills, positions, and reconciliation.
- `dry_run`: validates/submits orders but creates no fills.
- `manual_ticket`: marks orders for human ticket review.
- `real`: reserved for future broker APIs and disabled unless explicitly enabled in config.

The active mode is configured in `configs/execution.yaml` under `broker_adapter.mode`; real execution should stay disabled until paper/manual flows pass the same replay and gate checks.

The daily update does these steps in order:

- Incrementally update CSI500 Qlib bars in `data/qlib/cn_data_current`.
- Incrementally update CSI300 Qlib bars in `data/qlib/cn_data_csi300_current`.
- Refresh `data/security_master.csv` and `data/company_events.csv`.
- Build `data/fundamental_quality.csv`, `data/shareholder_capital.csv`, and `data/announcement_evidence.csv`.
- Write `reports/data_governance_<date>.md` and `reports/daily_data_update_<date>.md`.

`fundamental_quality.csv` has a stable PIT schema but is only populated when an offline source is supplied or AkShare fundamentals are explicitly enabled:

```bash
make daily-data-update DAILY_DATA_AS_OF=20260427 DAILY_DATA_FETCH_FUNDAMENTALS=1
```

When the upstream fundamental source does not expose an announcement date, Factor Lab uses a conservative PIT fallback:

- Q1 reports become available on `YYYY-04-30`.
- Half-year reports become available on `YYYY-08-31`.
- Q3 reports become available on `YYYY-10-31`.
- Annual reports become available on next year `YYYY-04-30`.

For a curated offline fundamental file:

```bash
make daily-data-update DAILY_DATA_AS_OF=20260427 DAILY_DATA_FUNDAMENTAL_SOURCE=/path/to/fundamental_source.csv
```

## Rebuild Common Reports

Generate the current candidate-factor table:

```bash
python scripts/mine_factors.py \
  --config configs/factor_mining.yaml \
  --provider-config configs/provider_current.yaml \
  --generate-only \
  --candidates-output reports/factor_mining_candidates_current.csv
```

Run CSI500 factor mining over 5-day and 20-day horizons:

```bash
python scripts/mine_factors.py \
  --config configs/factor_mining.yaml \
  --provider-config configs/provider_current.yaml \
  --output reports/factor_mining_current_h5_h20.csv \
  --candidates-output reports/factor_mining_candidates_current.csv \
  --horizon 5 \
  --horizon 20
```

Run a factor event backtest:

```bash
python scripts/backtest_factor_events.py \
  --factor arbr_26 \
  --config configs/factor_mining.yaml \
  --provider-config configs/provider_csi300_current.yaml \
  --horizon 5 \
  --horizon 20
```

Event backtests apply each factor's configured `direction` before percentile bucketing, so the `p95_p100` bucket means "highest configured score," not necessarily the highest raw factor value.

## CI Expectations

GitHub Actions runs unit tests only. It installs dependencies and executes:

```bash
python -m unittest discover -s tests
```

CI does not download market data, build Qlib binaries, train models, or run long backtests.
