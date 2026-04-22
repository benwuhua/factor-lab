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
