PYTHON ?= .venv/bin/python
PIP ?= .venv/bin/pip

FACTOR_CONFIG ?= configs/factor_mining.yaml
CSI500_PROVIDER ?= configs/provider_current.yaml
CSI300_PROVIDER ?= configs/provider_csi300_current.yaml
FACTOR ?= arbr_26
HORIZONS ?= --horizon 5 --horizon 20
SUMMARY ?= reports/factor_$(FACTOR)_event_backtest_summary_csi300.csv
SUMMARY_MD ?= reports/factor_$(FACTOR)_event_backtest_summary_csi300.md
AUTORESEARCH_CONTRACT ?= configs/autoresearch/contracts/csi500_current_v1.yaml
AUTORESEARCH_SPACE ?= configs/autoresearch/expression_space.yaml
AUTORESEARCH_CANDIDATE ?= configs/autoresearch/candidates/example_expression.yaml
AUTORESEARCH_LEDGER ?= reports/autoresearch/expression_results.tsv
AUTORESEARCH_LEDGER_MD ?= reports/autoresearch/expression_results_summary.md
AUTORESEARCH_CODEX_MODEL ?= gpt-5.4
AUTORESEARCH_CODEX_UNTIL ?= 08:30
AUTORESEARCH_CODEX_ITERATIONS ?= 30
FACTOR_SELECTION_CONFIG ?= configs/factor_selection.yaml
SIGNAL_CONFIG ?= configs/signal.yaml
SIGNAL_PROVIDER_CONFIG ?=
SIGNAL_PROVIDER_ARGS = $(if $(SIGNAL_PROVIDER_CONFIG),--provider-config $(SIGNAL_PROVIDER_CONFIG),)
SIGNAL_CSV ?= reports/signals_latest.csv
TRADING_CONFIG ?= configs/trading.yaml
PORTFOLIO_CONFIG ?= configs/portfolio.yaml
RISK_CONFIG ?= configs/risk.yaml
EXECUTION_CONFIG ?= configs/execution.yaml
CURRENT_POSITIONS ?= state/current_positions.csv
RUN_DATE ?= 20260420
TARGET_PORTFOLIO ?= reports/target_portfolio_$(RUN_DATE).csv
EXPECTED_POSITIONS ?= runs/$(RUN_DATE)/positions_expected.csv
ACTUAL_POSITIONS ?= runs/$(RUN_DATE)/positions_actual.csv
TARGET_GLOB ?= reports/paper_batch_targets/target_portfolio_*.csv

.PHONY: help install test check-env candidates mine-csi500 mine-csi300 event-csi500 event-csi300 summarize-event autoresearch-expression autoresearch-ledger autoresearch-codex-loop select-factors daily-signal check-data-quality target-portfolio paper-orders reconcile-account paper-batch lgb-dry-run clean-pyc

help:
	@printf "Qlib Factor Lab commands\n"
	@printf "\n"
	@printf "  make install          Create .venv and install project dependencies\n"
	@printf "  make test             Run unit tests\n"
	@printf "  make check-env        Check local Qlib provider environment\n"
	@printf "  make candidates       Generate candidate factor table for CSI500 config\n"
	@printf "  make mine-csi500      Run 5/20 day candidate mining on CSI500 config\n"
	@printf "  make mine-csi300      Run 5/20 day candidate mining on CSI300 config\n"
	@printf "  make event-csi500     Event backtest FACTOR on CSI500 config\n"
	@printf "  make event-csi300     Event backtest FACTOR on CSI300 config\n"
	@printf "  make summarize-event  Render Markdown from an event summary CSV\n"
	@printf "  make autoresearch-expression  Run one controlled expression-factor loop\n"
	@printf "  make autoresearch-ledger  Summarize expression autoresearch ledger\n"
	@printf "  make autoresearch-codex-loop  Run overnight Codex CLI expression autoresearch\n"
	@printf "  make select-factors   Build approved factor governance artifacts\n"
	@printf "  make daily-signal     Build daily explainable signal from approved factors\n"
	@printf "  make check-data-quality  Check a daily signal before portfolio construction\n"
	@printf "  make target-portfolio Build TopK target portfolio from a daily signal\n"
	@printf "  make paper-orders    Generate paper orders/fills from target portfolio\n"
	@printf "  make reconcile-account  Reconcile expected vs actual paper positions\n"
	@printf "  make paper-batch     Run rolling paper batch over target portfolios\n"
	@printf "  make lgb-dry-run      Render Qlib LightGBM workflow config\n"
	@printf "  make clean-pyc        Remove Python bytecode caches\n"
	@printf "\n"
	@printf "Examples:\n"
	@printf "  make event-csi300 FACTOR=arbr_26\n"
	@printf "  make summarize-event FACTOR=arbr_26 SUMMARY=reports/factor_arbr_26_event_backtest_summary_csi300.csv\n"
	@printf "  make autoresearch-expression AUTORESEARCH_CANDIDATE=configs/autoresearch/candidates/example_expression.yaml\n"
	@printf "  make autoresearch-ledger AUTORESEARCH_LEDGER=reports/autoresearch/expression_results.tsv\n"
	@printf "  make autoresearch-codex-loop AUTORESEARCH_CODEX_UNTIL=08:30 AUTORESEARCH_CODEX_ITERATIONS=30\n"
	@printf "  make select-factors FACTOR_SELECTION_CONFIG=configs/factor_selection.yaml\n"
	@printf "  make daily-signal SIGNAL_CONFIG=configs/signal.yaml SIGNAL_PROVIDER_CONFIG=configs/provider_current.yaml\n"
	@printf "  make check-data-quality SIGNAL_CSV=reports/signals_20260420.csv\n"
	@printf "  make target-portfolio SIGNAL_CSV=reports/signals_20260420.csv\n"
	@printf "  make paper-orders TARGET_PORTFOLIO=reports/target_portfolio_20260420.csv CURRENT_POSITIONS=state/current_positions.csv\n"
	@printf "  make paper-batch TARGET_GLOB='reports/paper_batch_targets/target_portfolio_*.csv'\n"
	@printf "  make mine-csi500 HORIZONS='--horizon 5 --horizon 20 --horizon 60'\n"

install:
	python3 -m venv .venv
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests

check-env:
	$(PYTHON) scripts/check_env.py --provider-config $(CSI500_PROVIDER)

candidates:
	$(PYTHON) scripts/mine_factors.py \
		--config $(FACTOR_CONFIG) \
		--provider-config $(CSI500_PROVIDER) \
		--generate-only \
		--candidates-output reports/factor_mining_candidates_current.csv

mine-csi500:
	$(PYTHON) scripts/mine_factors.py \
		--config $(FACTOR_CONFIG) \
		--provider-config $(CSI500_PROVIDER) \
		--output reports/factor_mining_current_h5_h20.csv \
		--candidates-output reports/factor_mining_candidates_current.csv \
		$(HORIZONS)

mine-csi300:
	$(PYTHON) scripts/mine_factors.py \
		--config $(FACTOR_CONFIG) \
		--provider-config $(CSI300_PROVIDER) \
		--output reports/factor_mining_csi300_h5_h20.csv \
		--candidates-output reports/factor_mining_candidates_csi300.csv \
		$(HORIZONS)

event-csi500:
	$(PYTHON) scripts/backtest_factor_events.py \
		--factor $(FACTOR) \
		--config $(FACTOR_CONFIG) \
		--provider-config $(CSI500_PROVIDER) \
		--trades-output reports/factor_$(FACTOR)_event_backtest_trades_csi500.csv \
		--summary-output reports/factor_$(FACTOR)_event_backtest_summary_csi500.csv \
		--yearly-output reports/factor_$(FACTOR)_event_backtest_yearly_csi500.csv \
		$(HORIZONS)

event-csi300:
	$(PYTHON) scripts/backtest_factor_events.py \
		--factor $(FACTOR) \
		--config $(FACTOR_CONFIG) \
		--provider-config $(CSI300_PROVIDER) \
		--trades-output reports/factor_$(FACTOR)_event_backtest_trades_csi300.csv \
		--summary-output reports/factor_$(FACTOR)_event_backtest_summary_csi300.csv \
		--yearly-output reports/factor_$(FACTOR)_event_backtest_yearly_csi300.csv \
		$(HORIZONS)

summarize-event:
	$(PYTHON) scripts/summarize_event_report.py \
		--summary $(SUMMARY) \
		--output $(SUMMARY_MD) \
		--name "$(FACTOR) event backtest" \
		--factor $(FACTOR) \
		--provider-config $(CSI300_PROVIDER) \
		--command "make event-csi300 FACTOR=$(FACTOR)"

autoresearch-expression:
	$(PYTHON) scripts/autoresearch/run_expression_loop.py \
		--contract $(AUTORESEARCH_CONTRACT) \
		--space $(AUTORESEARCH_SPACE) \
		--candidate $(AUTORESEARCH_CANDIDATE)

autoresearch-ledger:
	$(PYTHON) scripts/autoresearch/summarize_expression_ledger.py \
		--ledger $(AUTORESEARCH_LEDGER) \
		--output $(AUTORESEARCH_LEDGER_MD)

autoresearch-codex-loop:
	$(PYTHON) scripts/autoresearch/run_expression_codex_loop.py \
		--until $(AUTORESEARCH_CODEX_UNTIL) \
		--max-iterations $(AUTORESEARCH_CODEX_ITERATIONS) \
		--model $(AUTORESEARCH_CODEX_MODEL) \
		--candidate-file $(AUTORESEARCH_CANDIDATE) \
		--ledger $(AUTORESEARCH_LEDGER)

select-factors:
	$(PYTHON) scripts/select_factors.py \
		--config $(FACTOR_SELECTION_CONFIG)

daily-signal:
	$(PYTHON) scripts/build_daily_signal.py \
		--config $(SIGNAL_CONFIG) \
		$(SIGNAL_PROVIDER_ARGS)

check-data-quality:
	$(PYTHON) scripts/check_data_quality.py \
		--signal-csv $(SIGNAL_CSV) \
		--config $(TRADING_CONFIG)

target-portfolio:
	$(PYTHON) scripts/build_target_portfolio.py \
		--signal-csv $(SIGNAL_CSV) \
		--trading-config $(TRADING_CONFIG) \
		--portfolio-config $(PORTFOLIO_CONFIG) \
		--risk-config $(RISK_CONFIG)

paper-orders:
	$(PYTHON) scripts/generate_orders.py \
		--target-portfolio $(TARGET_PORTFOLIO) \
		--current-positions $(CURRENT_POSITIONS) \
		--execution-config $(EXECUTION_CONFIG)

reconcile-account:
	$(PYTHON) scripts/reconcile_account.py \
		--expected-positions $(EXPECTED_POSITIONS) \
		--actual-positions $(ACTUAL_POSITIONS) \
		--execution-config $(EXECUTION_CONFIG)

paper-batch:
	$(PYTHON) scripts/run_paper_batch.py \
		--target-glob "$(TARGET_GLOB)" \
		--initial-positions $(CURRENT_POSITIONS) \
		--execution-config $(EXECUTION_CONFIG)

lgb-dry-run:
	$(PYTHON) scripts/run_lgb_workflow.py \
		--provider-config $(CSI500_PROVIDER) \
		--output configs/qlib_lgb_workflow_current.yaml \
		--dry-run

clean-pyc:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
