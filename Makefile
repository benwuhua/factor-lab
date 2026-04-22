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

.PHONY: help install test check-env candidates mine-csi500 mine-csi300 event-csi500 event-csi300 summarize-event autoresearch-expression autoresearch-ledger lgb-dry-run clean-pyc

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
	@printf "  make lgb-dry-run      Render Qlib LightGBM workflow config\n"
	@printf "  make clean-pyc        Remove Python bytecode caches\n"
	@printf "\n"
	@printf "Examples:\n"
	@printf "  make event-csi300 FACTOR=arbr_26\n"
	@printf "  make summarize-event FACTOR=arbr_26 SUMMARY=reports/factor_arbr_26_event_backtest_summary_csi300.csv\n"
	@printf "  make autoresearch-expression AUTORESEARCH_CANDIDATE=configs/autoresearch/candidates/example_expression.yaml\n"
	@printf "  make autoresearch-ledger AUTORESEARCH_LEDGER=reports/autoresearch/expression_results.tsv\n"
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

lgb-dry-run:
	$(PYTHON) scripts/run_lgb_workflow.py \
		--provider-config $(CSI500_PROVIDER) \
		--output configs/qlib_lgb_workflow_current.yaml \
		--dry-run

clean-pyc:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
