PYTHON ?= .venv/bin/python
PIP ?= .venv/bin/pip

FACTOR_CONFIG ?= configs/factor_mining.yaml
CSI500_PROVIDER ?= configs/provider_current.yaml
CSI300_PROVIDER ?= configs/provider_csi300_current.yaml
FACTOR ?= arbr_26
HORIZONS ?= --horizon 5 --horizon 20

.PHONY: help install test check-env candidates mine-csi500 mine-csi300 event-csi500 event-csi300 lgb-dry-run clean-pyc

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
	@printf "  make lgb-dry-run      Render Qlib LightGBM workflow config\n"
	@printf "  make clean-pyc        Remove Python bytecode caches\n"
	@printf "\n"
	@printf "Examples:\n"
	@printf "  make event-csi300 FACTOR=arbr_26\n"
	@printf "  make mine-csi500 HORIZONS='--horizon 5 --horizon 20 --horizon 60'\n"

install:
	python3 -m venv .venv
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

test:
	$(PYTHON) -m unittest discover -s tests

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
		$(HORIZONS)

event-csi300:
	$(PYTHON) scripts/backtest_factor_events.py \
		--factor $(FACTOR) \
		--config $(FACTOR_CONFIG) \
		--provider-config $(CSI300_PROVIDER) \
		$(HORIZONS)

lgb-dry-run:
	$(PYTHON) scripts/run_lgb_workflow.py \
		--provider-config $(CSI500_PROVIDER) \
		--output configs/qlib_lgb_workflow_current.yaml \
		--dry-run

clean-pyc:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
