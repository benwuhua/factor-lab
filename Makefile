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
AUTORESEARCH_LANE_SPACE ?= configs/autoresearch/lane_space.yaml
AUTORESEARCH_MULTILANE_OUTPUT ?= reports/autoresearch/multilane_summary.md
AUTORESEARCH_MULTILANE_LOOP_ROOT ?= reports/autoresearch/multilane_loop
AUTORESEARCH_MULTILANE_UNTIL ?= 08:30
AUTORESEARCH_MULTILANE_ITERATIONS ?= 0
AUTORESEARCH_MULTILANE_MAX_HOURS ?=
AUTORESEARCH_MULTILANE_SLEEP_SEC ?= 60
AUTORESEARCH_MULTILANE_MAX_CRASHES ?= 5
AUTORESEARCH_MULTILANE_MAX_WORKERS ?= 4
AUTORESEARCH_MULTILANE_CANDIDATE_GLOB ?= configs/autoresearch/candidates/*.yaml
AUTORESEARCH_MULTILANE_LANE_FACTOR_BATCH_SIZE ?= 2
AUTORESEARCH_MULTILANE_STOP_ON_REPEAT ?= 1
AUTORESEARCH_MULTILANE_STOP_ON_REPEAT_ARG = $(if $(filter 1 true yes TRUE YES,$(AUTORESEARCH_MULTILANE_STOP_ON_REPEAT)),--stop-on-rotation-exhausted,)
AUTORESEARCH_MULTILANE_INCLUDE_SHADOW ?= 0
AUTORESEARCH_MULTILANE_INCLUDE_SHADOW_ARG = $(if $(filter 1 true yes TRUE YES,$(AUTORESEARCH_MULTILANE_INCLUDE_SHADOW)),--include-shadow,)
AUTORESEARCH_MULTILANE_INCLUDE_REVERSAL ?= 0
AUTORESEARCH_MULTILANE_INCLUDE_REVERSAL_ARG = $(if $(filter 1 true yes TRUE YES,$(AUTORESEARCH_MULTILANE_INCLUDE_REVERSAL)),--include-reversal-expression-candidates,)
AUTORESEARCH_DATA_GOVERNANCE_REPORT ?= reports/data_governance_$(RUN_DATE).md
AUTORESEARCH_START_TIME ?=
AUTORESEARCH_END_TIME ?=
AUTORESEARCH_WINDOW_ARGS = $(if $(AUTORESEARCH_START_TIME),--start-time $(AUTORESEARCH_START_TIME),) $(if $(AUTORESEARCH_END_TIME),--end-time $(AUTORESEARCH_END_TIME),)
AUTORESEARCH_CODEX_MODEL ?= gpt-5.4
AUTORESEARCH_CODEX_UNTIL ?= 08:30
AUTORESEARCH_CODEX_ITERATIONS ?= 30
DATA_GOVERNANCE_CONFIG ?= configs/data_governance.yaml
DATA_GOVERNANCE_OUTPUT ?= reports/data_governance_$(RUN_DATE).md
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
PORTFOLIO_INTRADAY_PORTFOLIO ?= runs/$(RUN_DATE)/execution_portfolio.csv
PORTFOLIO_INTRADAY_QUOTES ?=
PORTFOLIO_INTRADAY_QUOTES_ARG = $(if $(PORTFOLIO_INTRADAY_QUOTES),--quote-csv $(PORTFOLIO_INTRADAY_QUOTES),)
PORTFOLIO_INTRADAY_OUTPUT ?= runs/$(RUN_DATE)/portfolio_intraday_performance.csv
PORTFOLIO_INTRADAY_REPORT ?= runs/$(RUN_DATE)/portfolio_intraday_performance.md
STOCK_CARDS_OUTPUT ?= reports/stock_cards_$(RUN_DATE).jsonl
THEME_CONFIG ?= configs/themes/deepseek_ascend_semiconductor.yaml
THEME_SCAN_OUTPUT ?= reports/theme_scans/deepseek_ascend_semiconductor_$(RUN_DATE).csv
THEME_SCAN_REPORT ?= reports/theme_scans/deepseek_ascend_semiconductor_$(RUN_DATE).md
THEME_GATE_REPORT ?= reports/theme_scans/deepseek_ascend_semiconductor_$(RUN_DATE)_theme_gate.md
THEME_SCAN_TOP_K ?= 30
THEME_SCAN_FILL_MISSING ?= 0
THEME_SCAN_FILL_MISSING_ARG = $(if $(filter 1 true yes TRUE YES,$(THEME_SCAN_FILL_MISSING)),--fill-missing-from-provider,)
THEME_SCAN_PROVIDER_CONFIGS ?= $(CSI500_PROVIDER) $(CSI300_PROVIDER)
THEME_SCAN_PROVIDER_ARGS = $(foreach provider,$(THEME_SCAN_PROVIDER_CONFIGS),--provider-config $(provider))
EXPOSURE_INPUT ?= $(TARGET_PORTFOLIO)
EXPOSURE_OUTPUT_DIR ?= reports/exposure_attribution
EXPECTED_POSITIONS ?= runs/$(RUN_DATE)/positions_expected.csv
ACTUAL_POSITIONS ?= runs/$(RUN_DATE)/positions_actual.csv
TARGET_GLOB ?= reports/paper_batch_targets/target_portfolio_*.csv
ORDERS_CSV ?= runs/$(RUN_DATE)/orders.csv
FILLS_CSV ?= runs/$(RUN_DATE)/fills.csv
HISTORICAL_DAYS ?= 30
EXECUTION_CALENDAR_OUTPUT ?= reports/execution_calendar_$(RUN_DATE).csv
LIQUIDITY_MICROSTRUCTURE_OUTPUT ?= data/liquidity_microstructure.csv
EMOTION_ATMOSPHERE_OUTPUT ?= data/emotion_atmosphere.csv
REPLAY_RUN_DIR ?= runs/$(RUN_DATE)
REPLAY_OUTPUT ?= $(REPLAY_RUN_DIR)/replay_report.md
RESEARCH_CONTEXT_AS_OF ?= $(RUN_DATE)
RESEARCH_CONTEXT_NOTICE_START ?= $(RUN_DATE)
RESEARCH_CONTEXT_NOTICE_END ?= $(RUN_DATE)
RESEARCH_CONTEXT_UNIVERSES ?= csi300 csi500
EVENT_BACKFILL_AS_OF ?= $(RUN_DATE)
EVENT_BACKFILL_DAYS ?= 90
EVENT_BACKFILL_UNIVERSES ?= csi300 csi500
DAILY_DATA_AS_OF ?= $(shell PYTHONPATH=src $(PYTHON) -c "from qlib_factor_lab.akshare_data import today_for_daily_data; print(today_for_daily_data().replace('-', ''))")
DAILY_DATA_MARKET_PROVIDER ?= tushare
DAILY_DATA_MARKET_PROVIDER_ARG = $(if $(DAILY_DATA_MARKET_PROVIDER),--market-data-provider $(DAILY_DATA_MARKET_PROVIDER),)
DAILY_DATA_FORCE_MARKET_START ?=
DAILY_DATA_FORCE_MARKET_START_ARG = $(if $(DAILY_DATA_FORCE_MARKET_START),--force-market-start $(DAILY_DATA_FORCE_MARKET_START),)
DAILY_DATA_SKIP_MARKET ?= 0
DAILY_DATA_SKIP_MARKET_ARG = $(if $(filter 1 true yes TRUE YES,$(DAILY_DATA_SKIP_MARKET)),--skip-market-data,)
DAILY_DATA_FETCH_FUNDAMENTALS ?= 0
DAILY_DATA_FETCH_FUNDAMENTALS_ARG = $(if $(filter 1 true yes TRUE YES,$(DAILY_DATA_FETCH_FUNDAMENTALS)),--fetch-fundamentals,)
DAILY_DATA_FUNDAMENTAL_PROVIDER ?= tushare
DAILY_DATA_FUNDAMENTAL_PROVIDER_ARG = $(if $(DAILY_DATA_FUNDAMENTAL_PROVIDER),--fundamental-provider $(DAILY_DATA_FUNDAMENTAL_PROVIDER),)
DAILY_DATA_DERIVE_VALUATION_FIELDS ?= 0
DAILY_DATA_DERIVE_VALUATION_FIELDS_ARG = $(if $(filter 1 true yes TRUE YES,$(DAILY_DATA_DERIVE_VALUATION_FIELDS)),--derive-valuation-fields,)
DAILY_DATA_FETCH_CNINFO_DIVIDENDS ?= 0
DAILY_DATA_FETCH_CNINFO_DIVIDENDS_ARG = $(if $(filter 1 true yes TRUE YES,$(DAILY_DATA_FETCH_CNINFO_DIVIDENDS)),--fetch-cninfo-dividends,)
DAILY_DATA_FUNDAMENTAL_SOURCE ?=
DAILY_DATA_FUNDAMENTAL_SOURCE_ARG = $(if $(DAILY_DATA_FUNDAMENTAL_SOURCE),--fundamental-source $(DAILY_DATA_FUNDAMENTAL_SOURCE),)
DAILY_DATA_SECURITY_MASTER_HISTORY_SOURCE ?=
DAILY_DATA_SECURITY_MASTER_HISTORY_SOURCE_ARG = $(if $(DAILY_DATA_SECURITY_MASTER_HISTORY_SOURCE),--security-master-history-source $(DAILY_DATA_SECURITY_MASTER_HISTORY_SOURCE),)
DAILY_DATA_ENV_FILE ?= .env
DAILY_DATA_ENV_FILE_ARG = $(if $(DAILY_DATA_ENV_FILE),--env-file $(DAILY_DATA_ENV_FILE),)
DAILY_DATA_RQDATA_SECURITY_MASTER_HISTORY ?= 0
DAILY_DATA_RQDATA_SECURITY_MASTER_HISTORY_ARG = $(if $(filter 1 true yes TRUE YES,$(DAILY_DATA_RQDATA_SECURITY_MASTER_HISTORY)),--rqdata-security-master-history,)
DAILY_DATA_RQDATA_START_DATE ?= 2015-01-01
DAILY_DATA_RQDATA_START_DATE_ARG = $(if $(DAILY_DATA_RQDATA_START_DATE),--rqdata-start-date $(DAILY_DATA_RQDATA_START_DATE),)
DAILY_DATA_RQDATA_OUTPUT ?= data/vendor/security_master_history_rqdata.csv
DAILY_DATA_RQDATA_OUTPUT_ARG = $(if $(DAILY_DATA_RQDATA_OUTPUT),--rqdata-output $(DAILY_DATA_RQDATA_OUTPUT),)
DAILY_DATA_LIMIT ?=
DAILY_DATA_LIMIT_ARG = $(if $(DAILY_DATA_LIMIT),--limit $(DAILY_DATA_LIMIT),)
DAILY_DATA_OFFSET ?= 0
DAILY_DATA_OFFSET_ARG = $(if $(filter-out 0,$(DAILY_DATA_OFFSET)),--offset $(DAILY_DATA_OFFSET),)
DAILY_DATA_DRY_RUN ?= 0
DAILY_DATA_DRY_RUN_ARG = $(if $(filter 1 true yes TRUE YES,$(DAILY_DATA_DRY_RUN)),--dry-run,)
DAILY_DATA_MANIFEST ?= reports/daily_data_update_$(DAILY_DATA_AS_OF).md
INDUSTRY_OVERRIDES_OUTPUT ?= data/security_industry_overrides.csv
INDUSTRY_OVERRIDES_AS_OF ?= $(RUN_DATE)
INDUSTRY_OVERRIDES_UNIVERSES ?= csi300 csi500
COMBO_SPEC ?= configs/combo_specs/balanced_multifactor_v1.yaml
COMBO_DIAGNOSTICS_PROVIDER ?= $(CSI500_PROVIDER)
COMBO_DIAGNOSTICS_START_TIME ?=
COMBO_DIAGNOSTICS_END_TIME ?=
COMBO_DIAGNOSTICS_OUTPUT ?= reports/combo_member_diagnostics_balanced_multifactor_v1_$(RUN_DATE).csv
COMBO_DIAGNOSTICS_WINDOW_ARGS = $(if $(COMBO_DIAGNOSTICS_START_TIME),--start-time $(COMBO_DIAGNOSTICS_START_TIME),) $(if $(COMBO_DIAGNOSTICS_END_TIME),--end-time $(COMBO_DIAGNOSTICS_END_TIME),)
EXPERT_REVIEWER ?= manual-reviewer
EXPERT_CONFIRM_REASON ?= reviewed in workbench

.PHONY: help install test workbench workbench-e2e check-env industry-overrides research-context event-backfill research-data-domains daily-data-update after-close-daily-data-update data-governance factor-research candidates mine-csi500 mine-csi300 event-csi500 event-csi300 summarize-event autoresearch-expression autoresearch-multilane autoresearch-multilane-loop autoresearch-ledger autoresearch-codex-loop select-factors combo-diagnostics execution-calendar liquidity-microstructure emotion-atmosphere daily-signal check-data-quality target-portfolio combo-manual-confirm stock-cards theme-scan exposure-attribution portfolio-intraday-performance paper-orders reconcile-account paper-batch historical-paper-batch replay-daily-run manual-ticket lgb-dry-run clean-pyc

help:
	@printf "Qlib Factor Lab commands\n"
	@printf "\n"
	@printf "  make install          Create .venv and install project dependencies\n"
	@printf "  make test             Run unit tests\n"
	@printf "  make workbench        Start the local Streamlit research workbench\n"
	@printf "  make workbench-e2e    Run browser E2E checks for the workbench\n"
	@printf "  make check-env        Check local Qlib provider environment\n"
	@printf "  make industry-overrides  Refresh CSI300/CSI500 industry override table\n"
	@printf "  make research-context Refresh security master and company event evidence\n"
	@printf "  make event-backfill   Backfill historical company events with merge dedupe\n"
	@printf "  make research-data-domains Build fundamental/shareholder/evidence data domains\n"
	@printf "  make daily-data-update Incrementally refresh market data and research data domains\n"
	@printf "  make after-close-daily-data-update Wait until after close, then run full data refresh\n"
	@printf "  make data-governance Check PIT data-domain coverage and lane readiness\n"
	@printf "  make factor-research Run the one-command factor research pipeline\n"
	@printf "  make candidates       Generate candidate factor table for CSI500 config\n"
	@printf "  make mine-csi500      Run 5/20 day candidate mining on CSI500 config\n"
	@printf "  make mine-csi300      Run 5/20 day candidate mining on CSI300 config\n"
	@printf "  make event-csi500     Event backtest FACTOR on CSI500 config\n"
	@printf "  make event-csi300     Event backtest FACTOR on CSI300 config\n"
	@printf "  make summarize-event  Render Markdown from an event summary CSV\n"
	@printf "  make autoresearch-expression  Run one controlled expression-factor loop\n"
	@printf "  make autoresearch-multilane  Run configured autoresearch lanes with shadow gating\n"
	@printf "  make autoresearch-multilane-loop  Run multilane autoresearch repeatedly until a deadline\n"
	@printf "  make autoresearch-ledger  Summarize expression autoresearch ledger\n"
	@printf "  make autoresearch-codex-loop  Run overnight Codex CLI expression autoresearch\n"
	@printf "  make select-factors   Build approved factor governance artifacts\n"
	@printf "  make combo-diagnostics  Evaluate recent IC/LS diagnostics for combo members\n"
	@printf "  make execution-calendar  Build daily A-share execution status CSV\n"
	@printf "  make liquidity-microstructure  Build persistent liquidity/tradability data domain\n"
	@printf "  make emotion-atmosphere  Build market emotion data domain from liquidity data\n"
	@printf "  make daily-signal     Build daily explainable signal from approved factors\n"
	@printf "  make check-data-quality  Check a daily signal before portfolio construction\n"
	@printf "  make target-portfolio Build TopK target portfolio from a daily signal\n"
	@printf "  make stock-cards      Build JSONL stock research cards from target portfolio\n"
	@printf "  make theme-scan       Scan a hot theme universe with latest daily signal\n"
	@printf "  make exposure-attribution  Explain factor-family, industry, and style exposures\n"
	@printf "  make portfolio-intraday-performance  Build daily execution portfolio PnL attribution\n"
	@printf "  make paper-orders    Generate paper orders/fills from target portfolio\n"
	@printf "  make reconcile-account  Reconcile expected vs actual paper positions\n"
	@printf "  make paper-batch     Run rolling paper batch over target portfolios\n"
	@printf "  make historical-paper-batch  Generate historical targets and run paper batch\n"
	@printf "  make replay-daily-run  Replay and audit a daily run bundle\n"
	@printf "  make manual-ticket   Generate human-reviewed manual order ticket\n"
	@printf "  make lgb-dry-run      Render Qlib LightGBM workflow config\n"
	@printf "  make clean-pyc        Remove Python bytecode caches\n"
	@printf "\n"
	@printf "Examples:\n"
	@printf "  make workbench\n"
	@printf "  make event-csi300 FACTOR=arbr_26\n"
	@printf "  make summarize-event FACTOR=arbr_26 SUMMARY=reports/factor_arbr_26_event_backtest_summary_csi300.csv\n"
	@printf "  make autoresearch-expression AUTORESEARCH_CANDIDATE=configs/autoresearch/candidates/example_expression.yaml\n"
	@printf "  make autoresearch-multilane-loop AUTORESEARCH_MULTILANE_UNTIL=08:30 AUTORESEARCH_MULTILANE_INCLUDE_SHADOW=1\n"
	@printf "  make autoresearch-ledger AUTORESEARCH_LEDGER=reports/autoresearch/expression_results.tsv\n"
	@printf "  make autoresearch-codex-loop AUTORESEARCH_CODEX_UNTIL=08:30 AUTORESEARCH_CODEX_ITERATIONS=30\n"
	@printf "  make select-factors FACTOR_SELECTION_CONFIG=configs/factor_selection.yaml\n"
	@printf "  make combo-diagnostics RUN_DATE=20260420 COMBO_DIAGNOSTICS_START_TIME=2025-10-01 COMBO_DIAGNOSTICS_END_TIME=2026-04-20\n"
	@printf "  make execution-calendar RUN_DATE=20260420\n"
	@printf "  make daily-signal SIGNAL_CONFIG=configs/signal.yaml SIGNAL_PROVIDER_CONFIG=configs/provider_current.yaml\n"
	@printf "  make check-data-quality SIGNAL_CSV=reports/signals_20260420.csv\n"
	@printf "  make industry-overrides RUN_DATE=20260420\n"
	@printf "  make research-context RUN_DATE=20260420\n"
	@printf "  make research-data-domains RUN_DATE=20260420\n"
	@printf "  make daily-data-update DAILY_DATA_AS_OF=20260420 DAILY_DATA_DRY_RUN=1\n"
	@printf "  make daily-data-update DAILY_DATA_MARKET_PROVIDER=tushare DAILY_DATA_LIMIT=5\n"
	@printf "  make daily-data-update DAILY_DATA_FETCH_FUNDAMENTALS=1 DAILY_DATA_FUNDAMENTAL_PROVIDER=tushare\n"
	@printf "  make data-governance RUN_DATE=20260420\n"
	@printf "  make target-portfolio SIGNAL_CSV=reports/signals_20260420.csv\n"
	@printf "  make stock-cards TARGET_PORTFOLIO=reports/target_portfolio_20260420.csv\n"
	@printf "  make theme-scan SIGNAL_CSV=runs/20260427/signals.csv RUN_DATE=20260427 THEME_SCAN_FILL_MISSING=1\n"
	@printf "  make exposure-attribution EXPOSURE_INPUT=reports/target_portfolio_20260420.csv\n"
	@printf "  make paper-orders TARGET_PORTFOLIO=reports/target_portfolio_20260420.csv CURRENT_POSITIONS=state/current_positions.csv\n"
	@printf "  make paper-batch TARGET_GLOB='reports/paper_batch_targets/target_portfolio_*.csv'\n"
	@printf "  make historical-paper-batch HISTORICAL_DAYS=30\n"
	@printf "  make replay-daily-run RUN_DATE=20260420\n"
	@printf "  make manual-ticket RUN_DATE=20260420\n"
	@printf "  make mine-csi500 HORIZONS='--horizon 5 --horizon 20 --horizon 60'\n"

install:
	python3 -m venv .venv
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests

workbench:
	$(PYTHON) scripts/run_workbench.py

workbench-e2e:
	$(PYTHON) scripts/run_workbench_e2e.py

check-env:
	$(PYTHON) scripts/check_env.py --provider-config $(CSI500_PROVIDER)

industry-overrides:
	$(PYTHON) scripts/build_security_industry_overrides.py \
		--as-of-date $(INDUSTRY_OVERRIDES_AS_OF) \
		--output $(INDUSTRY_OVERRIDES_OUTPUT) \
		--universes $(INDUSTRY_OVERRIDES_UNIVERSES)

research-context:
	$(PYTHON) scripts/build_research_context_data.py \
		--as-of-date $(RESEARCH_CONTEXT_AS_OF) \
		--notice-start $(RESEARCH_CONTEXT_NOTICE_START) \
		--notice-end $(RESEARCH_CONTEXT_NOTICE_END) \
		--universes $(RESEARCH_CONTEXT_UNIVERSES) \
		--merge-existing-events

event-backfill:
	$(PYTHON) scripts/backfill_company_events.py \
		--as-of-date $(EVENT_BACKFILL_AS_OF) \
		--days $(EVENT_BACKFILL_DAYS) \
		--universes $(EVENT_BACKFILL_UNIVERSES)

research-data-domains:
	$(PYTHON) scripts/build_research_data_domains.py \
		--as-of-date $(RESEARCH_CONTEXT_AS_OF) \
		$(DAILY_DATA_FETCH_FUNDAMENTALS_ARG) \
		$(DAILY_DATA_FUNDAMENTAL_PROVIDER_ARG) \
		$(DAILY_DATA_DERIVE_VALUATION_FIELDS_ARG) \
		$(DAILY_DATA_FETCH_CNINFO_DIVIDENDS_ARG) \
		$(DAILY_DATA_FUNDAMENTAL_SOURCE_ARG) \
		$(DAILY_DATA_LIMIT_ARG) \
		$(DAILY_DATA_OFFSET_ARG)

daily-data-update:
	$(PYTHON) scripts/update_daily_data.py \
		--as-of-date $(DAILY_DATA_AS_OF) \
		--manifest $(DAILY_DATA_MANIFEST) \
		$(DAILY_DATA_MARKET_PROVIDER_ARG) \
		$(DAILY_DATA_FORCE_MARKET_START_ARG) \
		$(DAILY_DATA_SKIP_MARKET_ARG) \
		$(DAILY_DATA_FETCH_FUNDAMENTALS_ARG) \
		$(DAILY_DATA_FUNDAMENTAL_PROVIDER_ARG) \
		$(DAILY_DATA_DERIVE_VALUATION_FIELDS_ARG) \
		$(DAILY_DATA_FETCH_CNINFO_DIVIDENDS_ARG) \
		$(DAILY_DATA_FUNDAMENTAL_SOURCE_ARG) \
		$(DAILY_DATA_SECURITY_MASTER_HISTORY_SOURCE_ARG) \
		$(DAILY_DATA_ENV_FILE_ARG) \
		$(DAILY_DATA_RQDATA_SECURITY_MASTER_HISTORY_ARG) \
		$(DAILY_DATA_RQDATA_START_DATE_ARG) \
		$(DAILY_DATA_RQDATA_OUTPUT_ARG) \
		$(DAILY_DATA_LIMIT_ARG) \
		$(DAILY_DATA_OFFSET_ARG) \
		$(DAILY_DATA_DRY_RUN_ARG)

after-close-daily-data-update:
	$(PYTHON) scripts/run_after_close_daily_update.py \
		--env-file $(DAILY_DATA_ENV_FILE)

data-governance:
	$(PYTHON) scripts/check_data_governance.py \
		--config $(DATA_GOVERNANCE_CONFIG) \
		--as-of-date $(RUN_DATE) \
		--output $(DATA_GOVERNANCE_OUTPUT)

factor-research:
	$(MAKE) research-context
	$(MAKE) data-governance
	$(MAKE) autoresearch-multilane
	$(MAKE) select-factors

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

autoresearch-multilane:
	$(PYTHON) scripts/autoresearch/run_multilane_autoresearch.py \
		--lane-space $(AUTORESEARCH_LANE_SPACE) \
		--contract $(AUTORESEARCH_CONTRACT) \
		--expression-space $(AUTORESEARCH_SPACE) \
		--expression-candidate $(AUTORESEARCH_CANDIDATE) \
		--mining-config $(FACTOR_CONFIG) \
		--provider-config $(CSI500_PROVIDER) \
		--output $(AUTORESEARCH_MULTILANE_OUTPUT) \
		--data-governance-report $(AUTORESEARCH_DATA_GOVERNANCE_REPORT) \
		$(AUTORESEARCH_WINDOW_ARGS)

autoresearch-multilane-loop:
	$(PYTHON) scripts/autoresearch/run_multilane_loop.py \
		--lane-space $(AUTORESEARCH_LANE_SPACE) \
		--contract $(AUTORESEARCH_CONTRACT) \
		--expression-space $(AUTORESEARCH_SPACE) \
		--expression-candidate $(AUTORESEARCH_CANDIDATE) \
		--expression-candidate-glob "$(AUTORESEARCH_MULTILANE_CANDIDATE_GLOB)" \
		--mining-config $(FACTOR_CONFIG) \
		--provider-config $(CSI500_PROVIDER) \
		--output-root $(AUTORESEARCH_MULTILANE_LOOP_ROOT) \
		--data-governance-report $(AUTORESEARCH_DATA_GOVERNANCE_REPORT) \
		--until $(AUTORESEARCH_MULTILANE_UNTIL) \
		--max-iterations $(AUTORESEARCH_MULTILANE_ITERATIONS) \
		$(if $(AUTORESEARCH_MULTILANE_MAX_HOURS),--max-hours $(AUTORESEARCH_MULTILANE_MAX_HOURS),) \
		--sleep-sec $(AUTORESEARCH_MULTILANE_SLEEP_SEC) \
		--max-crashes $(AUTORESEARCH_MULTILANE_MAX_CRASHES) \
		--max-workers $(AUTORESEARCH_MULTILANE_MAX_WORKERS) \
		--lane-factor-batch-size $(AUTORESEARCH_MULTILANE_LANE_FACTOR_BATCH_SIZE) \
		$(AUTORESEARCH_MULTILANE_STOP_ON_REPEAT_ARG) \
		$(AUTORESEARCH_MULTILANE_INCLUDE_SHADOW_ARG) \
		$(AUTORESEARCH_MULTILANE_INCLUDE_REVERSAL_ARG) \
		$(AUTORESEARCH_WINDOW_ARGS)

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

combo-diagnostics:
	$(PYTHON) scripts/eval_combo_spec_diagnostics.py \
		--combo-spec $(COMBO_SPEC) \
		--provider-config $(COMBO_DIAGNOSTICS_PROVIDER) \
		--output-csv $(COMBO_DIAGNOSTICS_OUTPUT) \
		$(COMBO_DIAGNOSTICS_WINDOW_ARGS)

execution-calendar:
	$(PYTHON) scripts/build_execution_calendar.py \
		--provider-config $(CSI500_PROVIDER) \
		--run-date $(subst -,,$(RUN_DATE)) \
		--output $(EXECUTION_CALENDAR_OUTPUT)

liquidity-microstructure:
	$(PYTHON) scripts/build_liquidity_microstructure.py \
		--provider-config $(CSI500_PROVIDER) \
		--start-date $(RUN_DATE) \
		--end-date $(RUN_DATE) \
		--output $(LIQUIDITY_MICROSTRUCTURE_OUTPUT) \
		--merge-existing
	$(PYTHON) scripts/build_liquidity_microstructure.py \
		--provider-config $(CSI300_PROVIDER) \
		--start-date $(RUN_DATE) \
		--end-date $(RUN_DATE) \
		--output $(LIQUIDITY_MICROSTRUCTURE_OUTPUT) \
		--merge-existing

emotion-atmosphere:
	$(PYTHON) scripts/build_emotion_atmosphere.py \
		--liquidity-csv $(LIQUIDITY_MICROSTRUCTURE_OUTPUT) \
		--start-date $(RUN_DATE) \
		--end-date $(RUN_DATE) \
		--output $(EMOTION_ATMOSPHERE_OUTPUT) \
		--merge-existing

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

combo-manual-confirm:
	$(PYTHON) scripts/run_daily_pipeline.py \
		--combo-spec $(COMBO_SPEC) \
		--expert-manual-confirm \
		--expert-reviewer "$(EXPERT_REVIEWER)" \
		--expert-confirm-reason "$(EXPERT_CONFIRM_REASON)"

stock-cards:
	$(PYTHON) scripts/build_stock_cards.py \
		--target-portfolio $(TARGET_PORTFOLIO) \
		--as-of-date $(RUN_DATE) \
		--output $(STOCK_CARDS_OUTPUT)

theme-scan:
	$(PYTHON) scripts/run_theme_scanner.py \
		--theme-config $(THEME_CONFIG) \
		--signal-csv $(SIGNAL_CSV) \
		--top-k $(THEME_SCAN_TOP_K) \
		--output-csv $(THEME_SCAN_OUTPUT) \
		--output-md $(THEME_SCAN_REPORT) \
		--theme-gate-output $(THEME_GATE_REPORT) \
		$(THEME_SCAN_FILL_MISSING_ARG) \
		$(THEME_SCAN_PROVIDER_ARGS)

exposure-attribution:
	$(PYTHON) scripts/build_exposure_attribution.py \
		--input-csv $(EXPOSURE_INPUT) \
		--approved-factors reports/approved_factors.yaml \
		--output-dir $(EXPOSURE_OUTPUT_DIR)

portfolio-intraday-performance:
	$(PYTHON) scripts/build_portfolio_intraday_performance.py \
		--portfolio $(PORTFOLIO_INTRADAY_PORTFOLIO) \
		--run-date $(RUN_DATE) \
		--output-csv $(PORTFOLIO_INTRADAY_OUTPUT) \
		--output-md $(PORTFOLIO_INTRADAY_REPORT) \
		$(PORTFOLIO_INTRADAY_QUOTES_ARG)

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

historical-paper-batch:
	$(PYTHON) scripts/run_historical_paper_batch.py \
		--days $(HISTORICAL_DAYS) \
		--provider-config $(CSI500_PROVIDER) \
		--signal-config $(SIGNAL_CONFIG) \
		--trading-config $(TRADING_CONFIG) \
		--portfolio-config $(PORTFOLIO_CONFIG) \
		--risk-config $(RISK_CONFIG) \
		--execution-config $(EXECUTION_CONFIG) \
		--current-positions $(CURRENT_POSITIONS)

replay-daily-run:
	$(PYTHON) scripts/replay_daily_run.py \
		--run-dir $(REPLAY_RUN_DIR) \
		--output $(REPLAY_OUTPUT)

manual-ticket:
	$(PYTHON) scripts/generate_manual_ticket.py \
		--orders-csv $(ORDERS_CSV) \
		--fills-csv $(FILLS_CSV)

lgb-dry-run:
	$(PYTHON) scripts/run_lgb_workflow.py \
		--provider-config $(CSI500_PROVIDER) \
		--output configs/qlib_lgb_workflow_current.yaml \
		--dry-run

clean-pyc:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
