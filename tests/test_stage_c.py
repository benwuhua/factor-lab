import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.data_quality import DataQualityConfig, check_signal_quality
from qlib_factor_lab.portfolio import PortfolioConfig, build_target_portfolio, load_portfolio_config
from qlib_factor_lab.risk import RiskConfig, check_portfolio_risk, load_risk_config
from qlib_factor_lab.tradability import TradabilityConfig, apply_tradability_filter


class StageCTests(unittest.TestCase):
    def test_default_portfolio_config_does_not_gate_on_retired_factor_columns(self):
        config = load_portfolio_config(Path(__file__).resolve().parents[1] / "configs/portfolio.yaml")

        self.assertNotIn("quality_low_leverage_contribution", config.required_min_scores)

    def test_default_risk_config_enables_tushare_vendor_data_gate(self):
        config = load_risk_config(Path(__file__).resolve().parents[1] / "configs/risk.yaml")

        self.assertTrue(config.enable_vendor_data_gate)
        self.assertEqual(800, config.min_tushare_domain_instruments)

    def test_signal_quality_fails_closed_on_missing_required_column(self):
        signal = self._signal().drop(columns=["ensemble_score"])

        report = check_signal_quality(signal, DataQualityConfig(min_coverage_ratio=0.8))

        self.assertFalse(report.passed)
        self.assertIn("required_columns", set(report.to_frame()["check"]))

    def test_tradability_filter_blocks_risk_flags_and_low_liquidity(self):
        annotated = apply_tradability_filter(
            self._signal(),
            TradabilityConfig(
                min_amount_20d=10_000_000,
                liquidity_column="amount_20d",
                blocked_risk_flags=("not_tradable", "limit_locked"),
            ),
        )

        eligible = annotated[annotated["eligible"]]
        self.assertEqual(list(eligible["instrument"]), ["AAA", "BBB"])
        reasons = dict(zip(annotated["instrument"], annotated["rejection_reason"]))
        self.assertEqual(reasons["CCC"], "not_tradable;risk_flag:not_tradable")
        self.assertEqual(reasons["DDD"], "risk_flag:limit_locked")
        self.assertEqual(reasons["EEE"], "low_liquidity")

    def test_target_portfolio_uses_topk_equal_weight_after_filters(self):
        annotated = apply_tradability_filter(
            self._signal(),
            TradabilityConfig(min_amount_20d=10_000_000, liquidity_column="amount_20d"),
        )

        portfolio = build_target_portfolio(
            annotated,
            PortfolioConfig(top_k=2, cash_buffer=0.1, max_single_weight=0.5),
        )

        self.assertEqual(list(portfolio["instrument"]), ["AAA", "BBB"])
        self.assertAlmostEqual(float(portfolio["target_weight"].sum()), 0.9)
        self.assertTrue((portfolio["target_weight"] == 0.45).all())
        self.assertEqual(list(portfolio["rank"]), [1, 2])
        self.assertEqual(list(portfolio["last_price"]), [12.3, 45.6])
        self.assertIn("limit_up", portfolio.columns)
        self.assertEqual(
            list(portfolio["selection_explanation"]),
            [
                "selected by ensemble_score 5; main drivers: core_alpha 3.2, challenger_alpha 1.8",
                "selected by ensemble_score 4; main drivers: core_alpha 2.6, challenger_alpha 1.4",
            ],
        )
        self.assertIn("top_factor_1", portfolio.columns)
        self.assertIn("top_factor_2_contribution", portfolio.columns)

    def test_target_portfolio_keeps_current_positions_inside_dropout_rank(self):
        signal = self._signal()
        signal.loc[signal["instrument"] == "DDD", "risk_flags"] = ""
        annotated = apply_tradability_filter(
            signal,
            TradabilityConfig(min_amount_20d=10_000_000, liquidity_column="amount_20d"),
        )
        current_positions = pd.DataFrame(
            {
                "instrument": ["DDD"],
                "current_weight": [0.2],
            }
        )

        portfolio = build_target_portfolio(
            annotated,
            PortfolioConfig(top_k=3, cash_buffer=0.1, max_single_weight=0.5, dropout_rank=4),
            current_positions=current_positions,
        )

        self.assertEqual(list(portfolio["instrument"]), ["AAA", "BBB", "DDD"])
        self.assertIn("held_by_dropout", set(portfolio["selection_reason"]))

    def test_target_portfolio_requires_non_quality_confirmation_when_configured(self):
        signal = pd.DataFrame(
            {
                "date": ["2026-04-23"] * 3,
                "instrument": ["QUALITY_ONLY", "CONFIRMED", "WEAK_CONFIRM"],
                "ensemble_score": [9.0, 8.0, 7.0],
                "risk_flags": ["", "", ""],
                "family_fundamental_quality_score": [0.35, 0.30, 0.35],
                "family_value_score": [0.0, 0.08, 0.01],
                "family_gap_risk_score": [0.0, 0.00, 0.01],
            }
        )

        portfolio = build_target_portfolio(
            signal,
            PortfolioConfig(
                top_k=2,
                cash_buffer=0.1,
                max_single_weight=0.5,
                require_positive_non_quality_confirmation=True,
                confirmation_exclude_families=("fundamental_quality",),
                confirmation_min_score=0.05,
            ),
        )

        self.assertEqual(list(portfolio["instrument"]), ["CONFIRMED"])

    def test_target_portfolio_applies_required_min_score_gates_before_ranking(self):
        signal = pd.DataFrame(
            {
                "date": ["2026-04-23"] * 3,
                "instrument": ["LOW_QUALITY_HIGH_SCORE", "PASSING_QUALITY", "ALSO_PASSING"],
                "ensemble_score": [9.0, 8.0, 7.0],
                "risk_flags": ["", "", ""],
                "quality_low_leverage_contribution": [-0.2, 0.1, 0.5],
            }
        )

        portfolio = build_target_portfolio(
            signal,
            PortfolioConfig(
                top_k=2,
                cash_buffer=0.1,
                max_single_weight=0.5,
                required_min_scores={"quality_low_leverage_contribution": 0.0},
            ),
        )

        self.assertEqual(list(portfolio["instrument"]), ["PASSING_QUALITY", "ALSO_PASSING"])

    def test_target_portfolio_offensive_profile_requires_growth_or_momentum_confirmation(self):
        signal = pd.DataFrame(
            {
                "date": ["2026-04-23"] * 4,
                "instrument": ["QUALITY_ONLY", "GROWTH_OK", "MOMENTUM_OK", "NEGATIVE_CONFIRM"],
                "ensemble_score": [10.0, 9.0, 8.0, 7.0],
                "risk_flags": ["", "", "", ""],
                "family_fundamental_quality_score": [0.8, 0.2, 0.1, 0.1],
                "family_growth_improvement_score": [0.0, 0.3, 0.0, -0.1],
                "family_momentum_score": [0.0, 0.0, 0.2, 0.0],
            }
        )

        portfolio = build_target_portfolio(
            signal,
            PortfolioConfig(top_k=3, profile="offensive", cash_buffer=0.1, max_single_weight=0.5),
        )

        self.assertEqual(list(portfolio["instrument"]), ["GROWTH_OK", "MOMENTUM_OK"])

    def test_target_portfolio_offensive_profile_caps_defensive_family_dominance(self):
        signal = pd.DataFrame(
            {
                "date": ["2026-04-23"] * 3,
                "instrument": ["DEFENSIVE_HEAVY", "GROWTH_LED", "DIVIDEND_HEAVY"],
                "ensemble_score": [10.0, 9.0, 8.0],
                "risk_flags": ["", "", ""],
                "family_fundamental_quality_score": [0.8, 0.1, 0.0],
                "family_dividend_score": [0.0, 0.0, 0.6],
                "family_growth_improvement_score": [0.2, 0.4, 0.1],
                "family_momentum_score": [0.0, 0.0, 0.0],
            }
        )

        portfolio = build_target_portfolio(
            signal,
            PortfolioConfig(
                top_k=3,
                profile="offensive",
                profile_constraints={"offensive": {"max_defensive_family_weight": 0.5}},
                cash_buffer=0.1,
                max_single_weight=0.5,
            ),
        )

        self.assertEqual(list(portfolio["instrument"]), ["GROWTH_LED"])

    def test_target_portfolio_defensive_profile_applies_profile_max_single_weight(self):
        portfolio = build_target_portfolio(
            self._signal(),
            PortfolioConfig(
                top_k=2,
                cash_buffer=0.0,
                max_single_weight=0.5,
                profile="defensive",
                profile_constraints={"defensive": {"max_single_weight": 0.2}},
            ),
        )

        self.assertEqual(list(portfolio["target_weight"]), [0.2, 0.2])

    def test_portfolio_passthrough_keeps_p2_event_evidence_fields(self):
        signal = self._signal()
        signal["positive_event_types"] = ["buyback", "", "", "", ""]
        signal["positive_event_summary"] = ["buyback plan", "", "", "", ""]
        signal["risk_event_types"] = ["", "lawsuit", "", "", ""]
        signal["risk_event_summary"] = ["", "pending lawsuit", "", "", ""]
        signal["event_source_urls"] = ["https://example.com/a", "https://example.com/b", "", "", ""]

        portfolio = build_target_portfolio(
            signal,
            PortfolioConfig(top_k=2, cash_buffer=0.1, max_single_weight=0.5),
        )

        for column in [
            "positive_event_types",
            "positive_event_summary",
            "risk_event_types",
            "risk_event_summary",
            "event_source_urls",
        ]:
            self.assertIn(column, portfolio.columns)

    def test_risk_checks_report_concentration_failure(self):
        portfolio = pd.DataFrame(
            {
                "date": ["2026-04-23"],
                "instrument": ["AAA"],
                "target_weight": [0.7],
            }
        )

        report = check_portfolio_risk(
            portfolio,
            self._signal(),
            RiskConfig(max_single_weight=0.5, min_positions=2, min_signal_coverage=0.2),
        )

        self.assertFalse(report.passed)
        failed = report.to_frame().query("status == 'fail'")
        self.assertIn("max_single_weight", set(failed["check"]))
        self.assertIn("min_positions", set(failed["check"]))

    def test_risk_checks_report_exposure_maturity_failures(self):
        portfolio = pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "BBB"],
                "target_weight": [0.4, 0.4],
                "industry": ["tech", "tech"],
                "top_factor_1": ["core_alpha", "core_alpha"],
                "top_factor_1_contribution": [2.0, 1.0],
            }
        )

        report = check_portfolio_risk(
            portfolio,
            self._signal(),
            RiskConfig(
                max_single_weight=0.5,
                min_positions=2,
                min_signal_coverage=0.2,
                max_industry_weight=0.6,
                min_factor_family_count=2,
                max_factor_family_concentration=0.7,
            ),
            factor_family_map={"core_alpha": "momentum"},
        )

        failed = report.to_frame().query("status == 'fail'")
        self.assertIn("max_industry_weight", set(failed["check"]))
        self.assertIn("min_factor_family_count", set(failed["check"]))
        self.assertIn("max_factor_family_concentration", set(failed["check"]))

    def test_risk_checks_report_factor_logic_concentration_failure(self):
        portfolio = pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "BBB", "CCC"],
                "target_weight": [0.2, 0.2, 0.2],
                "family_reversal_a_score": [0.5, 0.3, 0.2],
                "family_reversal_b_score": [0.4, 0.2, 0.1],
                "family_liquidity_score": [0.02, -0.01, 0.0],
            }
        )

        report = check_portfolio_risk(
            portfolio,
            self._signal(),
            RiskConfig(
                max_single_weight=0.5,
                min_positions=2,
                min_signal_coverage=0.2,
                min_factor_logic_count=2,
                max_factor_logic_concentration=0.55,
            ),
            factor_logic_map={
                "reversal_a": "reversal_repair",
                "reversal_b": "reversal_repair",
                "liquidity": "liquidity_quality",
            },
        )

        failed = report.to_frame().query("status == 'fail'")
        self.assertIn("max_factor_logic_concentration", set(failed["check"]))

    def test_risk_industry_check_uses_industry_sw_fallback(self):
        portfolio = pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "BBB"],
                "target_weight": [0.4, 0.4],
                "industry_sw": ["证券", "证券"],
            }
        )

        report = check_portfolio_risk(
            portfolio,
            self._signal(),
            RiskConfig(
                max_single_weight=0.5,
                min_positions=2,
                min_signal_coverage=0.2,
                max_industry_weight=0.6,
            ),
        )

        failed = report.to_frame().query("status == 'fail'")
        row = failed[failed["check"] == "max_industry_weight"].iloc[0]
        self.assertEqual(row["detail"], "证券=0.8")

    def test_risk_checks_report_cost_liquidity_and_budget_failures(self):
        portfolio = pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-23"],
                "instrument": ["AAA", "BBB"],
                "target_weight": [0.4, 0.2],
                "amount_20d": [100_000_000.0, 5_000_000.0],
                "turnover_20d": [0.7, 0.1],
            }
        )

        report = check_portfolio_risk(
            portfolio,
            self._signal(),
            RiskConfig(
                max_single_weight=0.5,
                min_positions=2,
                min_signal_coverage=0.2,
                portfolio_value=100_000_000,
                min_amount_20d=10_000_000,
                max_position_amount_share=0.3,
                max_estimated_cost=40_000,
                commission_bps=5,
                slippage_bps=10,
                stamp_tax_bps=10,
                max_risk_budget_per_position=0.12,
            ),
        )

        failed = report.to_frame().query("status == 'fail'")
        self.assertIn("min_amount_20d", set(failed["check"]))
        self.assertIn("max_position_amount_share", set(failed["check"]))
        self.assertIn("max_estimated_cost", set(failed["check"]))
        self.assertIn("max_risk_budget_per_position", set(failed["check"]))

    def test_configured_risk_check_fails_closed_when_required_source_column_missing(self):
        portfolio = pd.DataFrame(
            {
                "date": ["2026-04-23"],
                "instrument": ["AAA"],
                "target_weight": [0.4],
            }
        )

        report = check_portfolio_risk(
            portfolio,
            self._signal(),
            RiskConfig(
                max_single_weight=0.5,
                min_positions=1,
                min_signal_coverage=0.2,
                portfolio_value=100_000_000,
                min_amount_20d=10_000_000,
            ),
        )

        failed = report.to_frame().query("status == 'fail'")
        row = failed[failed["check"] == "min_amount_20d"].iloc[0]
        self.assertEqual(row["detail"], "missing required column: amount_20d")

    def test_build_target_portfolio_cli_writes_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            signal_path, trading_path, portfolio_path, risk_path = self._write_cli_fixture(root)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_target_portfolio.py"),
                    "--signal-csv",
                    str(signal_path.relative_to(root)),
                    "--trading-config",
                    str(trading_path.relative_to(root)),
                    "--portfolio-config",
                    str(portfolio_path.relative_to(root)),
                    "--risk-config",
                    str(risk_path.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((root / "reports/target_portfolio_20260423.csv").exists())
            self.assertTrue((root / "reports/portfolio_risk_20260423.md").exists())
            output = pd.read_csv(root / "reports/target_portfolio_20260423.csv")
            self.assertEqual(list(output["instrument"]), ["AAA", "BBB"])
            self.assertIn("wrote:", result.stdout)

    def _signal(self):
        return pd.DataFrame(
            {
                "date": ["2026-04-23"] * 5,
                "instrument": ["AAA", "BBB", "CCC", "DDD", "EEE"],
                "tradable": [True, True, False, True, True],
                "ensemble_score": [5.0, 4.0, 3.0, 2.0, 1.0],
                "rule_score": [5.0, 4.0, 3.0, 2.0, 1.0],
                "model_score": [0.0] * 5,
                "active_regime": ["sideways"] * 5,
                "top_factor_1": ["core_alpha", "core_alpha", "core_alpha", "core_alpha", "core_alpha"],
                "top_factor_1_contribution": [3.2, 2.6, 1.5, 0.5, 0.1],
                "top_factor_2": ["challenger_alpha", "challenger_alpha", "", "", ""],
                "top_factor_2_contribution": [1.8, 1.4, 0.0, 0.0, 0.0],
                "risk_flags": ["", "", "not_tradable", "limit_locked", ""],
                "amount_20d": [100_000_000, 80_000_000, 90_000_000, 70_000_000, 1_000_000],
                "last_price": [12.3, 45.6, 8.9, 23.4, 5.6],
                "limit_up": [False, False, True, False, False],
                "limit_down": [False, False, False, True, False],
            }
        )

    def _write_cli_fixture(self, root: Path):
        signal_path = root / "reports/signals_20260423.csv"
        trading_path = root / "configs/trading.yaml"
        portfolio_path = root / "configs/portfolio.yaml"
        risk_path = root / "configs/risk.yaml"
        signal_path.parent.mkdir(parents=True)
        trading_path.parent.mkdir(parents=True)
        self._signal().to_csv(signal_path, index=False)
        trading_path.write_text(
            yaml.safe_dump(
                {
                    "tradability": {
                        "min_amount_20d": 10_000_000,
                        "liquidity_column": "amount_20d",
                        "blocked_risk_flags": ["not_tradable", "limit_locked"],
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        portfolio_path.write_text(
            yaml.safe_dump(
                {
                    "portfolio": {"top_k": 2, "cash_buffer": 0.1, "max_single_weight": 0.5},
                    "output": {
                        "target_portfolio": "reports/target_portfolio_{run_yyyymmdd}.csv",
                        "summary": "reports/target_portfolio_summary_{run_yyyymmdd}.md",
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        risk_path.write_text(
            yaml.safe_dump(
                {
                    "risk": {"max_single_weight": 0.5, "min_positions": 2, "min_signal_coverage": 0.2},
                    "output": {"report": "reports/portfolio_risk_{run_yyyymmdd}.md"},
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return signal_path, trading_path, portfolio_path, risk_path


if __name__ == "__main__":
    unittest.main()
