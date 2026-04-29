import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.expert_review import (
    ExpertReviewRunConfig,
    apply_expert_review_portfolio_gate,
    build_expert_review_packet,
    parse_expert_review_decision,
    parse_expert_review_manual_items,
    run_expert_review_command,
    write_expert_review_result,
)


class ExpertReviewTests(unittest.TestCase):
    def test_build_expert_review_packet_includes_portfolio_drivers_and_factor_warnings(self):
        packet = build_expert_review_packet(
            target_portfolio=self._target_portfolio(),
            factor_diagnostics=self._factor_diagnostics(),
            run_date="2026-04-23",
        )

        self.assertIn("# Expert Portfolio Review Packet", packet)
        self.assertIn("run_date: 2026-04-23", packet)
        self.assertIn("AAA", packet)
        self.assertIn("main drivers", packet)
        self.assertIn("shadow_review", packet)
        self.assertIn("Questions For Expert LLM", packet)
        self.assertNotIn(" nan ", packet)

    def test_build_expert_review_packet_includes_pre_trade_review_context(self):
        packet = build_expert_review_packet(
            target_portfolio=self._target_portfolio(),
            factor_diagnostics=self._factor_diagnostics(),
            run_date="2026-04-23",
        )

        self.assertIn("## Pre-Trade Review Context", packet)
        self.assertIn("industry", packet)
        self.assertIn("amount_20d", packet)
        self.assertIn("limit_up", packet)
        self.assertIn("abnormal_event", packet)
        self.assertIn("liquidity", packet)

    def test_build_expert_review_packet_includes_event_risk_context(self):
        packet = build_expert_review_packet(
            target_portfolio=self._target_portfolio(),
            factor_diagnostics=self._factor_diagnostics(),
            run_date="2026-04-23",
        )

        self.assertIn("## Event Risk Context", packet)
        self.assertIn("active_event_types", packet)
        self.assertIn("event_risk_summary", packet)
        self.assertIn("event_source_urls", packet)
        self.assertIn("disciplinary_action", packet)
        self.assertIn("selected name event context", packet)
        self.assertIn("https://example.test/events/evt-1", packet)
        self.assertIn("5. 哪些候选需要因为公告、监管、减持、解禁、ST/退市、诉讼或异常波动被阻断或人工复核？", packet)

    def test_build_expert_review_packet_includes_stock_card_context(self):
        packet = build_expert_review_packet(
            target_portfolio=self._target_portfolio(),
            stock_cards=[
                {
                    "instrument": "AAA",
                    "audit": {"review_decision": "caution"},
                    "current_signal": {"top_factor_1": "alpha_a", "ensemble_score": 5.0},
                    "evidence": {"event_count": 1, "max_event_severity": "watch", "event_risk_summary": "buyback watch"},
                    "review_questions": {"gate_reason": "max_industry_weight:fail"},
                }
            ],
            run_date="2026-04-23",
        )

        self.assertIn("## Stock Research Cards", packet)
        self.assertIn("buyback watch", packet)
        self.assertIn("max_industry_weight:fail", packet)

    def test_apply_expert_review_portfolio_gate_scales_caution_weights(self):
        portfolio = self._target_portfolio()

        gated, gate = apply_expert_review_portfolio_gate(
            portfolio,
            decision="caution",
            caution_action="scale",
            caution_weight_multiplier=0.5,
        )

        self.assertEqual(gate["status"], "scaled")
        self.assertAlmostEqual(float(gated["target_weight"].sum()), 0.0475)
        self.assertTrue(gated["risk_flags"].str.contains("expert_review_caution_scaled").all())

    def test_apply_expert_review_portfolio_gate_blocks_reject(self):
        portfolio = self._target_portfolio()

        gated, gate = apply_expert_review_portfolio_gate(portfolio, decision="reject")

        self.assertTrue(gated.empty)
        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["action"], "block")

    def test_apply_expert_review_portfolio_gate_blocks_reject_even_with_manual_confirmation(self):
        portfolio = self._target_portfolio()

        gated, gate = apply_expert_review_portfolio_gate(
            portfolio,
            decision="reject",
            manual_confirmation={"enabled": True, "reviewer": "ryan", "reason": "override attempt"},
        )

        self.assertTrue(gated.empty)
        self.assertEqual(gate["status"], "blocked")
        self.assertEqual(gate["action"], "block")

    def test_apply_expert_review_portfolio_gate_blocks_missing_required_review(self):
        portfolio = self._target_portfolio()

        gated, gate = apply_expert_review_portfolio_gate(
            portfolio,
            decision="unknown",
            review_status="timeout",
            review_required=True,
        )

        self.assertTrue(gated.empty)
        self.assertEqual(gate["status"], "blocked")
        self.assertIn("required expert review", gate["detail"])

    def test_apply_expert_review_portfolio_gate_requires_confirmation_for_hard_manual_list(self):
        portfolio = self._target_portfolio()
        portfolio["instrument"] = ["SH600580", "SZ002568"]
        output = """
结论：`caution`

建议“硬人工复核后再决定”：

- `SH600580`：公告密集，建议人工复核。
- `SZ002568`：流动性复核。
"""

        gated, gate = apply_expert_review_portfolio_gate(
            portfolio,
            decision="caution",
            review_output=output,
            caution_action="scale",
            caution_weight_multiplier=0.5,
        )

        self.assertEqual(gate["status"], "manual_confirmation_required")
        self.assertEqual(gate["action"], "require_manual_confirmation")
        self.assertIn("SH600580", gate["detail"])
        self.assertTrue(gated["risk_flags"].str.contains("expert_manual_review_required").all())
        self.assertAlmostEqual(float(gated["target_weight"].sum()), 0.0475)

    def test_apply_expert_review_portfolio_gate_allows_confirmed_hard_manual_list(self):
        portfolio = pd.DataFrame(
            {
                "instrument": ["SH600580", "SZ002568"],
                "target_weight": [0.05, 0.045],
                "risk_flags": ["", ""],
            }
        )

        gated, gate = apply_expert_review_portfolio_gate(
            portfolio,
            decision="caution",
            caution_weight_multiplier=0.5,
            review_output="硬人工复核名单：SH600580、SZ002568",
            manual_confirmation={"enabled": True, "reviewer": "ryan", "reason": "charts checked"},
        )

        self.assertEqual(gate["status"], "manual_confirmed")
        self.assertEqual(gate["action"], "allow_after_manual_confirmation")
        self.assertEqual(gate["reviewer"], "ryan")
        self.assertIn("charts checked", gate["detail"])
        self.assertTrue(gated["risk_flags"].str.contains("expert_manual_review_required").all())
        self.assertTrue(gated["risk_flags"].str.contains("expert_manual_confirmed").all())
        self.assertAlmostEqual(float(gated["target_weight"].sum()), 0.0475)

    def test_parse_expert_review_manual_items_extracts_hard_review_section(self):
        text = """
建议“硬人工复核后再决定”：

- `SZ002738`：权益变动、减持相关。
- `SH603899`：分拆上市相关。

建议流动性复核：

`SH601921`、`SH603899`
"""

        result = parse_expert_review_manual_items(text)

        self.assertEqual(result["hard_manual_review"], ["SZ002738", "SH603899"])
        self.assertEqual(result["liquidity_review"], ["SH601921", "SH603899"])

    def test_build_expert_review_packet_cli_writes_markdown(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_path = root / "runs/20260423/target_portfolio.csv"
            diagnostics_path = root / "reports/diagnostics.csv"
            output_path = root / "runs/20260423/expert_review_packet.md"
            target_path.parent.mkdir(parents=True)
            diagnostics_path.parent.mkdir(parents=True)
            self._target_portfolio().to_csv(target_path, index=False)
            self._factor_diagnostics().to_csv(diagnostics_path, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_expert_review_packet.py"),
                    "--target-portfolio",
                    str(target_path.relative_to(root)),
                    "--factor-diagnostics",
                    str(diagnostics_path.relative_to(root)),
                    "--run-date",
                    "2026-04-23",
                    "--output",
                    str(output_path.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(output_path.exists())
            self.assertIn("Expert Portfolio Review Packet", output_path.read_text(encoding="utf-8"))
            self.assertIn("wrote:", result.stdout)

    def test_build_expert_review_packet_cli_can_run_review_command(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target_path = root / "runs/20260423/target_portfolio.csv"
            output_path = root / "runs/20260423/expert_review_packet.md"
            result_path = root / "runs/20260423/expert_review_result.md"
            target_path.parent.mkdir(parents=True)
            self._target_portfolio().to_csv(target_path, index=False)
            repo = Path(__file__).resolve().parents[1]

            result = subprocess.run(
                [
                    sys.executable,
                    str(repo / "scripts/build_expert_review_packet.py"),
                    "--target-portfolio",
                    str(target_path.relative_to(root)),
                    "--run-date",
                    "2026-04-23",
                    "--output",
                    str(output_path.relative_to(root)),
                    "--run-review",
                    "--llm-command",
                    f"{sys.executable} -c \"import sys; sys.stdin.read(); print('research_review_status: reject')\"",
                    "--review-output",
                    str(result_path.relative_to(root)),
                    "--project-root",
                    str(root),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(result_path.exists())
            self.assertIn("decision: reject", result_path.read_text(encoding="utf-8"))

    def test_run_expert_review_command_records_decision(self):
        packet = "# packet\n"
        command = [
            sys.executable,
            "-c",
            "import sys; sys.stdin.read(); print('research_review_status: caution\\nreason: too concentrated')",
        ]

        result = run_expert_review_command(packet, ExpertReviewRunConfig(enabled=True, command=command))

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.decision, "caution")
        self.assertIn("too concentrated", result.output)

    def test_parse_expert_review_decision_accepts_markdown_and_defaults_to_unknown(self):
        self.assertEqual(parse_expert_review_decision("研究复核结论: reject"), "reject")
        self.assertEqual(parse_expert_review_decision("**结论：`caution`**，不是 `reject`。"), "caution")
        self.assertEqual(parse_expert_review_decision("结论：**`caution`**\n我不会给 `reject`。"), "caution")
        self.assertEqual(parse_expert_review_decision("No explicit decision."), "unknown")

    def test_write_expert_review_result_outputs_markdown(self):
        result = run_expert_review_command(
            "# packet\n",
            ExpertReviewRunConfig(
                enabled=True,
                command=[
                    sys.executable,
                    "-c",
                    "import sys; sys.stdin.read(); print('research_review_status: pass')",
                ],
            ),
        )
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "expert_review_result.md"

            write_expert_review_result(result, output)

            text = output.read_text(encoding="utf-8")
            self.assertIn("status: completed", text)
            self.assertIn("decision: pass", text)

    def _target_portfolio(self):
        return pd.DataFrame(
            {
                "instrument": ["AAA", "BBB"],
                "rank": [1, 2],
                "target_weight": [0.0475, 0.0475],
                "ensemble_score": [5.0, 4.0],
                "selection_explanation": [
                    "selected by ensemble_score 5; main drivers: alpha_a 3, alpha_b 2",
                    "selected by ensemble_score 4; main drivers: alpha_c 4",
                ],
                "top_factor_1": ["alpha_a", "alpha_c"],
                "top_factor_1_contribution": [3.0, 4.0],
                "risk_flags": ["", ""],
                "amount_20d": [100_000_000, 80_000_000],
                "turnover_20d": [0.03, 0.02],
                "industry": ["医药", "电力设备"],
                "limit_up": [False, False],
                "limit_down": [False, True],
                "suspended": [False, False],
                "abnormal_event": ["", "earnings_warning"],
                "announcement_flag": [False, True],
                "industry_sw": ["Pharma", "Power Equipment"],
                "event_count": [1, 0],
                "event_blocked": [True, False],
                "active_event_types": ["disciplinary_action", ""],
                "event_risk_summary": ["disciplinary_action | AAA event | selected name event context", ""],
                "event_source_urls": ["https://example.test/events/evt-1", ""],
            }
        )

    def _factor_diagnostics(self):
        return pd.DataFrame(
            {
                "factor": ["alpha_a", "alpha_b"],
                "family": ["family_one", "family_one"],
                "suggested_role": ["core_candidate", "shadow_review"],
                "neutral_rank_ic_h20": [0.04, 0.034],
                "neutral_long_short_h20": [0.003, -0.001],
                "concerns": ["", "negative_neutral_long_short"],
            }
        )


if __name__ == "__main__":
    unittest.main()
