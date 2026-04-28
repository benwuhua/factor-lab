import os
import tempfile
import unittest
from pathlib import Path
from datetime import datetime

import pandas as pd
import yaml

from qlib_factor_lab.workbench import (
    build_autoresearch_progress,
    build_execution_gate_card,
    build_event_evidence_library,
    build_portfolio_gate_explanation,
    build_portfolio_gate_trend,
    build_research_context_health,
    build_gate_review_items,
    build_pretrade_review,
    build_research_evidence_summary,
    build_research_pipeline_status,
    build_workbench_freshness,
    find_latest_stock_cards,
    find_latest_multilane_report,
    find_latest_run_dir,
    get_candidate_diagnostics,
    get_candidate_artifacts,
    load_autoresearch_queue,
    load_multilane_report,
    load_stock_cards,
    load_workbench_snapshot,
    parse_expert_review_result,
    summarize_autoresearch_queue,
    summarize_multilane_report,
)


class WorkbenchTests(unittest.TestCase):
    def test_load_autoresearch_queue_sorts_latest_first_and_summarizes_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = root / "reports/autoresearch/expression_results.tsv"
            ledger.parent.mkdir(parents=True)
            pd.DataFrame(
                [
                    {
                        "timestamp": "2026-04-22T21:00:00",
                        "candidate_name": "old",
                        "status": "discard_candidate",
                        "primary_metric": 0.001,
                        "neutral_rank_ic_mean_h20": 0.001,
                        "complexity_score": 0.2,
                        "decision_reason": "weak",
                        "artifact_dir": "reports/autoresearch/runs/old",
                    },
                    {
                        "timestamp": "2026-04-22T22:00:00",
                        "candidate_name": "new",
                        "status": "review",
                        "primary_metric": 0.04,
                        "neutral_rank_ic_mean_h20": 0.04,
                        "complexity_score": 0.3,
                        "decision_reason": "",
                        "artifact_dir": "reports/autoresearch/runs/new",
                    },
                ]
            ).to_csv(ledger, sep="\t", index=False)

            queue = load_autoresearch_queue(root)
            summary = summarize_autoresearch_queue(queue)

        self.assertEqual(list(queue["candidate_name"]), ["new", "old"])
        self.assertEqual(summary["review"], 1)
        self.assertEqual(summary["discard_candidate"], 1)
        self.assertEqual(summary["crash"], 0)

    def test_autoresearch_progress_combines_latest_loop_task_and_recent_candidates(self):
        queue = pd.DataFrame(
            [
                {
                    "timestamp": "2026-04-25T08:10:00",
                    "candidate_name": "alpha_new",
                    "status": "review",
                    "primary_metric": 0.041,
                    "neutral_rank_ic_mean_h20": 0.03,
                    "complexity_score": 0.22,
                },
                {
                    "timestamp": "2026-04-25T08:00:00",
                    "candidate_name": "alpha_old",
                    "status": "discard_candidate",
                    "primary_metric": -0.01,
                    "neutral_rank_ic_mean_h20": -0.02,
                    "complexity_score": 0.2,
                },
            ]
        )
        tasks = [
            {
                "task_id": "autoresearch-codex-loop",
                "status": "running",
                "created_at": "2026-04-25T08:05:00",
                "run_dir": "runs/workbench_tasks/20260425_080500_autoresearch-codex-loop",
            },
            {
                "task_id": "check-env",
                "status": "succeeded",
                "created_at": "2026-04-25T07:00:00",
            },
        ]

        progress = build_autoresearch_progress(queue=queue, task_runs=tasks)

        self.assertEqual(progress["loop_status"], "running")
        self.assertEqual(progress["loop_task_id"], "autoresearch-codex-loop")
        self.assertEqual(progress["candidate_count"], 2)
        self.assertEqual(progress["review_count"], 1)
        self.assertEqual(progress["discard_count"], 1)
        self.assertEqual(progress["latest_candidate"], "alpha_new")
        self.assertEqual(progress["recent_candidates"][0]["candidate_name"], "alpha_new")
        self.assertTrue(progress["is_active"])

    def test_portfolio_gate_explanation_marks_exposure_failures_as_caution(self):
        portfolio = pd.DataFrame(
            {
                "instrument": ["AAA", "BBB"],
                "target_weight": [0.4, 0.4],
                "industry": ["tech", "tech"],
                "top_factor_1": ["mom_20", "mom_20"],
                "top_factor_1_contribution": [2.0, 1.0],
            }
        )

        result = build_portfolio_gate_explanation(
            portfolio,
            risk_config={
                "max_single_weight": 0.5,
                "min_positions": 2,
                "min_signal_coverage": 0.2,
                "max_industry_weight": 0.6,
                "min_factor_family_count": 2,
                "max_factor_family_concentration": 0.7,
            },
            factor_family_map={"mom_20": "momentum"},
        )

        self.assertEqual(result.decision, "caution")
        failed = set(result.checks.query("status == 'fail'")["check"])
        self.assertIn("max_industry_weight", failed)
        self.assertIn("min_factor_family_count", failed)
        self.assertIn("max_factor_family_concentration", failed)
        self.assertEqual(list(result.industry["industry"]), ["tech"])
        self.assertEqual(list(result.family["family"]), ["momentum"])

    def test_gate_review_items_translate_failures_to_actions(self):
        checks = pd.DataFrame(
            [
                {"check": "max_industry_weight", "status": "fail", "value": 0.71, "limit": 0.5},
                {"check": "max_single_weight", "status": "fail", "value": 0.18, "limit": 0.1},
                {"check": "min_positions", "status": "pass", "value": 12, "limit": 10},
            ]
        )

        items = build_gate_review_items(checks)

        self.assertEqual(list(items["check"]), ["max_industry_weight", "max_single_weight"])
        self.assertEqual(list(items["decision_level"]), ["caution", "reject"])
        self.assertIn("降低行业集中", items.iloc[0]["review_focus"])
        self.assertIn("单票权重", items.iloc[1]["review_focus"])

    def test_portfolio_gate_trend_reports_coverage_and_family_concentration(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs/20260423"
            run.mkdir(parents=True)
            pd.DataFrame(
                {
                    "instrument": ["AAA", "BBB"],
                    "target_weight": [0.5, 0.5],
                    "industry_sw": ["tech", ""],
                    "event_count": [1, 0],
                    "family_momentum_score": [0.3, 0.3],
                    "family_reversal_score": [0.1, 0.1],
                    "logic_trend_following_score": [0.3, 0.3],
                    "logic_reversal_repair_score": [0.1, 0.1],
                }
            ).to_csv(run / "target_portfolio.csv", index=False)

            trend = build_portfolio_gate_trend(root)

        self.assertEqual(len(trend), 1)
        row = trend.iloc[0]
        self.assertEqual(row["run_date"], "20260423")
        self.assertAlmostEqual(float(row["industry_coverage"]), 0.5)
        self.assertAlmostEqual(float(row["event_coverage"]), 0.5)
        self.assertAlmostEqual(float(row["factor_family_concentration"]), 0.75)
        self.assertAlmostEqual(float(row["factor_logic_concentration"]), 0.75)

    def test_workbench_snapshot_counts_approved_factors_and_latest_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "reports").mkdir()
            (root / "reports/approved_factors.yaml").write_text(
                yaml.safe_dump({"approved_factors": [{"name": "a"}, {"name": "b"}]}),
                encoding="utf-8",
            )
            pd.DataFrame({"instrument": ["AAA"], "target_weight": [0.1]}).to_csv(
                root / "reports/target_portfolio_20260423.csv",
                index=False,
            )
            pd.DataFrame({"instrument": ["BBB"], "target_weight": [0.1]}).to_csv(
                root / "reports/target_portfolio_20260424.csv",
                index=False,
            )

            snapshot = load_workbench_snapshot(root)

        self.assertEqual(snapshot.approved_factor_count, 2)
        self.assertEqual(snapshot.latest_target_portfolio.name, "target_portfolio_20260424.csv")

    def test_load_stock_cards_reads_latest_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "reports").mkdir()
            older = root / "reports/stock_cards_20260423.jsonl"
            latest = root / "reports/stock_cards_20260424.jsonl"
            older.write_text('{"instrument": "OLD"}\n', encoding="utf-8")
            latest.write_text('{"instrument": "AAA", "audit": {"review_decision": "caution"}}\n', encoding="utf-8")

            path = find_latest_stock_cards(root)
            cards = load_stock_cards(root)

        self.assertEqual(path.name, "stock_cards_20260424.jsonl")
        self.assertEqual(cards[0]["instrument"], "AAA")

    def test_load_multilane_report_reads_latest_json_sidecar_and_summarizes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            report_dir = root / "reports/autoresearch"
            report_dir.mkdir(parents=True)
            older = report_dir / "multilane_smoke_20260424.md"
            latest = report_dir / "multilane_smoke_20260425.md"
            older.write_text("# old\n", encoding="utf-8")
            latest.write_text("# latest\n", encoding="utf-8")
            latest.with_suffix(".json").write_text(
                """
                [
                  {"lane": "emotion_atmosphere", "activation_status": "active", "run_status": "completed", "candidate": "heat", "primary_metric": 0.018, "detail": "review"},
                  {"lane": "expression_price_volume", "activation_status": "active", "run_status": "completed", "candidate": "mom", "primary_metric": -0.002, "detail": "discard_candidate"},
                  {"lane": "regime", "activation_status": "active", "run_status": "completed", "candidate": "", "primary_metric": 2.0, "detail": "review"},
                  {"lane": "liquidity_microstructure", "activation_status": "active", "run_status": "unsupported", "candidate": "", "primary_metric": null, "detail": "no runner implemented"}
                ]
                """,
                encoding="utf-8",
            )
            os.utime(older, (datetime(2026, 4, 24, 8, 0).timestamp(), datetime(2026, 4, 24, 8, 0).timestamp()))
            os.utime(latest, (datetime(2026, 4, 25, 8, 0).timestamp(), datetime(2026, 4, 25, 8, 0).timestamp()))

            path = find_latest_multilane_report(root)
            frame = load_multilane_report(root)
            summary = summarize_multilane_report(frame)

        self.assertEqual(path.name, "multilane_smoke_20260425.md")
        self.assertEqual(list(frame["lane"]), ["emotion_atmosphere", "expression_price_volume", "regime", "liquidity_microstructure"])
        self.assertEqual(summary["lanes"], 4)
        self.assertEqual(summary["completed"], 3)
        self.assertEqual(summary["unsupported"], 1)
        self.assertEqual(summary["review"], 2)
        self.assertEqual(summary["best_lane"], "emotion_atmosphere")
        self.assertAlmostEqual(summary["best_primary_metric"], 0.018)

    def test_latest_run_dir_ignores_workbench_task_monitor_runs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            research_run = root / "runs/20260423"
            task_run = root / "runs/workbench_tasks"
            research_run.mkdir(parents=True)
            task_run.mkdir(parents=True)
            os.utime(research_run, (datetime(2026, 4, 23, 8, 30).timestamp(), datetime(2026, 4, 23, 8, 30).timestamp()))
            os.utime(task_run, (datetime(2026, 4, 25, 20, 0).timestamp(), datetime(2026, 4, 25, 20, 0).timestamp()))

            latest = find_latest_run_dir(root)

        self.assertEqual(latest, research_run)

    def test_freshness_marks_existing_recent_artifacts_ready_and_missing_as_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "reports/autoresearch").mkdir(parents=True)
            ledger = root / "reports/autoresearch/expression_results.tsv"
            ledger.write_text("timestamp\tcandidate_name\tstatus\n2026-04-25T08:00:00\talpha\treview\n", encoding="utf-8")
            fixed_mtime = datetime(2026, 4, 25, 8, 0, 0).timestamp()
            ledger.touch()
            ledger.chmod(0o644)

            os.utime(ledger, (fixed_mtime, fixed_mtime))

            now = pd.Timestamp(ledger.stat().st_mtime, unit="s") + pd.Timedelta(hours=12)
            freshness = build_workbench_freshness(root, now=now)

        by_key = {row["artifact"]: row for row in freshness}
        self.assertEqual(by_key["autoresearch_ledger"]["status"], "ready")
        self.assertEqual(by_key["autoresearch_ledger"]["age_hours"], 12.0)
        self.assertEqual(by_key["target_portfolio"]["status"], "missing")

    def test_candidate_artifacts_resolves_summary_and_candidate_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact = root / "reports/autoresearch/runs/demo"
            artifact.mkdir(parents=True)
            (artifact / "summary.txt").write_text("primary_metric: 0.04", encoding="utf-8")
            (artifact / "candidate.yaml").write_text("name: demo", encoding="utf-8")

            result = get_candidate_artifacts(root, "reports/autoresearch/runs/demo")

        self.assertEqual(result["summary"], "primary_metric: 0.04")
        self.assertEqual(result["candidate"], "name: demo")
        self.assertEqual(result["artifact_dir"].name, "demo")

    def test_pretrade_review_flags_liquidity_limit_events_and_announcements(self):
        portfolio = pd.DataFrame(
            {
                "instrument": ["AAA", "BBB", "CCC"],
                "amount_20d": [50_000_000, 200_000_000, 120_000_000],
                "limit_up": [False, True, False],
                "limit_down": [False, False, False],
                "suspended": [False, False, False],
                "event_blocked": [False, False, True],
                "event_count": [0, 1, 1],
                "event_risk_summary": ["", "abnormal volatility notice", "disciplinary action"],
                "risk_flags": ["", "", "limit_locked"],
            }
        )

        review = build_pretrade_review(portfolio, min_amount_20d=100_000_000)
        by_check = review.set_index("check")

        self.assertEqual(by_check.loc["liquidity_floor", "status"], "caution")
        self.assertIn("AAA", by_check.loc["liquidity_floor", "detail"])
        self.assertEqual(by_check.loc["limit_or_suspended", "status"], "reject")
        self.assertEqual(by_check.loc["event_blocked", "status"], "reject")
        self.assertEqual(by_check.loc["announcement_watch", "status"], "caution")

    def test_research_evidence_summary_counts_event_and_master_evidence(self):
        portfolio = pd.DataFrame(
            {
                "instrument": ["AAA", "BBB", "CCC"],
                "event_count": [2, 1, 0],
                "event_blocked": [True, False, False],
                "max_event_severity": ["block", "watch", ""],
                "active_event_types": ["disciplinary_action; regulatory_inquiry", "earnings_watch", ""],
                "event_risk_summary": ["AAA sanction", "BBB earnings watch", ""],
                "event_source_urls": ["https://example.test/a; https://example.test/b", "https://example.test/c", ""],
                "announcement_flag": [True, False, False],
                "security_master_missing": [False, True, False],
                "risk_flags": ["event_blocked", "security_master_missing", ""],
            }
        )

        summary = build_research_evidence_summary(portfolio)

        self.assertEqual(summary["cards"]["positions"], 3)
        self.assertEqual(summary["cards"]["event_watch"], 2)
        self.assertEqual(summary["cards"]["event_block"], 1)
        self.assertEqual(summary["cards"]["master_missing"], 1)
        self.assertEqual(summary["cards"]["source_urls"], 3)
        self.assertEqual(summary["event_types"].set_index("event_type").loc["disciplinary_action", "count"], 1)
        self.assertEqual(summary["event_types"].set_index("event_type").loc["earnings_watch", "count"], 1)
        self.assertEqual(list(summary["detail"]["instrument"]), ["AAA", "BBB"])

    def test_event_evidence_library_loads_events_and_summarizes_types(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "configs").mkdir()
            (root / "data").mkdir()
            (root / "configs/event_risk.yaml").write_text(
                yaml.safe_dump({"event_risk": {"events_path": "data/company_events.csv"}}),
                encoding="utf-8",
            )
            pd.DataFrame(
                [
                    {
                        "event_id": "e1",
                        "instrument": "AAA",
                        "event_type": "disciplinary_action",
                        "event_date": "2026-04-20",
                        "source": "exchange",
                        "source_url": "https://example.test/e1",
                        "title": "Sanction",
                        "severity": "block",
                        "summary": "formal sanction",
                    },
                    {
                        "event_id": "e2",
                        "instrument": "BBB",
                        "event_type": "earnings_watch",
                        "event_date": "2026-04-21",
                        "source": "announcement",
                        "source_url": "https://example.test/e2",
                        "title": "Earnings watch",
                        "severity": "watch",
                        "summary": "earnings warning",
                    },
                ]
            ).to_csv(root / "data/company_events.csv", index=False)

            library = build_event_evidence_library(root)

        self.assertEqual(library["cards"]["events"], 2)
        self.assertEqual(library["cards"]["instruments"], 2)
        self.assertEqual(library["cards"]["block_events"], 1)
        self.assertEqual(library["cards"]["source_urls"], 2)
        self.assertEqual(library["event_types"].set_index("event_type").loc["disciplinary_action", "count"], 1)
        self.assertEqual(library["severity"].set_index("severity").loc["block", "count"], 1)
        self.assertEqual(list(library["detail"]["instrument"]), ["BBB", "AAA"])

    def test_research_context_health_reports_master_event_and_source_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "data").mkdir()
            pd.DataFrame(
                [
                    {"instrument": "AAA", "research_universes": "csi300"},
                    {"instrument": "BBB", "research_universes": "csi500"},
                    {"instrument": "CCC", "research_universes": "csi500"},
                ]
            ).to_csv(root / "data/security_master.csv", index=False)
            pd.DataFrame(
                [
                    {"instrument": "AAA", "source_url": "https://example.test/a", "event_date": "2026-04-24"},
                    {"instrument": "BBB", "source_url": "", "event_date": "2026-04-25"},
                ]
            ).to_csv(root / "data/company_events.csv", index=False)

            health = build_research_context_health(root)

        self.assertEqual(health["cards"]["master_instruments"], 3)
        self.assertEqual(health["cards"]["event_instruments"], 2)
        self.assertEqual(health["cards"]["master_universe_coverage_pct"], 100.0)
        self.assertEqual(health["cards"]["event_coverage_pct"], 66.7)
        self.assertEqual(health["cards"]["source_url_coverage_pct"], 50.0)
        self.assertEqual(health["cards"]["latest_event_date"], "2026-04-25")

    def test_execution_gate_card_rejects_hard_blocks_and_cautions_soft_blocks(self):
        pretrade = pd.DataFrame(
            [
                {"check": "liquidity_floor", "status": "caution", "detail": "AAA"},
                {"check": "limit_or_suspended", "status": "pass", "detail": ""},
            ]
        )
        expert = {"decision": "pass", "status": "completed"}

        caution = build_execution_gate_card("pass", pretrade, expert)
        self.assertEqual(caution["decision"], "caution")
        self.assertEqual(caution["action"], "require_manual_confirmation")
        self.assertIn("liquidity_floor", caution["reasons"][0])

        blocked = build_execution_gate_card(
            "pass",
            pd.DataFrame([{"check": "event_blocked", "status": "reject", "detail": "BBB"}]),
            expert,
        )
        self.assertEqual(blocked["decision"], "reject")
        self.assertEqual(blocked["action"], "block_paper_execution")

    def test_parse_expert_review_result_extracts_decision_summary_and_watchlist(self):
        text = """# Expert Review Result

- status: completed
- decision: caution
- error:

## Output

结论：`caution`，不建议直接无脑下单。

**2. 需要人工看图或基本面复核的“因子误伤”候选**

- `SH600580`：排名 19，score 已经接近尾部。
- `SZ002568`：20 日成交额最低。

**3. 下单前最值得拦截的风险**

第一是因子拥挤和逻辑单一。
"""

        result = parse_expert_review_result(text)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["decision"], "caution")
        self.assertIn("不建议直接无脑下单", result["summary"])
        self.assertEqual(result["watchlist"], ["SH600580", "SZ002568"])
        self.assertIn("第一是因子拥挤", result["risk_notes"])

    def test_research_pipeline_status_links_research_expert_gate_and_paper(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = root / "reports/autoresearch/expression_results.tsv"
            ledger.parent.mkdir(parents=True)
            pd.DataFrame([{"timestamp": "2026-04-25", "candidate_name": "alpha", "status": "review"}]).to_csv(
                ledger,
                sep="\t",
                index=False,
            )
            run = root / "runs/20260425"
            run.mkdir(parents=True)
            pd.DataFrame({"instrument": ["AAA"], "target_weight": [0.2]}).to_csv(run / "target_portfolio.csv", index=False)
            (run / "expert_review_result.md").write_text("- decision: caution\n", encoding="utf-8")
            pd.DataFrame({"instrument": ["AAA"]}).to_csv(run / "orders.csv", index=False)

            status = build_research_pipeline_status(root)

        by_stage = status.set_index("stage")
        self.assertEqual(by_stage.loc["autoresearch", "status"], "review")
        self.assertEqual(by_stage.loc["expert_review", "status"], "caution")
        self.assertEqual(by_stage.loc["portfolio_gate", "status"], "reject")
        self.assertEqual(by_stage.loc["paper_bundle", "status"], "ready")

    def test_candidate_diagnostics_loads_eval_yearly_and_redundancy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifact = root / "reports/autoresearch/runs/demo"
            artifact.mkdir(parents=True)
            pd.DataFrame(
                [
                    {
                        "factor": "alpha",
                        "horizon": 20,
                        "neutralization": "none",
                        "rank_ic_mean": 0.04,
                        "long_short_mean_return": 0.006,
                    }
                ]
            ).to_csv(artifact / "raw_eval.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "factor": "alpha",
                        "horizon": 20,
                        "neutralization": "size_proxy",
                        "rank_ic_mean": 0.03,
                        "long_short_mean_return": 0.004,
                    }
                ]
            ).to_csv(artifact / "neutralized_eval.csv", index=False)
            analysis = root / "reports/autoresearch/review_analysis_20260425"
            analysis.mkdir(parents=True)
            pd.DataFrame(
                [
                    {
                        "candidate_name": "alpha",
                        "segment": 2026,
                        "neutral_rank_ic_mean": 0.05,
                        "neutral_positive_rate": 0.7,
                    }
                ]
            ).to_csv(analysis / "stability_by_year.tsv", sep="\t", index=False)
            pd.DataFrame(
                [
                    {
                        "cluster_id": "C001",
                        "candidate_name": "alpha",
                        "representative": "alpha",
                        "cluster_size": 4,
                    }
                ]
            ).to_csv(analysis / "dedup_clusters.tsv", sep="\t", index=False)

            diagnostics = get_candidate_diagnostics(root, "alpha", "reports/autoresearch/runs/demo")

        self.assertEqual(diagnostics["eval"].loc[0, "neutralized_rank_ic_mean"], 0.03)
        self.assertEqual(diagnostics["yearly"].loc[0, "segment"], 2026)
        self.assertEqual(diagnostics["redundancy"].loc[0, "cluster_size"], 4)


if __name__ == "__main__":
    unittest.main()
