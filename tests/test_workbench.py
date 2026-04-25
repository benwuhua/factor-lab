import os
import tempfile
import unittest
from pathlib import Path
from datetime import datetime

import pandas as pd
import yaml

from qlib_factor_lab.workbench import (
    build_execution_gate_card,
    build_portfolio_gate_explanation,
    build_gate_review_items,
    build_pretrade_review,
    build_research_pipeline_status,
    build_workbench_freshness,
    find_latest_run_dir,
    get_candidate_diagnostics,
    get_candidate_artifacts,
    load_autoresearch_queue,
    load_workbench_snapshot,
    parse_expert_review_result,
    summarize_autoresearch_queue,
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
