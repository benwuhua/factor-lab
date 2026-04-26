import unittest

from app.streamlit_app import (
    _autoresearch_progress_cards_html,
    _autoresearch_smoke_env_overrides,
    _detail_card_html,
    _evidence_cards_html,
    _evidence_health_cards_html,
    _evidence_library_rows,
    _event_library_cards_html,
    _factor_research_rows,
    _page_topbar_html,
    _research_context_env_overrides,
    _resolve_nav_page,
    _sidebar_nav_html,
    _rerun_task_button_label,
    _stock_card_metric_cards_html,
    _short_text,
    _task_run_option_label,
    _task_status_cards_html,
    _workflow_card_grid_html,
)


class StreamlitAppUiTests(unittest.TestCase):
    def test_page_topbar_html_uses_shared_workbench_shell(self):
        html = _page_topbar_html(
            "自动挖掘",
            "Codex CLI 只改候选表达式，contract 固定 provider、horizon、净化和评估器。",
            ["expression", "pattern", "combo", "Export"],
        )

        self.assertIn('class="topbar detail-topbar"', html)
        self.assertIn("<h1>自动挖掘</h1>", html)
        self.assertIn("Codex CLI 只改候选表达式", html)
        self.assertIn("<span>expression</span>", html)
        self.assertIn("<span>Export</span>", html)

    def test_detail_card_html_keeps_key_value_rows_in_design_shell(self):
        html = _detail_card_html(
            "队列状态",
            [("Review", 3), ("Crash", 1)],
            note="Nightly 研究队列",
        )

        self.assertIn('class="detail-card"', html)
        self.assertIn("<h3>队列状态</h3>", html)
        self.assertIn("<span>Review</span><b>3</b>", html)
        self.assertIn("Nightly 研究队列", html)

    def test_short_text_truncates_long_candidate_names_for_rail_cards(self):
        text = _short_text("volume_confirmed_high_mean60_discount_reversal", 20)

        self.assertEqual(text, "volume_confirmed_...")
        self.assertLessEqual(len(text), 20)

    def test_workflow_card_grid_html_reuses_pipeline_card_language(self):
        html = _workflow_card_grid_html(
            [
                {
                    "step": "01 DATA",
                    "title": "固定数据边界",
                    "command": "configs/provider_current.yaml",
                    "status": "locked",
                    "action": "Open",
                }
            ]
        )

        self.assertIn('class="ops-grid workflow-grid"', html)
        self.assertIn("固定数据边界", html)
        self.assertIn("<code>configs/provider_current.yaml</code>", html)
        self.assertIn("locked", html)

    def test_task_status_cards_html_highlights_running_and_failed_counts(self):
        html = _task_status_cards_html({"queued": 1, "running": 2, "succeeded": 3, "failed": 4})

        self.assertIn('class="task-status-grid"', html)
        self.assertIn("<label>running</label><strong>2</strong>", html)
        self.assertIn("<label>failed</label><strong>4</strong>", html)

    def test_task_run_option_label_shows_run_name_status_and_code(self):
        label = _task_run_option_label(
            {
                "run_dir": "/tmp/runs/workbench_tasks/20260425_090000_check-env",
                "status": "succeeded",
                "returncode": 0,
            }
        )

        self.assertEqual(label, "20260425_090000_check-env · succeeded · rc=0")

    def test_rerun_task_button_label_uses_selected_manifest_task_id(self):
        label = _rerun_task_button_label({"task_id": "check-env"})

        self.assertEqual(label, "重跑同类任务 · check-env")

    def test_autoresearch_progress_cards_html_shows_active_loop_and_latest_candidate(self):
        html = _autoresearch_progress_cards_html(
            {
                "loop_status": "running",
                "loop_task_id": "autoresearch-codex-loop",
                "candidate_count": 12,
                "review_count": 2,
                "discard_count": 9,
                "crash_count": 1,
                "latest_candidate": "alpha_new",
                "is_active": True,
            }
        )

        self.assertIn('class="autoresearch-progress-grid"', html)
        self.assertIn("running", html)
        self.assertIn("alpha_new", html)
        self.assertIn("active", html)

    def test_evidence_cards_html_shows_event_and_source_counts(self):
        html = _evidence_cards_html(
            {
                "positions": 12,
                "event_watch": 3,
                "event_block": 1,
                "master_missing": 2,
                "source_urls": 4,
            }
        )

        self.assertIn('class="evidence-grid"', html)
        self.assertIn("Event Watch", html)
        self.assertIn("<strong>3</strong>", html)
        self.assertIn("Source URLs", html)

    def test_event_library_cards_html_shows_library_counts(self):
        html = _event_library_cards_html(
            {
                "events": 20,
                "instruments": 12,
                "block_events": 2,
                "source_urls": 18,
            }
        )

        self.assertIn('class="evidence-grid"', html)
        self.assertIn("Events", html)
        self.assertIn("<strong>20</strong>", html)
        self.assertIn("Block Events", html)

    def test_evidence_library_rows_launch_research_context(self):
        rows = _evidence_library_rows()

        self.assertEqual(rows[0]["task_id"], "research-context")
        self.assertEqual(rows[0]["command"], "make research-context")
        self.assertIn("刷新证据库", rows[0]["title"])

    def test_evidence_health_cards_html_shows_coverage_percentages(self):
        html = _evidence_health_cards_html(
            {
                "master_instruments": 800,
                "event_instruments": 120,
                "master_universe_coverage_pct": 100.0,
                "event_coverage_pct": 15.0,
                "source_url_coverage_pct": 92.5,
                "latest_event_date": "2026-04-25",
            }
        )

        self.assertIn("Master Coverage", html)
        self.assertIn("<strong>100.0%</strong>", html)
        self.assertIn("Event Coverage", html)
        self.assertIn("<strong>15.0%</strong>", html)
        self.assertIn("Source Coverage", html)
        self.assertIn("2026-04-25", html)

    def test_stock_card_metric_cards_html_shows_card_counts(self):
        html = _stock_card_metric_cards_html({"cards": 3, "caution": 1, "reject": 0, "event_watch": 2})

        self.assertIn("Stock Cards", html)
        self.assertIn("<strong>3</strong>", html)
        self.assertIn("Caution", html)

    def test_research_context_env_overrides_normalize_dates_and_universes(self):
        env = _research_context_env_overrides(
            as_of_date="2026-04-25",
            notice_start="2026-04-01",
            notice_end="2026-04-25",
            universes=["csi500", "all"],
        )

        self.assertEqual(env["RUN_DATE"], "20260425")
        self.assertEqual(env["RESEARCH_CONTEXT_AS_OF"], "20260425")
        self.assertEqual(env["RESEARCH_CONTEXT_NOTICE_START"], "20260401")
        self.assertEqual(env["RESEARCH_CONTEXT_NOTICE_END"], "20260425")
        self.assertEqual(env["RESEARCH_CONTEXT_UNIVERSES"], "csi500")

    def test_factor_research_rows_include_short_window_multilane_smoke(self):
        rows = _factor_research_rows()
        smoke = next(row for row in rows if row.get("task_id") == "autoresearch-multilane-smoke")

        self.assertIn("短窗", smoke["title"])
        self.assertIn("AUTORESEARCH_START_TIME", smoke["command"])
        self.assertEqual(smoke["action"], "Smoke")

    def test_autoresearch_smoke_env_overrides_normalize_dates_and_output(self):
        env = _autoresearch_smoke_env_overrides(start_date="2026-01-01", end_date="2026-04-20")

        self.assertEqual(env["AUTORESEARCH_START_TIME"], "2026-01-01")
        self.assertEqual(env["AUTORESEARCH_END_TIME"], "2026-04-20")
        self.assertEqual(env["AUTORESEARCH_MULTILANE_OUTPUT"], "reports/autoresearch/multilane_smoke_20260420.md")

    def test_resolve_nav_page_supports_deep_link_shortcuts(self):
        pages = ["01 总览仪表盘", "08 证据库"]

        self.assertEqual(_resolve_nav_page("08", pages), "08 证据库")
        self.assertEqual(_resolve_nav_page("08 证据库", pages), "08 证据库")
        self.assertEqual(_resolve_nav_page("missing", pages), "01 总览仪表盘")

    def test_sidebar_nav_html_uses_links_and_marks_current_page(self):
        html = _sidebar_nav_html(["01 总览仪表盘", "03 因子研究"], "03 因子研究")

        self.assertIn('href="?page=03+%E5%9B%A0%E5%AD%90%E7%A0%94%E7%A9%B6"', html)
        self.assertIn('class="side-nav-item active"', html)
        self.assertIn("03 因子研究", html)


if __name__ == "__main__":
    unittest.main()
