import unittest

from app.streamlit_app import (
    _autoresearch_progress_cards_html,
    _detail_card_html,
    _evidence_cards_html,
    _event_library_cards_html,
    _page_topbar_html,
    _rerun_task_button_label,
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


if __name__ == "__main__":
    unittest.main()
