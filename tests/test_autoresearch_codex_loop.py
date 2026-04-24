import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from qlib_factor_lab.autoresearch.codex_loop import (
    build_codex_command,
    build_candidate_prompt,
    find_disallowed_changes,
    is_protected_branch,
    parse_until_deadline,
    resolve_max_iterations,
)


class AutoresearchCodexLoopTests(unittest.TestCase):
    def test_parse_until_deadline_uses_next_occurrence_for_time_only_value(self):
        now = datetime(2026, 4, 22, 20, 15, tzinfo=ZoneInfo("Asia/Shanghai"))

        deadline = parse_until_deadline("08:30", now=now, timezone="Asia/Shanghai")

        self.assertEqual(deadline.isoformat(), "2026-04-23T08:30:00+08:00")

    def test_parse_until_deadline_uses_same_day_when_time_is_still_future(self):
        now = datetime(2026, 4, 22, 7, 15, tzinfo=ZoneInfo("Asia/Shanghai"))

        deadline = parse_until_deadline("08:30", now=now, timezone="Asia/Shanghai")

        self.assertEqual(deadline.isoformat(), "2026-04-22T08:30:00+08:00")

    def test_parse_until_deadline_accepts_explicit_datetime(self):
        now = datetime(2026, 4, 22, 20, 15, tzinfo=ZoneInfo("Asia/Shanghai"))

        deadline = parse_until_deadline("2026-04-23 08:30", now=now, timezone="Asia/Shanghai")

        self.assertEqual(deadline.isoformat(), "2026-04-23T08:30:00+08:00")

    def test_resolve_max_iterations_keeps_unbounded_when_deadline_present(self):
        resolved = resolve_max_iterations(max_iterations=None, has_deadline=True, max_hours=None)

        self.assertIsNone(resolved)

    def test_resolve_max_iterations_uses_safe_default_without_stop_condition(self):
        resolved = resolve_max_iterations(max_iterations=None, has_deadline=False, max_hours=None)

        self.assertEqual(resolved, 30)

    def test_find_disallowed_changes_allows_only_candidate_file(self):
        changed = [
            "configs/autoresearch/candidates/example_expression.yaml",
            "README.md",
            "reports/autoresearch/expression_results.tsv",
        ]

        disallowed = find_disallowed_changes(
            changed,
            candidate_file="configs/autoresearch/candidates/example_expression.yaml",
        )

        self.assertEqual(disallowed, ["README.md"])

    def test_build_codex_command_targets_workspace_and_model(self):
        command = build_codex_command(
            root=Path("/repo"),
            prompt="do one thing",
            model="gpt-5.4",
            sandbox="workspace-write",
        )

        self.assertEqual(command[:4], ["codex", "exec", "-C", "/repo"])
        self.assertIn("--sandbox", command)
        self.assertIn("workspace-write", command)
        self.assertIn("-m", command)
        self.assertIn("gpt-5.4", command)
        self.assertEqual(command[-1], "do one thing")

    def test_build_candidate_prompt_keeps_codex_inside_candidate_scope(self):
        prompt = build_candidate_prompt(
            iteration=3,
            candidate_file="configs/autoresearch/candidates/example_expression.yaml",
            ledger_text="discard_candidate: 2",
            allowed_families=["reversal", "turnover"],
        )

        self.assertIn("第 3 轮", prompt)
        self.assertIn("只允许修改", prompt)
        self.assertIn("configs/autoresearch/candidates/example_expression.yaml", prompt)
        self.assertIn("不要运行 make autoresearch-expression", prompt)
        self.assertIn("reversal, turnover", prompt)

    def test_is_protected_branch_flags_main_and_master(self):
        self.assertTrue(is_protected_branch("main"))
        self.assertTrue(is_protected_branch("master"))
        self.assertFalse(is_protected_branch("autoresearch/nightly"))


if __name__ == "__main__":
    unittest.main()
