import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from qlib_factor_lab.workbench_tasks import (
    WORKBENCH_TASKS,
    latest_workbench_task_runs,
    launch_workbench_task,
    load_workbench_task_detail,
    rerun_workbench_task,
    run_workbench_task,
    summarize_workbench_task_runs,
    task_manifest_path,
    tail_workbench_task_log,
)


class WorkbenchTaskTests(unittest.TestCase):
    def test_task_registry_contains_only_safe_make_targets(self):
        self.assertIn("check-env", WORKBENCH_TASKS)
        self.assertEqual(WORKBENCH_TASKS["check-env"].command, ("make", "check-env"))

        for task in WORKBENCH_TASKS.values():
            self.assertEqual(task.command[0], "make")
            self.assertNotIn(";", " ".join(task.command))

    def test_task_registry_includes_research_context_refresh(self):
        task = WORKBENCH_TASKS["research-context"]

        self.assertEqual(task.command, ("make", "research-context"))
        self.assertIn("security_master", task.description)
        self.assertIn("company_events", task.description)

    def test_task_registry_includes_north_star_pipeline_tasks(self):
        self.assertEqual(WORKBENCH_TASKS["data-governance"].command, ("make", "data-governance"))
        self.assertEqual(WORKBENCH_TASKS["autoresearch-multilane"].command, ("make", "autoresearch-multilane"))
        self.assertEqual(WORKBENCH_TASKS["autoresearch-multilane-smoke"].command, ("make", "autoresearch-multilane"))
        self.assertEqual(WORKBENCH_TASKS["stock-cards"].command, ("make", "stock-cards"))

    def test_launch_workbench_task_writes_manifest_and_starts_runner(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "scripts").mkdir()
            (root / "scripts/run_workbench_task.py").write_text("print('stub')", encoding="utf-8")

            with patch("qlib_factor_lab.workbench_tasks.subprocess.Popen") as popen:
                record = launch_workbench_task(root, "check-env")

            manifest = json.loads(task_manifest_path(record.run_dir).read_text(encoding="utf-8"))
            self.assertEqual(manifest["task_id"], "check-env")
            self.assertEqual(manifest["status"], "queued")
            self.assertEqual(manifest["command"], ["make", "check-env"])
            popen.assert_called_once()
            args = popen.call_args.args[0]
            self.assertTrue(any(str(arg).endswith("scripts/run_workbench_task.py") for arg in args))
            self.assertIn("--task-id", args)

    def test_launch_workbench_task_persists_allowlisted_env_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "scripts").mkdir()
            (root / "scripts/run_workbench_task.py").write_text("print('stub')", encoding="utf-8")

            with patch("qlib_factor_lab.workbench_tasks.subprocess.Popen"):
                record = launch_workbench_task(
                    root,
                    "autoresearch-multilane-smoke",
                    env_overrides={
                        "AUTORESEARCH_START_TIME": "2026-01-01",
                        "AUTORESEARCH_END_TIME": "2026-04-20",
                        "AUTORESEARCH_MULTILANE_OUTPUT": "reports/autoresearch/multilane_smoke_20260420.md",
                        "UNSAFE": "ignored",
                    },
                )

            manifest = json.loads(task_manifest_path(record.run_dir).read_text(encoding="utf-8"))
            self.assertEqual(
                manifest["env_overrides"],
                {
                    "AUTORESEARCH_START_TIME": "2026-01-01",
                    "AUTORESEARCH_END_TIME": "2026-04-20",
                    "AUTORESEARCH_MULTILANE_OUTPUT": "reports/autoresearch/multilane_smoke_20260420.md",
                },
            )

    def test_launch_workbench_task_rejects_unknown_task_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(KeyError):
                launch_workbench_task(tmp, "rm-rf")

    def test_summarize_workbench_task_runs_counts_statuses(self):
        rows = [{"status": "queued"}, {"status": "running"}, {"status": "running"}, {"status": "failed"}]

        summary = summarize_workbench_task_runs(rows)

        self.assertEqual(summary["queued"], 1)
        self.assertEqual(summary["running"], 2)
        self.assertEqual(summary["succeeded"], 0)
        self.assertEqual(summary["failed"], 1)

    def test_latest_runs_and_tail_log_include_monitoring_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs/workbench_tasks/20260425_090000_check-env"
            run.mkdir(parents=True)
            task_manifest_path(run).write_text(
                json.dumps({"task_id": "check-env", "status": "succeeded", "returncode": 0}),
                encoding="utf-8",
            )
            (run / "task.log").write_text("\n".join([f"line-{index}" for index in range(8)]), encoding="utf-8")

            rows = latest_workbench_task_runs(root)
            tail = tail_workbench_task_log(run, lines=3)

        self.assertEqual(rows[0]["task_id"], "check-env")
        self.assertIn("line-7", rows[0]["log_tail"])
        self.assertEqual(tail, "line-5\nline-6\nline-7")

    def test_load_workbench_task_detail_reads_manifest_and_full_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = Path(tmp) / "runs/workbench_tasks/20260425_090000_check-env"
            run.mkdir(parents=True)
            task_manifest_path(run).write_text(
                json.dumps({"task_id": "check-env", "status": "succeeded", "returncode": 0}),
                encoding="utf-8",
            )
            (run / "task.log").write_text("alpha\nbeta\ngamma\n", encoding="utf-8")

            detail = load_workbench_task_detail(run)

        self.assertEqual(detail["manifest"]["task_id"], "check-env")
        self.assertEqual(detail["manifest"]["status"], "succeeded")
        self.assertEqual(detail["log"], "alpha\nbeta\ngamma\n")
        self.assertEqual(detail["log_line_count"], 3)
        self.assertEqual(Path(detail["run_dir"]).name, "20260425_090000_check-env")

    def test_rerun_workbench_task_uses_task_id_from_existing_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "scripts").mkdir()
            (root / "scripts/run_workbench_task.py").write_text("print('stub')", encoding="utf-8")
            run = root / "runs/workbench_tasks/20260425_090000_check-env"
            run.mkdir(parents=True)
            task_manifest_path(run).write_text(
                json.dumps({"task_id": "check-env", "status": "succeeded", "returncode": 0}),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.workbench_tasks.subprocess.Popen") as popen:
                record = rerun_workbench_task(root, run)

            self.assertEqual(record.task_id, "check-env")
            self.assertNotEqual(record.run_dir, run)
            self.assertEqual(json.loads(record.manifest_path.read_text(encoding="utf-8"))["command"], ["make", "check-env"])
            popen.assert_called_once()

    def test_run_workbench_task_applies_manifest_env_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = root / "runs/workbench_tasks/20260425_090000_research-context"
            run.mkdir(parents=True)
            task_manifest_path(run).write_text(
                json.dumps(
                    {
                        "task_id": "research-context",
                        "status": "queued",
                        "env_overrides": {"RESEARCH_CONTEXT_UNIVERSES": "csi300"},
                    }
                ),
                encoding="utf-8",
            )

            with patch("qlib_factor_lab.workbench_tasks.subprocess.run") as run_process:
                run_process.return_value.returncode = 0
                code = run_workbench_task(root, "research-context", run)

            self.assertEqual(code, 0)
            process_env = run_process.call_args.kwargs["env"]
            self.assertEqual(process_env["RESEARCH_CONTEXT_UNIVERSES"], "csi300")
            manifest = json.loads(task_manifest_path(run).read_text(encoding="utf-8"))
            self.assertEqual(manifest["status"], "succeeded")


if __name__ == "__main__":
    unittest.main()
