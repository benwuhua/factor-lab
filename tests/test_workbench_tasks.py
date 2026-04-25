import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from qlib_factor_lab.workbench_tasks import WORKBENCH_TASKS, launch_workbench_task, task_manifest_path


class WorkbenchTaskTests(unittest.TestCase):
    def test_task_registry_contains_only_safe_make_targets(self):
        self.assertIn("check-env", WORKBENCH_TASKS)
        self.assertEqual(WORKBENCH_TASKS["check-env"].command, ("make", "check-env"))

        for task in WORKBENCH_TASKS.values():
            self.assertEqual(task.command[0], "make")
            self.assertNotIn(";", " ".join(task.command))

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

    def test_launch_workbench_task_rejects_unknown_task_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(KeyError):
                launch_workbench_task(tmp, "rm-rf")


if __name__ == "__main__":
    unittest.main()
