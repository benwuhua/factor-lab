import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class WorkbenchE2eContractTests(unittest.TestCase):
    def test_makefile_exposes_workbench_e2e_target(self):
        makefile = ROOT.joinpath("Makefile").read_text(encoding="utf-8")

        self.assertIn("workbench-e2e:", makefile)
        self.assertIn("scripts/run_workbench_e2e.py", makefile)

    def test_e2e_script_covers_navigation_actions_and_queue_refresh(self):
        script = ROOT.joinpath("scripts", "run_workbench_e2e.py").read_text(encoding="utf-8")

        for page in ["03 因子研究", "04 自动挖掘", "08 证据库"]:
            with self.subTest(page=page):
                self.assertIn(page, script)
        self.assertIn("Queue · 启动自动挖掘", script)
        self.assertIn("刷新任务状态", script)
        self.assertIn("_click_nav_button", script)


if __name__ == "__main__":
    unittest.main()
