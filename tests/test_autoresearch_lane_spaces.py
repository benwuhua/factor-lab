import unittest
from pathlib import Path

import yaml


class AutoresearchLaneSpaceTests(unittest.TestCase):
    def test_active_lane_editable_spaces_exist_and_define_logic_buckets(self):
        repo_root = Path(__file__).resolve().parents[1]
        lane_space = yaml.safe_load((repo_root / "configs/autoresearch/lane_space.yaml").read_text(encoding="utf-8"))

        missing = []
        missing_logic = []
        for lane_name, lane in lane_space.get("lanes", {}).items():
            if lane.get("activation_status") != "active":
                continue
            editable = repo_root / lane["editable_space"]
            if not editable.exists():
                missing.append(lane["editable_space"])
                continue
            data = yaml.safe_load(editable.read_text(encoding="utf-8")) or {}
            if lane_name != "expression_price_volume" and not data.get("logic_buckets"):
                missing_logic.append(lane_name)

        self.assertEqual(missing, [])
        self.assertEqual(missing_logic, [])


if __name__ == "__main__":
    unittest.main()
