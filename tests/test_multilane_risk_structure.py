import unittest
from pathlib import Path

from qlib_factor_lab.autoresearch.multilane import _lane_factor_specs


class RiskStructureLaneTests(unittest.TestCase):
    def test_risk_structure_lane_includes_drawdown_downside_gap_and_excursion_candidates(self):
        repo_root = Path(__file__).resolve().parents[1]

        specs = _lane_factor_specs(repo_root, "configs/factor_mining.yaml", "risk_structure")
        names = {spec["name"] for spec in specs}
        categories = {spec["category"] for spec in specs}

        self.assertLessEqual({"max_drawdown_20", "max_drawdown_60"}, names)
        self.assertLessEqual({"downside_vol_20", "downside_vol_60"}, names)
        self.assertIn("gap_risk_20", names)
        self.assertIn("intraday_excursion_20", names)
        self.assertLessEqual(
            {
                "candidate_drawdown_quality",
                "candidate_downside_volatility",
                "candidate_gap_risk",
                "candidate_intraday_excursion",
            },
            categories,
        )


if __name__ == "__main__":
    unittest.main()
