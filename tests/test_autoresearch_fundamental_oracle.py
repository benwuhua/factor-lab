from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd
import yaml

from qlib_factor_lab.autoresearch.fundamental_oracle import (
    DEFAULT_FUNDAMENTAL_FACTOR_SPECS,
    build_fundamental_combo_frame,
    build_fundamental_factor_frame,
    evaluate_fundamental_factor_frame,
    load_fundamental_factor_specs,
    run_fundamental_lane_oracle,
)


class FundamentalOracleTest(unittest.TestCase):
    def test_build_fundamental_factor_frame_uses_available_at_point_in_time(self) -> None:
        close = _close_frame()
        fundamentals = pd.DataFrame(
            [
                {
                    "instrument": "SH600000",
                    "report_period": "2026-03-31",
                    "announce_date": "2026-04-02",
                    "available_at": "2026-04-03",
                    "roe": 10.0,
                },
                {
                    "instrument": "SH600001",
                    "report_period": "2026-03-31",
                    "announce_date": "2026-04-01",
                    "available_at": "2026-04-01",
                    "roe": 5.0,
                },
            ]
        )

        result = build_fundamental_factor_frame(fundamentals, close, "roe")

        self.assertNotIn((pd.Timestamp("2026-04-02"), "SH600000"), result.index)
        self.assertEqual(10.0, result.loc[(pd.Timestamp("2026-04-03"), "SH600000"), "roe"])
        self.assertEqual(5.0, result.loc[(pd.Timestamp("2026-04-01"), "SH600001"), "roe"])

    def test_evaluate_fundamental_factor_frame_outputs_ic_rows(self) -> None:
        close = _close_frame()
        fundamentals = pd.DataFrame(
            [
                {"instrument": "SH600000", "available_at": "2026-04-01", "roe": 10.0},
                {"instrument": "SH600001", "available_at": "2026-04-01", "roe": 5.0},
            ]
        )

        frame = build_fundamental_factor_frame(fundamentals, close, "roe")
        result = evaluate_fundamental_factor_frame(frame, factor_name="roe", direction=1, horizons=(1,), quantiles=2)

        self.assertEqual(["roe"], result["factor"].tolist())
        self.assertEqual([1], result["horizon"].tolist())
        self.assertGreater(int(result.loc[0, "observations"]), 0)
        self.assertIn("rank_ic_mean", result.columns)

    def test_build_fundamental_combo_frame_standardizes_weighted_components(self) -> None:
        close = _close_frame()
        fundamentals = pd.DataFrame(
            [
                {"instrument": "SH600000", "available_at": "2026-04-01", "roe": 10.0, "debt_ratio": 80.0},
                {"instrument": "SH600001", "available_at": "2026-04-01", "roe": 5.0, "debt_ratio": 10.0},
            ]
        )
        spec = {
            "name": "quality_low_leverage",
            "components": [
                {"field": "roe", "direction": 1, "weight": 1.0},
                {"field": "debt_ratio", "direction": -1, "weight": 0.25},
            ],
        }

        result = build_fundamental_combo_frame(fundamentals, close, spec)

        self.assertIn("quality_low_leverage", result.columns)
        date = pd.Timestamp("2026-04-01")
        high_roe_high_debt = result.loc[(date, "SH600000"), "quality_low_leverage"]
        low_roe_low_debt = result.loc[(date, "SH600001"), "quality_low_leverage"]
        self.assertGreater(high_roe_high_debt, low_roe_low_debt)

    def test_evaluate_fundamental_factor_frame_outputs_size_industry_neutralized_rows(self) -> None:
        close = _close_frame(with_volume=True)
        fundamentals = pd.DataFrame(
            [
                {"instrument": "SH600000", "available_at": "2026-04-01", "roe": 10.0},
                {"instrument": "SH600001", "available_at": "2026-04-01", "roe": 5.0},
            ]
        )
        industry_map = pd.Series({"SH600000": "bank", "SH600001": "tech"})

        frame = build_fundamental_factor_frame(fundamentals, close, "roe")
        result = evaluate_fundamental_factor_frame(
            frame,
            factor_name="roe",
            direction=1,
            horizons=(1,),
            quantiles=2,
            neutralize_size=True,
            industry_map=industry_map,
        )

        self.assertEqual(["size_proxy+industry"], result["neutralization"].tolist())
        self.assertGreater(int(result.loc[0, "observations"]), 0)

    def test_run_fundamental_lane_oracle_discards_empty_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_contract(root)
            (root / "data").mkdir()
            pd.DataFrame(columns=["instrument", "available_at", "roe"]).to_csv(root / "data/fundamental_quality.csv", index=False)

            payload, block = run_fundamental_lane_oracle(
                lane_name="fundamental_quality",
                contract_path="configs/autoresearch/contracts/csi500_current_v1.yaml",
                project_root=root,
                close_frame=_close_frame(),
            )

            self.assertEqual("discard_candidate", payload["status"])
            self.assertIn("no fundamental rows", payload["decision_reason"])
            self.assertIn("fundamental_quality", block)

    def test_run_fundamental_lane_oracle_uses_active_candidates_and_writes_neutralized_eval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_contract(root, size_proxy=True)
            _write_fundamental_space(root)
            _write_security_master(root)
            (root / "data").mkdir(exist_ok=True)
            pd.DataFrame(
                [
                    {"instrument": "SH600000", "available_at": "2026-04-01", "roe": 10.0, "debt_ratio": 20.0, "gross_margin": 30.0},
                    {"instrument": "SH600001", "available_at": "2026-04-01", "roe": 5.0, "debt_ratio": 50.0, "gross_margin": 40.0},
                ]
            ).to_csv(root / "data/fundamental_quality.csv", index=False)

            payload, _ = run_fundamental_lane_oracle(
                lane_name="fundamental_quality",
                contract_path="configs/autoresearch/contracts/csi500_current_v1.yaml",
                project_root=root,
                close_frame=_close_frame(with_volume=True),
            )

            summaries = pd.read_csv(Path(payload["artifact_dir"]) / "factor_summaries.csv")
            self.assertEqual({"roe", "debt_ratio", "quality_low_leverage"}, set(summaries["factor"]))
            self.assertIn("none", set(summaries["neutralization"]))
            self.assertIn("size_proxy+industry", set(summaries["neutralization"]))
            self.assertTrue((Path(payload["artifact_dir"]) / "roe/neutralized_eval.csv").exists())
            self.assertTrue((Path(payload["artifact_dir"]) / "quality_low_leverage/neutralized_eval.csv").exists())

    def test_run_fundamental_lane_oracle_light_artifacts_skip_factor_frames(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_contract(root, size_proxy=True)
            _write_fundamental_space(root)
            _write_security_master(root)
            (root / "data").mkdir(exist_ok=True)
            pd.DataFrame(
                [
                    {"instrument": "SH600000", "available_at": "2026-04-01", "roe": 10.0, "debt_ratio": 20.0},
                    {"instrument": "SH600001", "available_at": "2026-04-01", "roe": 5.0, "debt_ratio": 50.0},
                ]
            ).to_csv(root / "data/fundamental_quality.csv", index=False)

            payload, block = run_fundamental_lane_oracle(
                lane_name="fundamental_quality",
                contract_path="configs/autoresearch/contracts/csi500_current_v1.yaml",
                project_root=root,
                close_frame=_close_frame(with_volume=True),
                artifact_mode="light",
            )

            artifact_dir = Path(payload["artifact_dir"])
            self.assertEqual("light", payload["artifact_mode"])
            self.assertIn("artifact_mode: light", block)
            self.assertTrue((artifact_dir / "roe/raw_eval.csv").exists())
            self.assertTrue((artifact_dir / "roe/neutralized_eval.csv").exists())
            self.assertTrue((artifact_dir / "factor_summaries.csv").exists())
            self.assertFalse((artifact_dir / "roe/factor_frame.csv").exists())
            self.assertFalse((artifact_dir / "quality_low_leverage/factor_frame.csv").exists())

    def test_load_fundamental_factor_specs_uses_active_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_fundamental_space(root)

            specs = load_fundamental_factor_specs(root / "configs/autoresearch/fundamental_space.yaml")

        self.assertEqual(["roe", "debt_ratio", "quality_low_leverage"], [spec["name"] for spec in specs])

    def test_default_specs_include_value_quality_growth_fields(self) -> None:
        names = {spec["name"] for spec in DEFAULT_FUNDAMENTAL_FACTOR_SPECS}

        self.assertIn("roe", names)
        self.assertIn("gross_margin", names)
        self.assertIn("debt_ratio", names)
        self.assertIn("revenue_growth_yoy", names)


def _close_frame(*, with_volume: bool = False) -> pd.DataFrame:
    dates = pd.date_range("2026-04-01", periods=5, freq="D")
    rows = []
    for instrument, base in [("SH600000", 10.0), ("SH600001", 20.0)]:
        for i, date in enumerate(dates):
            row = {"datetime": date, "instrument": instrument, "close": base + i}
            if with_volume:
                row["volume"] = 1000 + i * 10 + (100 if instrument == "SH600001" else 0)
            rows.append(row)
    return pd.DataFrame(rows).set_index(["datetime", "instrument"])


def _write_contract(root: Path, *, size_proxy: bool = False) -> None:
    path = root / "configs/autoresearch/contracts/csi500_current_v1.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        yaml.safe_dump(
            {
                "name": "test_contract",
                "provider_config": "configs/provider_current.yaml",
                "artifact_root": "reports/autoresearch/runs",
                "universe": "csi500_current",
                "benchmark": "SH000905",
                "start_time": "2026-04-01",
                "end_time": "2026-04-05",
                "horizons": [1],
                "metric": "rank_ic_mean_h1",
                "neutralization": {"raw": True, "size_proxy": size_proxy},
                "minimum_observations": 10,
                "ledger_path": "reports/autoresearch/expression_results.tsv",
            }
        ),
        encoding="utf-8",
    )


def _write_fundamental_space(root: Path) -> None:
    path = root / "configs/autoresearch/fundamental_space.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "candidate_factors": [
                    {"name": "roe", "direction": 1, "category": "candidate_quality", "active": True},
                    {"name": "debt_ratio", "direction": -1, "category": "candidate_quality", "active": True},
                    {
                        "name": "quality_low_leverage",
                        "direction": 1,
                        "category": "candidate_fundamental_combo",
                        "active": True,
                        "components": [
                            {"field": "roe", "direction": 1, "weight": 1.0},
                            {"field": "debt_ratio", "direction": -1, "weight": 0.25},
                        ],
                    },
                    {"name": "gross_margin", "direction": 1, "category": "candidate_quality", "active": False},
                ]
            }
        ),
        encoding="utf-8",
    )


def _write_security_master(root: Path) -> None:
    (root / "data").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"instrument": "SH600000", "industry_sw": "bank"},
            {"instrument": "SH600001", "industry_sw": "tech"},
        ]
    ).to_csv(root / "data/security_master.csv", index=False)


if __name__ == "__main__":
    unittest.main()
