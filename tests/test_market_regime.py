import unittest

import pandas as pd

from qlib_factor_lab.event_backtest import summarize_trades
from qlib_factor_lab.market_regime import (
    annotate_trades_with_market_regime,
    compute_equal_weight_market_regime,
)


class MarketRegimeTests(unittest.TestCase):
    def test_compute_equal_weight_market_regime_classifies_trends(self):
        dates = pd.date_range("2026-01-01", periods=8, freq="D")
        index = pd.MultiIndex.from_product([dates, ["A", "B"]], names=["datetime", "instrument"])
        frame = pd.DataFrame(index=index)
        frame["close"] = [
            10.0,
            20.0,
            11.0,
            22.0,
            12.0,
            24.0,
            13.0,
            26.0,
            12.0,
            24.0,
            11.0,
            22.0,
            10.0,
            20.0,
            10.0,
            20.0,
        ]

        regime = compute_equal_weight_market_regime(
            frame,
            close_col="close",
            fast_window=2,
            slow_window=3,
            trend_window=1,
            trend_threshold=0.02,
        )

        self.assertEqual(regime.loc[pd.Timestamp("2026-01-04"), "market_regime"], "up")
        self.assertEqual(regime.loc[pd.Timestamp("2026-01-07"), "market_regime"], "down")
        self.assertEqual(regime.loc[pd.Timestamp("2026-01-08"), "market_regime"], "sideways")
        self.assertIn("market_ret", regime.columns)
        self.assertIn("market_proxy", regime.columns)

    def test_annotate_trades_with_market_regime_allows_regime_summary(self):
        trades = pd.DataFrame(
            {
                "bucket": ["p95_p100", "p95_p100", "p95_p100"],
                "horizon": [20, 20, 20],
                "signal_date": pd.to_datetime(["2026-01-02", "2026-01-03", "2026-01-04"]),
                "entry_date": pd.to_datetime(["2026-01-03", "2026-01-04", "2026-01-05"]),
                "return": [0.10, -0.05, 0.02],
                "mfe": [0.15, 0.01, 0.04],
                "mae": [-0.02, -0.08, -0.01],
            }
        )
        regime = pd.DataFrame(
            {
                "datetime": pd.to_datetime(["2026-01-02", "2026-01-03", "2026-01-04"]),
                "market_regime": ["up", "down", "up"],
            }
        ).set_index("datetime")

        annotated = annotate_trades_with_market_regime(trades, regime)
        summary = summarize_trades(annotated, group_cols=("market_regime", "bucket", "horizon"))

        self.assertEqual(annotated["market_regime"].tolist(), ["up", "down", "up"])
        up = summary[summary["market_regime"] == "up"].iloc[0]
        self.assertEqual(up["trade_count"], 2)
        self.assertAlmostEqual(up["mean_return"], 0.06)


if __name__ == "__main__":
    unittest.main()
