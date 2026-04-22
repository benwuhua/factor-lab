import unittest

import pandas as pd

from qlib_factor_lab.event_backtest import EventBacktestConfig, build_event_trades, summarize_trades


class EventBacktestTests(unittest.TestCase):
    def test_build_event_trades_enters_next_open_exits_after_horizon_and_skips_overlaps(self):
        frame = self._sample_frame()

        trades = build_event_trades(
            frame,
            "signal",
            EventBacktestConfig(horizons=(3,), buckets=((0.50, 1.0),)),
        )

        a_trades = trades[trades["instrument"] == "A"].reset_index(drop=True)
        self.assertEqual(len(a_trades), 2)
        self.assertEqual(a_trades.loc[0, "signal_date"], pd.Timestamp("2026-01-01"))
        self.assertEqual(a_trades.loc[0, "entry_date"], pd.Timestamp("2026-01-02"))
        self.assertEqual(a_trades.loc[0, "exit_date"], pd.Timestamp("2026-01-04"))
        self.assertAlmostEqual(a_trades.loc[0, "return"], 14 / 11 - 1)
        self.assertEqual(a_trades.loc[1, "signal_date"], pd.Timestamp("2026-01-05"))

    def test_build_event_trades_records_bucket_percentile_mfe_and_mae(self):
        frame = self._sample_frame()

        trades = build_event_trades(
            frame,
            "signal",
            EventBacktestConfig(horizons=(3,), buckets=((0.50, 1.0),)),
        )

        first = trades[trades["instrument"] == "A"].iloc[0]
        self.assertEqual(first["bucket"], "p50_p100")
        self.assertAlmostEqual(first["score_pct"], 1.0)
        self.assertAlmostEqual(first["mfe"], 15 / 11 - 1)
        self.assertAlmostEqual(first["mae"], 9 / 11 - 1)

    def test_build_event_trades_applies_negative_factor_direction_before_bucket_ranking(self):
        frame = self._sample_frame()

        trades = build_event_trades(
            frame,
            "signal",
            EventBacktestConfig(horizons=(3,), buckets=((0.50, 1.0),)),
            signal_direction=-1,
        )

        first = trades[trades["instrument"] == "B"].iloc[0]
        self.assertEqual(first["signal_date"], pd.Timestamp("2026-01-01"))
        self.assertEqual(first["score"], 1)
        self.assertAlmostEqual(first["score_pct"], 1.0)

    def test_build_event_trades_waits_for_breakout_volume_confirmation(self):
        frame = self._confirmation_frame()

        trades = build_event_trades(
            frame,
            "signal",
            EventBacktestConfig(
                horizons=(3,),
                buckets=((0.85, 1.0),),
                confirmation_window=3,
                confirmation_breakout_lookback=3,
                confirmation_volume_lookback=2,
                confirmation_volume_ratio=1.2,
            ),
        )

        a_trade = trades[trades["instrument"] == "A"].iloc[0]
        self.assertEqual(a_trade["signal_date"], pd.Timestamp("2026-01-03"))
        self.assertEqual(a_trade["confirmation_date"], pd.Timestamp("2026-01-05"))
        self.assertEqual(a_trade["entry_date"], pd.Timestamp("2026-01-06"))
        self.assertEqual(a_trade["exit_date"], pd.Timestamp("2026-01-08"))
        self.assertAlmostEqual(a_trade["breakout_level"], 12.0)
        self.assertAlmostEqual(a_trade["confirmation_volume_ratio"], 240 / 100)
        self.assertAlmostEqual(a_trade["return"], 14 / 13 - 1)

    def test_build_event_trades_skips_signal_without_confirmation(self):
        frame = self._confirmation_frame()
        for date in pd.date_range("2026-01-04", "2026-01-09", freq="D"):
            frame.loc[(date, "A"), "close"] = 11.8
            frame.loc[(date, "A"), "high"] = 12.0
            frame.loc[(date, "A"), "volume"] = 110

        trades = build_event_trades(
            frame,
            "signal",
            EventBacktestConfig(
                horizons=(3,),
                buckets=((0.85, 1.0),),
                confirmation_window=3,
                confirmation_breakout_lookback=3,
                confirmation_volume_lookback=2,
                confirmation_volume_ratio=1.2,
            ),
        )

        self.assertTrue(trades[trades["instrument"] == "A"].empty)

    def test_build_event_trades_accepts_named_factor_column_with_confirmation(self):
        frame = self._confirmation_frame().rename(columns={"signal": "alpha"})

        trades = build_event_trades(
            frame,
            "alpha",
            EventBacktestConfig(
                horizons=(3,),
                buckets=((0.85, 1.0),),
                confirmation_window=3,
                confirmation_breakout_lookback=3,
                confirmation_volume_lookback=2,
                confirmation_volume_ratio=1.2,
            ),
        )

        self.assertFalse(trades[trades["instrument"] == "A"].empty)

    def test_summarize_trades_reports_return_quality_metrics(self):
        trades = pd.DataFrame(
            {
                "bucket": ["p50_p100", "p50_p100", "p50_p100"],
                "horizon": [3, 3, 3],
                "entry_date": pd.to_datetime(["2026-01-02", "2026-01-03", "2027-01-02"]),
                "return": [0.10, -0.05, 0.20],
                "mfe": [0.15, 0.02, 0.25],
                "mae": [-0.02, -0.08, -0.03],
            }
        )

        summary = summarize_trades(trades)
        yearly = summarize_trades(trades, by_year=True)

        row = summary.iloc[0]
        self.assertEqual(row["trade_count"], 3)
        self.assertAlmostEqual(row["mean_return"], (0.10 - 0.05 + 0.20) / 3)
        self.assertAlmostEqual(row["win_rate"], 2 / 3)
        self.assertAlmostEqual(row["avg_win"], 0.15)
        self.assertAlmostEqual(row["avg_loss"], -0.05)
        self.assertAlmostEqual(row["payoff_ratio"], 3.0)
        self.assertEqual(yearly["year"].tolist(), [2026, 2027])

    def _sample_frame(self):
        dates = pd.date_range("2026-01-01", periods=8, freq="D")
        instruments = ["A", "B"]
        index = pd.MultiIndex.from_product([dates, instruments], names=["datetime", "instrument"])
        frame = pd.DataFrame(index=index)
        frame["signal"] = [2, 1, 3, 1, 3, 1, 3, 1, 4, 1, 4, 1, 4, 1, 4, 1]
        frame["open"] = [10, 20, 11, 20, 12, 20, 13, 20, 14, 20, 15, 20, 16, 20, 17, 20]
        frame["close"] = [10, 20, 11, 20, 12, 20, 14, 20, 15, 20, 16, 20, 17, 20, 18, 20]
        frame["high"] = [10, 20, 12, 20, 15, 20, 14, 20, 16, 20, 17, 20, 18, 20, 19, 20]
        frame["low"] = [10, 20, 10, 20, 9, 20, 13, 20, 14, 20, 14, 20, 16, 20, 17, 20]
        return frame

    def _confirmation_frame(self):
        dates = pd.date_range("2026-01-01", periods=9, freq="D")
        instruments = ["A", "B", "C"]
        index = pd.MultiIndex.from_product([dates, instruments], names=["datetime", "instrument"])
        frame = pd.DataFrame(index=index)
        frame["signal"] = [
            1, 3, 2,
            1, 3, 2,
            4, 2, 1,
            4, 2, 1,
            4, 2, 1,
            4, 2, 1,
            4, 2, 1,
            4, 2, 1,
            4, 2, 1,
        ]
        frame["open"] = [
            9, 20, 30,
            10, 20, 30,
            11, 20, 30,
            11.5, 20, 30,
            12.8, 20, 30,
            13, 20, 30,
            13.5, 20, 30,
            14, 20, 30,
            14.5, 20, 30,
        ]
        frame["close"] = [
            10, 20, 30,
            11, 20, 30,
            11.5, 20, 30,
            11.8, 20, 30,
            12.6, 20, 30,
            13.2, 20, 30,
            13.6, 20, 30,
            14, 20, 30,
            14.8, 20, 30,
        ]
        frame["high"] = [
            10.5, 20, 30,
            11.5, 20, 30,
            12, 20, 30,
            12.2, 20, 30,
            12.8, 20, 30,
            13.5, 20, 30,
            13.8, 20, 30,
            14.2, 20, 30,
            15, 20, 30,
        ]
        frame["low"] = [
            8.8, 20, 30,
            9.8, 20, 30,
            10.8, 20, 30,
            11.2, 20, 30,
            12.1, 20, 30,
            12.6, 20, 30,
            13.2, 20, 30,
            13.7, 20, 30,
            14.1, 20, 30,
        ]
        frame["volume"] = [
            100, 100, 100,
            100, 100, 100,
            100, 100, 100,
            100, 100, 100,
            240, 100, 100,
            130, 100, 100,
            140, 100, 100,
            150, 100, 100,
            160, 100, 100,
        ]
        return frame


if __name__ == "__main__":
    unittest.main()
