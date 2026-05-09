import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.config import load_project_config
from qlib_factor_lab.qlib_bootstrap import init_qlib
from qlib_factor_lab.wangji_patterns import (
    FACTOR2_BREAKOUT_COLUMN,
    WangjiFactor2EventConfig,
    compute_wangji_factor2_events,
)


def _synthetic_2b_frame() -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=115, freq="D")
    close = []
    high = []
    low = []
    open_ = []
    volume = []
    for idx in range(len(dates)):
        if idx < 20:
            c = 10.0 + idx * 0.03
        elif idx == 20:
            c = 12.0
        elif idx < 55:
            c = 10.9 - (idx - 21) * 0.035
        elif idx < 88:
            c = 9.7 + (idx - 55) * 0.105
        elif idx == 88:
            c = 13.25
        elif idx < 96:
            c = 13.10 - (idx - 89) * 0.06
        elif idx < 104:
            c = 12.85 + (idx - 96) * 0.05
        elif idx == 104:
            c = 14.20
        else:
            c = 14.25 + (idx - 104) * 0.03
        close.append(c)
        open_.append(c * (0.965 if idx == 104 else 0.995))
        high.append(c * (1.002 if idx == 104 else (1.012 if idx != 20 else 1.10)))
        low.append(c * (0.988 if idx not in range(89, 104) else 0.975))
        volume.append(2_400_000 if idx == 104 else 1_000_000 + idx * 2_000)

    frame = pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        },
        index=pd.MultiIndex.from_product([dates, ["SZTEST"]], names=["datetime", "instrument"]),
    )
    return frame


def _synthetic_intraday_only_breakout_frame() -> pd.DataFrame:
    frame = _synthetic_2b_frame().copy()
    dates = frame.index.get_level_values("datetime")
    target = dates == pd.Timestamp("2026-04-15")
    frame.loc[target, "open"] = 13.40
    frame.loc[target, "high"] = 14.80
    frame.loc[target, "low"] = 13.10
    frame.loc[target, "close"] = 13.55
    frame.loc[target, "volume"] = 3_400_000
    return frame


def _synthetic_breakout_confirmation_frame(close_breakout: bool = True, confirm_bull: bool = True) -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=125, freq="D")
    open_, high, low, close, volume = [], [], [], [], []
    for idx, _ in enumerate(dates):
        if idx < 30:
            c = 10.0 + idx * 0.04
        elif idx == 30:
            c = 12.0
        elif idx < 75:
            c = 10.8 + (idx - 30) * 0.01
        elif idx < 105:
            c = 11.2 + (idx - 75) * 0.055
        elif idx == 105:
            c = 12.60
        elif idx == 106:
            c = 13.10 if close_breakout else 11.95
        elif idx == 107:
            c = 13.25 if confirm_bull else 12.40
        else:
            c = 12.95 + (idx - 107) * 0.01
        close.append(c)
        if idx == 106:
            open_.append(12.05)
            high.append(13.65)
            low.append(11.95)
            volume.append(3_000_000)
        elif idx == 107:
            open_.append(13.12 if confirm_bull else 12.80)
            high.append(13.90)
            low.append(13.45 if confirm_bull else 12.60)
            volume.append(2_200_000)
        else:
            open_.append(c * 0.992)
            high.append(c * (1.012 if idx != 30 else 1.08))
            low.append(c * 0.988)
            volume.append(1_000_000 + idx * 3_000)

    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": volume},
        index=pd.MultiIndex.from_product([dates, ["SZATTACK"]], names=["datetime", "instrument"]),
    )


class WangjiPatternTests(unittest.TestCase):
    def test_factor2_detector_does_not_treat_later_2b_pullback_as_diamond_point(self):
        events = compute_wangji_factor2_events(
            _synthetic_2b_frame(),
            WangjiFactor2EventConfig(
                pressure_lookback=80,
                pressure_exclusion=18,
                min_pressure_history=15,
                breakout_min_age=3,
                breakout_max_age=25,
            ),
        )

        signal_rows = events[events["wangji-factor2"] == 1.0].reset_index()

        self.assertTrue(signal_rows.empty)

    def test_factor2_detector_rejects_without_recent_local_confirmation(self):
        frame = _synthetic_2b_frame()
        idx = frame.index.get_level_values("datetime") == pd.Timestamp("2026-04-15")
        frame.loc[idx, "close"] = frame.loc[idx, "close"] * 0.92

        events = compute_wangji_factor2_events(
            frame,
            WangjiFactor2EventConfig(
                pressure_lookback=80,
                pressure_exclusion=18,
                min_pressure_history=15,
                breakout_min_age=3,
                breakout_max_age=25,
            ),
        )

        self.assertEqual(float(events["wangji-factor2"].max()), 0.0)

    def test_factor2_detector_does_not_emit_pullback_as_second_diamond_point(self):
        events = compute_wangji_factor2_events(
            _synthetic_2b_frame(),
            WangjiFactor2EventConfig(
                pressure_lookback=80,
                pressure_exclusion=18,
                min_pressure_history=15,
                breakout_min_age=3,
                breakout_max_age=25,
            ),
        )

        signal_rows = events[events["wangji-factor2"] == 1.0].reset_index()

        self.assertTrue(signal_rows.empty)

    def test_factor2_breakout_attack_requires_close_breakout_then_next_day_shrink_bull_confirmation(self):
        events = compute_wangji_factor2_events(
            _synthetic_breakout_confirmation_frame(),
            WangjiFactor2EventConfig(
                pressure_lookback=80,
                pressure_exclusion=18,
                min_pressure_history=15,
                breakout_min_age=3,
                breakout_max_age=25,
            ),
        )

        signal_rows = events[events["wangji-factor2"] == 1.0].reset_index()

        self.assertEqual(len(signal_rows), 1)
        self.assertEqual(signal_rows.iloc[0]["datetime"], pd.Timestamp("2026-04-18"))
        self.assertEqual(signal_rows.iloc[0]["breakout_date"], pd.Timestamp("2026-04-17"))
        self.assertEqual(signal_rows.iloc[0]["diamond_type"], "breakout_attack")

        breakout_rows = events[events[FACTOR2_BREAKOUT_COLUMN] == 1.0].reset_index()
        self.assertEqual(len(breakout_rows), 1)
        self.assertEqual(breakout_rows.iloc[0]["datetime"], pd.Timestamp("2026-04-17"))
        self.assertEqual(float(breakout_rows.iloc[0]["wangji-factor2"]), 0.0)

    def test_factor2_breakout_attack_rejects_intraday_high_without_breakout_close(self):
        events = compute_wangji_factor2_events(
            _synthetic_breakout_confirmation_frame(close_breakout=False),
            WangjiFactor2EventConfig(
                pressure_lookback=80,
                pressure_exclusion=18,
                min_pressure_history=15,
                breakout_min_age=3,
                breakout_max_age=25,
            ),
        )

        signal_rows = events[events["wangji-factor2"] == 1.0].reset_index()

        self.assertTrue(signal_rows.empty)

    def test_factor2_breakout_attack_rejects_next_day_without_shrink_bull_confirmation(self):
        events = compute_wangji_factor2_events(
            _synthetic_breakout_confirmation_frame(confirm_bull=False),
            WangjiFactor2EventConfig(
                pressure_lookback=80,
                pressure_exclusion=18,
                min_pressure_history=15,
                breakout_min_age=3,
                breakout_max_age=25,
            ),
        )

        signal_rows = events[events["wangji-factor2"] == 1.0].reset_index()

        self.assertTrue(signal_rows.empty)

    def test_factor2_breakout_attack_rejects_breakout_day_return_too_hot(self):
        frame = _synthetic_breakout_confirmation_frame()
        breakout_date = pd.Timestamp("2026-04-17")
        confirm_date = pd.Timestamp("2026-04-18")
        frame.loc[(breakout_date, "SZATTACK"), "open"] = 12.20
        frame.loc[(breakout_date, "SZATTACK"), "high"] = 14.90
        frame.loc[(breakout_date, "SZATTACK"), "low"] = 12.10
        frame.loc[(breakout_date, "SZATTACK"), "close"] = 14.50
        frame.loc[(breakout_date, "SZATTACK"), "volume"] = 3_000_000
        frame.loc[(confirm_date, "SZATTACK"), "open"] = 14.55
        frame.loc[(confirm_date, "SZATTACK"), "high"] = 14.85
        frame.loc[(confirm_date, "SZATTACK"), "low"] = 14.45
        frame.loc[(confirm_date, "SZATTACK"), "close"] = 14.70
        frame.loc[(confirm_date, "SZATTACK"), "volume"] = 2_200_000

        events = compute_wangji_factor2_events(
            frame,
            WangjiFactor2EventConfig(
                pressure_lookback=80,
                pressure_exclusion=18,
                min_pressure_history=15,
                breakout_min_age=3,
                breakout_max_age=25,
            ),
        )

        self.assertEqual(float(events["wangji-factor2"].max()), 0.0)
        self.assertEqual(float(events[FACTOR2_BREAKOUT_COLUMN].max()), 0.0)

    def test_factor2_detector_does_not_reemit_xingsen_continuation_as_second_diamond(self):
        provider_path = Path("configs/provider_current.yaml")
        if not provider_path.exists():
            self.skipTest("local provider config is not available")

        from qlib.data import D

        config = load_project_config(provider_path)
        init_qlib(config)
        frame = D.features(
            ["SZ002436"],
            ["$open", "$high", "$low", "$close", "$volume"],
            start_time="2025-09-01",
            end_time="2026-05-07",
            freq=config.freq,
        )
        if frame.empty:
            self.skipTest("SZ002436 local sample data is not available")
        frame.columns = ["open", "high", "low", "close", "volume"]

        events = compute_wangji_factor2_events(frame.dropna()).reset_index()
        xingsen = events[events["instrument"] == "SZ002436"].set_index("datetime")

        self.assertEqual(float(xingsen.loc[pd.Timestamp("2026-04-27"), "wangji-factor2"]), 0.0)
        self.assertEqual(float(xingsen.loc[pd.Timestamp("2026-05-07"), "wangji-factor2"]), 0.0)

    def test_factor2_detector_rejects_xunyou_hot_breakout_under_tight_breakout_limit(self):
        csv_path = Path("data/tushare/wangji_samples/sz300467.csv")
        if not csv_path.exists():
            self.skipTest("SZ300467 local sample data is not available")

        raw = pd.read_csv(csv_path)
        raw["datetime"] = pd.to_datetime(raw["date"])
        raw["instrument"] = "SZ300467"
        frame = raw[["datetime", "instrument", "open", "high", "low", "close", "volume"]]
        frame = frame.set_index(["datetime", "instrument"]).sort_index()

        events = compute_wangji_factor2_events(frame.dropna()).reset_index()
        xunyou = events[events["instrument"] == "SZ300467"].set_index("datetime")

        self.assertEqual(float(xunyou.loc[pd.Timestamp("2026-04-27"), "wangji-factor2"]), 0.0)
        self.assertEqual(float(xunyou.loc[pd.Timestamp("2026-04-28"), "wangji-factor2"]), 0.0)
        self.assertEqual(float(xunyou.loc[pd.Timestamp("2026-05-07"), "wangji-factor2"]), 0.0)

    def test_factor2_detector_prefers_pullback_confirm_when_prior_breakout_exists(self):
        provider_path = Path("configs/provider_csi300_current.yaml")
        if not provider_path.exists():
            self.skipTest("local CSI300 provider config is not available")

        from qlib.data import D

        config = load_project_config(provider_path)
        init_qlib(config)
        frame = D.features(
            ["SH603986"],
            ["$open", "$high", "$low", "$close", "$volume"],
            start_time="2025-07-01",
            end_time="2026-02-10",
            freq=config.freq,
        )
        if frame.empty:
            self.skipTest("SH603986 local sample data is not available")
        frame.columns = ["open", "high", "low", "close", "volume"]

        events = compute_wangji_factor2_events(frame.dropna()).reset_index()
        zhaoyi = events[events["instrument"] == "SH603986"].set_index("datetime")

        self.assertEqual(float(zhaoyi.loc[pd.Timestamp("2026-01-16"), "wangji-factor2"]), 0.0)

    def test_factor2_detector_allows_platform_pullback_confirm_after_sideways_reset(self):
        csv_path = Path("data/tushare/wangji_samples/sz301018.csv")
        if not csv_path.exists():
            self.skipTest("SZ301018 local sample data is not available")

        raw = pd.read_csv(csv_path)
        raw["datetime"] = pd.to_datetime(raw["date"])
        raw["instrument"] = "SZ301018"
        frame = raw[["datetime", "instrument", "open", "high", "low", "close", "volume"]]
        frame = frame.set_index(["datetime", "instrument"]).sort_index()

        events = compute_wangji_factor2_events(frame.dropna()).reset_index()
        shenling = events[events["instrument"] == "SZ301018"].set_index("datetime")

        self.assertEqual(float(shenling.loc[pd.Timestamp("2026-05-07"), "wangji-factor2"]), 0.0)

    def test_factor2_detector_rejects_stale_or_deep_pullback_as_diamond_point(self):
        provider_path = Path("configs/provider_current.yaml")
        if not provider_path.exists():
            self.skipTest("local provider config is not available")

        from qlib.data import D

        config = load_project_config(provider_path)
        init_qlib(config)
        frame = D.features(
            ["SH600995", "SH601016"],
            ["$open", "$high", "$low", "$close", "$volume"],
            start_time="2025-11-01",
            end_time="2026-05-07",
            freq=config.freq,
        )
        if frame.empty:
            self.skipTest("local latest signal sample data is not available")
        frame.columns = ["open", "high", "low", "close", "volume"]

        events = compute_wangji_factor2_events(frame.dropna()).reset_index()
        latest = events[events["datetime"] == pd.Timestamp("2026-05-07")].set_index("instrument")

        self.assertEqual(float(latest.loc["SH600995", "wangji-factor2"]), 0.0)
        self.assertEqual(float(latest.loc["SH601016", "wangji-factor2"]), 0.0)

    def test_factor2_detector_rejects_latest_repeated_or_extended_breakout_watch(self):
        provider_path = Path("configs/provider_current.yaml")
        if not provider_path.exists():
            self.skipTest("local provider config is not available")

        from qlib.data import D

        config = load_project_config(provider_path)
        init_qlib(config)
        frame = D.features(
            ["SZ000582", "SZ300570"],
            ["$open", "$high", "$low", "$close", "$volume"],
            start_time="2025-11-01",
            end_time="2026-05-08",
            freq=config.freq,
        )
        if frame.empty:
            self.skipTest("local latest signal sample data is not available")
        frame.columns = ["open", "high", "low", "close", "volume"]

        events = compute_wangji_factor2_events(frame.dropna()).reset_index()
        latest = events[events["datetime"] == pd.Timestamp("2026-05-08")].set_index("instrument")

        self.assertEqual(float(latest.loc["SZ000582", FACTOR2_BREAKOUT_COLUMN]), 0.0)
        self.assertEqual(float(latest.loc["SZ300570", FACTOR2_BREAKOUT_COLUMN]), 0.0)


if __name__ == "__main__":
    unittest.main()
