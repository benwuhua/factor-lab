from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from qlib_factor_lab.daily_update_schedule import build_daily_update_command, next_run_at


class DailyUpdateScheduleTest(unittest.TestCase):
    def test_next_run_at_uses_today_before_after_close_time(self) -> None:
        tz = ZoneInfo("Asia/Shanghai")

        result = next_run_at(datetime(2026, 5, 5, 14, 0, tzinfo=tz), run_time="15:45", timezone="Asia/Shanghai")

        self.assertEqual(result.isoformat(), "2026-05-05T15:45:00+08:00")

    def test_next_run_at_moves_to_next_day_after_after_close_time(self) -> None:
        tz = ZoneInfo("Asia/Shanghai")

        result = next_run_at(datetime(2026, 5, 5, 16, 0, tzinfo=tz), run_time="15:45", timezone="Asia/Shanghai")

        self.assertEqual(result.isoformat(), "2026-05-06T15:45:00+08:00")

    def test_build_daily_update_command_defaults_to_full_research_refresh(self) -> None:
        command = build_daily_update_command(
            python_bin=".venv/bin/python",
            as_of_date="20260505",
            env_file=".env",
        )

        self.assertEqual(command[:2], (".venv/bin/python", "scripts/update_daily_data.py"))
        self.assertIn("--as-of-date", command)
        self.assertIn("20260505", command)
        self.assertIn("--fetch-fundamentals", command)
        self.assertIn("--derive-valuation-fields", command)
        self.assertNotIn("--fetch-cninfo-dividends", command)
        self.assertIn("--fetch-dividends", command)
        self.assertIn("--dividend-provider", command)
        self.assertIn("tushare", command)
        self.assertIn("--fetch-disclosure-events", command)
        self.assertIn("--env-file", command)
        self.assertIn(".env", command)


if __name__ == "__main__":
    unittest.main()
