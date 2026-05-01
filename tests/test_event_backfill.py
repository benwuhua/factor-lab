from __future__ import annotations

import unittest
from pathlib import Path

from qlib_factor_lab.event_backfill import EventBackfillConfig, build_event_backfill_command, event_backfill_window


class EventBackfillTests(unittest.TestCase):
    def test_event_backfill_window_uses_calendar_day_lookback(self) -> None:
        start, end = event_backfill_window("2026-04-30", days=90)

        self.assertEqual(start, "2026-01-31")
        self.assertEqual(end, "2026-04-30")

    def test_build_event_backfill_command_uses_merge_existing_events(self) -> None:
        command = build_event_backfill_command(
            EventBackfillConfig(
                project_root=Path("/repo"),
                as_of_date="2026-04-30",
                days=180,
                universes=("csi300", "csi500"),
            )
        )

        self.assertIn("scripts/build_research_context_data.py", command)
        self.assertIn("--notice-start", command)
        self.assertIn("2025-11-02", command)
        self.assertIn("--notice-end", command)
        self.assertIn("2026-04-30", command)
        self.assertIn("--merge-existing-events", command)
        self.assertIn("csi300", command)
        self.assertIn("csi500", command)


if __name__ == "__main__":
    unittest.main()
