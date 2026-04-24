import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.company_events import (
    COMPANY_EVENT_COLUMNS,
    EVENT_RISK_SNAPSHOT_COLUMNS,
    EventRiskConfig,
    build_event_risk_snapshot,
    load_company_events,
    load_event_risk_config,
)


class CompanyEventTests(unittest.TestCase):
    def test_event_summary_flags_blocking_event_within_window(self):
        signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["AAA"], "score": [0.7]})
        events = pd.DataFrame(
            {
                "event_id": ["evt-1"],
                "instrument": ["AAA"],
                "event_type": ["disciplinary_action"],
                "event_date": ["2026-04-10"],
                "source": ["exchange"],
                "source_url": ["https://example.test/evt-1"],
                "title": ["Exchange sanction"],
                "severity": ["block"],
                "summary": ["Company received a formal exchange sanction."],
                "evidence": ["notice"],
                "active_until": [""],
            }
        )

        snapshot = build_event_risk_snapshot(signal, events, EventRiskConfig(default_lookback_days=30))

        self.assertEqual(list(snapshot.columns), EVENT_RISK_SNAPSHOT_COLUMNS)
        self.assertEqual(snapshot.loc[0, "date"], "2026-04-23")
        self.assertEqual(snapshot.loc[0, "instrument"], "AAA")
        self.assertEqual(snapshot.loc[0, "event_count"], 1)
        self.assertTrue(bool(snapshot.loc[0, "event_blocked"]))
        self.assertEqual(snapshot.loc[0, "max_event_severity"], "block")
        self.assertEqual(snapshot.loc[0, "active_event_types"], "disciplinary_action")
        self.assertIn("disciplinary_action", snapshot.loc[0, "event_risk_summary"])
        self.assertIn("Exchange sanction", snapshot.loc[0, "event_risk_summary"])
        self.assertIn("formal exchange sanction", snapshot.loc[0, "event_risk_summary"])
        self.assertEqual(snapshot.loc[0, "event_source_urls"], "https://example.test/evt-1")

    def test_expired_events_are_ignored(self):
        signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["AAA"]})
        events = pd.DataFrame(
            {
                "event_id": ["evt-1", "evt-2"],
                "instrument": ["AAA", "AAA"],
                "event_type": ["regulatory_inquiry", "shareholder_reduction"],
                "event_date": ["2026-01-01", "2026-04-01"],
                "source": ["exchange", "filing"],
                "source_url": ["https://example.test/expired", "https://example.test/active"],
                "title": ["Expired inquiry", "Active reduction"],
                "severity": ["block", "watch"],
                "summary": ["Expired event should not appear.", "Shareholder reduction remains active."],
                "evidence": ["notice", "filing"],
                "active_until": ["2026-01-10", ""],
            }
        )
        config = EventRiskConfig(
            default_lookback_days=10,
            event_type_lookbacks={"shareholder_reduction": 60},
        )

        snapshot = build_event_risk_snapshot(signal, events, config)

        self.assertEqual(snapshot.loc[0, "event_count"], 1)
        self.assertFalse(bool(snapshot.loc[0, "event_blocked"]))
        self.assertEqual(snapshot.loc[0, "max_event_severity"], "watch")
        self.assertEqual(snapshot.loc[0, "active_event_types"], "shareholder_reduction")
        self.assertNotIn("Expired inquiry", snapshot.loc[0, "event_risk_summary"])
        self.assertEqual(snapshot.loc[0, "event_source_urls"], "https://example.test/active")

    def test_date_only_active_until_includes_full_calendar_day(self):
        signal = pd.DataFrame({"date": ["2026-04-23 15:00:00"], "instrument": ["AAA"]})
        events = pd.DataFrame(
            {
                "event_id": ["evt-1"],
                "instrument": ["AAA"],
                "event_type": ["regulatory_inquiry"],
                "event_date": ["2026-04-01"],
                "source": ["exchange"],
                "source_url": ["https://example.test/date-only"],
                "title": ["Inquiry"],
                "severity": ["watch"],
                "summary": ["Inquiry remains active through the end date."],
                "evidence": ["notice"],
                "active_until": ["2026-04-23"],
            }
        )

        snapshot = build_event_risk_snapshot(signal, events, EventRiskConfig())

        self.assertEqual(snapshot.loc[0, "event_count"], 1)
        self.assertEqual(snapshot.loc[0, "active_event_types"], "regulatory_inquiry")

    def test_timezone_event_date_compares_with_naive_signal_date(self):
        signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["AAA"]})
        events = pd.DataFrame(
            {
                "event_id": ["evt-1"],
                "instrument": ["AAA"],
                "event_type": ["regulatory_inquiry"],
                "event_date": ["2026-04-01T00:00:00Z"],
                "source": ["exchange"],
                "source_url": ["https://example.test/timezone"],
                "title": ["Inquiry"],
                "severity": ["watch"],
                "summary": ["Timezone-stamped event remains comparable."],
                "evidence": ["notice"],
                "active_until": [""],
            }
        )

        snapshot = build_event_risk_snapshot(signal, events, EventRiskConfig(default_lookback_days=30))

        self.assertEqual(snapshot.loc[0, "event_count"], 1)
        self.assertEqual(snapshot.loc[0, "active_event_types"], "regulatory_inquiry")

    def test_empty_events_returns_default_snapshot_rows(self):
        signal = pd.DataFrame(
            {
                "date": ["2026-04-23", "2026-04-24"],
                "instrument": ["AAA", "BBB"],
                "score": [0.7, -0.1],
            }
        )
        events = pd.DataFrame(columns=COMPANY_EVENT_COLUMNS)

        snapshot = build_event_risk_snapshot(signal, events, EventRiskConfig())

        self.assertEqual(list(snapshot.columns), EVENT_RISK_SNAPSHOT_COLUMNS)
        self.assertEqual(len(snapshot), 2)
        self.assertEqual(list(snapshot["event_count"]), [0, 0])
        self.assertEqual(list(snapshot["event_blocked"]), [False, False])
        self.assertEqual(list(snapshot["max_event_severity"]), ["", ""])
        self.assertEqual(list(snapshot["active_event_types"]), ["", ""])
        self.assertEqual(list(snapshot["event_risk_summary"]), ["", ""])
        self.assertEqual(list(snapshot["event_source_urls"]), ["", ""])

    def test_load_event_risk_config_parses_yaml(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "event_risk.yaml"
            config_path.write_text(
                "\n".join(
                    [
                        "event_risk:",
                        "  security_master_path: data/security_master.csv",
                        "  events_path: data/company_events.csv",
                        "  default_lookback_days: 45",
                        "  event_type_lookbacks:",
                        "    shareholder_reduction: 60",
                        "    disciplinary_action: 180",
                        "  block_event_types:",
                        "    - disciplinary_action",
                        "    - delisting_risk",
                        "  block_severities:",
                        "    - block",
                        "    - risk",
                        "  max_events_per_name: 2",
                    ]
                )
            )

            config = load_event_risk_config(config_path)

        self.assertEqual(config.security_master_path, Path("data/security_master.csv"))
        self.assertEqual(config.events_path, Path("data/company_events.csv"))
        self.assertEqual(config.default_lookback_days, 45)
        self.assertEqual(
            config.event_type_lookbacks,
            {"shareholder_reduction": 60, "disciplinary_action": 180},
        )
        self.assertEqual(config.block_event_types, ("disciplinary_action", "delisting_risk"))
        self.assertEqual(config.block_severities, ("block", "risk"))
        self.assertEqual(config.max_events_per_name, 2)

    def test_load_company_events_returns_required_columns_when_missing(self):
        none_loaded = load_company_events(None)

        self.assertTrue(none_loaded.empty)
        self.assertEqual(list(none_loaded.columns), COMPANY_EVENT_COLUMNS)

        with tempfile.TemporaryDirectory() as tmp:
            missing_loaded = load_company_events(Path(tmp) / "missing.csv")

        self.assertTrue(missing_loaded.empty)
        self.assertEqual(list(missing_loaded.columns), COMPANY_EVENT_COLUMNS)


if __name__ == "__main__":
    unittest.main()
