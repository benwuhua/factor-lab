import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from qlib_factor_lab.company_events import (
    COMPANY_EVENT_COLUMNS,
    EVENT_RISK_SNAPSHOT_COLUMNS,
    EventRiskConfig,
    build_event_risk_snapshot,
    classify_event_type,
    load_company_events,
    load_event_risk_config,
)


class CompanyEventTests(unittest.TestCase):
    def test_classify_event_type_maps_p2_taxonomy(self):
        expected = {
            "buyback": ("positive_catalyst", "info", "boost"),
            "shareholder_increase": ("positive_catalyst", "info", "boost"),
            "order_contract": ("positive_catalyst", "info", "boost"),
            "earnings_preannouncement_up": ("positive_catalyst", "info", "boost"),
            "equity_incentive": ("positive_catalyst", "info", "boost"),
            "shareholder_reduction": ("watch_risk", "watch", "watch"),
            "large_unlock": ("watch_risk", "watch", "watch"),
            "regulatory_inquiry": ("watch_risk", "watch", "watch"),
            "pledge_risk": ("watch_risk", "watch", "watch"),
            "guarantee": ("watch_risk", "watch", "watch"),
            "lawsuit": ("watch_risk", "watch", "watch"),
            "disciplinary_action": ("block_risk", "block", "block"),
            "investigation": ("block_risk", "block", "block"),
            "st_risk": ("block_risk", "block", "block"),
            "delisting_risk": ("block_risk", "block", "block"),
            "nonstandard_audit": ("block_risk", "block", "block"),
            "major_penalty": ("block_risk", "block", "block"),
        }

        for event_type, (event_class, severity, action) in expected.items():
            with self.subTest(event_type=event_type):
                self.assertEqual(
                    classify_event_type(event_type),
                    {
                        "event_class": event_class,
                        "default_severity": severity,
                        "portfolio_action": action,
                    },
                )

        self.assertEqual(
            classify_event_type("unknown_notice"),
            {
                "event_class": "watch_risk",
                "default_severity": "watch",
                "portfolio_action": "watch",
            },
        )

    def test_snapshot_fills_missing_severity_and_summarizes_classes_and_actions(self):
        signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["AAA"]})
        events = pd.DataFrame(
            {
                "event_id": ["evt-1", "evt-2", "evt-3"],
                "instrument": ["AAA", "AAA", "AAA"],
                "event_type": ["buyback", "shareholder_reduction", "investigation"],
                "event_date": ["2026-04-20", "2026-04-21", "2026-04-22"],
                "source": ["filing", "filing", "exchange"],
                "source_url": [
                    "https://example.test/buyback",
                    "https://example.test/reduction",
                    "https://example.test/investigation",
                ],
                "title": ["Buyback plan", "Reduction plan", "Investigation"],
                "severity": ["", "risk", ""],
                "summary": ["Board approved buyback.", "Explicit risk remains authoritative.", "Formal investigation."],
                "evidence": ["notice", "notice", "notice"],
                "active_until": ["", "", ""],
            }
        )

        snapshot = build_event_risk_snapshot(signal, events, EventRiskConfig(default_lookback_days=30))

        self.assertIn("event_classes", snapshot.columns)
        self.assertIn("event_actions", snapshot.columns)
        self.assertEqual(snapshot.loc[0, "event_count"], 3)
        self.assertTrue(bool(snapshot.loc[0, "event_blocked"]))
        self.assertEqual(snapshot.loc[0, "max_event_severity"], "block")
        self.assertEqual(
            snapshot.loc[0, "event_classes"],
            "positive_catalyst; watch_risk; block_risk",
        )
        self.assertEqual(snapshot.loc[0, "event_actions"], "boost; watch; block")

    def test_snapshot_separates_positive_and_risk_event_evidence(self):
        signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["AAA"]})
        events = pd.DataFrame(
            {
                "event_id": ["evt-1", "evt-2"],
                "instrument": ["AAA", "AAA"],
                "event_type": ["order_contract", "regulatory_inquiry"],
                "event_date": ["2026-04-20", "2026-04-21"],
                "source": ["filing", "exchange"],
                "source_url": ["https://example.test/order", "https://example.test/inquiry"],
                "title": ["Large order", "Inquiry letter"],
                "severity": ["", ""],
                "summary": ["Won a material contract.", "Received an exchange inquiry."],
                "evidence": ["notice", "letter"],
                "active_until": ["", ""],
            }
        )

        snapshot = build_event_risk_snapshot(signal, events, EventRiskConfig(default_lookback_days=30))

        self.assertEqual(snapshot.loc[0, "positive_event_types"], "order_contract")
        self.assertIn("Large order", snapshot.loc[0, "positive_event_summary"])
        self.assertNotIn("Inquiry letter", snapshot.loc[0, "positive_event_summary"])
        self.assertEqual(snapshot.loc[0, "risk_event_types"], "regulatory_inquiry")
        self.assertIn("Inquiry letter", snapshot.loc[0, "risk_event_summary"])
        self.assertNotIn("Large order", snapshot.loc[0, "risk_event_summary"])

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
                        "  event_taxonomy:",
                        "    positive_catalyst:",
                        "      default_severity: info",
                        "      portfolio_action: boost",
                        "      event_types:",
                        "        - custom_order",
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
        self.assertEqual(
            config.event_taxonomy["custom_order"],
            {
                "event_class": "positive_catalyst",
                "default_severity": "info",
                "portfolio_action": "boost",
            },
        )

    def test_configured_event_taxonomy_drives_snapshot_classification(self):
        signal = pd.DataFrame({"date": ["2026-04-23"], "instrument": ["AAA"]})
        events = pd.DataFrame(
            {
                "event_id": ["evt-1"],
                "instrument": ["AAA"],
                "event_type": ["custom_order"],
                "event_date": ["2026-04-20"],
                "source": ["filing"],
                "source_url": ["https://example.test/custom"],
                "title": ["Custom order"],
                "severity": [""],
                "summary": ["Configured taxonomy should classify this as positive."],
                "evidence": ["notice"],
                "active_until": [""],
            }
        )
        config = EventRiskConfig(
            default_lookback_days=30,
            event_taxonomy={
                **EventRiskConfig().event_taxonomy,
                "custom_order": {
                    "event_class": "positive_catalyst",
                    "default_severity": "info",
                    "portfolio_action": "boost",
                },
            },
        )

        snapshot = build_event_risk_snapshot(signal, events, config)

        self.assertEqual("positive_catalyst", snapshot.loc[0, "event_classes"])
        self.assertEqual("boost", snapshot.loc[0, "event_actions"])
        self.assertFalse(bool(snapshot.loc[0, "event_blocked"]))
        self.assertEqual("custom_order", snapshot.loc[0, "positive_event_types"])

    def test_load_company_events_returns_required_columns_when_missing(self):
        none_loaded = load_company_events(None)

        self.assertTrue(none_loaded.empty)
        self.assertEqual(list(none_loaded.columns), COMPANY_EVENT_COLUMNS)

        with tempfile.TemporaryDirectory() as tmp:
            missing_loaded = load_company_events(Path(tmp) / "missing.csv")

        self.assertTrue(missing_loaded.empty)
        self.assertEqual(list(missing_loaded.columns), COMPANY_EVENT_COLUMNS)

    def test_build_event_risk_snapshot_cli_writes_blocking_snapshot(self):
        repo_root = Path(__file__).resolve().parents[1]
        script = repo_root / "scripts" / "build_event_risk_snapshot.py"

        with tempfile.TemporaryDirectory() as tmp:
            project_root = Path(tmp)
            (project_root / "configs").mkdir()
            (project_root / "runs" / "20260423").mkdir(parents=True)
            (project_root / "data").mkdir()
            (project_root / "configs" / "event_risk.yaml").write_text(
                "\n".join(
                    [
                        "event_risk:",
                        "  events_path: data/company_events.csv",
                        "  default_lookback_days: 30",
                        "  block_event_types:",
                        "    - disciplinary_action",
                        "  block_severities:",
                        "    - block",
                    ]
                ),
                encoding="utf-8",
            )
            (project_root / "runs" / "20260423" / "signals.csv").write_text(
                "\n".join(
                    [
                        "date,instrument,score",
                        "2026-04-23,AAA,0.7",
                    ]
                ),
                encoding="utf-8",
            )
            (project_root / "data" / "company_events.csv").write_text(
                "\n".join(
                    [
                        "event_id,instrument,event_type,event_date,source,source_url,title,severity,summary,evidence,active_until",
                        "evt-1,AAA,disciplinary_action,2026-04-10,exchange,https://example.test/evt-1,Exchange sanction,block,Formal sanction,notice,",
                    ]
                ),
                encoding="utf-8",
            )
            output = project_root / "runs" / "20260423" / "event_risk_snapshot.csv"

            result = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--signals",
                    "runs/20260423/signals.csv",
                    "--event-risk-config",
                    "configs/event_risk.yaml",
                    "--output",
                    "runs/20260423/event_risk_snapshot.csv",
                    "--project-root",
                    str(project_root),
                ],
                check=False,
                cwd=repo_root,
                text=True,
                capture_output=True,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(output.exists())
            snapshot = pd.read_csv(output)
            self.assertEqual(len(snapshot), 1)
            self.assertTrue(bool(snapshot.loc[0, "event_blocked"]))


if __name__ == "__main__":
    unittest.main()
