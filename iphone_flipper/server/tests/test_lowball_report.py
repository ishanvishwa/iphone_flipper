from __future__ import annotations

import sys
import types
import unittest
from datetime import datetime, timedelta, timezone

sys.modules.setdefault("asyncpg", types.SimpleNamespace(Connection=object, Pool=object))

from server.services.common import lowball_report


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class LowballReportScoringTests(unittest.TestCase):
    def test_classify_price_zone_respects_boundaries(self) -> None:
        self.assertEqual(
            lowball_report.classify_price_zone(
                250,
                400,
                700,
                scam_floor_pct=0.70,
                capture_ceiling_pct=1.35,
            ),
            lowball_report.ZONE_EXCLUDE_SCAM,
        )
        self.assertEqual(
            lowball_report.classify_price_zone(
                399,
                400,
                700,
                scam_floor_pct=0.70,
                capture_ceiling_pct=1.35,
            ),
            lowball_report.ZONE_RED,
        )
        self.assertEqual(
            lowball_report.classify_price_zone(
                700,
                400,
                700,
                scam_floor_pct=0.70,
                capture_ceiling_pct=1.35,
            ),
            lowball_report.ZONE_SWEET,
        )
        self.assertEqual(
            lowball_report.classify_price_zone(
                720,
                300,
                700,
                scam_floor_pct=0.70,
                capture_ceiling_pct=1.35,
            ),
            lowball_report.ZONE_PREMIUM,
        )
        self.assertEqual(
            lowball_report.classify_price_zone(
                950,
                300,
                700,
                scam_floor_pct=0.70,
                capture_ceiling_pct=1.35,
            ),
            lowball_report.ZONE_EXCLUDE_OVERPRICED,
        )

    def test_build_scored_candidates_applies_triggers_and_exclusions(self) -> None:
        now = datetime(2026, 4, 19, 1, 0, tzinfo=timezone.utc)
        base_first_seen = now - timedelta(days=20)
        recent_search_seen = now - timedelta(hours=1)
        old_report_seen = now - timedelta(days=8)
        config = lowball_report.LowballReportConfig()

        rows = [
            {
                "id": "listing-new",
                "title": "iPhone 14 Pro",
                "url": "https://example.com/new",
                "model": "iPhone 14 Pro 128GB",
                "condition": "Good",
                "current_price": 400,
                "max_buy_price": 380,
                "selling_price": 460,
                "availability_status": "active",
                "last_seen_in_search_at": _iso(recent_search_seen),
                "consecutive_check_failures": 0,
                "first_seen_at": _iso(base_first_seen),
                "created_at": _iso(base_first_seen),
                "last_seen_at": _iso(recent_search_seen),
                "last_shown_in_report_at": None,
                "price_when_last_shown": None,
                "last_staleness_bracket_shown": 0,
                "last_zone_shown": None,
                "report_action_taken": None,
            },
            {
                "id": "listing-zone-change",
                "title": "iPhone 15 Pro Max",
                "url": "https://example.com/zone-change",
                "model": "iPhone 15 Pro Max 256GB",
                "condition": "Excellent",
                "current_price": 510,
                "max_buy_price": 500,
                "selling_price": 580,
                "availability_status": "active",
                "last_seen_in_search_at": _iso(recent_search_seen),
                "consecutive_check_failures": 0,
                "first_seen_at": _iso(now - timedelta(days=14)),
                "created_at": _iso(now - timedelta(days=14)),
                "last_seen_at": _iso(recent_search_seen),
                "last_shown_in_report_at": _iso(old_report_seen),
                "price_when_last_shown": 640,
                "last_staleness_bracket_shown": 7,
                "last_zone_shown": lowball_report.ZONE_PREMIUM,
                "report_action_taken": None,
            },
            {
                "id": "listing-big-drop",
                "title": "iPhone 13 Pro",
                "url": "https://example.com/big-drop",
                "model": "iPhone 13 Pro 256GB",
                "condition": "Good",
                "current_price": 390,
                "max_buy_price": 360,
                "selling_price": 480,
                "availability_status": "active",
                "last_seen_in_search_at": _iso(recent_search_seen),
                "consecutive_check_failures": 0,
                "first_seen_at": _iso(now - timedelta(days=18)),
                "created_at": _iso(now - timedelta(days=18)),
                "last_seen_at": _iso(recent_search_seen),
                "last_shown_in_report_at": _iso(old_report_seen),
                "price_when_last_shown": 520,
                "last_staleness_bracket_shown": 14,
                "last_zone_shown": lowball_report.ZONE_SWEET,
                "report_action_taken": "dismissed",
            },
            {
                "id": "listing-contacted-no-drop",
                "title": "iPhone 13",
                "url": "https://example.com/no-drop",
                "model": "iPhone 13 128GB",
                "condition": "Good",
                "current_price": 390,
                "max_buy_price": 350,
                "selling_price": 420,
                "availability_status": "active",
                "last_seen_in_search_at": _iso(recent_search_seen),
                "consecutive_check_failures": 0,
                "first_seen_at": _iso(now - timedelta(days=15)),
                "created_at": _iso(now - timedelta(days=15)),
                "last_seen_at": _iso(recent_search_seen),
                "last_shown_in_report_at": _iso(old_report_seen),
                "price_when_last_shown": 400,
                "last_staleness_bracket_shown": 7,
                "last_zone_shown": lowball_report.ZONE_SWEET,
                "report_action_taken": "contacted",
            },
            {
                "id": "listing-premium",
                "title": "iPhone 15 Pro Premium",
                "url": "https://example.com/premium",
                "model": "iPhone 15 Pro 512GB",
                "condition": "Excellent",
                "current_price": 720,
                "max_buy_price": 300,
                "selling_price": 700,
                "availability_status": "active",
                "last_seen_in_search_at": _iso(recent_search_seen),
                "consecutive_check_failures": 0,
                "first_seen_at": _iso(now - timedelta(days=40)),
                "created_at": _iso(now - timedelta(days=40)),
                "last_seen_at": _iso(recent_search_seen),
                "last_shown_in_report_at": None,
                "price_when_last_shown": None,
                "last_staleness_bracket_shown": 0,
                "last_zone_shown": None,
                "report_action_taken": None,
            },
        ]

        history_by_listing = {
            "listing-new": [
                lowball_report.PriceHistoryPoint(470, now - timedelta(days=20)),
                lowball_report.PriceHistoryPoint(450, now - timedelta(days=12)),
                lowball_report.PriceHistoryPoint(400, now - timedelta(days=2)),
            ],
            "listing-big-drop": [
                lowball_report.PriceHistoryPoint(540, now - timedelta(days=18)),
                lowball_report.PriceHistoryPoint(500, now - timedelta(days=11)),
                lowball_report.PriceHistoryPoint(390, now - timedelta(days=1)),
            ],
        }

        candidates = lowball_report.build_scored_candidates(
            rows,
            history_by_listing,
            now=now,
            config=config,
        )

        ids = [candidate.listing_id for candidate in candidates]
        self.assertIn("listing-new", ids)
        self.assertIn("listing-zone-change", ids)
        self.assertIn("listing-big-drop", ids)
        self.assertIn("listing-premium", ids)
        self.assertNotIn("listing-contacted-no-drop", ids)

        by_id = {candidate.listing_id: candidate for candidate in candidates}
        self.assertEqual(by_id["listing-new"].reason_tag, "NEW")
        self.assertEqual(by_id["listing-zone-change"].reason_tag, "ENTERED SWEET SPOT")
        self.assertEqual(by_id["listing-big-drop"].reason_tag, "BIG DROP")
        self.assertEqual(by_id["listing-premium"].zone, lowball_report.ZONE_PREMIUM)
        self.assertGreaterEqual(by_id["listing-premium"].als, 0)

    def test_build_report_message_marks_unverified_entries(self) -> None:
        entry = lowball_report.LowballCandidate(
            listing_id="listing-1",
            title="Example Listing",
            url="https://example.com/listing-1",
            model="iPhone 14 Pro 128GB",
            condition="Good",
            current_price=430,
            target_price=380,
            resale_price=460,
            zone=lowball_report.ZONE_SWEET,
            reason_tag="NEW",
            days_since_first_seen=10,
            days_since_last_price_change=2,
            last_drop_days=2,
            price_drop_count=1,
            seller_discount=0.12,
            reachability=0.034,
            required_discount=0.116,
            acceptable_discount=0.15,
            max_acceptable_price=440,
            walkaway_price=420,
            staleness_raw=10.0,
            trajectory_raw=15.0,
            als=82.0,
            verification_status=lowball_report.VERIFICATION_UNVERIFIED,
        )

        message = lowball_report.build_report_message(report_date=datetime(2026, 4, 19).date(), entries=[entry])

        self.assertIn("Lowball Report - 2026-04-19", message)
        self.assertIn("\u26a0 unverified - check before messaging", message)
        self.assertIn("https://example.com/listing-1", message)


if __name__ == "__main__":
    unittest.main()
