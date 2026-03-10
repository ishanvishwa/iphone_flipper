from __future__ import annotations

import unittest

from server.services.common.notification_dead_letter import (
    NOTIFICATION_DEAD_LETTER_SCHEMA_VERSION,
    NotificationDeadLetterEvent,
    build_notification_dead_letter_event,
)
from server.services.common.stream_events import LISTING_STREAM_SCHEMA_VERSION, ListingStreamEvent


class NotificationDeadLetterEventTests(unittest.TestCase):
    def test_round_trip_and_rebuild_listing_stream_event(self) -> None:
        listing_event = ListingStreamEvent(
            schema_version=LISTING_STREAM_SCHEMA_VERSION,
            event_name="listing_created",
            listing_id="listing-1",
            worker_name="worker",
            route_name="route-a",
            query="iPhone 15 Pro",
            query_index=1,
            query_total=3,
            query_shard_key="QUERY_SHARD:test",
            persisted_at="2026-03-10T00:00:00+00:00",
            price=1200.0,
            potential_profit=150.0,
            title="iPhone 15 Pro",
            url="https://example.com/listing-1",
            thumbnail_url="https://example.com/thumb.jpg",
            source="marketplace",
        )

        dead_letter = build_notification_dead_letter_event(
            stream_event_id="1741604400000-0",
            event=listing_event,
            failure_status="failed_terminal",
            last_error="telegram failed",
            attempt_count=3,
            dead_lettered_at="2026-03-10T00:05:00+00:00",
        )

        round_trip = NotificationDeadLetterEvent.from_redis_fields(dead_letter.to_redis_fields())
        rebuilt = round_trip.to_listing_stream_event()

        self.assertEqual(round_trip.schema_version, NOTIFICATION_DEAD_LETTER_SCHEMA_VERSION)
        self.assertEqual(round_trip.original_stream_event_id, "1741604400000-0")
        self.assertEqual(round_trip.attempt_count, 3)
        self.assertEqual(rebuilt.listing_id, listing_event.listing_id)
        self.assertEqual(rebuilt.title, listing_event.title)
        self.assertEqual(rebuilt.potential_profit, listing_event.potential_profit)


if __name__ == "__main__":
    unittest.main()
