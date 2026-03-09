from __future__ import annotations

import unittest
from decimal import Decimal

from server.services.common.stream_events import (
    LISTING_STREAM_SCHEMA_VERSION,
    ListingStreamEvent,
    build_listing_stream_event,
    extract_listing_stream_state,
    extract_row_stream_state,
    has_meaningful_stream_state_change,
)


class ListingStreamEventTests(unittest.TestCase):
    def test_round_trip_serialization_preserves_schema(self) -> None:
        event = ListingStreamEvent(
            schema_version=LISTING_STREAM_SCHEMA_VERSION,
            event_name="listing_created",
            listing_id="listing-1",
            worker_name="worker",
            route_name="env_default",
            query="iPhone 15 Pro",
            query_index=2,
            query_total=10,
            query_shard_key="QUERY_SHARD:abc",
            persisted_at="2026-03-09T08:00:00+00:00",
            price=799.0,
            potential_profit=220.0,
            title="iPhone 15 Pro 256GB",
            url="https://example.com/listing-1",
            source="marketplace",
        )

        encoded = event.to_redis_fields()
        decoded = ListingStreamEvent.from_redis_fields(encoded)

        self.assertEqual(decoded.schema_version, LISTING_STREAM_SCHEMA_VERSION)
        self.assertEqual(decoded.listing_id, "listing-1")
        self.assertEqual(decoded.query_index, 2)
        self.assertEqual(decoded.query_total, 10)
        self.assertEqual(decoded.price, 799.0)
        self.assertEqual(decoded.potential_profit, 220.0)

    def test_build_listing_stream_event_uses_metadata_fields(self) -> None:
        listing = {
            "id": "listing-2",
            "title": "iPhone 14 Pro",
            "price": 650,
            "potential_profit": 90,
            "url": "https://example.com/listing-2",
        }
        metadata = {
            "route_name": "profile_1",
            "query": "iPhone 14 Pro",
            "query_index": 1,
            "query_total": 9,
            "query_shard_key": "QUERY_SHARD:def",
            "source": "marketplace",
        }

        event = build_listing_stream_event(
            event_name="listing_created",
            listing=listing,
            metadata=metadata,
            worker_name="worker_2",
            persisted_at="2026-03-09T08:01:00+00:00",
        )

        self.assertEqual(event.worker_name, "worker_2")
        self.assertEqual(event.route_name, "profile_1")
        self.assertEqual(event.query_shard_key, "QUERY_SHARD:def")
        self.assertEqual(event.source, "marketplace")


class ListingStreamStateTests(unittest.TestCase):
    def test_extract_listing_stream_state_normalizes_hot_path_fields(self) -> None:
        state = extract_listing_stream_state(
            {
                "title": "iPhone 13",
                "price": Decimal("525.00"),
                "url": "https://example.com/3",
                "model": "iPhone 13",
                "condition": "Used",
                "status": "",
                "max_offer": Decimal("430.00"),
                "potential_profit": Decimal("95.00"),
                "location": "Perth",
            }
        )

        self.assertEqual(state["price"], "525.00")
        self.assertEqual(state["status"], "new")
        self.assertEqual(state["max_buy_price"], "430.00")

    def test_meaningful_change_detects_only_hot_path_differences(self) -> None:
        previous_state = extract_row_stream_state(
            {
                "title": "iPhone 15 Pro",
                "price": Decimal("850"),
                "url": "https://example.com/4",
                "model": "iPhone 15 Pro",
                "condition": "Used",
                "status": "new",
                "max_buy_price": Decimal("700"),
                "potential_profit": Decimal("150"),
                "location": "Perth",
            }
        )
        same_state = extract_listing_stream_state(
            {
                "title": "iPhone 15 Pro",
                "price": 850,
                "url": "https://example.com/4",
                "model": "iPhone 15 Pro",
                "condition": "Used",
                "status": "new",
                "max_offer": 700,
                "potential_profit": 150,
                "location": "Perth",
            }
        )
        changed_state = dict(same_state)
        changed_state["price"] = "875"

        self.assertFalse(has_meaningful_stream_state_change(previous_state, same_state))
        self.assertTrue(has_meaningful_stream_state_change(previous_state, changed_state))


if __name__ == "__main__":
    unittest.main()
