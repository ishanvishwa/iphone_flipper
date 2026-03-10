from __future__ import annotations

import unittest

from server.services.common.enrichment_events import (
    LISTING_ENRICHMENT_STREAM_SCHEMA_VERSION,
    ListingEnrichmentEvent,
    build_enrichment_source_hash,
    build_listing_enrichment_event,
    normalize_thumbnail_url,
    split_listing_for_fast_path,
)


class ListingEnrichmentEventTests(unittest.TestCase):
    def test_round_trip_serialization_preserves_enrichment_fields(self) -> None:
        event = ListingEnrichmentEvent(
            schema_version=LISTING_ENRICHMENT_STREAM_SCHEMA_VERSION,
            listing_id="listing-1",
            worker_name="worker",
            route_name="env_default",
            query="iphone 15 pro",
            query_index=1,
            query_total=3,
            query_shard_key="QUERY_SHARD:test",
            persisted_at="2026-03-10T10:00:00+00:00",
            enqueued_at="2026-03-10T10:00:01+00:00",
            enrichment_source_hash="hash-1",
            description="Clean condition with box",
            seller_name="Seller A",
            thumbnail_url="https://example.com/thumb.jpg",
        )

        decoded = ListingEnrichmentEvent.from_redis_fields(event.to_redis_fields())

        self.assertEqual(decoded.schema_version, LISTING_ENRICHMENT_STREAM_SCHEMA_VERSION)
        self.assertEqual(decoded.listing_id, "listing-1")
        self.assertEqual(decoded.query_index, 1)
        self.assertEqual(decoded.thumbnail_url, "https://example.com/thumb.jpg")

    def test_split_listing_for_fast_path_keeps_hot_fields_and_hashes_cold_fields(self) -> None:
        hot_listing, cold_fields, source_hash = split_listing_for_fast_path(
            {
                "id": "listing-2",
                "title": "iPhone 15 Pro",
                "price": 1000,
                "url": "https://example.com/listing-2",
                "description": "Battery 89%",
                "seller_name": "Seller B",
                "thumbnail": {"uri": "https://example.com/thumb-2.jpg"},
                "model": "iPhone 15 Pro",
                "condition": "used",
                "max_offer": 800,
                "potential_profit": 150,
                "status": "new",
            }
        )

        self.assertEqual(hot_listing["thumbnail_url"], "https://example.com/thumb-2.jpg")
        self.assertEqual(hot_listing["max_offer"], 800)
        self.assertEqual(cold_fields["description"], "Battery 89%")
        self.assertEqual(cold_fields["seller_name"], "Seller B")
        self.assertEqual(source_hash, build_enrichment_source_hash(cold_fields))

    def test_normalize_thumbnail_url_accepts_nested_photo_shapes(self) -> None:
        thumbnail_url = normalize_thumbnail_url(
            {
                "listing_photos": [
                    {"thumbnail_image": {"uri": "https://example.com/photo-thumb.jpg"}},
                ]
            }
        )

        self.assertEqual(thumbnail_url, "https://example.com/photo-thumb.jpg")

    def test_build_listing_enrichment_event_uses_metadata_fields(self) -> None:
        event = build_listing_enrichment_event(
            listing_id="listing-3",
            metadata={
                "route_name": "profile_3",
                "query": "iphone 14",
                "query_index": 2,
                "query_total": 5,
                "query_shard_key": "QUERY_SHARD:def",
            },
            worker_name="worker_3",
            persisted_at="2026-03-10T10:01:00+00:00",
            enqueued_at="2026-03-10T10:01:01+00:00",
            cold_fields={
                "description": "Face ID issue",
                "seller_name": "Seller C",
                "thumbnail_url": "https://example.com/thumb-3.jpg",
            },
            enrichment_source_hash="hash-3",
        )

        self.assertEqual(event.route_name, "profile_3")
        self.assertEqual(event.query_total, 5)
        self.assertEqual(event.enrichment_source_hash, "hash-3")
        self.assertEqual(event.thumbnail_url, "https://example.com/thumb-3.jpg")


if __name__ == "__main__":
    unittest.main()
