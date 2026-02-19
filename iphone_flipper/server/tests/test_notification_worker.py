"""Tests for notification_worker.py — dedup, rate limiting, and priority tiers."""
from __future__ import annotations

import asyncio
import json
import unittest

from server.services.worker.notification_worker import (
    Deduplicator,
    ListingEvent,
    NotificationDispatcher,
    RateLimiter,
    _build_telegram_card,
    _parse_listing_event,
)


class ParseListingEventTests(unittest.TestCase):
    def test_valid_event(self) -> None:
        raw = json.dumps({
            "event": "listing_created",
            "listing_id": "abc123",
            "model": "iPhone 15 Pro Max",
            "price": 800,
            "profit": 120,
            "url": "https://example.com/listing/abc123",
            "condition": "Like New",
        })
        event = _parse_listing_event(raw)
        self.assertIsNotNone(event)
        self.assertEqual(event.event_type, "listing_created")
        self.assertEqual(event.model, "iPhone 15 Pro Max")
        self.assertEqual(event.price, 800)
        self.assertEqual(event.profit, 120)

    def test_invalid_json(self) -> None:
        self.assertIsNone(_parse_listing_event("not-json"))

    def test_missing_event_type(self) -> None:
        raw = json.dumps({"listing_id": "x"})
        self.assertIsNone(_parse_listing_event(raw))


class DeduplicatorTests(unittest.TestCase):
    def test_first_occurrence_is_not_duplicate(self) -> None:
        dedup = Deduplicator(window_seconds=60)
        self.assertFalse(dedup.is_duplicate("listing_1"))

    def test_same_id_is_duplicate(self) -> None:
        dedup = Deduplicator(window_seconds=60)
        dedup.is_duplicate("listing_1")
        self.assertTrue(dedup.is_duplicate("listing_1"))

    def test_different_ids_are_not_duplicates(self) -> None:
        dedup = Deduplicator(window_seconds=60)
        dedup.is_duplicate("listing_1")
        self.assertFalse(dedup.is_duplicate("listing_2"))


class RateLimiterTests(unittest.TestCase):
    def test_allows_within_limit(self) -> None:
        limiter = RateLimiter(max_per_minute=5)
        for _ in range(5):
            self.assertTrue(limiter.allow())

    def test_blocks_over_limit(self) -> None:
        limiter = RateLimiter(max_per_minute=3)
        for _ in range(3):
            limiter.allow()
        self.assertFalse(limiter.allow())

    def test_remaining_count(self) -> None:
        limiter = RateLimiter(max_per_minute=5)
        limiter.allow()
        limiter.allow()
        self.assertEqual(limiter.remaining, 3)


class TelegramCardTests(unittest.TestCase):
    def test_card_contains_profit(self) -> None:
        event = ListingEvent(
            event_type="listing_created",
            listing_id="abc",
            model="iPhone 14",
            price=500,
            profit=75,
            url="https://example.com/abc",
        )
        card = _build_telegram_card(event)
        self.assertIn("75", card)
        self.assertIn("500", card)
        self.assertIn("🟢", card)  # profit >= 50

    def test_card_yellow_for_moderate_profit(self) -> None:
        event = ListingEvent(
            event_type="listing_created",
            listing_id="xyz",
            model="iPhone 13",
            price=400,
            profit=25,
            url="https://example.com/xyz",
        )
        card = _build_telegram_card(event)
        self.assertIn("🟡", card)  # profit >= 0 but < 50


class DispatcherTests(unittest.TestCase):
    def test_instant_tier_for_high_profit(self) -> None:
        dispatcher = NotificationDispatcher(
            instant_threshold=50.0,
            batch_interval=10.0,
            max_per_minute=30,
            dedup_window=60,
        )
        event = ListingEvent(
            event_type="listing_created",
            listing_id="high_profit_1",
            model="iPhone 15 Pro",
            price=700,
            profit=100,
            url="https://example.com/1",
        )
        # Run the process — Telegram won't actually send without config
        asyncio.run(dispatcher.process_event(event))
        self.assertEqual(dispatcher.stats.events_received, 1)

    def test_batch_tier_for_moderate_profit(self) -> None:
        dispatcher = NotificationDispatcher(
            instant_threshold=50.0,
            batch_interval=10.0,
            max_per_minute=30,
            dedup_window=60,
        )
        event = ListingEvent(
            event_type="listing_created",
            listing_id="moderate_1",
            model="iPhone 13",
            price=400,
            profit=25,
            url="https://example.com/2",
        )
        asyncio.run(dispatcher.process_event(event))
        self.assertEqual(dispatcher.stats.batched, 1)

    def test_suppressed_for_negative_profit(self) -> None:
        dispatcher = NotificationDispatcher(
            instant_threshold=50.0,
            batch_interval=10.0,
            max_per_minute=30,
            dedup_window=60,
        )
        event = ListingEvent(
            event_type="listing_created",
            listing_id="negative_1",
            model="iPhone SE",
            price=200,
            profit=-30,
            url="https://example.com/3",
        )
        asyncio.run(dispatcher.process_event(event))
        self.assertEqual(dispatcher.stats.suppressed_negative, 1)

    def test_deduplication(self) -> None:
        dispatcher = NotificationDispatcher(
            instant_threshold=50.0,
            batch_interval=10.0,
            max_per_minute=30,
            dedup_window=60,
        )
        event = ListingEvent(
            event_type="listing_created",
            listing_id="dup_test_1",
            model="iPhone 14",
            price=500,
            profit=60,
            url="https://example.com/4",
        )
        async def _run() -> None:
            await dispatcher.process_event(event)
            await dispatcher.process_event(event)

        asyncio.run(_run())
        self.assertEqual(dispatcher.stats.deduplicated, 1)

    def test_non_created_events_are_ignored(self) -> None:
        dispatcher = NotificationDispatcher(
            instant_threshold=50.0,
            batch_interval=10.0,
            max_per_minute=30,
            dedup_window=60,
        )
        event = ListingEvent(
            event_type="listing_updated",
            listing_id="update_1",
            model="iPhone 14",
            price=500,
            profit=60,
            url="https://example.com/5",
        )
        asyncio.run(dispatcher.process_event(event))
        # Should be received but not sent or batched
        self.assertEqual(dispatcher.stats.events_received, 1)
        self.assertEqual(dispatcher.stats.batched, 0)
        self.assertEqual(dispatcher.stats.notifications_sent, 0)


if __name__ == "__main__":
    unittest.main()
