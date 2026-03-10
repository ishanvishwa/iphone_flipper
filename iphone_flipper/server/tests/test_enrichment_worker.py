from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from redis.exceptions import ResponseError

from server.services.common.enrichment_events import (
    LISTING_ENRICHMENT_STREAM_SCHEMA_VERSION,
    ListingEnrichmentEvent,
)
from server.services.worker import enrichment_worker


class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeLedgerConnection:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetchval(self, query: str, listing_id: str) -> None:
        return None

    async def fetchrow(self, query: str, listing_id: str):
        row = self.rows.get(str(listing_id))
        if row is None:
            return None
        return dict(row)

    async def execute(self, query: str, *args) -> str:
        normalized = " ".join(str(query).split())
        listing_id = str(args[0])
        row = dict(self.rows.get(listing_id, {}))

        if normalized.startswith("INSERT INTO listing_enrichment_ledger"):
            row = {
                "listing_id": listing_id,
                "first_stream_event_id": str(args[1]),
                "last_stream_event_id": str(args[1]),
                "enrichment_source_hash": str(args[2]),
                "status": "pending",
                "attempt_count": 0,
                "last_error": None,
                "last_attempt_at": None,
                "enriched_at": None,
            }
        elif "SET last_stream_event_id = $2, updated_at = NOW()" in normalized:
            row["last_stream_event_id"] = str(args[1])
        elif "status = 'pending'" in normalized:
            row["last_stream_event_id"] = str(args[1])
            row["enrichment_source_hash"] = str(args[2])
            row["status"] = "pending"
            row["last_error"] = None
        elif "status = 'processing'" in normalized:
            row["last_stream_event_id"] = str(args[1])
            row["enrichment_source_hash"] = str(args[2])
            row["status"] = "processing"
            row["attempt_count"] = int(row.get("attempt_count", 0)) + 1
            row["last_error"] = None
            row["last_attempt_at"] = "attempted"
        elif "status = 'complete'" in normalized:
            row["last_stream_event_id"] = str(args[1])
            row["enrichment_source_hash"] = str(args[2])
            row["status"] = "complete"
            row["last_error"] = None
            row["enriched_at"] = "enriched"
        elif "status = 'failed'" in normalized:
            row["last_stream_event_id"] = str(args[1])
            row["enrichment_source_hash"] = str(args[2])
            row["status"] = "failed"
            row["last_error"] = args[3]

        self.rows[listing_id] = row
        return "OK"


class _FakeLedgerAcquire:
    def __init__(self, conn: _FakeLedgerConnection) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeLedgerConnection:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeLedgerPool:
    def __init__(self) -> None:
        self._conn = _FakeLedgerConnection()

    def acquire(self) -> _FakeLedgerAcquire:
        return _FakeLedgerAcquire(self._conn)


class _FakeRedis:
    def __init__(self) -> None:
        self.acked: list[tuple[str, str, str]] = []
        self.published: list[tuple[str, str]] = []
        self.group_create_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.xautoclaim_result: object = ("0-0", [], [])
        self.xreadgroup_result: object = []
        self.group_create_error: Exception | None = None

    async def xgroup_create(self, *args, **kwargs):
        self.group_create_calls.append((args, kwargs))
        if self.group_create_error is not None:
            raise self.group_create_error
        return True

    async def xack(self, stream_name: str, group_name: str, stream_event_id: str) -> int:
        self.acked.append((stream_name, group_name, stream_event_id))
        return 1

    async def publish(self, channel: str, payload: str) -> int:
        self.published.append((channel, payload))
        return 1

    async def xautoclaim(self, *args, **kwargs):
        return self.xautoclaim_result

    async def xreadgroup(self, *args, **kwargs):
        return self.xreadgroup_result


def _build_event(
    *,
    listing_id: str = "listing-1",
    enrichment_source_hash: str = "hash-1",
) -> tuple[str, dict[str, str]]:
    event = ListingEnrichmentEvent(
        schema_version=LISTING_ENRICHMENT_STREAM_SCHEMA_VERSION,
        listing_id=listing_id,
        worker_name="worker",
        route_name="profile_1",
        query="iphone 15 pro",
        query_index=1,
        query_total=1,
        query_shard_key="QUERY_SHARD:test",
        persisted_at="2026-03-10T10:00:00+00:00",
        enqueued_at="2026-03-10T10:00:01+00:00",
        enrichment_source_hash=enrichment_source_hash,
        description="Battery 89%",
        seller_name="Seller A",
        thumbnail_url="https://example.com/thumb.jpg",
    )
    return "1741696800000-0", event.to_redis_fields()


class ListingEnrichmentLedgerStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_marks_duplicate_complete_when_hash_already_processed(self) -> None:
        pool = _FakeLedgerPool()
        store = enrichment_worker.ListingEnrichmentLedgerStore(pool)

        self.assertEqual(await store.prepare("listing-1", "event-1", "hash-1"), "enrich")
        await store.mark_attempt_started("listing-1", "event-1", "hash-1")
        await store.mark_complete("listing-1", "event-1", "hash-1")

        action = await store.prepare("listing-1", "event-2", "hash-1")

        self.assertEqual(action, "duplicate_complete")
        entry = await store.get("listing-1")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.status, "complete")
        self.assertEqual(entry.last_stream_event_id, "event-2")

    async def test_prepare_allows_new_hash_after_failure(self) -> None:
        pool = _FakeLedgerPool()
        store = enrichment_worker.ListingEnrichmentLedgerStore(pool)

        self.assertEqual(await store.prepare("listing-2", "event-1", "hash-1"), "enrich")
        await store.mark_failed("listing-2", "event-1", "hash-1", "boom")

        action = await store.prepare("listing-2", "event-2", "hash-2")

        self.assertEqual(action, "enrich")
        entry = await store.get("listing-2")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.enrichment_source_hash, "hash-2")
        self.assertEqual(entry.status, "pending")


class EnrichmentConsumerTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_consumer_group_ignores_busygroup(self) -> None:
        redis_client = _FakeRedis()
        redis_client.group_create_error = ResponseError("BUSYGROUP Consumer Group name already exists")
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        consumer = enrichment_worker.EnrichmentConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            pool=MagicMock(),
            ledger=MagicMock(),
        )

        await consumer.ensure_consumer_group()

        self.assertEqual(len(redis_client.group_create_calls), 1)

    async def test_process_stream_entry_updates_listing_and_acks(self) -> None:
        redis_client = _FakeRedis()
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="enrich")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_complete = AsyncMock()
        ledger.mark_failed = AsyncMock()
        stream_event_id, fields = _build_event()
        consumer = enrichment_worker.EnrichmentConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            pool=MagicMock(),
            ledger=ledger,
        )

        with (
            patch.object(consumer, "_apply_enrichment", AsyncMock(return_value=True)),
            patch.object(consumer, "_publish_listing_updated", AsyncMock()) as publish_mock,
            patch.object(enrichment_worker, "emit_json_log"),
        ):
            result = await consumer.process_stream_entry(stream_event_id, fields)

        self.assertEqual(result, "complete")
        ledger.mark_attempt_started.assert_awaited_once()
        ledger.mark_complete.assert_awaited_once()
        ledger.mark_failed.assert_not_awaited()
        publish_mock.assert_awaited_once()
        self.assertEqual(
            redis_client.acked,
            [(enrichment_worker.LISTING_ENRICHMENT_STREAM_NAME, consumer.consumer_group, stream_event_id)],
        )

    async def test_process_stream_entry_suppresses_duplicate_hash_and_acks(self) -> None:
        redis_client = _FakeRedis()
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="duplicate_complete")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_complete = AsyncMock()
        ledger.mark_failed = AsyncMock()
        stream_event_id, fields = _build_event(listing_id="listing-dup")
        consumer = enrichment_worker.EnrichmentConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            pool=MagicMock(),
            ledger=ledger,
        )

        with patch.object(enrichment_worker, "emit_json_log"):
            result = await consumer.process_stream_entry(stream_event_id, fields)

        self.assertEqual(result, "duplicate_complete")
        ledger.mark_attempt_started.assert_not_awaited()
        self.assertEqual(
            redis_client.acked,
            [(enrichment_worker.LISTING_ENRICHMENT_STREAM_NAME, consumer.consumer_group, stream_event_id)],
        )

    async def test_process_stream_entry_marks_failed_after_bounded_retries(self) -> None:
        redis_client = _FakeRedis()
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="enrich")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_complete = AsyncMock()
        ledger.mark_failed = AsyncMock()
        stream_event_id, fields = _build_event(listing_id="listing-failed")
        pool = MagicMock()
        acquire_cm = MagicMock()
        acquire_cm.__aenter__ = AsyncMock(return_value=MagicMock(execute=AsyncMock(return_value="OK")))
        acquire_cm.__aexit__ = AsyncMock(return_value=None)
        pool.acquire.return_value = acquire_cm
        consumer = enrichment_worker.EnrichmentConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            pool=pool,
            ledger=ledger,
            retry_attempts=2,
            retry_base_delay_seconds=1.0,
        )

        with (
            patch.object(consumer, "_apply_enrichment", AsyncMock(side_effect=RuntimeError("boom"))),
            patch.object(enrichment_worker.asyncio, "sleep", AsyncMock()) as sleep_mock,
            patch.object(enrichment_worker, "emit_json_log"),
        ):
            result = await consumer.process_stream_entry(stream_event_id, fields)

        self.assertEqual(result, "failed")
        self.assertEqual(ledger.mark_attempt_started.await_count, 2)
        ledger.mark_complete.assert_not_awaited()
        ledger.mark_failed.assert_awaited_once()
        sleep_mock.assert_awaited_once_with(1.0)
        self.assertEqual(
            redis_client.acked,
            [(enrichment_worker.LISTING_ENRICHMENT_STREAM_NAME, consumer.consumer_group, stream_event_id)],
        )

    async def test_run_once_reclaims_pending_entries_with_xautoclaim(self) -> None:
        redis_client = _FakeRedis()
        stream_event_id, fields = _build_event(listing_id="listing-pending")
        redis_client.xautoclaim_result = ("0-0", [(stream_event_id, fields)], [])
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="enrich")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_complete = AsyncMock()
        ledger.mark_failed = AsyncMock()
        consumer = enrichment_worker.EnrichmentConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            pool=MagicMock(),
            ledger=ledger,
        )

        with (
            patch.object(consumer, "_apply_enrichment", AsyncMock(return_value=True)),
            patch.object(consumer, "_publish_listing_updated", AsyncMock()),
            patch.object(enrichment_worker, "emit_json_log"),
        ):
            processed = await consumer.run_once()

        self.assertEqual(processed, 1)
        self.assertEqual(
            redis_client.acked,
            [(enrichment_worker.LISTING_ENRICHMENT_STREAM_NAME, consumer.consumer_group, stream_event_id)],
        )


if __name__ == "__main__":
    unittest.main()
