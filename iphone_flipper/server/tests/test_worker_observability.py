from __future__ import annotations

import os
import unittest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("IPHONE_FLIPPER_DB_PATH", "/tmp/iphone_flipper_worker_test.db")

try:
    from server.services.worker import worker
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    worker = None


class _FakeTransaction:
    async def __aenter__(self) -> "_FakeTransaction":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakePoolAcquire:
    def __init__(self, conn: "_FakeConnection") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_FakeConnection":
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeConnection:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, object]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._held_lock: asyncio.Lock | None = None

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetchval(self, query: str, listing_id: str) -> None:
        if "pg_advisory_xact_lock" in query:
            lock = self._locks.setdefault(str(listing_id), asyncio.Lock())
            await lock.acquire()
            self._held_lock = lock
        return None

    async def fetchrow(self, query: str, listing_id: str):
        row = self.rows.get(str(listing_id))
        if row is None:
            return None
        return dict(row)

    async def execute(self, query: str, *args) -> str:
        listing_id = str(args[0])
        if len(args) >= 13:
            model_index = 8
            condition_index = 9
            max_buy_price_index = 10
            potential_profit_index = 11
            status_index = 12
        else:
            model_index = 7
            condition_index = 8
            max_buy_price_index = 9
            potential_profit_index = 10
            status_index = 11
        self.rows[listing_id] = {
            "title": args[1],
            "price": args[2],
            "location": args[3],
            "url": args[4],
            "model": args[model_index],
            "condition": args[condition_index],
            "max_buy_price": args[max_buy_price_index],
            "potential_profit": args[potential_profit_index],
            "status": args[status_index],
        }
        await asyncio.sleep(0)
        return "OK"


class _FakePool:
    def __init__(self) -> None:
        self._conn = _FakeConnection()

    def acquire(self) -> _FakePoolAcquire:
        conn = self._conn

        class _Acquire(_FakePoolAcquire):
            async def __aexit__(self, exc_type, exc, tb) -> None:
                try:
                    await super().__aexit__(exc_type, exc, tb)
                finally:
                    if conn._held_lock is not None and conn._held_lock.locked():
                        conn._held_lock.release()
                    conn._held_lock = None

        return _Acquire(conn)


def _base_cycle_metrics() -> dict[str, int]:
    return {
        "postgres_upsert_latency_ms_sum": 0,
        "postgres_upsert_latency_ms_count": 0,
        "redis_publish_latency_ms_sum": 0,
        "redis_publish_latency_ms_count": 0,
        "redis_stream_publish_latency_ms_sum": 0,
        "redis_stream_publish_latency_ms_count": 0,
        "redis_stream_publish_failure_count": 0,
        "v4_first_seen_gate_latency_ms_sum": 0,
        "v4_first_seen_gate_latency_ms_count": 0,
        "v4_first_seen_claim_count": 0,
        "v4_first_seen_duplicate_count": 0,
        "v4_update_gate_latency_ms_sum": 0,
        "v4_update_gate_latency_ms_count": 0,
        "v4_update_event_claim_count": 0,
        "v4_update_event_duplicate_count": 0,
        "v4_dedupe_fallback_count": 0,
        "notification_delivery_latency_ms_sum": 0,
        "notification_delivery_latency_ms_count": 0,
        "end_to_end_alert_latency_ms_sum": 0,
        "end_to_end_alert_latency_ms_count": 0,
        "duplicate_listing_count": 0,
    }


@unittest.skipIf(worker is None, "Worker dependencies are not installed.")
class WorkerObservabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_publish_listing_stream_event_uses_capped_xadd(self) -> None:
        redis_client = AsyncMock()
        redis_client.xadd = AsyncMock(return_value="1741514400000-1")

        with (
            patch.object(worker, "monotonic_duration_ms", return_value=5),
            patch.object(worker, "utc_now_iso", return_value="2026-03-08T10:00:00.400000+00:00"),
        ):
            stream_id, published_ts, latency_ms = await worker._publish_listing_stream_event(
                redis_client,
                {"listing_id": "listing-1", "event_name": "listing_created"},
            )

        redis_client.xadd.assert_awaited_once_with(
            worker.LISTING_STREAM_NAME,
            {"listing_id": "listing-1", "event_name": "listing_created"},
            maxlen=worker.LISTING_STREAM_MAXLEN,
            approximate=True,
        )
        self.assertEqual(stream_id, "1741514400000-1")
        self.assertEqual(published_ts, "2026-03-08T10:00:00.400000+00:00")
        self.assertEqual(latency_ms, 5)

    async def test_process_listing_event_publishes_stream_when_flag_enabled(self) -> None:
        cycle_metrics = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
            "redis_stream_publish_latency_ms_sum": 0,
            "redis_stream_publish_latency_ms_count": 0,
            "redis_stream_publish_failure_count": 0,
            "notification_delivery_latency_ms_sum": 0,
            "notification_delivery_latency_ms_count": 0,
            "end_to_end_alert_latency_ms_sum": 0,
            "end_to_end_alert_latency_ms_count": 0,
        }
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name == "ENABLE_REDIS_STREAM_EVENTS"

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)
        metadata = {
            "route_name": "profile_stream",
            "query": "iPhone 15 Pro",
            "query_index": 1,
            "query_total": 10,
            "query_shard_key": "QUERY_SHARD:stream",
            "listing_seen_ts": "2026-03-08T10:00:00+00:00",
        }
        listing = {"id": "listing-stream-1", "model": "iPhone 15 Pro", "potential_profit": 75}
        loop = MagicMock()
        loop.run_in_executor = AsyncMock(return_value=True)

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(return_value=worker.ListingUpsertResult(created=True, stream_state_changed=True)),
            ),
            patch.object(worker, "_publish_listing_stream_event", AsyncMock(return_value=("1741514400000-0", "2026-03-08T10:00:00.250000+00:00", 4))) as stream_mock,
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-08T10:00:00.300000+00:00", 7))) as publish_mock,
            patch.object(worker, "_should_notify_telegram", return_value=True),
            patch.object(worker.asyncio, "get_running_loop", return_value=loop),
            patch.object(worker, "monotonic_duration_ms", side_effect=[13, 17]),
            patch.object(worker, "utc_now_iso", side_effect=["2026-03-08T10:00:00.100000+00:00", "2026-03-08T10:00:02+00:00"]),
            patch.object(worker, "timestamp_delta_ms", return_value=2000),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), object(), feature_flags, listing, metadata, cycle_metrics)

        stream_mock.assert_awaited_once()
        publish_mock.assert_awaited_once()
        self.assertEqual(cycle_metrics["redis_stream_publish_latency_ms_sum"], 4)
        self.assertEqual(cycle_metrics["redis_stream_publish_latency_ms_count"], 1)
        _, pipeline_kwargs = emit_mock.call_args_list[0]
        self.assertEqual(pipeline_kwargs["event_id"], "1741514400000-0")
        self.assertEqual(pipeline_kwargs["stream_publish_status"], "published")

    async def test_process_listing_event_publishes_stream_for_meaningful_update(self) -> None:
        cycle_metrics = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
            "redis_stream_publish_latency_ms_sum": 0,
            "redis_stream_publish_latency_ms_count": 0,
            "redis_stream_publish_failure_count": 0,
            "notification_delivery_latency_ms_sum": 0,
            "notification_delivery_latency_ms_count": 0,
            "end_to_end_alert_latency_ms_sum": 0,
            "end_to_end_alert_latency_ms_count": 0,
        }
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name == "ENABLE_REDIS_STREAM_EVENTS"

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)
        metadata = {
            "route_name": "profile_update",
            "listing_seen_ts": "2026-03-08T10:02:00+00:00",
        }
        listing = {"id": "listing-stream-2", "model": "iPhone 14 Pro", "potential_profit": 35}

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(return_value=worker.ListingUpsertResult(created=False, stream_state_changed=True)),
            ),
            patch.object(worker, "_publish_listing_stream_event", AsyncMock(return_value=("1741514520000-0", "2026-03-08T10:02:00.250000+00:00", 3))) as stream_mock,
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-08T10:02:00.300000+00:00", 6))),
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=12),
            patch.object(worker, "utc_now_iso", return_value="2026-03-08T10:02:00.100000+00:00"),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), object(), feature_flags, listing, metadata, cycle_metrics)

        stream_mock.assert_awaited_once()
        _, pipeline_kwargs = emit_mock.call_args
        self.assertEqual(pipeline_kwargs["event_name"], "listing_updated")
        self.assertEqual(pipeline_kwargs["stream_event_id"], "1741514520000-0")

    async def test_process_listing_event_records_publish_and_notification_latency(self) -> None:
        cycle_metrics = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
            "redis_stream_publish_latency_ms_sum": 0,
            "redis_stream_publish_latency_ms_count": 0,
            "redis_stream_publish_failure_count": 0,
            "notification_delivery_latency_ms_sum": 0,
            "notification_delivery_latency_ms_count": 0,
            "end_to_end_alert_latency_ms_sum": 0,
            "end_to_end_alert_latency_ms_count": 0,
        }
        metadata = {
            "route_name": "profile_1",
            "listing_seen_ts": "2026-03-08T10:00:00+00:00",
        }
        listing = {"id": "listing-1", "model": "iPhone 15 Pro", "potential_profit": 50}
        loop = MagicMock()
        loop.run_in_executor = AsyncMock(return_value=True)
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=False)

        with (
            patch.object(worker, "_upsert_listing", AsyncMock(return_value=True)),
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-08T10:00:01+00:00", 7))),
            patch.object(worker, "_should_notify_telegram", return_value=True),
            patch.object(worker.asyncio, "get_running_loop", return_value=loop),
            patch.object(worker, "monotonic_duration_ms", side_effect=[13, 17]),
            patch.object(worker, "utc_now_iso", side_effect=["2026-03-08T10:00:00.500000+00:00", "2026-03-08T10:00:02+00:00"]),
            patch.object(worker, "timestamp_delta_ms", return_value=2000),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), object(), feature_flags, listing, metadata, cycle_metrics)

        self.assertEqual(cycle_metrics["postgres_upsert_latency_ms_sum"], 13)
        self.assertEqual(cycle_metrics["postgres_upsert_latency_ms_count"], 1)
        self.assertEqual(cycle_metrics["redis_publish_latency_ms_sum"], 7)
        self.assertEqual(cycle_metrics["redis_publish_latency_ms_count"], 1)
        self.assertEqual(cycle_metrics["redis_stream_publish_latency_ms_count"], 0)
        self.assertEqual(cycle_metrics["notification_delivery_latency_ms_sum"], 17)
        self.assertEqual(cycle_metrics["notification_delivery_latency_ms_count"], 1)
        self.assertEqual(cycle_metrics["end_to_end_alert_latency_ms_sum"], 2000)
        self.assertEqual(cycle_metrics["end_to_end_alert_latency_ms_count"], 1)
        self.assertEqual(emit_mock.call_count, 2)

    async def test_process_listing_event_keeps_publish_path_when_notification_gate_is_false(self) -> None:
        cycle_metrics = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
            "redis_stream_publish_latency_ms_sum": 0,
            "redis_stream_publish_latency_ms_count": 0,
            "redis_stream_publish_failure_count": 0,
            "notification_delivery_latency_ms_sum": 0,
            "notification_delivery_latency_ms_count": 0,
            "end_to_end_alert_latency_ms_sum": 0,
            "end_to_end_alert_latency_ms_count": 0,
        }
        metadata = {
            "route_name": "profile_2",
            "listing_seen_ts": "2026-03-08T10:05:00+00:00",
        }
        listing = {"id": "listing-2", "model": "iPhone 14", "potential_profit": 10}
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(return_value=worker.ListingUpsertResult(created=False, stream_state_changed=False)),
            ),
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-08T10:05:01+00:00", 5))) as publish_mock,
            patch.object(worker, "_publish_listing_stream_event", AsyncMock()) as stream_mock,
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=11),
            patch.object(worker, "utc_now_iso", return_value="2026-03-08T10:05:00.500000+00:00"),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), object(), feature_flags, listing, metadata, cycle_metrics)

        publish_mock.assert_awaited_once()
        stream_mock.assert_not_awaited()
        self.assertEqual(cycle_metrics["redis_stream_publish_latency_ms_count"], 0)
        self.assertEqual(cycle_metrics["notification_delivery_latency_ms_count"], 0)
        self.assertEqual(emit_mock.call_count, 1)

    async def test_process_listing_event_logs_stream_publish_failure_but_keeps_pubsub_path(self) -> None:
        cycle_metrics = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
            "redis_stream_publish_latency_ms_sum": 0,
            "redis_stream_publish_latency_ms_count": 0,
            "redis_stream_publish_failure_count": 0,
            "notification_delivery_latency_ms_sum": 0,
            "notification_delivery_latency_ms_count": 0,
            "end_to_end_alert_latency_ms_sum": 0,
            "end_to_end_alert_latency_ms_count": 0,
        }
        metadata = {
            "route_name": "profile_3",
            "listing_seen_ts": "2026-03-08T10:10:00+00:00",
        }
        listing = {"id": "listing-3", "model": "iPhone 15", "potential_profit": 25}
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(return_value=worker.ListingUpsertResult(created=True, stream_state_changed=True)),
            ),
            patch.object(worker, "_publish_listing_stream_event", AsyncMock(side_effect=RuntimeError("xadd failed"))),
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-08T10:10:01+00:00", 6))) as publish_mock,
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=9),
            patch.object(worker, "utc_now_iso", return_value="2026-03-08T10:10:00.500000+00:00"),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), object(), feature_flags, listing, metadata, cycle_metrics)

        publish_mock.assert_awaited_once()
        self.assertEqual(cycle_metrics["redis_stream_publish_failure_count"], 1)
        self.assertEqual(cycle_metrics["redis_stream_publish_latency_ms_count"], 0)
        self.assertEqual(emit_mock.call_args_list[0].args[0], "listing_stream_publish_failed")
        self.assertEqual(emit_mock.call_args_list[1].kwargs["stream_publish_status"], "failed")

    async def test_process_listing_event_delegates_notification_when_consumer_flag_enabled(self) -> None:
        cycle_metrics = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
            "redis_stream_publish_latency_ms_sum": 0,
            "redis_stream_publish_latency_ms_count": 0,
            "redis_stream_publish_failure_count": 0,
            "notification_delivery_latency_ms_sum": 0,
            "notification_delivery_latency_ms_count": 0,
            "end_to_end_alert_latency_ms_sum": 0,
            "end_to_end_alert_latency_ms_count": 0,
        }
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name in {"ENABLE_REDIS_STREAM_EVENTS", "ENABLE_NOTIFICATION_CONSUMER"}

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)
        metadata = {
            "route_name": "profile_delegate",
            "listing_seen_ts": "2026-03-10T00:00:00+00:00",
        }
        listing = {"id": "listing-delegate-1", "model": "iPhone 15 Pro", "potential_profit": 80}
        loop = MagicMock()
        loop.run_in_executor = AsyncMock(return_value=True)

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(return_value=worker.ListingUpsertResult(created=True, stream_state_changed=True)),
            ),
            patch.object(worker, "_publish_listing_stream_event", AsyncMock(return_value=("1741600800000-0", "2026-03-10T00:00:00.250000+00:00", 4))),
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-10T00:00:00.300000+00:00", 5))),
            patch.object(worker, "_should_notify_telegram", return_value=True),
            patch.object(worker.asyncio, "get_running_loop", return_value=loop),
            patch.object(worker, "monotonic_duration_ms", return_value=8),
            patch.object(worker, "utc_now_iso", side_effect=["2026-03-10T00:00:00.100000+00:00", "2026-03-10T00:00:00.400000+00:00"]),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), object(), feature_flags, listing, metadata, cycle_metrics)

        loop.run_in_executor.assert_not_awaited()
        self.assertEqual(cycle_metrics["notification_delivery_latency_ms_count"], 0)
        self.assertEqual(emit_mock.call_args_list[1].args[0], "notification_delivery_delegated")
        self.assertEqual(emit_mock.call_args_list[1].kwargs["notification_status"], "delegated")

    async def test_process_listing_event_enqueues_background_enrichment_after_pubsub(self) -> None:
        cycle_metrics = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
            "redis_stream_publish_latency_ms_sum": 0,
            "redis_stream_publish_latency_ms_count": 0,
            "redis_stream_publish_failure_count": 0,
            "notification_delivery_latency_ms_sum": 0,
            "notification_delivery_latency_ms_count": 0,
            "end_to_end_alert_latency_ms_sum": 0,
            "end_to_end_alert_latency_ms_count": 0,
            "enrichment_enqueue_latency_ms_sum": 0,
            "enrichment_enqueue_latency_ms_count": 0,
            "enrichment_enqueue_failure_count": 0,
        }
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name == "ENABLE_BACKGROUND_ENRICHMENT"

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)
        metadata = {
            "route_name": "profile_enrichment",
            "query": "iPhone 15 Pro",
            "query_index": 1,
            "query_total": 2,
            "query_shard_key": "QUERY_SHARD:enrich",
            "listing_seen_ts": "2026-03-10T00:05:00+00:00",
        }
        listing = {
            "id": "listing-enrich-1",
            "title": "iPhone 15 Pro",
            "price": 950,
            "url": "https://example.com/listing-enrich-1",
            "description": "Battery 89%",
            "seller_name": "Seller A",
            "thumbnail_url": "https://example.com/thumb.jpg",
            "model": "iPhone 15 Pro",
            "condition": "used",
            "potential_profit": 150,
        }
        publish_order: list[str] = []

        async def _publish_listing(*_args, **_kwargs):
            publish_order.append("pubsub")
            return ("2026-03-10T00:05:00.300000+00:00", 5)

        async def _publish_enrichment(*_args, **_kwargs):
            publish_order.append("enrichment")
            return ("1741601100000-1", "2026-03-10T00:05:00.450000+00:00", 7)

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(
                    return_value=worker.ListingUpsertResult(
                        created=True,
                        stream_state_changed=True,
                        should_enqueue_enrichment=True,
                        enrichment_source_hash="hash-1",
                    )
                ),
            ),
            patch.object(worker, "_publish_listing_event", AsyncMock(side_effect=_publish_listing)),
            patch.object(worker, "_publish_listing_enrichment_event", AsyncMock(side_effect=_publish_enrichment)) as enrichment_mock,
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=9),
            patch.object(worker, "utc_now_iso", side_effect=["2026-03-10T00:05:00.100000+00:00", "2026-03-10T00:05:00.400000+00:00"]),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), object(), feature_flags, listing, metadata, cycle_metrics)

        enrichment_mock.assert_awaited_once()
        self.assertEqual(publish_order, ["pubsub", "enrichment"])
        self.assertEqual(cycle_metrics["enrichment_enqueue_latency_ms_sum"], 7)
        self.assertEqual(cycle_metrics["enrichment_enqueue_latency_ms_count"], 1)
        self.assertEqual(emit_mock.call_args_list[0].args[0], "listing_enrichment_enqueued")
        self.assertEqual(emit_mock.call_args_list[1].kwargs["enrichment_publish_status"], "queued")

    async def test_process_listing_event_keeps_current_behavior_when_background_enrichment_is_off(self) -> None:
        cycle_metrics = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
            "redis_stream_publish_latency_ms_sum": 0,
            "redis_stream_publish_latency_ms_count": 0,
            "redis_stream_publish_failure_count": 0,
            "notification_delivery_latency_ms_sum": 0,
            "notification_delivery_latency_ms_count": 0,
            "end_to_end_alert_latency_ms_sum": 0,
            "end_to_end_alert_latency_ms_count": 0,
            "enrichment_enqueue_latency_ms_sum": 0,
            "enrichment_enqueue_latency_ms_count": 0,
            "enrichment_enqueue_failure_count": 0,
        }
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=False)
        metadata = {
            "route_name": "profile_off",
            "listing_seen_ts": "2026-03-10T00:06:00+00:00",
        }
        listing = {"id": "listing-enrich-2", "model": "iPhone 15", "potential_profit": 15}

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(
                    return_value=worker.ListingUpsertResult(
                        created=True,
                        stream_state_changed=True,
                        should_enqueue_enrichment=True,
                        enrichment_source_hash="hash-2",
                    )
                ),
            ),
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-10T00:06:00.300000+00:00", 5))),
            patch.object(worker, "_publish_listing_enrichment_event", AsyncMock()) as enrichment_mock,
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=8),
            patch.object(worker, "utc_now_iso", return_value="2026-03-10T00:06:00.100000+00:00"),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), object(), feature_flags, listing, metadata, cycle_metrics)

        enrichment_mock.assert_not_awaited()
        self.assertEqual(cycle_metrics["enrichment_enqueue_latency_ms_count"], 0)
        self.assertEqual(emit_mock.call_args_list[0].kwargs["background_enrichment_enabled"], False)

    def test_listing_needs_enrichment_suppresses_duplicate_hash_for_completed_listing(self) -> None:
        existing_row = {
            "description": "",
            "seller_name": "",
            "thumbnail_url": "",
            "enrichment_status": "complete",
            "enrichment_source_hash": "hash-3",
        }

        should_enqueue = worker._listing_needs_enrichment(
            existing_row=existing_row,
            cold_fields={"description": "", "seller_name": "", "thumbnail_url": ""},
            source_hash="hash-3",
        )

        self.assertFalse(should_enqueue)

    async def test_concurrent_duplicate_listing_processing_publishes_one_stream_event(self) -> None:
        pool = _FakePool()
        redis_client = object()
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name == "ENABLE_REDIS_STREAM_EVENTS"

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)
        listing = {
            "id": "listing-concurrent-1",
            "title": "iPhone 15 Pro",
            "price": 900,
            "url": "https://example.com/concurrent",
            "model": "iPhone 15 Pro",
            "condition": "Used",
            "potential_profit": 120,
            "max_offer": 700,
            "status": "new",
            "location": "Perth",
        }
        metadata = {
            "route_name": "profile_concurrent",
            "query": "iPhone 15 Pro",
            "query_index": 1,
            "query_total": 1,
            "query_shard_key": "QUERY_SHARD:concurrent",
            "listing_seen_ts": "2026-03-10T00:01:00+00:00",
        }
        cycle_metrics_a = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
            "redis_stream_publish_latency_ms_sum": 0,
            "redis_stream_publish_latency_ms_count": 0,
            "redis_stream_publish_failure_count": 0,
            "notification_delivery_latency_ms_sum": 0,
            "notification_delivery_latency_ms_count": 0,
            "end_to_end_alert_latency_ms_sum": 0,
            "end_to_end_alert_latency_ms_count": 0,
        }
        cycle_metrics_b = dict(cycle_metrics_a)

        with (
            patch.object(worker, "_publish_listing_stream_event", AsyncMock(return_value=("1741600860000-0", "2026-03-10T00:01:00.250000+00:00", 4))) as stream_mock,
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-10T00:01:00.300000+00:00", 5))),
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=6),
            patch.object(worker, "utc_now_iso", return_value="2026-03-10T00:01:00.100000+00:00"),
            patch.object(worker, "emit_json_log"),
        ):
            await asyncio.gather(
                worker._process_listing_event(pool, redis_client, feature_flags, dict(listing), dict(metadata), cycle_metrics_a),
                worker._process_listing_event(pool, redis_client, feature_flags, dict(listing), dict(metadata), cycle_metrics_b),
            )

        stream_mock.assert_awaited_once()

    async def test_process_listing_event_v4_first_seen_duplicate_suppresses_before_upsert(self) -> None:
        cycle_metrics = _base_cycle_metrics()
        redis_client = AsyncMock()
        redis_client.set = AsyncMock(return_value=None)
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name == "ENABLE_V4_FIRST_SEEN_DEDUPE"

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)
        metadata = {
            "route_name": "iphone_broad",
            "query_shard_key": "v4-family:1",
            "source": "v4",
            "discovery_ts": "2026-03-11T00:00:00+00:00",
        }
        listing = {"id": "listing-v4-dup", "model": "iPhone 15 Pro", "price": 900, "potential_profit": 100}

        with (
            patch.object(worker, "_upsert_listing", AsyncMock()) as upsert_mock,
            patch.object(worker, "_publish_listing_event", AsyncMock()) as publish_mock,
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), redis_client, feature_flags, listing, metadata, cycle_metrics)

        upsert_mock.assert_not_awaited()
        publish_mock.assert_not_awaited()
        self.assertEqual(cycle_metrics["duplicate_listing_count"], 1)
        self.assertEqual(cycle_metrics["v4_first_seen_duplicate_count"], 1)
        self.assertEqual(emit_mock.call_args.kwargs["first_seen_status"], "duplicate")

    async def test_process_listing_event_v4_first_seen_claim_forces_created_stream_publish(self) -> None:
        cycle_metrics = _base_cycle_metrics()
        redis_client = AsyncMock()
        redis_client.set = AsyncMock(return_value=True)
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name in {"ENABLE_V4_FIRST_SEEN_DEDUPE", "ENABLE_REDIS_STREAM_EVENTS"}

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)
        metadata = {
            "route_name": "iphone_broad",
            "query_shard_key": "v4-family:1",
            "source": "v4",
            "discovery_ts": "2026-03-11T00:01:00+00:00",
        }
        listing = {"id": "listing-v4-first", "model": "iPhone 15 Pro", "price": 875, "potential_profit": 125}

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(return_value=worker.ListingUpsertResult(created=False, stream_state_changed=False)),
            ) as upsert_mock,
            patch.object(worker, "_publish_listing_stream_event", AsyncMock(return_value=("1741699260000-0", "2026-03-11T00:01:00.250000+00:00", 4))) as stream_mock,
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-11T00:01:00.300000+00:00", 5))) as publish_mock,
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=7),
            patch.object(worker, "utc_now_iso", return_value="2026-03-11T00:01:00.100000+00:00"),
            patch.object(worker, "emit_json_log"),
        ):
            await worker._process_listing_event(object(), redis_client, feature_flags, listing, metadata, cycle_metrics)

        self.assertEqual(upsert_mock.await_args.kwargs["event_name"], "listing_created")
        self.assertEqual(upsert_mock.await_args.kwargs["discovery_ts"], "2026-03-11T00:01:00+00:00")
        self.assertEqual(publish_mock.await_args.kwargs["event_name"], "listing_created")
        stream_mock.assert_awaited_once()
        self.assertEqual(cycle_metrics["v4_first_seen_claim_count"], 1)
        self.assertEqual(cycle_metrics["duplicate_listing_count"], 0)

    async def test_process_listing_event_v4_price_change_claim_publishes_update(self) -> None:
        cycle_metrics = _base_cycle_metrics()
        redis_client = AsyncMock()
        redis_client.set = AsyncMock(side_effect=[None, True])
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name in {"ENABLE_V4_FIRST_SEEN_DEDUPE", "ENABLE_V4_UPDATE_EVENTS"}

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)
        metadata = {
            "route_name": "iphone_broad",
            "query_shard_key": "v4-family:1",
            "source": "v4",
            "discovery_ts": "2026-03-11T00:02:00+00:00",
        }
        listing = {"id": "listing-v4-update", "model": "iPhone 15", "price": 810, "potential_profit": 80}

        with (
            patch.object(worker, "PRICE_DROP_UPDATE_MODE", "price_change"),
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(return_value=worker.ListingUpsertResult(created=False, stream_state_changed=False)),
            ) as upsert_mock,
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-11T00:02:00.300000+00:00", 5))) as publish_mock,
            patch.object(worker, "_publish_listing_stream_event", AsyncMock(return_value=("1741699320000-0", "2026-03-11T00:02:00.250000+00:00", 4))),
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=6),
            patch.object(worker, "utc_now_iso", return_value="2026-03-11T00:02:00.100000+00:00"),
            patch.object(worker, "emit_json_log"),
        ):
            await worker._process_listing_event(object(), redis_client, feature_flags, listing, metadata, cycle_metrics)

        self.assertEqual(upsert_mock.await_args.kwargs["event_name"], "listing_updated")
        self.assertEqual(publish_mock.await_args.kwargs["event_name"], "listing_updated")
        self.assertEqual(
            publish_mock.await_args.kwargs["metadata"]["dedupe_kind"],
            "price_change",
        )
        self.assertEqual(cycle_metrics["v4_update_event_claim_count"], 1)
        self.assertEqual(cycle_metrics["duplicate_listing_count"], 0)

    async def test_process_listing_event_v4_dedupe_fail_open_preserves_publish_path(self) -> None:
        cycle_metrics = _base_cycle_metrics()
        redis_client = AsyncMock()
        redis_client.set = AsyncMock(side_effect=RuntimeError("redis unavailable"))
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name == "ENABLE_V4_FIRST_SEEN_DEDUPE"

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)
        metadata = {
            "route_name": "iphone_broad",
            "query_shard_key": "v4-family:1",
            "source": "v4",
            "discovery_ts": "2026-03-11T00:03:00+00:00",
        }
        listing = {"id": "listing-v4-fallback", "model": "iPhone 14 Pro", "price": 700, "potential_profit": 60}

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(return_value=worker.ListingUpsertResult(created=True, stream_state_changed=True)),
            ) as upsert_mock,
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-11T00:03:00.300000+00:00", 5))) as publish_mock,
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "_should_send_v4_pipeline_alert", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=8),
            patch.object(worker, "utc_now_iso", return_value="2026-03-11T00:03:00.100000+00:00"),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), redis_client, feature_flags, listing, metadata, cycle_metrics)

        self.assertIsNone(upsert_mock.await_args.kwargs["event_name"])
        self.assertEqual(publish_mock.await_args.kwargs["event_name"], "listing_created")
        self.assertEqual(cycle_metrics["v4_dedupe_fallback_count"], 1)
        self.assertEqual(emit_mock.call_args_list[0].args[0], "v4_dedupe_fail_open")

    async def test_try_acquire_priority_query_lock_skips_locked_candidate(self) -> None:
        redis_client = AsyncMock()
        redis_client.set = AsyncMock(side_effect=[None, True])

        selected_query, lock_key, lock_token, skipped = await worker._try_acquire_priority_query_lock(
            redis_client=redis_client,
            route={"route_name": "profile_hot"},
            queries=["iPhone", "iPhone 15 Pro"],
        )

        self.assertEqual(selected_query, "iPhone 15 Pro")
        self.assertEqual(skipped, 1)
        self.assertTrue(str(lock_key).startswith("query-lock:"))
        self.assertTrue(bool(lock_token))

    async def test_run_scrape_cycle_uses_selected_priority_query(self) -> None:
        route = {
            "route_name": "profile_priority",
            "worker_name": "worker",
            "user_data_dir": "/profiles/priority",
            "search_queries": "iPhone, iPhone 15 Pro",
            "effective_lane": "hot",
            "computed_lane": "hot",
            "lane_override": "hot",
            "priority_score": 6.5,
            "priority_components": {"profit": 1.5},
            "due_age_seconds": 12.0,
            "revisit_age_seconds": 18.0,
            "lane_interval_multiplier": 1.0,
            "status": "ENABLED",
            "source": "db",
        }
        feature_flags = MagicMock()

        async def _flag_enabled(flag_name: str) -> bool:
            return flag_name in {"ENABLE_ROUTE_LANES", "ENABLE_PRIORITY_SCHEDULER"}

        feature_flags.is_enabled = AsyncMock(side_effect=_flag_enabled)

        with (
            patch.dict(worker.os.environ, {"DOLPHIN_PROFILE_ID": "phase4-test-profile"}, clear=False),
            patch.object(worker, "_try_acquire_profile_lock", AsyncMock(return_value="profile-lock")),
            patch.object(worker, "_build_playwright_proxy", AsyncMock(return_value=(None, None, None, None, None, None))),
            patch.object(worker, "_try_acquire_priority_query_lock", AsyncMock(return_value=("iPhone 15 Pro", "query-lock:1", "token-1", 1))),
            patch.object(worker, "_release_proxy_lease", AsyncMock()),
            patch.object(worker, "_release_profile_lock", AsyncMock()),
            patch.object(worker, "_release_query_shard_lock", AsyncMock()),
            patch.object(worker, "_release_priority_query_lock", AsyncMock()),
            patch.object(worker, "_cleanup_old_scrape_events", AsyncMock()),
            patch.object(worker, "_record_proxy_success", AsyncMock()),
            patch.object(worker, "_persist_route_preferred_proxy_key", AsyncMock()),
            patch.object(worker, "scrape_marketplace", AsyncMock(return_value=None)) as scrape_mock,
        ):
            metrics, details = await worker._run_scrape_cycle(
                pool=AsyncMock(),
                redis_client=AsyncMock(),
                feature_flags=feature_flags,
                route=route,
            )

        self.assertEqual(metrics["query_lock_acquired_count"], 1)
        self.assertEqual(metrics["query_lock_skipped_count"], 1)
        self.assertEqual(details["selected_query"], "iPhone 15 Pro")
        self.assertEqual(details["query_lock_status"], "acquired")
        self.assertEqual(details["query_lock_key"], "query-lock:1")
        self.assertEqual(scrape_mock.await_args.kwargs["search_queries"], ["iPhone 15 Pro"])

    async def test_run_scrape_cycle_with_retry_waits_when_all_priority_queries_are_locked(self) -> None:
        route = {
            "route_name": "profile_locked",
            "effective_lane": "hot",
            "computed_lane": "hot",
            "lane_override": None,
            "priority_score": 6.2,
            "priority_components": {"profit": 1.2},
            "due_age_seconds": 10.0,
            "revisit_age_seconds": 10.0,
        }
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)

        with patch.object(
            worker,
            "_run_scrape_cycle",
            AsyncMock(side_effect=worker.QueryShardLockUnavailableError("Query shard lock unavailable for route profile_locked")),
        ):
            result = await worker._run_scrape_cycle_with_retry(
                pool=AsyncMock(),
                redis_client=AsyncMock(),
                feature_flags=feature_flags,
                route=route,
            )

        self.assertEqual(result.outcome, worker.CycleOutcome.WAIT_QUERY_SHARD)
        self.assertEqual(result.error_category, worker.ErrorCategory.QUERY_SHARD_LOCK_UNAVAILABLE)
        self.assertEqual(result.details["query_lock_status"], "all_locked")

    async def test_query_lock_contention_temporarily_suppresses_route_selection(self) -> None:
        now = datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc)
        route = {
            "worker_name": "worker",
            "route_name": "worker_3__env_default",
            "source": "central",
            "route_interval_seconds": 30,
        }
        worker._ROUTE_LOCK_CONTENTION_UNTIL.clear()
        worker._PROFILE_LOCK_CONTENTION_UNTIL.clear()
        self.addCleanup(worker._ROUTE_LOCK_CONTENTION_UNTIL.clear)
        self.addCleanup(worker._PROFILE_LOCK_CONTENTION_UNTIL.clear)

        backoff_seconds = worker._record_lock_contention_backoff(
            route=route,
            profile=None,
            outcome=worker.CycleOutcome.WAIT_QUERY_SHARD,
            now=now,
        )

        filtered_routes = worker._filter_routes_for_lock_contention([route], now=now)
        self.assertEqual(backoff_seconds, 15.0)
        self.assertEqual(filtered_routes, [])
        self.assertAlmostEqual(
            worker._seconds_until_route_lock_contention_release([route], now=now) or 0.0,
            15.0,
            places=3,
        )

    async def test_profile_lock_contention_temporarily_suppresses_profile_selection(self) -> None:
        now = datetime(2026, 3, 12, 0, 0, tzinfo=timezone.utc)
        route = {
            "worker_name": "worker",
            "route_name": "worker_2__env_default",
            "source": "central",
            "route_interval_seconds": 60,
        }
        profile = {"user_data_dir": "/app/runtime/browser_profile_1"}
        worker._ROUTE_LOCK_CONTENTION_UNTIL.clear()
        worker._PROFILE_LOCK_CONTENTION_UNTIL.clear()
        self.addCleanup(worker._ROUTE_LOCK_CONTENTION_UNTIL.clear)
        self.addCleanup(worker._PROFILE_LOCK_CONTENTION_UNTIL.clear)

        backoff_seconds = worker._record_lock_contention_backoff(
            route=route,
            profile=profile,
            outcome=worker.CycleOutcome.WAIT_PROFILE_LOCK,
            now=now,
        )

        filtered_profiles = worker._filter_execution_profiles_for_lock_contention([profile], now=now)
        filtered_routes = worker._filter_routes_for_lock_contention([route], now=now)
        self.assertEqual(backoff_seconds, 10.0)
        self.assertEqual(filtered_profiles, [])
        self.assertEqual(filtered_routes, [route])
        self.assertAlmostEqual(
            worker._seconds_until_profile_lock_contention_release([profile], now=now) or 0.0,
            10.0,
            places=3,
        )

    async def test_central_profile_lock_does_not_globally_reschedule_route(self) -> None:
        route = {"worker_name": "worker", "route_name": "worker_2__env_default", "source": "central"}
        worker_route = {"worker_name": "worker", "route_name": "env_default", "source": "db"}

        self.assertFalse(
            worker._should_schedule_route_after_cycle(route, worker.CycleOutcome.WAIT_PROFILE_LOCK)
        )
        self.assertTrue(
            worker._should_schedule_route_after_cycle(route, worker.CycleOutcome.WAIT_QUERY_SHARD)
        )
        self.assertTrue(
            worker._should_schedule_route_after_cycle(worker_route, worker.CycleOutcome.WAIT_PROFILE_LOCK)
        )
