from __future__ import annotations

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("IPHONE_FLIPPER_DB_PATH", "/tmp/iphone_flipper_worker_test.db")

try:
    from server.services.worker import worker
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    worker = None


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
        feature_flags.is_enabled = AsyncMock(return_value=True)
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
        feature_flags.is_enabled = AsyncMock(return_value=True)
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
