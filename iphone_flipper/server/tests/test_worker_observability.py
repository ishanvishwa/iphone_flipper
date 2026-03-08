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
    async def test_process_listing_event_records_publish_and_notification_latency(self) -> None:
        cycle_metrics = {
            "postgres_upsert_latency_ms_sum": 0,
            "postgres_upsert_latency_ms_count": 0,
            "redis_publish_latency_ms_sum": 0,
            "redis_publish_latency_ms_count": 0,
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
            await worker._process_listing_event(object(), object(), listing, metadata, cycle_metrics)

        self.assertEqual(cycle_metrics["postgres_upsert_latency_ms_sum"], 13)
        self.assertEqual(cycle_metrics["postgres_upsert_latency_ms_count"], 1)
        self.assertEqual(cycle_metrics["redis_publish_latency_ms_sum"], 7)
        self.assertEqual(cycle_metrics["redis_publish_latency_ms_count"], 1)
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

        with (
            patch.object(worker, "_upsert_listing", AsyncMock(return_value=False)),
            patch.object(worker, "_publish_listing_event", AsyncMock(return_value=("2026-03-08T10:05:01+00:00", 5))) as publish_mock,
            patch.object(worker, "_should_notify_telegram", return_value=False),
            patch.object(worker, "monotonic_duration_ms", return_value=11),
            patch.object(worker, "utc_now_iso", return_value="2026-03-08T10:05:00.500000+00:00"),
            patch.object(worker, "emit_json_log") as emit_mock,
        ):
            await worker._process_listing_event(object(), object(), listing, metadata, cycle_metrics)

        publish_mock.assert_awaited_once()
        self.assertEqual(cycle_metrics["notification_delivery_latency_ms_count"], 0)
        self.assertEqual(emit_mock.call_count, 1)
