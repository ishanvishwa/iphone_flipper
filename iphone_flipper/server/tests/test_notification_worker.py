from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from redis.exceptions import ResponseError

from server.services.common.stream_events import LISTING_STREAM_SCHEMA_VERSION, ListingStreamEvent
from server.services.worker import notification_worker


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

        if normalized.startswith("INSERT INTO notification_delivery_ledger"):
            row = {
                "listing_id": listing_id,
                "first_stream_event_id": str(args[1]),
                "last_stream_event_id": str(args[1]),
                "status": "suppressed" if "'suppressed'" in normalized else "pending",
                "attempt_count": 0,
                "last_error": args[2] if len(args) > 2 else None,
                "last_attempt_at": None,
                "sent_at": None,
            }
        elif "SET last_stream_event_id = $2, updated_at = NOW()" in normalized:
            row["last_stream_event_id"] = str(args[1])
        elif "SET last_stream_event_id = $2, status = 'suppressed'" in normalized:
            row["last_stream_event_id"] = str(args[1])
            row["status"] = "suppressed"
            row["last_error"] = args[2]
        elif "SET last_stream_event_id = $2, status = 'sending'" in normalized:
            row["last_stream_event_id"] = str(args[1])
            row["status"] = "sending"
            row["attempt_count"] = int(row.get("attempt_count", 0)) + 1
            row["last_error"] = None
            row["last_attempt_at"] = "attempted"
        elif "SET last_stream_event_id = $2, status = 'retry_pending'" in normalized:
            row["last_stream_event_id"] = str(args[1])
            row["status"] = "retry_pending"
            row["last_error"] = args[2]
        elif "SET last_stream_event_id = $2, status = 'sent'" in normalized:
            row["last_stream_event_id"] = str(args[1])
            row["status"] = "sent"
            row["last_error"] = None
            row["sent_at"] = "sent"
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

    async def xautoclaim(self, *args, **kwargs):
        return self.xautoclaim_result

    async def xreadgroup(self, *args, **kwargs):
        return self.xreadgroup_result


def _build_stream_event(
    *,
    listing_id: str = "listing-1",
    event_name: str = "listing_created",
    potential_profit: float = 100.0,
) -> tuple[str, dict[str, str]]:
    event = ListingStreamEvent(
        schema_version=LISTING_STREAM_SCHEMA_VERSION,
        event_name=event_name,
        listing_id=listing_id,
        worker_name="worker",
        route_name="env_default",
        query="iPhone 15 Pro",
        query_index=1,
        query_total=1,
        query_shard_key="QUERY_SHARD:test",
        persisted_at="2026-03-10T01:00:00+00:00",
        price=900.0,
        potential_profit=potential_profit,
        title="iPhone 15 Pro",
        url="https://example.com/listing-1",
        source="marketplace",
    )
    return "1741604400000-0", event.to_redis_fields()


class NotificationLedgerStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_prepare_marks_duplicate_when_listing_already_sent(self) -> None:
        pool = _FakeLedgerPool()
        store = notification_worker.NotificationLedgerStore(pool)

        await store.prepare("listing-1", "1741604400000-0", True, "eligible")
        await store.mark_attempt_started("listing-1", "1741604400000-0")
        await store.mark_sent("listing-1", "1741604400000-0")

        result = await store.prepare("listing-1", "1741604460000-0", True, "eligible")

        self.assertEqual(result, "duplicate_already_sent")
        entry = await store.get("listing-1")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.status, "sent")
        self.assertEqual(entry.last_stream_event_id, "1741604460000-0")

    async def test_prepare_suppressed_creates_terminal_row(self) -> None:
        pool = _FakeLedgerPool()
        store = notification_worker.NotificationLedgerStore(pool)

        result = await store.prepare("listing-2", "1741604520000-0", False, "suppressed_below_profit_threshold")

        self.assertEqual(result, "suppressed")
        entry = await store.get("listing-2")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.status, "suppressed")
        self.assertEqual(entry.last_error, "suppressed_below_profit_threshold")


class NotificationSendPacerTests(unittest.IsolatedAsyncioTestCase):
    async def test_wait_turn_enforces_one_second_spacing(self) -> None:
        pacer = notification_worker.NotificationSendPacer(min_interval_seconds=1.0)

        with (
            patch.object(notification_worker.time, "monotonic", side_effect=[10.0, 10.2, 10.2]),
            patch.object(notification_worker.asyncio, "sleep", AsyncMock()) as sleep_mock,
        ):
            await pacer.wait_turn()
            await pacer.wait_turn()

        sleep_mock.assert_awaited_once()
        self.assertAlmostEqual(sleep_mock.await_args.args[0], 0.8, places=3)


class NotificationConsumerTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_consumer_group_ignores_busygroup(self) -> None:
        redis_client = _FakeRedis()
        redis_client.group_create_error = ResponseError("BUSYGROUP Consumer Group name already exists")
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        consumer = notification_worker.NotificationConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            ledger=ledger,
            send_fcm_func=None,
        )

        await consumer.ensure_consumer_group()

        self.assertEqual(len(redis_client.group_create_calls), 1)

    async def test_process_stream_entry_sends_and_acks(self) -> None:
        redis_client = _FakeRedis()
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="send")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_sent = AsyncMock()
        ledger.mark_retry_pending = AsyncMock()
        stream_event_id, fields = _build_stream_event()
        consumer = notification_worker.NotificationConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            ledger=ledger,
            send_telegram_func=AsyncMock(return_value=True),
            send_fcm_func=None,
            pacer=MagicMock(wait_turn=AsyncMock()),
        )

        with patch.object(notification_worker, "emit_json_log") as emit_mock:
            result = await consumer.process_stream_entry(stream_event_id, fields)

        self.assertEqual(result, "sent")
        ledger.mark_attempt_started.assert_awaited_once()
        ledger.mark_sent.assert_awaited_once()
        self.assertEqual(redis_client.acked, [(notification_worker.LISTING_STREAM_NAME, consumer.consumer_group, stream_event_id)])
        self.assertEqual(emit_mock.call_args.kwargs["notification_status"], "sent")

    async def test_process_stream_entry_suppresses_and_acks(self) -> None:
        redis_client = _FakeRedis()
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="suppressed")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_sent = AsyncMock()
        ledger.mark_retry_pending = AsyncMock()
        stream_event_id, fields = _build_stream_event(potential_profit=-5.0)
        consumer = notification_worker.NotificationConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            ledger=ledger,
            send_telegram_func=AsyncMock(return_value=True),
            send_fcm_func=None,
            pacer=MagicMock(wait_turn=AsyncMock()),
        )

        with patch.object(notification_worker, "emit_json_log") as emit_mock:
            result = await consumer.process_stream_entry(stream_event_id, fields)

        self.assertEqual(result, "suppressed")
        ledger.mark_attempt_started.assert_not_awaited()
        self.assertEqual(redis_client.acked, [(notification_worker.LISTING_STREAM_NAME, consumer.consumer_group, stream_event_id)])
        self.assertEqual(emit_mock.call_args.kwargs["notification_status"], "suppressed")

    async def test_process_stream_entry_dedupes_already_sent(self) -> None:
        redis_client = _FakeRedis()
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="duplicate_already_sent")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_sent = AsyncMock()
        ledger.mark_retry_pending = AsyncMock()
        stream_event_id, fields = _build_stream_event(listing_id="listing-dup")
        consumer = notification_worker.NotificationConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            ledger=ledger,
            send_telegram_func=AsyncMock(return_value=True),
            send_fcm_func=None,
            pacer=MagicMock(wait_turn=AsyncMock()),
        )

        with patch.object(notification_worker, "emit_json_log") as emit_mock:
            result = await consumer.process_stream_entry(stream_event_id, fields)

        self.assertEqual(result, "duplicate_already_sent")
        ledger.mark_attempt_started.assert_not_awaited()
        self.assertEqual(redis_client.acked, [(notification_worker.LISTING_STREAM_NAME, consumer.consumer_group, stream_event_id)])
        self.assertEqual(emit_mock.call_args.kwargs["notification_status"], "duplicate_already_sent")

    async def test_process_stream_entry_retries_and_leaves_unacked_on_failure(self) -> None:
        redis_client = _FakeRedis()
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="send")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_sent = AsyncMock()
        ledger.mark_retry_pending = AsyncMock()
        stream_event_id, fields = _build_stream_event(listing_id="listing-retry")
        consumer = notification_worker.NotificationConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            ledger=ledger,
            send_telegram_func=AsyncMock(return_value=False),
            send_fcm_func=None,
            pacer=MagicMock(wait_turn=AsyncMock()),
            retry_attempts=2,
            retry_base_delay_seconds=1.0,
        )

        with (
            patch.object(notification_worker.asyncio, "sleep", AsyncMock()) as sleep_mock,
            patch.object(notification_worker, "emit_json_log") as emit_mock,
        ):
            result = await consumer.process_stream_entry(stream_event_id, fields)

        self.assertEqual(result, "retry_pending")
        self.assertEqual(ledger.mark_attempt_started.await_count, 2)
        ledger.mark_sent.assert_not_awaited()
        ledger.mark_retry_pending.assert_awaited_once()
        self.assertEqual(redis_client.acked, [])
        sleep_mock.assert_awaited_once_with(1.0)
        self.assertEqual(emit_mock.call_args.kwargs["notification_status"], "retry_pending")

    async def test_run_once_reclaims_pending_entries_with_xautoclaim(self) -> None:
        redis_client = _FakeRedis()
        stream_event_id, fields = _build_stream_event(listing_id="listing-pending")
        redis_client.xautoclaim_result = ("0-0", [(stream_event_id, fields)], [])
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="send")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_sent = AsyncMock()
        ledger.mark_retry_pending = AsyncMock()
        consumer = notification_worker.NotificationConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            ledger=ledger,
            send_telegram_func=AsyncMock(return_value=True),
            send_fcm_func=None,
            pacer=MagicMock(wait_turn=AsyncMock()),
        )

        processed = await consumer.run_once()

        self.assertEqual(processed, 1)
        self.assertEqual(redis_client.acked, [(notification_worker.LISTING_STREAM_NAME, consumer.consumer_group, stream_event_id)])

    async def test_run_once_reads_new_entries_from_group(self) -> None:
        redis_client = _FakeRedis()
        stream_event_id, fields = _build_stream_event(listing_id="listing-new")
        redis_client.xreadgroup_result = [
            (notification_worker.LISTING_STREAM_NAME, [(stream_event_id, fields)])
        ]
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=True)
        ledger = MagicMock()
        ledger.prepare = AsyncMock(return_value="send")
        ledger.mark_attempt_started = AsyncMock()
        ledger.mark_sent = AsyncMock()
        ledger.mark_retry_pending = AsyncMock()
        consumer = notification_worker.NotificationConsumer(
            redis_client=redis_client,
            feature_flags=feature_flags,
            ledger=ledger,
            send_telegram_func=AsyncMock(return_value=True),
            send_fcm_func=None,
            pacer=MagicMock(wait_turn=AsyncMock()),
        )

        processed = await consumer.run_once()

        self.assertEqual(processed, 1)
        self.assertEqual(redis_client.acked, [(notification_worker.LISTING_STREAM_NAME, consumer.consumer_group, stream_event_id)])


if __name__ == "__main__":
    unittest.main()
