from __future__ import annotations

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("IPHONE_FLIPPER_DB_PATH", "/tmp/iphone_flipper_worker_test.db")

try:
    from server.services.worker import worker
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    worker = None


class _FakeAcquire:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.execute = AsyncMock(return_value="OK")


class _FakePool:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


@unittest.skipIf(worker is None, "Worker dependencies are not installed.")
class WorkerPublishHealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_record_worker_event_publish_health_updates_heartbeat_fields(self) -> None:
        pool = _FakePool()

        await worker._record_worker_event_publish_health(
            pool=pool,
            route_name="route-a",
            publish_status="ok",
            published_at="2026-03-10T00:00:00+00:00",
            stream_event_id="1741604400000-0",
        )

        args = pool.conn.execute.await_args.args
        self.assertEqual(args[1], worker.WORKER_NAME)
        self.assertEqual(args[2], "route-a")
        self.assertEqual(args[3], "2026-03-10T00:00:00+00:00")
        self.assertEqual(args[4], "ok")
        self.assertEqual(args[6], "1741604400000-0")

    async def test_upsert_worker_heartbeat_persists_route_snapshot_fields(self) -> None:
        pool = _FakePool()

        await worker._upsert_worker_heartbeat(
            pool=pool,
            route_name="env_default",
            status="running",
            route={
                "source": "env",
                "user_data_dir": "/app/runtime/browser_profile_3",
                "search_queries": "iPhone",
                "proxy_mode": "fixed",
                "proxy_server": "",
                "proxy_username": "",
                "proxy_password": "",
                "proxy_pool": "",
            },
        )

        args = pool.conn.execute.await_args.args
        self.assertEqual(args[1], worker.WORKER_NAME)
        self.assertEqual(args[2], "env_default")
        self.assertEqual(args[8], "env")
        self.assertEqual(args[9], "/app/runtime/browser_profile_3")
        self.assertEqual(args[10], "iPhone")
        self.assertEqual(args[11], "fixed")

    def test_build_v4_family_route_includes_active_variant_queries(self) -> None:
        route = worker._build_v4_family_route(
            {"user_data_dir": "/app/runtime/browser_profile_3"},
            {"name": "iphone_broad"},
            search_queries=["iPhone"],
        )

        self.assertEqual(route["route_name"], "iphone_broad")
        self.assertEqual(route["source"], "v4")
        self.assertEqual(route["search_queries"], "iPhone")

    async def test_process_listing_event_records_failed_publish_health(self) -> None:
        feature_flags = MagicMock()
        feature_flags.is_enabled = AsyncMock(return_value=False)
        metadata = {"route_name": "route-a", "listing_seen_ts": "2026-03-10T00:00:00+00:00"}
        listing = {"id": "listing-1", "model": "iPhone 15", "potential_profit": 100}

        with (
            patch.object(
                worker,
                "_upsert_listing",
                AsyncMock(return_value=worker.ListingUpsertResult(created=True, stream_state_changed=True)),
            ),
            patch.object(worker, "_publish_listing_event", AsyncMock(side_effect=RuntimeError("pubsub down"))),
            patch.object(worker, "_record_worker_event_publish_health", AsyncMock()) as publish_health_mock,
            patch.object(worker, "emit_json_log"),
        ):
            with self.assertRaises(RuntimeError):
                await worker._process_listing_event(object(), object(), feature_flags, listing, metadata, {})

        publish_health_mock.assert_awaited_once()
        self.assertEqual(publish_health_mock.await_args.kwargs["publish_status"], "failed")


if __name__ == "__main__":
    unittest.main()
