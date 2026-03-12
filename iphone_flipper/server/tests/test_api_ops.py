from __future__ import annotations

import contextlib
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

try:
    from server.services.api.app import main as api_main
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local test env
    api_main = None


class _FakeAcquire:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeConn:
    def __init__(self, rows=None, replay_success_listing_ids=None) -> None:
        self._rows = list(rows or [])
        self._replay_success_listing_ids = set(replay_success_listing_ids or [])

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetch(self, _query: str):
        return list(self._rows)

    async def fetchrow(self, _query: str, listing_id: str):
        if listing_id in self._replay_success_listing_ids:
            return {"listing_id": listing_id}
        return None


class _RouteOpsConn:
    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetch(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if "FROM central_routes" in normalized:
            return [
                {
                    "route_name": "route-hot",
                    "legacy_worker_name": "worker_3",
                    "legacy_route_name": "env_default",
                    "is_enabled": True,
                    "proxy_server": "",
                    "proxy_username": None,
                    "proxy_password": None,
                    "proxy_mode": "fixed",
                    "proxy_pool": None,
                    "preferred_proxy_key": None,
                    "preferred_proxy_updated_at": None,
                    "priority": 10,
                    "status": "ENABLED",
                    "status_reason": None,
                    "status_since": None,
                    "next_run_at": None,
                    "route_interval_seconds": 150,
                    "avg_result_count": 3.0,
                    "profitable_hit_rate": 0.4,
                    "recent_duplicate_ratio": 0.1,
                    "avg_page_load_ms": None,
                    "successful_cycles": 5,
                    "last_selected_at": None,
                    "last_success_at": None,
                    "consecutive_failures": 0,
                    "cooldown_until": None,
                    "lane_override": None,
                    "computed_lane": "hot",
                    "effective_lane": "hot",
                    "priority_score": 6.2,
                    "priority_score_updated_at": None,
                    "last_error": None,
                    "created_at": None,
                    "updated_at": None,
                }
            ]
        if "FROM route_queries" in normalized:
            return [
                {
                    "route_name": "route-hot",
                    "query_text": "iPhone 15 Pro",
                    "query_order": 0,
                    "is_enabled": True,
                    "last_selected_at": None,
                    "last_success_at": None,
                    "avg_result_count": 4.0,
                    "profitable_hit_rate": 0.5,
                    "recent_duplicate_ratio": 0.0,
                    "selection_count": 2,
                    "last_error": None,
                    "created_at": None,
                    "updated_at": None,
                }
            ]
        if "FROM execution_profiles" in normalized:
            return [
                {
                    "user_data_dir": "/app/runtime/browser_profile_3",
                    "worker_name": "worker_3",
                    "is_enabled": True,
                    "status": "READY",
                    "status_reason": None,
                    "status_since": None,
                    "cooldown_until": None,
                    "manual_login_required": False,
                    "manual_login_reason": None,
                    "manual_login_required_at": None,
                    "quarantined_at": None,
                    "quarantine_reason": None,
                    "quarantine_evidence": None,
                    "last_selected_at": None,
                    "last_success_at": None,
                    "consecutive_failures": 0,
                    "last_error": None,
                    "created_at": None,
                    "updated_at": None,
                }
            ]
        return []

    async def fetchrow(self, query: str, *args):
        return None


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._conn)


class _FakeRedis:
    def __init__(self) -> None:
        self.xadd = AsyncMock(return_value="1741604800000-0")
        self._stream_info = {
            "stream:listings": {
                "length": 15,
                "last-generated-id": "1741604400000-2",
                "last-entry": ("1741604400000-2", {"listing_id": "listing-2"}),
            },
            "stream:listing_enrichment": {
                "length": 5,
                "last-generated-id": "1741604500000-1",
                "last-entry": ("1741604500000-1", {"listing_id": "listing-2"}),
            },
            "stream:notification_dead_letter": {
                "length": 2,
                "last-generated-id": "1741604600000-1",
                "last-entry": ("1741604600000-1", {"listing_id": "listing-dlq"}),
            },
        }

    async def xinfo_stream(self, stream_name: str):
        return dict(self._stream_info[stream_name])

    async def xinfo_groups(self, stream_name: str):
        if stream_name == "stream:listings":
            return [
                {
                    "name": "listing_notifications",
                    "consumers": 1,
                    "pending": 3,
                    "lag": 4,
                    "last-delivered-id": "1741604400000-1",
                    "entries-read": 10,
                }
            ]
        if stream_name == "stream:listing_enrichment":
            return [
                {
                    "name": "listing_enrichment",
                    "consumers": 1,
                    "pending": 1,
                    "lag": 0,
                    "last-delivered-id": "1741604500000-1",
                    "entries-read": 5,
                }
            ]
        return []

    async def xpending(self, stream_name: str, group_name: str):
        if stream_name == "stream:listings" and group_name == "listing_notifications":
            return {"pending": 3, "min": "1741604400000-0", "max": "1741604400000-2", "consumers": []}
        return {"pending": 0, "min": None, "max": None, "consumers": []}


@unittest.skipIf(api_main is None, "API dependencies are not installed.")
class ApiOpsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._original_state = {
            "redis": getattr(api_main.app.state, "redis", None),
            "feature_flags": getattr(api_main.app.state, "feature_flags", None),
            "runtime_config": getattr(api_main.app.state, "runtime_config", None),
            "db_pool": getattr(api_main.app.state, "db_pool", None),
            "ws_manager": getattr(api_main.app.state, "ws_manager", None),
        }
        api_main.app.state.ws_manager = api_main.WebSocketConnectionManager()

    async def asyncTearDown(self) -> None:
        for name, value in self._original_state.items():
            if value is None:
                with contextlib.suppress(Exception):
                    delattr(api_main.app.state, name)
            else:
                setattr(api_main.app.state, name, value)

    async def test_get_stream_backlog_returns_group_summaries(self) -> None:
        api_main.app.state.redis = _FakeRedis()

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.get_stream_backlog(x_api_token="test-token")

        listings = payload["streams"]["listings"]
        self.assertEqual(listings["length"], 15)
        self.assertEqual(listings["groups"][0]["name"], "listing_notifications")
        self.assertEqual(listings["groups"][0]["pending_summary"]["pending"], 3)

    async def test_update_runtime_config_mutates_flags_and_runtime_config(self) -> None:
        feature_flags = MagicMock()
        feature_flags._defaults = {"ENABLE_GUI_WEBSOCKET_PUSH": False}
        feature_flags.snapshot = AsyncMock(return_value={"ENABLE_GUI_WEBSOCKET_PUSH": False})
        feature_flags.set_flag_values = AsyncMock(return_value={"ENABLE_GUI_WEBSOCKET_PUSH": False})
        runtime_config = MagicMock()
        runtime_config.snapshot = AsyncMock(return_value=api_main.DEFAULT_RUNTIME_CONFIG)
        runtime_config.set_values = AsyncMock(return_value={**api_main.DEFAULT_RUNTIME_CONFIG, "NOTIFICATION_CONSUMER_DRAIN": True})
        api_main.app.state.feature_flags = feature_flags
        api_main.app.state.runtime_config = runtime_config

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(api_main, "_close_all_ws_clients_disabled", AsyncMock()) as close_ws,
            patch.object(api_main, "emit_json_log"),
        ):
            payload = await api_main.update_runtime_config(
                payload=api_main.RuntimeConfigUpdateRequest(
                    feature_flags={"ENABLE_GUI_WEBSOCKET_PUSH": False},
                    runtime_config={"NOTIFICATION_CONSUMER_DRAIN": True},
                ),
                x_api_token="test-token",
            )

        feature_flags.set_flag_values.assert_awaited_once()
        runtime_config.set_values.assert_awaited_once()
        close_ws.assert_awaited_once_with("feature_flag_off")
        self.assertTrue(payload["runtime_config"]["NOTIFICATION_CONSUMER_DRAIN"])

    async def test_get_realtime_health_reports_publish_health_and_counts(self) -> None:
        api_main.app.state.redis = _FakeRedis()
        api_main.app.state.feature_flags = MagicMock(snapshot=AsyncMock(return_value={"ENABLE_NOTIFICATION_CONSUMER": True}))
        api_main.app.state.runtime_config = MagicMock(snapshot=AsyncMock(return_value={**api_main.DEFAULT_RUNTIME_CONFIG, "NOTIFICATION_CONSUMER_DRAIN": True}))
        api_main.app.state.db_pool = _FakePool(
            _FakeConn(
                rows=[
                    {
                        "worker_name": "worker",
                        "route_name": "route-a",
                        "status": "ok",
                        "updated_at": "2026-03-10T00:00:00+00:00",
                        "last_event_publish_at": "2026-03-10T00:00:01+00:00",
                        "last_event_publish_status": "ok",
                        "last_event_publish_error": None,
                        "last_stream_event_id": "1741604400000-0",
                    }
                ]
            )
        )

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.get_realtime_health(x_api_token="test-token")

        self.assertEqual(payload["websocket_connection_count"], 0)
        self.assertTrue(payload["notification_consumer"]["drain_enabled"])
        self.assertEqual(payload["dead_letter_queue_size"], 2)
        self.assertEqual(payload["worker_event_publish_health"][0]["last_event_publish_status"], "ok")

    async def test_get_worker_health_includes_effective_route_snapshot_fields(self) -> None:
        api_main.app.state.db_pool = _FakePool(
            _FakeConn(
                rows=[
                    {
                        "worker_name": "worker_3",
                        "route_name": "env_default",
                        "status": "ok",
                        "listings_saved": 0,
                        "query_count": 1,
                        "last_run_started_at": "2026-03-10T00:00:00+00:00",
                        "last_run_finished_at": "2026-03-10T00:00:05+00:00",
                        "route_source": "env",
                        "route_user_data_dir": "/app/runtime/browser_profile_3",
                        "route_search_queries": "iPhone",
                        "route_proxy_mode": "fixed",
                        "route_proxy_server": "",
                        "route_proxy_username": "",
                        "route_proxy_password": "",
                        "route_proxy_pool": "",
                        "last_error": None,
                        "updated_at": "2026-03-10T00:00:05+00:00",
                        "listings_scraped_last_minute": 12,
                        "route_cooldown_until": None,
                        "cooldown_remaining_seconds": 0,
                        "leased_proxy_server": None,
                        "leased_proxy_route": None,
                        "leased_proxy_until": None,
                        "lease_remaining_seconds": 0,
                    }
                ]
            )
        )

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.get_worker_health(x_api_token="test-token")

        item = payload["items"][0]
        self.assertEqual(item["route_source"], "env")
        self.assertEqual(item["route_user_data_dir"], "/app/runtime/browser_profile_3")
        self.assertEqual(item["route_search_queries"], "iPhone")
        self.assertEqual(item["route_proxy_mode"], "fixed")

    async def test_get_central_routes_returns_queries_and_search_csv(self) -> None:
        api_main.app.state.db_pool = _FakePool(_RouteOpsConn())

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.get_central_routes(route_name=None, x_api_token="test-token")

        self.assertEqual(payload["count"], 1)
        item = payload["items"][0]
        self.assertEqual(item["route_name"], "route-hot")
        self.assertEqual(item["legacy_worker_name"], "worker_3")
        self.assertEqual(item["queries"][0]["query_text"], "iPhone 15 Pro")
        self.assertEqual(item["search_queries"], "iPhone 15 Pro")

    async def test_get_execution_profiles_returns_profile_health_rows(self) -> None:
        api_main.app.state.db_pool = _FakePool(_RouteOpsConn())

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.get_execution_profiles(worker_name=None, x_api_token="test-token")

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["worker_name"], "worker_3")
        self.assertEqual(payload["items"][0]["user_data_dir"], "/app/runtime/browser_profile_3")

    async def test_clear_execution_profile_manual_login_updates_execution_and_v4_profiles(self) -> None:
        class _ExecutionProfileClearConn:
            def __init__(self) -> None:
                self.execution_profile_args: tuple | None = None
                self.profile_args: tuple | None = None

            async def fetchrow(self, query: str, *args):
                normalized = " ".join(str(query).split())
                if normalized.startswith("UPDATE execution_profiles SET"):
                    self.execution_profile_args = args
                    return {
                        "user_data_dir": "/app/runtime/browser_profile_3",
                        "worker_name": "worker_3",
                        "is_enabled": True,
                        "status": "READY",
                        "status_reason": "manual login cleared by operator",
                        "status_since": None,
                        "cooldown_until": None,
                        "manual_login_required": False,
                        "manual_login_reason": None,
                        "manual_login_required_at": None,
                        "quarantined_at": None,
                        "quarantine_reason": None,
                        "quarantine_evidence": None,
                        "last_selected_at": None,
                        "last_success_at": None,
                        "consecutive_failures": 0,
                        "last_error": None,
                        "created_at": None,
                        "updated_at": None,
                    }
                if normalized.startswith("UPDATE profiles SET"):
                    self.profile_args = args
                    return {
                        "profile_id": 7,
                        "worker_name": "worker_3",
                        "user_data_dir": "/app/runtime/browser_profile_3",
                        "is_enabled": True,
                        "status": "READY",
                        "status_reason": "manual login cleared by operator",
                        "status_since": None,
                        "available_after": None,
                        "cooldown_until": None,
                        "manual_login_required": False,
                        "manual_login_reason": None,
                        "manual_login_required_at": None,
                        "quarantined_at": None,
                        "quarantine_reason": None,
                        "quarantine_evidence": None,
                        "failure_count": 0,
                        "consecutive_empty_claims": 0,
                        "last_started_at": None,
                        "last_success_at": None,
                        "last_failure_at": None,
                        "last_heartbeat_at": None,
                        "last_error": None,
                        "profile_lease_token": None,
                        "profile_lease_expires_at": None,
                        "created_at": None,
                        "updated_at": None,
                    }
                return None

        conn = _ExecutionProfileClearConn()
        api_main.app.state.db_pool = _FakePool(conn)

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.clear_execution_profile_manual_login(
                worker_name="worker_3",
                payload=api_main.ExecutionProfileManualLoginClearRequest(
                    user_data_dir="/app/runtime/browser_profile_3"
                ),
                x_api_token="test-token",
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["worker_name"], "worker_3")
        self.assertEqual(payload["execution_profile"]["status"], "READY")
        self.assertEqual(payload["profile"]["profile_id"], 7)
        self.assertEqual(
            conn.execution_profile_args,
            ("worker_3", "/app/runtime/browser_profile_3", "manual login cleared by operator"),
        )
        self.assertEqual(
            conn.profile_args,
            ("worker_3", "/app/runtime/browser_profile_3", "manual login cleared by operator"),
        )

    async def test_bootstrap_worker_routes_from_health_creates_missing_live_routes(self) -> None:
        class _BootstrapConn:
            def __init__(self) -> None:
                self.insert_calls: list[tuple] = []

            async def fetch(self, query: str, *_args):
                if "FROM worker_heartbeats" in query:
                    return [
                        {
                            "worker_name": "worker_3",
                            "route_name": "env_default",
                            "route_source": "env",
                            "route_user_data_dir": "/app/runtime/browser_profile_3",
                            "route_search_queries": "iPhone",
                            "route_proxy_mode": "fixed",
                            "route_proxy_server": None,
                            "route_proxy_username": None,
                            "route_proxy_password": None,
                            "route_proxy_pool": None,
                        }
                    ]
                return []

            async def fetchval(self, query: str, worker_name: str):
                if "FROM worker_routes" in query and worker_name == "worker_3":
                    return None
                return None

            async def fetchrow(self, query: str, *args):
                if "COALESCE(BTRIM(user_data_dir), '')" in query:
                    return None
                if "INSERT INTO worker_routes" in query:
                    self.insert_calls.append(args)
                    return {
                        "worker_name": args[0],
                        "route_name": args[1],
                        "user_data_dir": args[7],
                        "search_queries": args[8],
                    }
                return None

        conn = _BootstrapConn()
        api_main.app.state.db_pool = _FakePool(conn)

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.bootstrap_worker_routes_from_health(x_api_token="test-token")

        self.assertEqual(payload["created_count"], 1)
        self.assertEqual(payload["created"][0]["worker_name"], "worker_3")
        self.assertEqual(payload["created"][0]["route_name"], "env_default")
        self.assertEqual(conn.insert_calls[0][2], "")
        self.assertEqual(conn.insert_calls[0][7], "/app/runtime/browser_profile_3")
        self.assertEqual(conn.insert_calls[0][8], "iPhone")

    async def test_replay_notification_dead_letters_resets_failed_terminal_and_republishes(self) -> None:
        api_main.app.state.redis = _FakeRedis()
        api_main.app.state.db_pool = _FakePool(_FakeConn(replay_success_listing_ids={"listing-replay"}))
        dead_letter = api_main.NotificationDeadLetterEvent(
            schema_version="1",
            original_stream_event_id="1741604400000-0",
            listing_id="listing-replay",
            worker_name="worker",
            route_name="route-a",
            event_name="listing_created",
            query="iPhone 15",
            query_index=1,
            query_total=1,
            query_shard_key="QUERY_SHARD:test",
            persisted_at="2026-03-10T00:00:00+00:00",
            price=1000.0,
            potential_profit=100.0,
            title="iPhone 15",
            url="https://example.com/listing-replay",
            thumbnail_url="",
            source="marketplace",
            failure_status="failed_terminal",
            last_error="telegram failed",
            attempt_count=3,
            dead_lettered_at="2026-03-10T00:05:00+00:00",
        )

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(api_main, "_load_dead_letter_entries", AsyncMock(return_value=[("1741604600000-0", dead_letter)])),
            patch.object(api_main, "emit_json_log"),
        ):
            payload = await api_main.replay_notification_dead_letters(
                payload=api_main.NotificationReplayRequest(window_minutes=60, limit=10, dry_run=False),
                x_api_token="test-token",
            )

        api_main.app.state.redis.xadd.assert_awaited_once()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["listing_id"], "listing-replay")


if __name__ == "__main__":
    unittest.main()
