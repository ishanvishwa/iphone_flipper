from __future__ import annotations

import contextlib
import unittest
from datetime import datetime, timezone
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
                    "dolphin_profile_id": "746386753",
                    "dolphin_profile_name": "Profile 3",
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


class _QueryFamilyListConn:
    async def fetch(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if "FROM query_families qf" in normalized:
            return [
                {
                    "family_id": 11,
                    "name": "iphone_broad",
                    "legacy_route_name": "worker_3__env_default",
                    "legacy_worker_name": "worker_3",
                    "is_enabled": True,
                    "priority": 300,
                    "priority_score": 8.5,
                    "lane": "hot",
                    "next_due_at": None,
                    "min_gap_s": 5,
                    "max_gap_s": 5,
                    "variant_cursor": 0,
                    "variant_count": 1,
                    "consecutive_hits": 0,
                    "consecutive_empty": 0,
                    "last_claimed_at": None,
                    "last_discovery_at": None,
                    "last_success_at": None,
                    "last_error": None,
                    "family_lease_token": None,
                    "family_lease_expires_at": None,
                    "created_at": None,
                    "updated_at": None,
                    "active_worker_name": "worker_3",
                    "active_query_text": "iPhone",
                    "active_user_data_dir": "/app/runtime/browser_profile_3",
                    "active_worker_status": "ok",
                }
            ]
        return []


class _QueryFamilyUpsertConn:
    def __init__(self) -> None:
        self.deleted_args: tuple | None = None
        self.variant_upserts: list[tuple] = []
        self.family_insert_args: tuple | None = None

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetchval(self, query: str, *args):
        return None

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if normalized.startswith("INSERT INTO query_families"):
            self.family_insert_args = args
            return {
                "family_id": 15,
                "name": args[0],
                "legacy_route_name": args[1],
                "legacy_worker_name": args[2],
                "is_enabled": args[3],
                "priority": args[4],
                "priority_score": None,
                "lane": args[5],
                "next_due_at": None,
                "min_gap_s": args[6],
                "max_gap_s": args[7],
                "variant_cursor": 0,
                "variant_count": 0,
                "consecutive_hits": 0,
                "consecutive_empty": 0,
                "last_claimed_at": None,
                "last_discovery_at": None,
                "last_success_at": None,
                "last_error": None,
                "family_lease_token": None,
                "family_lease_expires_at": None,
                "created_at": None,
                "updated_at": None,
            }
        if normalized.startswith("UPDATE query_families SET variant_count"):
            return {
                "family_id": 15,
                "name": "iphone_15_pro",
                "legacy_route_name": None,
                "legacy_worker_name": None,
                "is_enabled": True,
                "priority": 215,
                "priority_score": None,
                "lane": "warm",
                "next_due_at": None,
                "min_gap_s": 20,
                "max_gap_s": None,
                "variant_cursor": 0,
                "variant_count": 2,
                "consecutive_hits": 0,
                "consecutive_empty": 0,
                "last_claimed_at": None,
                "last_discovery_at": None,
                "last_success_at": None,
                "last_error": None,
                "family_lease_token": None,
                "family_lease_expires_at": None,
                "created_at": None,
                "updated_at": None,
            }
        return None

    async def execute(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if normalized.startswith("DELETE FROM query_variants"):
            self.deleted_args = args
        if normalized.startswith("INSERT INTO query_variants"):
            self.variant_upserts.append(args)
        return "OK"

    async def fetch(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if "FROM query_families qf" in normalized and "WHERE qf.family_id = $1" in normalized:
            return [
                {
                    "family_id": 15,
                    "name": "iphone_15_pro",
                    "legacy_route_name": None,
                    "legacy_worker_name": None,
                    "is_enabled": True,
                    "priority": 215,
                    "priority_score": None,
                    "lane": "warm",
                    "next_due_at": None,
                    "min_gap_s": 20,
                    "max_gap_s": None,
                    "variant_cursor": 0,
                    "variant_count": 2,
                    "consecutive_hits": 0,
                    "consecutive_empty": 0,
                    "last_claimed_at": None,
                    "last_discovery_at": None,
                    "last_success_at": None,
                    "last_error": None,
                    "family_lease_token": None,
                    "family_lease_expires_at": None,
                    "created_at": None,
                    "updated_at": None,
                    "active_worker_name": None,
                    "active_query_text": None,
                    "active_user_data_dir": None,
                    "active_worker_status": None,
                }
            ]
        return []


class _QueryFamilyDeleteConn:
    def __init__(self, *, leased: bool = False) -> None:
        self.lookup_args: tuple | None = None
        self.delete_args: tuple | None = None
        self.leased = leased

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if normalized.startswith("SELECT family_id, family_lease_token, family_lease_expires_at FROM query_families"):
            self.lookup_args = args
            return {
                "family_id": 15,
                "family_lease_token": "lease-15" if self.leased else None,
                "family_lease_expires_at": datetime.now(timezone.utc) if self.leased else None,
            }
        return None

    async def execute(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if normalized.startswith("DELETE FROM query_families"):
            self.delete_args = args
            return "DELETE 1"
        return "OK"


class _ExecutionProfilePoolSyncConn:
    def __init__(self) -> None:
        self.execution_profile_upserts: list[tuple] = []
        self.v4_profile_upserts: list[tuple] = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if normalized.startswith("INSERT INTO execution_profiles"):
            self.execution_profile_upserts.append(args)
            return {
                "user_data_dir": args[0],
                "worker_name": args[1],
                "dolphin_profile_id": args[2],
                "dolphin_profile_name": args[3],
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
        if normalized.startswith("INSERT INTO profiles"):
            self.v4_profile_upserts.append(args)
            return {"profile_id": len(self.v4_profile_upserts)}
        return None


class _BootstrapCatalogConn:
    def __init__(self) -> None:
        self._family_rows: dict[str, dict] = {}
        self._variants_by_family: dict[int, dict[str, dict]] = {}
        self._next_family_id = 1

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if normalized.startswith("INSERT INTO query_families"):
            family_name = str(args[0])
            row = self._family_rows.get(family_name)
            if row is None:
                row = {"family_id": self._next_family_id, "name": family_name}
                self._family_rows[family_name] = row
                self._variants_by_family[row["family_id"]] = {}
                self._next_family_id += 1
            return dict(row)
        return None

    async def execute(self, query: str, *args):
        normalized = " ".join(str(query).split())
        if normalized.startswith("INSERT INTO query_variants"):
            family_id = int(args[0])
            query_text = str(args[1])
            self._variants_by_family.setdefault(family_id, {})[query_text] = {
                "validation_state": args[2],
                "weight": args[3],
                "variant_order": args[4],
                "is_enabled": args[5],
                "notes": args[6],
            }
        return "OK"


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self._conn)


class _MinimalConn:
    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()


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

    async def test_get_query_families_returns_variants_and_live_metadata(self) -> None:
        api_main.app.state.db_pool = _FakePool(_QueryFamilyListConn())

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(
                api_main,
                "_load_query_variants_by_family_id",
                AsyncMock(
                    return_value={
                        11: [
                            {
                                "variant_id": 31,
                                "family_id": 11,
                                "query_text": "iPhone",
                                "validation_state": "validated",
                                "weight": 1.0,
                                "variant_order": 0,
                                "is_enabled": True,
                                "notes": "Broad search",
                            }
                        ]
                    }
                ),
            ),
        ):
            payload = await api_main.get_query_families(family_name=None, x_api_token="test-token")

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["catalog_version"], api_main.V42_FAMILY_CATALOG_VERSION)
        item = payload["items"][0]
        self.assertEqual(item["name"], "iphone_broad")
        self.assertEqual(item["active_worker_name"], "worker_3")
        self.assertEqual(item["active_query_text"], "iPhone")
        self.assertEqual(item["validated_variant_count"], 1)
        self.assertEqual(item["search_queries"], "iPhone")

    async def test_upsert_query_family_persists_family_metadata_and_variants(self) -> None:
        conn = _QueryFamilyUpsertConn()
        api_main.app.state.db_pool = _FakePool(conn)

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(
                api_main,
                "_load_query_variants_by_family_id",
                AsyncMock(
                    return_value={
                        15: [
                            {
                                "variant_id": 71,
                                "family_id": 15,
                                "query_text": "iPhone 15 Pro",
                                "validation_state": "validated",
                                "weight": 1.0,
                                "variant_order": 0,
                                "is_enabled": True,
                                "notes": "Primary",
                            },
                            {
                                "variant_id": 72,
                                "family_id": 15,
                                "query_text": "iPhone 15 Pro 256GB",
                                "validation_state": "pending_validation",
                                "weight": 0.7,
                                "variant_order": 1,
                                "is_enabled": False,
                                "notes": "Staged",
                            },
                        ]
                    }
                ),
            ),
        ):
            payload = await api_main.upsert_query_family(
                family_name="iphone_15_pro",
                payload=api_main.QueryFamilyUpsertRequest(
                    is_enabled=True,
                    priority=215,
                    lane="warm",
                    min_gap_s=20,
                    variants=[
                        api_main.QueryVariantUpsertRequest(
                            query_text="iPhone 15 Pro",
                            validation_state="validated",
                            is_enabled=True,
                            weight=1.0,
                            notes="Primary",
                        ),
                        api_main.QueryVariantUpsertRequest(
                            query_text="iPhone 15 Pro 256GB",
                            validation_state="pending_validation",
                            is_enabled=False,
                            weight=0.7,
                            notes="Staged",
                        ),
                    ],
                ),
                x_api_token="test-token",
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(conn.family_insert_args[0], "iphone_15_pro")
        self.assertEqual(conn.deleted_args, (15, ["iPhone 15 Pro", "iPhone 15 Pro 256GB"]))
        self.assertEqual(len(conn.variant_upserts), 2)
        self.assertEqual(conn.variant_upserts[0][1], "iPhone 15 Pro")
        self.assertEqual(payload["item"]["validated_variant_count"], 1)
        self.assertEqual(payload["item"]["enabled_variant_count"], 1)

    async def test_bootstrap_query_family_presets_is_idempotent(self) -> None:
        conn = _BootstrapCatalogConn()

        await api_main._bootstrap_query_family_presets(conn)
        await api_main._bootstrap_query_family_presets(conn)

        self.assertEqual(len(conn._family_rows), len(api_main.V42_FAMILY_PRESETS))
        total_variants = sum(len(variants) for variants in conn._variants_by_family.values())
        preset_variants = sum(len(preset.variants) for preset in api_main.V42_FAMILY_PRESETS)
        self.assertEqual(total_variants, preset_variants)

    async def test_delete_query_family_removes_family(self) -> None:
        conn = _QueryFamilyDeleteConn()
        api_main.app.state.db_pool = _FakePool(conn)

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.delete_query_family(
                family_name="iphone_15_pro",
                x_api_token="test-token",
            )

        self.assertTrue(payload["ok"])
        self.assertTrue(payload["deleted"])
        self.assertEqual(conn.lookup_args, ("iphone_15_pro",))
        self.assertEqual(conn.delete_args, (15,))

    async def test_delete_query_family_rejects_active_lease(self) -> None:
        conn = _QueryFamilyDeleteConn(leased=True)
        api_main.app.state.db_pool = _FakePool(conn)

        with patch.object(api_main, "API_TOKEN", "test-token"):
            with self.assertRaises(api_main.HTTPException) as exc_info:
                await api_main.delete_query_family(
                    family_name="iphone_15_pro",
                    x_api_token="test-token",
                )

        self.assertEqual(exc_info.exception.status_code, 409)
        self.assertEqual(conn.delete_args, None)

    async def test_sync_execution_profile_pool_upserts_slots(self) -> None:
        conn = _ExecutionProfilePoolSyncConn()
        api_main.app.state.db_pool = _FakePool(conn)

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.sync_execution_profile_pool(
                payload=api_main.ExecutionProfilePoolSyncRequest(
                    slots=[4, 5, 5, -1],
                    profiles=[
                        api_main.ExecutionProfilePoolSyncItem(
                            slot=4,
                            dolphin_profile_id="100",
                            dolphin_profile_name="Profile 4",
                        ),
                        api_main.ExecutionProfilePoolSyncItem(
                            slot=5,
                            dolphin_profile_id="101",
                            dolphin_profile_name="Seller Browser",
                        ),
                    ],
                ),
                x_api_token="test-token",
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["synced_count"], 2)
        self.assertEqual(payload["slots"], [4, 5])
        self.assertEqual(
            conn.execution_profile_upserts,
            [
                ("/app/runtime/browser_profile_4", "worker", "100", "Profile 4"),
                ("/app/runtime/browser_profile_5", "worker_2", "101", "Seller Browser"),
            ],
        )
        self.assertEqual(
            conn.v4_profile_upserts,
            [
                ("worker", "/app/runtime/browser_profile_4", "100", "Profile 4"),
                ("worker_2", "/app/runtime/browser_profile_5", "101", "Seller Browser"),
            ],
        )

    async def test_get_execution_profiles_returns_profile_health_rows(self) -> None:
        api_main.app.state.db_pool = _FakePool(_RouteOpsConn())

        with patch.object(api_main, "API_TOKEN", "test-token"):
            payload = await api_main.get_execution_profiles(worker_name=None, x_api_token="test-token")

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["worker_name"], "worker_3")
        self.assertEqual(payload["items"][0]["user_data_dir"], "/app/runtime/browser_profile_3")
        self.assertEqual(payload["items"][0]["dolphin_profile_name"], "Profile 3")

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
                        "dolphin_profile_id": "746386753",
                        "dolphin_profile_name": "Profile 3",
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
                        "dolphin_profile_id": "746386753",
                        "dolphin_profile_name": "Profile 3",
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

    async def test_put_model_prices_replaces_server_price_sheet(self) -> None:
        api_main.app.state.db_pool = _FakePool(_MinimalConn())
        stored_rows = [
            {
                "model": "iPhone 14 Pro 128GB",
                "buying_price": 380.0,
                "selling_price": 460.0,
                "backglass_repair": 50.0,
                "screen_repair": 180.0,
                "battery_repair": 35.0,
                "camera_lens_repair": 25.0,
            }
        ]

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(api_main, "replace_model_prices", AsyncMock(return_value=stored_rows)) as replace_mock,
            patch.object(api_main, "fetch_model_price_rows", AsyncMock(return_value=stored_rows)) as fetch_mock,
            patch.object(api_main, "emit_json_log"),
        ):
            payload = await api_main.put_model_prices(
                payload=api_main.ModelPricesUpdateRequest(
                    items=[
                        api_main.ModelPriceItemRequest(
                            model="iPhone 14 Pro 128GB",
                            buying_price=380,
                            selling_price=460,
                            backglass_repair=50,
                            screen_repair=180,
                            battery_repair=35,
                            camera_lens_repair=25,
                        )
                    ]
                ),
                x_api_token="test-token",
            )

        replace_mock.assert_awaited_once()
        fetch_mock.assert_awaited_once()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["model"], "iPhone 14 Pro 128GB")

    async def test_run_lowball_report_endpoint_uses_worker_module(self) -> None:
        api_main.app.state.db_pool = _FakePool(_MinimalConn())
        from server.services.worker import lowball_report_worker as report_worker

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(report_worker, "run_lowball_report_once", AsyncMock(return_value={"status": "dry_run", "listing_count": 2})) as run_mock,
            patch.object(api_main, "emit_json_log"),
        ):
            payload = await api_main.run_lowball_report(
                payload=api_main.LowballReportRunRequest(dry_run=True, send_telegram=True),
                x_api_token="test-token",
            )

        run_mock.assert_awaited_once()
        self.assertFalse(run_mock.await_args.kwargs["send_telegram"])
        self.assertFalse(run_mock.await_args.kwargs["verify_candidates"])
        self.assertEqual(payload["status"], "dry_run")

    async def test_get_latest_lowball_report_returns_latest_item(self) -> None:
        api_main.app.state.db_pool = _FakePool(_MinimalConn())

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(
                api_main,
                "load_latest_report",
                AsyncMock(return_value={"report_date": "2026-04-19", "listing_count": 3}),
            ) as latest_mock,
        ):
            payload = await api_main.get_latest_lowball_report(x_api_token="test-token")

        latest_mock.assert_awaited_once()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["item"]["listing_count"], 3)

    async def test_set_listing_report_action_updates_listing(self) -> None:
        api_main.app.state.db_pool = _FakePool(_MinimalConn())
        action_time = datetime(2026, 4, 19, 6, 0, tzinfo=timezone.utc)

        with (
            patch.object(api_main, "API_TOKEN", "test-token"),
            patch.object(
                api_main,
                "mark_listing_report_action",
                AsyncMock(
                    return_value={
                        "id": "listing-1",
                        "report_action_taken": "contacted",
                        "report_action_taken_at": action_time,
                    }
                ),
            ) as action_mock,
            patch.object(api_main, "emit_json_log"),
        ):
            payload = await api_main.set_listing_report_action(
                listing_id="listing-1",
                payload=api_main.ListingReportActionRequest(action="contacted"),
                x_api_token="test-token",
            )

        action_mock.assert_awaited_once()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["item"]["report_action_taken"], "contacted")
        self.assertEqual(payload["item"]["report_action_taken_at"], action_time.isoformat())


if __name__ == "__main__":
    unittest.main()
