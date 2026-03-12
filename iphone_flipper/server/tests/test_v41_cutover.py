from __future__ import annotations

import unittest

from server.services.common import v41_cutover


class ChooseV41SeedRouteTests(unittest.TestCase):
    def test_prefers_single_exact_broad_query_route(self) -> None:
        routes = [
            {
                "route_name": "env_default",
                "legacy_worker_name": "worker",
                "priority": 100,
                "priority_score": 4.1,
                "effective_lane": "warm",
                "route_interval_seconds": None,
                "queries": ["iPhone 12", "iPhone 13"],
            },
            {
                "route_name": "worker_3__env_default",
                "legacy_worker_name": "worker_3",
                "priority": 100,
                "priority_score": 5.9,
                "effective_lane": "hot",
                "route_interval_seconds": None,
                "queries": ["iPhone"],
            },
        ]

        candidate = v41_cutover.choose_v41_seed_route(routes)

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.route_name, "worker_3__env_default")
        self.assertEqual(candidate.queries, ("iPhone",))

    def test_respects_preferred_route_name(self) -> None:
        routes = [
            {
                "route_name": "worker_3__env_default",
                "legacy_worker_name": "worker_3",
                "priority": 100,
                "priority_score": 5.9,
                "effective_lane": "hot",
                "route_interval_seconds": None,
                "queries": ["iPhone"],
            },
            {
                "route_name": "manual_pick",
                "legacy_worker_name": "worker",
                "priority": 100,
                "priority_score": 3.0,
                "effective_lane": "warm",
                "route_interval_seconds": None,
                "queries": ["iPhone", "iPhone 14"],
            },
        ]

        candidate = v41_cutover.choose_v41_seed_route(routes, preferred_route_name="manual_pick")

        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.route_name, "manual_pick")


class BuildFlagRestoreMappingTests(unittest.TestCase):
    def test_uses_snapshot_values_when_present(self) -> None:
        snapshot = {
            "feature_flags": {
                "ENABLE_CENTRAL_ROUTE_DISPATCH": "0",
                "ENABLE_V4_WARM_RUNTIME": "1",
                "ENABLE_V4_FIRST_SEEN_DEDUPE": "1",
                "ENABLE_V4_UPDATE_EVENTS": "0",
            }
        }

        mapping = v41_cutover.build_flag_restore_mapping(snapshot)

        self.assertEqual(
            mapping,
            {
                "ENABLE_CENTRAL_ROUTE_DISPATCH": "0",
                "ENABLE_V4_WARM_RUNTIME": "1",
                "ENABLE_V4_FIRST_SEEN_DEDUPE": "1",
                "ENABLE_V4_UPDATE_EVENTS": "0",
            },
        )

    def test_falls_back_to_safe_rollback_defaults(self) -> None:
        mapping = v41_cutover.build_flag_restore_mapping(snapshot=None)

        self.assertEqual(mapping, v41_cutover.ROLLBACK_FLAG_DEFAULTS)


class _FakeAcquire:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        _ = (exc_type, exc, tb)
        return False


class _FakeStatusConn:
    async def fetchrow(self, query: str, *args) -> dict[str, object] | None:
        _ = args
        if "AS central_routes" in query:
            return {
                "central_routes": 3,
                "route_queries": 20,
                "execution_profiles": 3,
                "profiles": 3,
                "query_families": 3,
                "query_variants": 20,
            }
        if "FROM query_families" in query:
            return {
                "family_id": 70,
                "name": v41_cutover.V41_FAMILY_NAME,
                "legacy_route_name": "worker_3__env_default",
                "legacy_worker_name": "worker_3",
                "is_enabled": True,
                "priority": 100,
                "lane": "hot",
                "next_due_at": None,
                "min_gap_s": 5,
                "max_gap_s": None,
                "variant_count": 1,
                "leased": False,
            }
        raise AssertionError(f"Unexpected fetchrow query: {query}")

    async def fetch(self, query: str, *args) -> list[dict[str, object]]:
        _ = args
        if "FROM query_variants" in query:
            return [
                {
                    "variant_id": 440,
                    "query_text": v41_cutover.V41_BROAD_QUERY,
                    "validation_state": "validated",
                    "is_enabled": True,
                    "variant_order": 0,
                }
            ]
        if "FROM profiles" in query:
            return [
                {
                    "profile_id": 1,
                    "worker_name": "worker_3",
                    "user_data_dir": "/app/runtime/browser_profile_3",
                    "status": "READY",
                    "available_after": None,
                    "leased": False,
                }
            ]
        raise AssertionError(f"Unexpected fetch query: {query}")


class _FakeStatusPool:
    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(_FakeStatusConn())


class _FakeRedis:
    async def hgetall(self, key: str) -> dict[str, str]:
        _ = key
        return {
            "ENABLE_CENTRAL_ROUTE_DISPATCH": "1",
            "ENABLE_V4_WARM_RUNTIME": "0",
        }


class CollectV41StatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_collect_status_reads_existing_state(self) -> None:
        status = await v41_cutover.collect_v41_status(_FakeStatusPool(), _FakeRedis())

        self.assertEqual(status["counts"]["profiles"], 3)
        self.assertEqual(status["feature_flags"]["ENABLE_V4_WARM_RUNTIME"], "0")
        self.assertEqual(status["family"]["name"], v41_cutover.V41_FAMILY_NAME)
        self.assertEqual(status["variants"][0]["validation_state"], "validated")
        self.assertEqual(status["profiles"][0]["worker_name"], "worker_3")


if __name__ == "__main__":
    unittest.main()
