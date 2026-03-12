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


if __name__ == "__main__":
    unittest.main()
