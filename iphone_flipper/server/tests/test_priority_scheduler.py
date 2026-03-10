from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from server.services.worker.scheduler import (
    annotate_routes_with_priority,
    lane_interval_multiplier,
    rank_route_query_records,
    rank_route_queries,
    select_next_route,
)


class PrioritySchedulerTests(unittest.TestCase):
    def test_pin_override_wins_over_computed_lane(self) -> None:
        now = datetime(2026, 3, 10, tzinfo=timezone.utc)
        route = {
            "route_name": "route-hot-override",
            "priority": 100,
            "next_run_at": now - timedelta(seconds=15),
            "route_interval_seconds": 30,
            "avg_result_count": 1.0,
            "profitable_hit_rate": 0.0,
            "recent_duplicate_ratio": 0.0,
            "lane_override": "hot",
            "status": "ENABLED",
            "consecutive_failures": 0,
        }

        annotate_routes_with_priority([route], now=now, fallback_interval_seconds=30.0)

        self.assertEqual(route["lane_override"], "hot")
        self.assertEqual(route["effective_lane"], "hot")
        self.assertIn(route["computed_lane"], {"warm", "sweep"})

    def test_priority_scheduler_prefers_hot_due_route_over_sweep(self) -> None:
        now = datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc)
        routes = [
            {
                "route_name": "route_sweep",
                "priority": 100,
                "next_run_at": now - timedelta(seconds=30),
                "route_interval_seconds": 30,
                "avg_result_count": 0.5,
                "profitable_hit_rate": 0.0,
                "recent_duplicate_ratio": 0.8,
                "status": "ENABLED",
                "consecutive_failures": 0,
                "lane_override": "sweep",
            },
            {
                "route_name": "route_hot",
                "priority": 50,
                "next_run_at": now - timedelta(seconds=5),
                "route_interval_seconds": 30,
                "avg_result_count": 12.0,
                "profitable_hit_rate": 0.5,
                "recent_duplicate_ratio": 0.05,
                "status": "ENABLED",
                "consecutive_failures": 0,
                "lane_override": "hot",
            },
        ]

        annotate_routes_with_priority(routes, now=now, fallback_interval_seconds=30.0)
        selected = select_next_route(routes, now=now, use_priority_scheduler=True)

        self.assertIsNotNone(selected)
        self.assertEqual(selected["route_name"], "route_hot")

    def test_flags_off_scheduler_path_keeps_flat_due_order(self) -> None:
        now = datetime(2026, 3, 10, 1, 0, tzinfo=timezone.utc)
        routes = [
            {"route_name": "a", "priority": 10, "next_run_at": now + timedelta(seconds=10)},
            {"route_name": "b", "priority": 20, "next_run_at": now - timedelta(seconds=2)},
            {"route_name": "c", "priority": 5, "next_run_at": now - timedelta(seconds=20)},
        ]

        selected = select_next_route(routes, now=now, use_priority_scheduler=False)

        self.assertIsNotNone(selected)
        self.assertEqual(selected["route_name"], "c")

    def test_rank_route_queries_respects_hot_lane_specificity(self) -> None:
        route = {"effective_lane": "hot", "recent_duplicate_ratio": 0.4}
        ranked = rank_route_queries(route, ["iPhone", "iPhone 15 Pro", "broken iphone"])
        self.assertEqual(ranked[0], "iPhone 15 Pro")

    def test_lane_interval_multiplier_does_not_speed_up_hot_below_baseline(self) -> None:
        self.assertEqual(lane_interval_multiplier("hot"), 1.0)
        self.assertGreater(lane_interval_multiplier("sweep"), lane_interval_multiplier("warm"))

    def test_runtime_config_thresholds_can_promote_route_to_hot(self) -> None:
        now = datetime(2026, 3, 10, tzinfo=timezone.utc)
        route = {
            "route_name": "route-thresholds",
            "priority": 100,
            "next_run_at": now - timedelta(seconds=15),
            "route_interval_seconds": 30,
            "avg_result_count": 1.0,
            "profitable_hit_rate": 0.2,
            "recent_duplicate_ratio": 0.0,
            "status": "ENABLED",
            "consecutive_failures": 0,
        }

        annotate_routes_with_priority(
            [route],
            now=now,
            fallback_interval_seconds=30.0,
            runtime_config={"ROUTE_LANE_HOT_PROFITABLE_HIT_RATE_MIN": 0.15},
        )

        self.assertEqual(route["computed_lane"], "hot")

    def test_prefer_non_hot_can_force_exploration_dispatch(self) -> None:
        now = datetime(2026, 3, 10, 2, 0, tzinfo=timezone.utc)
        routes = [
            {
                "route_name": "route_hot",
                "priority": 5,
                "next_run_at": now - timedelta(seconds=5),
                "route_interval_seconds": 30,
                "avg_result_count": 12.0,
                "profitable_hit_rate": 0.5,
                "recent_duplicate_ratio": 0.05,
                "status": "ENABLED",
                "consecutive_failures": 0,
                "lane_override": "hot",
            },
            {
                "route_name": "route_warm",
                "priority": 50,
                "next_run_at": now - timedelta(seconds=30),
                "route_interval_seconds": 30,
                "avg_result_count": 2.0,
                "profitable_hit_rate": 0.05,
                "recent_duplicate_ratio": 0.0,
                "status": "ENABLED",
                "consecutive_failures": 0,
                "lane_override": "warm",
            },
        ]

        annotate_routes_with_priority(routes, now=now, fallback_interval_seconds=30.0)
        selected = select_next_route(
            routes,
            now=now,
            use_priority_scheduler=True,
            prefer_non_hot=True,
        )

        self.assertIsNotNone(selected)
        self.assertEqual(selected["route_name"], "route_warm")

    def test_rank_route_query_records_uses_query_metrics(self) -> None:
        route = {"effective_lane": "warm"}
        ranked = rank_route_query_records(
            route,
            [
                {
                    "query_text": "iPhone",
                    "is_enabled": True,
                    "avg_result_count": 1.0,
                    "profitable_hit_rate": 0.0,
                    "recent_duplicate_ratio": 0.8,
                    "selection_count": 10,
                },
                {
                    "query_text": "iPhone 15 Pro",
                    "is_enabled": True,
                    "avg_result_count": 4.0,
                    "profitable_hit_rate": 0.4,
                    "recent_duplicate_ratio": 0.0,
                    "selection_count": 2,
                },
            ],
        )

        self.assertEqual(ranked[0]["query_text"], "iPhone 15 Pro")


if __name__ == "__main__":
    unittest.main()
