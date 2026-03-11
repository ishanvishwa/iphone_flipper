from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from server.services.worker.runtime import (
    CycleOutcome,
    ErrorCategory,
    RouteStatus,
    classify_error,
    compute_sleep_seconds,
    compute_worker_effective_interval,
    derive_route_status_for_bad_cycles,
    next_bad_cycle_count,
    route_status_interval_multiplier,
    seconds_until_next_route,
    select_due_route,
)


class _FixedRng:
    def __init__(self, value: float) -> None:
        self._value = value

    def uniform(self, low: float, high: float) -> float:
        _ = low
        _ = high
        return self._value


class WorkerRuntimeTests(unittest.TestCase):
    def test_compute_sleep_seconds_when_elapsed_exceeds_interval_returns_zero(self) -> None:
        self.assertEqual(
            compute_sleep_seconds(base_interval=10, elapsed=12, jitter_pct=0.25, rng=_FixedRng(0.0)),
            0.0,
        )

    def test_compute_sleep_seconds_with_jitter(self) -> None:
        # remaining=8s, jitter +25% => 10s
        result = compute_sleep_seconds(base_interval=10, elapsed=2, jitter_pct=0.25, rng=_FixedRng(0.25))
        self.assertAlmostEqual(result, 10.0, places=5)

    def test_select_due_route_picks_earliest_due(self) -> None:
        now = datetime(2026, 2, 16, tzinfo=timezone.utc)
        routes = [
            {"route_name": "b", "priority": 10, "next_run_at": now + timedelta(seconds=5)},
            {"route_name": "a", "priority": 1, "next_run_at": now - timedelta(seconds=1)},
            {"route_name": "c", "priority": 5, "next_run_at": now - timedelta(seconds=10)},
        ]
        selected = select_due_route(routes=routes, now=now)
        self.assertIsNotNone(selected)
        self.assertEqual(selected["route_name"], "c")

    def test_seconds_until_next_route(self) -> None:
        now = datetime(2026, 2, 16, tzinfo=timezone.utc)
        routes = [{"route_name": "r1", "next_run_at": now + timedelta(seconds=12)}]
        self.assertAlmostEqual(seconds_until_next_route(routes=routes, now=now) or 0.0, 12.0, places=4)

    def test_wait_proxy_does_not_increment_bad_cycles(self) -> None:
        bad_cycles = 0
        for _ in range(20):
            bad_cycles = next_bad_cycle_count(
                current_bad_cycles=bad_cycles,
                outcome=CycleOutcome.WAIT_PROXY,
                category=ErrorCategory.NO_PROXY_AVAILABLE,
            )
        self.assertEqual(bad_cycles, 0)

    def test_worker_effective_interval_scales_with_low_capacity(self) -> None:
        # active ratio = 1/4 => 4x interval
        interval = compute_worker_effective_interval(
            base_interval=10,
            enabled_count=4,
            eligible_count=1,
            max_multiplier=5,
        )
        self.assertEqual(interval, 40)

    def test_wait_query_shard_does_not_increment_bad_cycles(self) -> None:
        bad_cycles = 2
        bad_cycles = next_bad_cycle_count(
            current_bad_cycles=bad_cycles,
            outcome=CycleOutcome.WAIT_QUERY_SHARD,
            category=ErrorCategory.QUERY_SHARD_LOCK_UNAVAILABLE,
        )
        self.assertEqual(bad_cycles, 2)

    def test_wait_proxy_mismatch_does_not_increment_bad_cycles(self) -> None:
        bad_cycles = 4
        bad_cycles = next_bad_cycle_count(
            current_bad_cycles=bad_cycles,
            outcome=CycleOutcome.WAIT_PROXY_MISMATCH,
            category=ErrorCategory.PROXY_MISMATCH,
        )
        self.assertEqual(bad_cycles, 4)

    def test_error_classification_profile_and_proxy_wait(self) -> None:
        self.assertEqual(
            classify_error("Profile lock unavailable for /tmp/profile_1"),
            ErrorCategory.PROFILE_LOCK_UNAVAILABLE,
        )
        self.assertEqual(
            classify_error("Selected route has no available proxy right now."),
            ErrorCategory.NO_PROXY_AVAILABLE,
        )
        self.assertEqual(
            classify_error("Query shard lock unavailable for QUERY_SHARD:abc"),
            ErrorCategory.QUERY_SHARD_LOCK_UNAVAILABLE,
        )
        self.assertEqual(
            classify_error("Pre-checkpoint signal risk score is critical."),
            ErrorCategory.SIGNAL_RISK,
        )
        self.assertEqual(
            classify_error("proxy_mismatch: expected=1.2.3.4 observed=5.6.7.8"),
            ErrorCategory.PROXY_MISMATCH,
        )
        self.assertEqual(
            classify_error("BROWSER_LAUNCH_FAILED: CDP connect failed for profile 7: ECONNREFUSED"),
            ErrorCategory.BROWSER_SESSION_LOST,
        )
        self.assertEqual(
            classify_error("Page.evaluate: Target page, context or browser has been closed"),
            ErrorCategory.BROWSER_SESSION_LOST,
        )

    def test_browser_session_loss_increments_bad_cycles(self) -> None:
        bad_cycles = next_bad_cycle_count(
            current_bad_cycles=1,
            outcome=CycleOutcome.FAIL,
            category=ErrorCategory.BROWSER_SESSION_LOST,
        )
        self.assertEqual(bad_cycles, 2)

    def test_route_state_machine_thresholds(self) -> None:
        self.assertEqual(
            derive_route_status_for_bad_cycles(bad_cycles=0, degraded_after=3, throttled_after=5),
            RouteStatus.ENABLED,
        )
        self.assertEqual(
            derive_route_status_for_bad_cycles(bad_cycles=3, degraded_after=3, throttled_after=5),
            RouteStatus.DEGRADED,
        )
        self.assertEqual(
            derive_route_status_for_bad_cycles(bad_cycles=5, degraded_after=3, throttled_after=5),
            RouteStatus.THROTTLED,
        )

    def test_route_status_interval_multiplier(self) -> None:
        self.assertEqual(route_status_interval_multiplier(RouteStatus.ENABLED), 1.0)
        self.assertEqual(route_status_interval_multiplier(RouteStatus.DEGRADED), 2.0)
        self.assertEqual(route_status_interval_multiplier(RouteStatus.THROTTLED), 4.0)


if __name__ == "__main__":
    unittest.main()
