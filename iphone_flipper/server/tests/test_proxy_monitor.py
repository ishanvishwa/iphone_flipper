"""Tests for proxy_monitor.py — proxy provider health monitoring."""
from __future__ import annotations

import unittest

from server.services.worker.proxy_monitor import (
    ProxyProviderHealth,
    _compute_pacing_multiplier,
    _parse_gateway_response,
)


class ProxyMonitorTests(unittest.TestCase):
    """Unit tests for the proxy provider health monitor."""

    def test_parse_gateway_response_normal(self) -> None:
        data = {
            "threadsConnected": 2,
            "threadsUtilization": "2/5",
            "successfulConnections": 1975,
            "threadLimitReachedErrors": 100,
            "bandwidthTotalMB": "50.2",
        }
        health = _parse_gateway_response(data)
        self.assertEqual(health.threads_connected, 2)
        self.assertEqual(health.threads_total, 5)
        self.assertAlmostEqual(health.utilization_ratio, 0.4, places=2)
        self.assertEqual(health.success_count, 1975)
        self.assertEqual(health.thread_limit_errors, 100)
        self.assertAlmostEqual(health.bandwidth_mb, 50.2, places=1)
        self.assertFalse(health.is_saturated)

    def test_parse_gateway_response_saturated(self) -> None:
        data = {
            "threadsConnected": 5,
            "threadsUtilization": "5/5",
            "successfulConnections": 1975,
            "threadLimitReachedErrors": 14543,
            "bandwidthTotalMB": "50.0",
        }
        health = _parse_gateway_response(data)
        self.assertAlmostEqual(health.utilization_ratio, 1.0, places=2)
        self.assertTrue(health.is_saturated)
        self.assertTrue(health.should_back_off)
        self.assertGreaterEqual(health.pacing_multiplier, 3.0)

    def test_pacing_multiplier_normal(self) -> None:
        self.assertEqual(_compute_pacing_multiplier(0.3, 0.05), 1.0)

    def test_pacing_multiplier_warn(self) -> None:
        self.assertEqual(_compute_pacing_multiplier(0.65, 0.1), 1.5)

    def test_pacing_multiplier_high_utilization(self) -> None:
        self.assertEqual(_compute_pacing_multiplier(0.85, 0.1), 3.0)

    def test_pacing_multiplier_high_errors(self) -> None:
        self.assertEqual(_compute_pacing_multiplier(0.3, 0.85), 5.0)

    def test_parse_gateway_response_empty(self) -> None:
        health = _parse_gateway_response({})
        self.assertEqual(health.threads_connected, 0)
        self.assertEqual(health.success_count, 0)
        self.assertFalse(health.is_saturated)

    def test_health_is_stale_when_never_fetched(self) -> None:
        health = ProxyProviderHealth()
        self.assertTrue(health.is_stale)

    def test_error_rate_calculation(self) -> None:
        data = {
            "threadsConnected": 3,
            "threadsUtilization": "3/5",
            "successfulConnections": 500,
            "threadLimitReachedErrors": 500,
        }
        health = _parse_gateway_response(data)
        self.assertAlmostEqual(health.error_rate, 0.5, places=2)

    def test_zero_attempts_error_rate(self) -> None:
        data = {
            "threadsConnected": 0,
            "threadsUtilization": "0/5",
            "successfulConnections": 0,
            "threadLimitReachedErrors": 0,
        }
        health = _parse_gateway_response(data)
        self.assertAlmostEqual(health.error_rate, 0.0, places=2)


if __name__ == "__main__":
    unittest.main()
