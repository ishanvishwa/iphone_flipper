from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from server.services.worker.runtime import CycleOutcome, CycleResult, ErrorCategory
from server.services.worker.telemetry import build_cycle_telemetry_payload


class TelemetryPayloadTests(unittest.TestCase):
    def test_payload_shape_with_success_result(self) -> None:
        started = datetime(2026, 2, 16, 10, 0, 0, tzinfo=timezone.utc)
        finished = started + timedelta(seconds=2)
        result = CycleResult(
            metrics={
                "listings_saved": 3,
                "listings_scraped": 5,
                "listings_parsed": 5,
                "query_count": 2,
                "query_result_count": 2,
                "postgres_upsert_latency_ms_sum": 15,
                "postgres_upsert_latency_ms_count": 3,
                "redis_publish_latency_ms_sum": 9,
                "redis_publish_latency_ms_count": 3,
                "notification_delivery_latency_ms_sum": 11,
                "notification_delivery_latency_ms_count": 1,
                "end_to_end_alert_latency_ms_sum": 120,
                "end_to_end_alert_latency_ms_count": 1,
            },
            outcome=CycleOutcome.OK,
            retry_count=1,
            details={
                "proxy_key": "socks5:1.2.3.4:1080:",
                "query_shard_key": "iphone_15",
                "signal_action": "throttle",
                "signal_risk_score": 0.32,
                "soft_signals": ["LOW_RESULTS"],
                "persona_hash": "abc123",
                "persona": {
                    "viewport_width": 1440,
                    "viewport_height": 900,
                    "screen_width": 1460,
                    "screen_height": 980,
                    "device_scale_factor": 1.25,
                    "timezone_id": "America/Los_Angeles",
                    "locale": "en-US",
                    "color_scheme": "light",
                },
                "websocket_broadcast_latency_ms": 18,
                "websocket_broadcast_client_count": 2,
            },
        )
        payload = build_cycle_telemetry_payload(
            cycle_id="cycle-1",
            worker_name="worker",
            route_name="profile_1",
            started_at=started,
            finished_at=finished,
            result=result,
            fallback_proxy_key="socks5:fallback:1080:",
        )
        self.assertEqual(payload["event"], "scrape_cycle")
        self.assertEqual(payload["cycle_id"], "cycle-1")
        self.assertEqual(payload["worker_name"], "worker")
        self.assertEqual(payload["route_name"], "profile_1")
        self.assertEqual(payload["proxy_key"], "socks5:1.2.3.4:1080:")
        self.assertEqual(payload["duration_ms"], 2000)
        self.assertEqual(payload["outcome"], CycleOutcome.OK.value)
        self.assertEqual(payload["error_category"], ErrorCategory.NONE.value)
        self.assertEqual(payload["retry_count"], 1)
        self.assertEqual(payload["listings_saved"], 3)
        self.assertEqual(payload["listings_scraped"], 5)
        self.assertEqual(payload["listings_parsed"], 5)
        self.assertEqual(payload["query_count"], 2)
        self.assertEqual(payload["postgres_upsert_latency_ms_sum"], 15)
        self.assertEqual(payload["postgres_upsert_latency_ms_count"], 3)
        self.assertEqual(payload["redis_publish_latency_ms_count"], 3)
        self.assertEqual(payload["notification_delivery_latency_ms_sum"], 11)
        self.assertEqual(payload["notification_delivery_latency_ms_count"], 1)
        self.assertEqual(payload["end_to_end_alert_latency_ms_count"], 1)
        self.assertEqual(payload["signal_action"], "throttle")
        self.assertEqual(payload["query_shard_key"], "iphone_15")
        self.assertEqual(payload["soft_signals"], ["LOW_RESULTS"])
        self.assertEqual(payload["persona_hash"], "abc123")
        self.assertEqual(payload["persona"]["timezone_id"], "America/Los_Angeles")
        self.assertEqual(payload["persona"]["locale"], "en-US")
        self.assertEqual(payload["websocket_broadcast_latency_ms"], 18)
        self.assertEqual(payload["websocket_broadcast_client_count"], 2)

    def test_payload_uses_fallback_values_without_result(self) -> None:
        started = datetime(2026, 2, 16, 10, 0, 0, tzinfo=timezone.utc)
        finished = started + timedelta(milliseconds=450)
        payload = build_cycle_telemetry_payload(
            cycle_id="cycle-2",
            worker_name="worker_2",
            route_name="profile_2",
            started_at=started,
            finished_at=finished,
            result=None,
            fallback_proxy_key="auto_rotation",
        )
        self.assertEqual(payload["proxy_key"], "auto_rotation")
        self.assertEqual(payload["outcome"], CycleOutcome.FAIL.value)
        self.assertEqual(payload["error_category"], ErrorCategory.UNKNOWN.value)
        self.assertEqual(payload["duration_ms"], 450)

    def test_payload_includes_proxy_check_details(self) -> None:
        started = datetime(2026, 2, 16, 10, 0, 0, tzinfo=timezone.utc)
        finished = started + timedelta(seconds=1)
        result = CycleResult(
            metrics={},
            outcome=CycleOutcome.WAIT_PROXY_MISMATCH,
            reason="proxy_mismatch: expected=1.1.1.1 observed=2.2.2.2",
            error_category=ErrorCategory.PROXY_MISMATCH,
            details={
                "proxy_expected_ip": "1.1.1.1",
                "proxy_observed_ip": "2.2.2.2",
                "proxy_ip_check_status": "mismatch",
            },
        )
        payload = build_cycle_telemetry_payload(
            cycle_id="cycle-3",
            worker_name="worker",
            route_name="profile_3",
            started_at=started,
            finished_at=finished,
            result=result,
            fallback_proxy_key=None,
        )
        self.assertEqual(payload["outcome"], CycleOutcome.WAIT_PROXY_MISMATCH.value)
        self.assertEqual(payload["error_category"], ErrorCategory.PROXY_MISMATCH.value)
        self.assertEqual(payload["proxy_expected_ip"], "1.1.1.1")
        self.assertEqual(payload["proxy_observed_ip"], "2.2.2.2")
        self.assertEqual(payload["proxy_ip_check_status"], "mismatch")


if __name__ == "__main__":
    unittest.main()
