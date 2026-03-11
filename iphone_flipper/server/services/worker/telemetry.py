from __future__ import annotations

from datetime import datetime
from typing import Any

from .runtime import CycleOutcome, CycleResult, ErrorCategory


def build_cycle_telemetry_payload(
    cycle_id: str,
    worker_name: str,
    route_name: str,
    started_at: datetime,
    finished_at: datetime,
    result: CycleResult | None,
    fallback_proxy_key: str | None = None,
) -> dict[str, Any]:
    duration_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
    details = result.details if result and isinstance(result.details, dict) else {}
    metrics = result.metrics if result and isinstance(result.metrics, dict) else {}
    details_proxy_key = str(details.get("proxy_key") or "").strip() if details else ""
    payload: dict[str, Any] = {
        "event": "scrape_cycle",
        "cycle_id": cycle_id,
        "worker_name": str(worker_name or "unknown"),
        "route_name": str(route_name or "unknown"),
        "proxy_key": details_proxy_key or (str(fallback_proxy_key or "").strip() or None),
        "start_ts": started_at.isoformat(),
        "duration_ms": duration_ms,
        "outcome": (result.outcome.value if result else CycleOutcome.FAIL.value),
        "error_category": (result.error_category.value if result else ErrorCategory.UNKNOWN.value),
        "retry_count": int(result.retry_count if result else 0),
        "error": (result.reason or "")[:500] if result and result.reason else None,
    }

    if metrics:
        payload["listings_saved"] = int(metrics.get("listings_saved", 0) or 0)
        payload["listings_scraped"] = int(metrics.get("listings_scraped", 0) or 0)
        payload["listings_parsed"] = int(metrics.get("listings_parsed", metrics.get("listings_scraped", 0)) or 0)
        payload["query_count"] = int(metrics.get("query_count", 0) or 0)
        payload["query_result_count"] = int(metrics.get("query_result_count", 0) or 0)
        payload["profitable_listing_count"] = int(metrics.get("profitable_listing_count", 0) or 0)
        payload["duplicate_listing_count"] = int(metrics.get("duplicate_listing_count", 0) or 0)
        payload["query_lock_skipped_count"] = int(metrics.get("query_lock_skipped_count", 0) or 0)
        payload["query_lock_acquired_count"] = int(metrics.get("query_lock_acquired_count", 0) or 0)
        payload["postgres_upsert_latency_ms_sum"] = int(metrics.get("postgres_upsert_latency_ms_sum", 0) or 0)
        payload["postgres_upsert_latency_ms_count"] = int(metrics.get("postgres_upsert_latency_ms_count", 0) or 0)
        payload["redis_publish_latency_ms_sum"] = int(metrics.get("redis_publish_latency_ms_sum", 0) or 0)
        payload["redis_publish_latency_ms_count"] = int(metrics.get("redis_publish_latency_ms_count", 0) or 0)
        payload["redis_stream_publish_latency_ms_sum"] = int(
            metrics.get("redis_stream_publish_latency_ms_sum", 0) or 0
        )
        payload["redis_stream_publish_latency_ms_count"] = int(
            metrics.get("redis_stream_publish_latency_ms_count", 0) or 0
        )
        payload["redis_stream_publish_failure_count"] = int(
            metrics.get("redis_stream_publish_failure_count", 0) or 0
        )
        payload["v4_first_seen_gate_latency_ms_sum"] = int(
            metrics.get("v4_first_seen_gate_latency_ms_sum", 0) or 0
        )
        payload["v4_first_seen_gate_latency_ms_count"] = int(
            metrics.get("v4_first_seen_gate_latency_ms_count", 0) or 0
        )
        payload["v4_first_seen_claim_count"] = int(metrics.get("v4_first_seen_claim_count", 0) or 0)
        payload["v4_first_seen_duplicate_count"] = int(
            metrics.get("v4_first_seen_duplicate_count", 0) or 0
        )
        payload["v4_update_gate_latency_ms_sum"] = int(metrics.get("v4_update_gate_latency_ms_sum", 0) or 0)
        payload["v4_update_gate_latency_ms_count"] = int(
            metrics.get("v4_update_gate_latency_ms_count", 0) or 0
        )
        payload["v4_update_event_claim_count"] = int(metrics.get("v4_update_event_claim_count", 0) or 0)
        payload["v4_update_event_duplicate_count"] = int(
            metrics.get("v4_update_event_duplicate_count", 0) or 0
        )
        payload["v4_dedupe_fallback_count"] = int(metrics.get("v4_dedupe_fallback_count", 0) or 0)
        payload["notification_delivery_latency_ms_sum"] = int(
            metrics.get("notification_delivery_latency_ms_sum", 0) or 0
        )
        payload["notification_delivery_latency_ms_count"] = int(
            metrics.get("notification_delivery_latency_ms_count", 0) or 0
        )
        payload["end_to_end_alert_latency_ms_sum"] = int(metrics.get("end_to_end_alert_latency_ms_sum", 0) or 0)
        payload["end_to_end_alert_latency_ms_count"] = int(
            metrics.get("end_to_end_alert_latency_ms_count", 0) or 0
        )
        payload["enrichment_enqueue_latency_ms_sum"] = int(
            metrics.get("enrichment_enqueue_latency_ms_sum", 0) or 0
        )
        payload["enrichment_enqueue_latency_ms_count"] = int(
            metrics.get("enrichment_enqueue_latency_ms_count", 0) or 0
        )
        payload["enrichment_enqueue_failure_count"] = int(
            metrics.get("enrichment_enqueue_failure_count", 0) or 0
        )

    if details:
        signal_action = str(details.get("signal_action") or "").strip().lower()
        if signal_action:
            payload["signal_action"] = signal_action
        risk_score = details.get("signal_risk_score")
        if risk_score is not None:
            try:
                payload["signal_risk_score"] = round(float(risk_score), 4)
            except (TypeError, ValueError):
                pass
        soft_signals = details.get("soft_signals")
        if isinstance(soft_signals, list):
            payload["soft_signals"] = [str(item) for item in soft_signals if str(item).strip()]
        query_shard_key = str(details.get("query_shard_key") or "").strip()
        if query_shard_key:
            payload["query_shard_key"] = query_shard_key
        query_lock_key = str(details.get("query_lock_key") or "").strip()
        if query_lock_key:
            payload["query_lock_key"] = query_lock_key
        query_lock_status = str(details.get("query_lock_status") or "").strip()
        if query_lock_status:
            payload["query_lock_status"] = query_lock_status
        browser_launch_mode = str(details.get("browser_launch_mode") or "").strip()
        if browser_launch_mode:
            payload["browser_launch_mode"] = browser_launch_mode
        browser_failure_stage = str(details.get("browser_failure_stage") or "").strip()
        if browser_failure_stage:
            payload["browser_failure_stage"] = browser_failure_stage
        selected_query = str(details.get("selected_query") or "").strip()
        if selected_query:
            payload["selected_query"] = selected_query
        lane_override = details.get("lane_override")
        if lane_override is None or str(lane_override).strip() == "":
            payload["lane_override"] = None
        else:
            payload["lane_override"] = str(lane_override).strip()
        computed_lane = str(details.get("computed_lane") or "").strip()
        if computed_lane:
            payload["computed_lane"] = computed_lane
        effective_lane = str(details.get("effective_lane") or "").strip()
        if effective_lane:
            payload["effective_lane"] = effective_lane
        priority_score = details.get("priority_score")
        if priority_score is not None:
            try:
                payload["priority_score"] = round(float(priority_score), 4)
            except (TypeError, ValueError):
                pass
        priority_components = details.get("priority_components")
        if isinstance(priority_components, dict):
            payload["priority_components"] = {
                str(key): round(float(value), 4)
                for key, value in priority_components.items()
                if value is not None
            }
        route_due_age_seconds = details.get("route_due_age_seconds")
        if route_due_age_seconds is not None:
            try:
                payload["route_due_age_seconds"] = round(float(route_due_age_seconds), 3)
            except (TypeError, ValueError):
                pass
        route_revisit_age_seconds = details.get("route_revisit_age_seconds")
        if route_revisit_age_seconds is not None:
            try:
                payload["route_revisit_age_seconds"] = round(float(route_revisit_age_seconds), 3)
            except (TypeError, ValueError):
                pass
        lane_interval_multiplier = details.get("lane_interval_multiplier")
        if lane_interval_multiplier is not None:
            try:
                payload["lane_interval_multiplier"] = round(float(lane_interval_multiplier), 3)
            except (TypeError, ValueError):
                pass
        persona_hash = str(details.get("persona_hash") or "").strip()
        if persona_hash:
            payload["persona_hash"] = persona_hash
        persona = details.get("persona")
        if isinstance(persona, dict):
            payload["persona"] = {
                "viewport_width": int(persona.get("viewport_width") or 0),
                "viewport_height": int(persona.get("viewport_height") or 0),
                "screen_width": int(persona.get("screen_width") or 0),
                "screen_height": int(persona.get("screen_height") or 0),
                "device_scale_factor": float(persona.get("device_scale_factor") or 1.0),
                "timezone_id": str(persona.get("timezone_id") or "").strip(),
                "locale": str(persona.get("locale") or "").strip(),
                "color_scheme": str(persona.get("color_scheme") or "").strip(),
            }
        proxy_expected_ip = str(details.get("proxy_expected_ip") or "").strip()
        proxy_observed_ip = str(details.get("proxy_observed_ip") or "").strip()
        proxy_ip_check_status = str(details.get("proxy_ip_check_status") or "").strip().lower()
        if proxy_expected_ip:
            payload["proxy_expected_ip"] = proxy_expected_ip
        if proxy_observed_ip:
            payload["proxy_observed_ip"] = proxy_observed_ip
        if proxy_ip_check_status:
            payload["proxy_ip_check_status"] = proxy_ip_check_status

        # Proxy provider health metrics
        proxy_provider_utilization = details.get("proxy_provider_utilization")
        if proxy_provider_utilization is not None:
            try:
                payload["proxy_provider_utilization"] = round(float(proxy_provider_utilization), 4)
            except (TypeError, ValueError):
                pass
        proxy_provider_error_rate = details.get("proxy_provider_error_rate")
        if proxy_provider_error_rate is not None:
            try:
                payload["proxy_provider_error_rate"] = round(float(proxy_provider_error_rate), 4)
            except (TypeError, ValueError):
                pass
        proxy_provider_pacing = details.get("proxy_provider_pacing_multiplier")
        if proxy_provider_pacing is not None:
            try:
                payload["proxy_provider_pacing_multiplier"] = round(float(proxy_provider_pacing), 2)
            except (TypeError, ValueError):
                pass
        websocket_broadcast_latency_ms = details.get("websocket_broadcast_latency_ms")
        if websocket_broadcast_latency_ms is not None:
            try:
                payload["websocket_broadcast_latency_ms"] = int(websocket_broadcast_latency_ms)
            except (TypeError, ValueError):
                pass
        websocket_broadcast_client_count = details.get("websocket_broadcast_client_count")
        if websocket_broadcast_client_count is not None:
            try:
                payload["websocket_broadcast_client_count"] = int(websocket_broadcast_client_count)
            except (TypeError, ValueError):
                pass
    return payload
