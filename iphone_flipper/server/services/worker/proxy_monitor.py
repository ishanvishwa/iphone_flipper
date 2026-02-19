"""
Proxy Provider Real-Time Health Monitor

Polls the proxy gateway API to track utilization, thread-limit errors,
and bandwidth. Feeds health data into worker pacing decisions to prevent
account bans caused by proxy saturation.

Usage:
    from server.services.worker.proxy_monitor import get_proxy_health, start_monitor

    # Start the background polling loop (call once at worker startup)
    await start_monitor(gateway_token="<token>")

    # In each scrape cycle, check health before proceeding
    health = get_proxy_health()
    if health.should_back_off:
        # extend sleep interval
        sleep *= health.pacing_multiplier
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

try:
    import aiohttp
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore[assignment]

try:
    from notifications import send_telegram
except ImportError:  # pragma: no cover
    send_telegram = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

PROXY_PROVIDER_GATEWAY_TOKEN = os.getenv("PROXY_PROVIDER_GATEWAY_TOKEN", "").strip()
PROXY_PROVIDER_POLL_INTERVAL_SECONDS = max(
    5, int(os.getenv("PROXY_PROVIDER_POLL_INTERVAL_SECONDS", "15"))
)
PROXY_PROVIDER_API_BASE = "https://api.proxyrotator.com/proxy-gateways/realtime-usage"

# Pacing thresholds
_UTILIZATION_WARN = 0.60
_UTILIZATION_HIGH = 0.80
_ERROR_RATE_WARN = 0.50
_ERROR_RATE_HIGH = 0.80

# Telegram alert deduplication
_ALERT_COOLDOWN_SECONDS = 300  # at most once per 5 minutes
_last_alert_sent_at: float = 0.0


@dataclass(frozen=True)
class ProxyProviderHealth:
    """Snapshot of proxy gateway health metrics."""

    threads_connected: int = 0
    threads_total: int = 5
    utilization_ratio: float = 0.0  # 0.0 - 1.0
    success_count: int = 0
    thread_limit_errors: int = 0
    error_rate: float = 0.0  # thread_limit_errors / total_attempts
    bandwidth_mb: float = 0.0
    is_saturated: bool = False
    should_back_off: bool = False
    pacing_multiplier: float = 1.0
    last_fetched_at: float = 0.0  # time.monotonic()
    fetch_error: str | None = None

    @property
    def is_stale(self) -> bool:
        """Health data is stale if older than 3x poll interval."""
        if self.last_fetched_at <= 0:
            return True
        return (time.monotonic() - self.last_fetched_at) > (
            PROXY_PROVIDER_POLL_INTERVAL_SECONDS * 3
        )


def _compute_pacing_multiplier(utilization: float, error_rate: float) -> float:
    """Compute how much to multiply the scrape interval based on gateway health."""
    if error_rate >= _ERROR_RATE_HIGH:
        return 5.0
    if utilization >= _UTILIZATION_HIGH:
        return 3.0
    if utilization >= _UTILIZATION_WARN or error_rate >= _ERROR_RATE_WARN:
        return 1.5
    return 1.0


def _parse_gateway_response(data: dict[str, Any]) -> ProxyProviderHealth:
    """Parse raw gateway API JSON into a ProxyProviderHealth snapshot."""
    threads_connected = int(data.get("threadsConnected") or 0)

    threads_utilization_raw = str(data.get("threadsUtilization") or "0/5")
    parts = threads_utilization_raw.split("/")
    threads_used = int(parts[0]) if len(parts) >= 1 else 0
    threads_total = int(parts[1]) if len(parts) >= 2 else 5
    threads_total = max(1, threads_total)

    utilization_ratio = min(1.0, threads_used / threads_total)

    success_count = int(data.get("successfulConnections") or 0)
    thread_limit_errors = int(data.get("threadLimitReachedErrors") or 0)
    total_attempts = success_count + thread_limit_errors
    error_rate = (thread_limit_errors / total_attempts) if total_attempts > 0 else 0.0

    bandwidth_str = str(data.get("bandwidthTotalMB") or "0")
    try:
        bandwidth_mb = float(bandwidth_str)
    except (TypeError, ValueError):
        bandwidth_mb = 0.0

    is_saturated = utilization_ratio >= _UTILIZATION_HIGH
    pacing_multiplier = _compute_pacing_multiplier(utilization_ratio, error_rate)
    should_back_off = pacing_multiplier > 1.0

    return ProxyProviderHealth(
        threads_connected=threads_connected,
        threads_total=threads_total,
        utilization_ratio=round(utilization_ratio, 4),
        success_count=success_count,
        thread_limit_errors=thread_limit_errors,
        error_rate=round(error_rate, 4),
        bandwidth_mb=round(bandwidth_mb, 2),
        is_saturated=is_saturated,
        should_back_off=should_back_off,
        pacing_multiplier=pacing_multiplier,
        last_fetched_at=time.monotonic(),
    )


# ---------------------------------------------------------------------------
# Singleton state
# ---------------------------------------------------------------------------

_latest_health: ProxyProviderHealth = ProxyProviderHealth()
_monitor_task: asyncio.Task[None] | None = None
_previous_thread_limit_errors: int | None = None


def get_proxy_health() -> ProxyProviderHealth:
    """Return the most recent proxy provider health snapshot (non-blocking)."""
    return _latest_health


def get_proxy_health_dict() -> dict[str, Any]:
    """Return a dict representation suitable for API responses."""
    h = _latest_health
    return {
        "threads_connected": h.threads_connected,
        "threads_total": h.threads_total,
        "utilization_ratio": h.utilization_ratio,
        "success_count": h.success_count,
        "thread_limit_errors": h.thread_limit_errors,
        "error_rate": h.error_rate,
        "bandwidth_mb": h.bandwidth_mb,
        "is_saturated": h.is_saturated,
        "should_back_off": h.should_back_off,
        "pacing_multiplier": h.pacing_multiplier,
        "is_stale": h.is_stale,
        "fetch_error": h.fetch_error,
    }


async def fetch_proxy_provider_health(
    gateway_token: str | None = None,
) -> ProxyProviderHealth:
    """Fetch fresh health data from the proxy provider API."""
    global _latest_health, _previous_thread_limit_errors

    token = (gateway_token or PROXY_PROVIDER_GATEWAY_TOKEN).strip()
    if not token:
        return ProxyProviderHealth(
            fetch_error="No PROXY_PROVIDER_GATEWAY_TOKEN configured",
            last_fetched_at=time.monotonic(),
        )

    if aiohttp is None:
        return ProxyProviderHealth(
            fetch_error="aiohttp is not installed",
            last_fetched_at=time.monotonic(),
        )

    url = f"{PROXY_PROVIDER_API_BASE}/{token}/"
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    return ProxyProviderHealth(
                        fetch_error=f"HTTP {resp.status}: {error_text[:200]}",
                        last_fetched_at=time.monotonic(),
                    )
                data = await resp.json()
    except Exception as exc:
        return ProxyProviderHealth(
            fetch_error=f"Request failed: {str(exc)[:200]}",
            last_fetched_at=time.monotonic(),
        )

    health = _parse_gateway_response(data)

    # Detect error trend (rising thread-limit errors since last check)
    if _previous_thread_limit_errors is not None:
        error_delta = health.thread_limit_errors - _previous_thread_limit_errors
        if error_delta > 50:
            # Errors are rapidly increasing — escalate pacing
            health = ProxyProviderHealth(
                threads_connected=health.threads_connected,
                threads_total=health.threads_total,
                utilization_ratio=health.utilization_ratio,
                success_count=health.success_count,
                thread_limit_errors=health.thread_limit_errors,
                error_rate=health.error_rate,
                bandwidth_mb=health.bandwidth_mb,
                is_saturated=True,
                should_back_off=True,
                pacing_multiplier=max(health.pacing_multiplier, 5.0),
                last_fetched_at=health.last_fetched_at,
            )
            logger.warning(
                "Proxy provider thread-limit errors rising rapidly: "
                "+%d since last poll (total: %d)",
                error_delta,
                health.thread_limit_errors,
            )

    _previous_thread_limit_errors = health.thread_limit_errors
    _latest_health = health
    return health


def _should_send_proxy_health_alert() -> bool:
    """Return True if enough time has passed since the last Telegram alert."""
    global _last_alert_sent_at
    now = time.monotonic()
    if now - _last_alert_sent_at < _ALERT_COOLDOWN_SECONDS:
        return False
    _last_alert_sent_at = now
    return True


def _send_telegram_proxy_health_alert(health: ProxyProviderHealth) -> None:
    """Send a Telegram message about proxy provider degradation."""
    if send_telegram is None:
        return
    severity = "⛔ SATURATED" if health.is_saturated else "⚠️ HIGH LOAD"
    message = (
        f"🔌 Proxy Provider {severity}\n"
        f"Threads: {health.threads_connected}/{health.threads_total} "
        f"({health.utilization_ratio:.0%})\n"
        f"Error rate: {health.error_rate:.1%} "
        f"({health.thread_limit_errors} errors / {health.success_count + health.thread_limit_errors} total)\n"
        f"Bandwidth: {health.bandwidth_mb:.1f} MB\n"
        f"Pacing multiplier: {health.pacing_multiplier:.1f}x"
    )
    try:
        send_telegram(message)
    except Exception:
        logger.warning("Failed to send proxy health Telegram alert", exc_info=True)


async def _poll_loop(gateway_token: str) -> None:
    """Background loop that polls the proxy provider API."""
    while True:
        try:
            health = await fetch_proxy_provider_health(gateway_token)
            if health.fetch_error:
                logger.warning(
                    "Proxy provider health fetch error: %s", health.fetch_error
                )
            elif health.is_saturated or health.error_rate >= _ERROR_RATE_HIGH:
                logger.warning(
                    "Proxy provider SATURATED: %d/%d threads, "
                    "%.1f%% error rate, pacing=%.1fx",
                    health.threads_connected,
                    health.threads_total,
                    health.error_rate * 100,
                    health.pacing_multiplier,
                )
                if _should_send_proxy_health_alert():
                    _send_telegram_proxy_health_alert(health)
            else:
                logger.debug(
                    "Proxy provider OK: %d/%d threads, "
                    "%.1f%% error rate, pacing=%.1fx",
                    health.threads_connected,
                    health.threads_total,
                    health.error_rate * 100,
                    health.pacing_multiplier,
                )
        except Exception:
            logger.exception("Unexpected error in proxy provider poll loop")

        await asyncio.sleep(PROXY_PROVIDER_POLL_INTERVAL_SECONDS)


async def start_monitor(gateway_token: str | None = None) -> None:
    """Start background proxy provider monitoring (idempotent)."""
    global _monitor_task
    if _monitor_task is not None and not _monitor_task.done():
        return

    token = (gateway_token or PROXY_PROVIDER_GATEWAY_TOKEN).strip()
    if not token:
        logger.info(
            "Proxy provider monitoring disabled: no PROXY_PROVIDER_GATEWAY_TOKEN"
        )
        return

    logger.info("Starting proxy provider health monitor (poll every %ds)", PROXY_PROVIDER_POLL_INTERVAL_SECONDS)
    _monitor_task = asyncio.create_task(_poll_loop(token))


def stop_monitor() -> None:
    """Cancel the background monitor task."""
    global _monitor_task
    if _monitor_task is not None and not _monitor_task.done():
        _monitor_task.cancel()
        _monitor_task = None
