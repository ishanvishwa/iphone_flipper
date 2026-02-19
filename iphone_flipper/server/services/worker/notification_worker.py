"""
Event-Driven Notification Worker

Subscribes to Redis `listing_events` channel and delivers notifications
with sub-second latency for profitable listings.

Features:
- Priority tiers: instant (profit >= $50), fast-batch (>= $0), suppressed (< $0)
- Deduplication via Redis SET (60s window)
- Rate limiting (max 30 notifications/minute)
- Telegram delivery with listing card formatting

Usage:
    python -m server.services.worker.notification_worker
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

NOTIFY_INSTANT_PROFIT_THRESHOLD = float(
    os.getenv("NOTIFY_INSTANT_PROFIT_THRESHOLD", "50")
)
NOTIFY_BATCH_INTERVAL_SECONDS = float(
    os.getenv("NOTIFY_BATCH_INTERVAL_SECONDS", "10")
)
NOTIFY_MAX_PER_MINUTE = int(os.getenv("NOTIFY_MAX_PER_MINUTE", "30"))
NOTIFY_DEDUP_WINDOW_SECONDS = int(os.getenv("NOTIFY_DEDUP_WINDOW_SECONDS", "60"))

LISTING_EVENTS_CHANNEL = "listing_events"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class ListingEvent:
    """Parsed listing event from Redis pub/sub."""

    event_type: str  # "listing_created", "listing_updated"
    listing_id: str | int
    model: str
    price: float
    profit: float
    url: str
    condition: str = ""
    description: str = ""
    source: str = ""
    worker_name: str = ""
    route_name: str = ""
    timestamp: str = ""


def _parse_listing_event(raw: str) -> ListingEvent | None:
    """Parse a raw Redis message into a ListingEvent."""
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Failed to parse listing event: %s", raw[:200])
        return None

    event_type = str(data.get("event") or data.get("event_type") or "").strip()
    if not event_type:
        return None

    return ListingEvent(
        event_type=event_type,
        listing_id=data.get("listing_id") or data.get("id") or "",
        model=str(data.get("model") or data.get("title") or "Unknown"),
        price=float(data.get("price") or 0),
        profit=float(data.get("profit") or data.get("estimated_profit") or 0),
        url=str(data.get("url") or data.get("link") or ""),
        condition=str(data.get("condition") or ""),
        description=str(data.get("description") or "")[:200],
        source=str(data.get("source") or ""),
        worker_name=str(data.get("worker_name") or ""),
        route_name=str(data.get("route_name") or ""),
        timestamp=str(data.get("timestamp") or data.get("created_at") or ""),
    )


# ---------------------------------------------------------------------------
# Telegram delivery
# ---------------------------------------------------------------------------


def _build_telegram_card(event: ListingEvent) -> str:
    """Build a Telegram message card for a listing."""
    profit_emoji = "🟢" if event.profit >= 50 else "🟡" if event.profit >= 0 else "🔴"
    lines = [
        f"{profit_emoji} *New Listing Found*",
        f"📱 *{_escape_md(event.model)}*",
        f"💰 Price: ${event.price:,.0f}",
        f"📈 Est. Profit: ${event.profit:,.0f}",
    ]
    if event.condition:
        lines.append(f"📋 Condition: {_escape_md(event.condition)}")
    if event.url:
        lines.append(f"🔗 [View Listing]({event.url})")
    if event.source:
        lines.append(f"🏪 Source: {_escape_md(event.source)}")
    return "\n".join(lines)


def _escape_md(text: str) -> str:
    """Escape Telegram MarkdownV2 special characters."""
    special = r"_*[]()~`>#+-=|{}.!"
    result = []
    for char in text:
        if char in special:
            result.append(f"\\{char}")
        else:
            result.append(char)
    return "".join(result)


async def _send_telegram(message: str) -> bool:
    """Send a message via Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured, skipping notification")
        return False

    try:
        import aiohttp

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": False,
        }
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(
                        "Telegram API returned %d: %s", resp.status, body[:200]
                    )
                    return False
                return True
    except Exception as exc:
        logger.warning("Telegram send failed: %s", str(exc)[:200])
        return False


# ---------------------------------------------------------------------------
# Rate limiter and deduplication
# ---------------------------------------------------------------------------


class RateLimiter:
    """Sliding-window rate limiter for notifications."""

    def __init__(self, max_per_minute: int = 30) -> None:
        self._max = max(1, max_per_minute)
        self._timestamps: deque[float] = deque()

    def allow(self) -> bool:
        now = time.monotonic()
        # Remove timestamps older than 60 seconds
        while self._timestamps and (now - self._timestamps[0]) > 60:
            self._timestamps.popleft()
        if len(self._timestamps) >= self._max:
            return False
        self._timestamps.append(now)
        return True

    @property
    def remaining(self) -> int:
        now = time.monotonic()
        while self._timestamps and (now - self._timestamps[0]) > 60:
            self._timestamps.popleft()
        return max(0, self._max - len(self._timestamps))


class Deduplicator:
    """In-memory deduplication for listing notifications."""

    def __init__(self, window_seconds: int = 60) -> None:
        self._window = max(1, window_seconds)
        self._seen: dict[str, float] = {}

    def is_duplicate(self, listing_id: str | int) -> bool:
        key = str(listing_id)
        now = time.monotonic()
        # Clean expired entries
        expired_keys = [
            k for k, ts in self._seen.items() if (now - ts) > self._window
        ]
        for k in expired_keys:
            del self._seen[k]

        if key in self._seen:
            return True
        self._seen[key] = now
        return False


# ---------------------------------------------------------------------------
# Notification dispatcher
# ---------------------------------------------------------------------------


@dataclass
class NotificationStats:
    """Runtime statistics for the notification worker."""

    events_received: int = 0
    notifications_sent: int = 0
    deduplicated: int = 0
    rate_limited: int = 0
    suppressed_negative: int = 0
    batched: int = 0
    errors: int = 0
    started_at: float = field(default_factory=time.monotonic)


class NotificationDispatcher:
    """Processes listing events and dispatches notifications by priority tier."""

    def __init__(
        self,
        instant_threshold: float = 50.0,
        batch_interval: float = 10.0,
        max_per_minute: int = 30,
        dedup_window: int = 60,
    ) -> None:
        self.instant_threshold = instant_threshold
        self.batch_interval = batch_interval
        self.rate_limiter = RateLimiter(max_per_minute)
        self.deduplicator = Deduplicator(dedup_window)
        self.stats = NotificationStats()
        self._batch_queue: list[ListingEvent] = []
        self._batch_task: asyncio.Task[None] | None = None

    async def process_event(self, event: ListingEvent) -> None:
        """Route an event to the appropriate notification tier."""
        self.stats.events_received += 1

        # Only notify for new listings
        if event.event_type != "listing_created":
            return

        # Deduplication
        if self.deduplicator.is_duplicate(event.listing_id):
            self.stats.deduplicated += 1
            return

        # Suppress negative profit unless configured otherwise
        if event.profit < 0:
            self.stats.suppressed_negative += 1
            return

        # Instant tier: high-profit listings
        if event.profit >= self.instant_threshold:
            await self._send_instant(event)
        else:
            # Fast-batch tier
            self._batch_queue.append(event)
            self.stats.batched += 1
            self._ensure_batch_flush()

    async def _send_instant(self, event: ListingEvent) -> None:
        """Send a notification immediately for high-profit listings."""
        if not self.rate_limiter.allow():
            self.stats.rate_limited += 1
            logger.warning(
                "Rate limited: skipping instant notification for listing %s (profit=$%.0f)",
                event.listing_id,
                event.profit,
            )
            return

        card = _build_telegram_card(event)
        success = await _send_telegram(card)
        if success:
            self.stats.notifications_sent += 1
            logger.info(
                "Instant notification sent for listing %s (profit=$%.0f)",
                event.listing_id,
                event.profit,
            )
        else:
            self.stats.errors += 1

    def _ensure_batch_flush(self) -> None:
        """Start the batch flush timer if not already running."""
        if self._batch_task is None or self._batch_task.done():
            self._batch_task = asyncio.create_task(self._flush_batch_after_delay())

    async def _flush_batch_after_delay(self) -> None:
        """Wait for batch interval, then flush all queued notifications."""
        await asyncio.sleep(self.batch_interval)
        await self.flush_batch()

    async def flush_batch(self) -> None:
        """Send all queued batch notifications."""
        if not self._batch_queue:
            return

        to_send = self._batch_queue[:]
        self._batch_queue.clear()

        # Cap batch to rate limiter capacity
        for event in to_send:
            if not self.rate_limiter.allow():
                self.stats.rate_limited += 1
                continue
            card = _build_telegram_card(event)
            success = await _send_telegram(card)
            if success:
                self.stats.notifications_sent += 1
            else:
                self.stats.errors += 1
            # Small delay between batch sends to avoid Telegram rate limits
            await asyncio.sleep(0.1)


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------


async def run_notification_worker() -> None:
    """Main entry point: subscribe to Redis and process listing events."""
    try:
        import redis.asyncio as aioredis
    except ImportError:
        logger.error("redis.asyncio is required. Install with: pip install redis[hiredis]")
        return

    logger.info(
        "Notification worker starting (instant>=$%.0f, batch=%ds, max=%d/min)",
        NOTIFY_INSTANT_PROFIT_THRESHOLD,
        NOTIFY_BATCH_INTERVAL_SECONDS,
        NOTIFY_MAX_PER_MINUTE,
    )

    dispatcher = NotificationDispatcher(
        instant_threshold=NOTIFY_INSTANT_PROFIT_THRESHOLD,
        batch_interval=NOTIFY_BATCH_INTERVAL_SECONDS,
        max_per_minute=NOTIFY_MAX_PER_MINUTE,
        dedup_window=NOTIFY_DEDUP_WINDOW_SECONDS,
    )

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(LISTING_EVENTS_CHANNEL)

    logger.info("Subscribed to Redis channel: %s", LISTING_EVENTS_CHANNEL)

    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue

            raw_data = str(message.get("data") or "")
            if not raw_data:
                continue

            event = _parse_listing_event(raw_data)
            if event is None:
                continue

            try:
                await dispatcher.process_event(event)
            except Exception:
                dispatcher.stats.errors += 1
                logger.exception(
                    "Error processing event for listing %s", event.listing_id
                )

            # Log stats periodically
            if dispatcher.stats.events_received % 100 == 0:
                s = dispatcher.stats
                logger.info(
                    "Notification stats: received=%d sent=%d dedup=%d "
                    "rate_limited=%d suppressed=%d errors=%d",
                    s.events_received,
                    s.notifications_sent,
                    s.deduplicated,
                    s.rate_limited,
                    s.suppressed_negative,
                    s.errors,
                )
    finally:
        await pubsub.unsubscribe(LISTING_EVENTS_CHANNEL)
        await redis_client.aclose()
        logger.info("Notification worker shut down")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run_notification_worker())


if __name__ == "__main__":
    main()
