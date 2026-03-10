"""
Redis Streams notification consumer for listing alerts.

Consumes `stream:listings` through a Redis consumer group, deduplicates
notifications durably in Postgres, and sends Telegram alerts outside the
worker hot path.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from html import escape
import logging
import os
import socket
import time
from typing import Any

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from server.services.common.feature_flags import FLAG_HASH_KEY, RedisFeatureFlags
from server.services.common.notification_dead_letter import (
    NOTIFICATION_DEAD_LETTER_STREAM_MAXLEN,
    NOTIFICATION_DEAD_LETTER_STREAM_NAME,
    build_notification_dead_letter_event,
)
from server.services.common.observability import emit_json_log, monotonic_duration_ms, timestamp_delta_ms, utc_now_iso
from server.services.common.runtime_config import (
    RUNTIME_CONFIG_HASH_KEY,
    RedisRuntimeConfig,
)
from server.services.common.schema_ensure import ensure_worker_tables as ensure_common_worker_tables
from server.services.common.stream_events import LISTING_STREAM_NAME, ListingStreamEvent

logger = logging.getLogger(__name__)

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "iphone_flipper")
PGUSER = os.getenv("PGUSER", "flipper_app")
PGPASSWORD = os.getenv("PGPASSWORD", "")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_NOTIFY_MIN_PROFIT = float(os.getenv("TELEGRAM_NOTIFY_MIN_PROFIT", "0") or 0)

FCM_SERVICE_ACCOUNT_PATH = os.getenv("FCM_SERVICE_ACCOUNT_PATH", "firebase-adminsdk.json")
FCM_TOPIC = os.getenv("FCM_TOPIC", "new_iphones")

NOTIFICATION_CONSUMER_GROUP = "listing_notifications"
NOTIFICATION_CONSUMER_PREFIX = "notification-worker"
NOTIFICATION_STREAM_BLOCK_MS = 1000
NOTIFICATION_STREAM_READ_COUNT = 10
NOTIFICATION_CLAIM_IDLE_MS = 5000
NOTIFICATION_RETRY_ATTEMPTS = 3
NOTIFICATION_RETRY_BASE_DELAY_SECONDS = 1.0
NOTIFICATION_SEND_MIN_INTERVAL_SECONDS = 1.0
NOTIFICATION_LEDGER_TABLE = "notification_delivery_ledger"

_firebase_initialized = False


def _decode_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _parse_optional_float(value: Any) -> float | None:
    raw = _decode_text(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _fmt_money(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"${value:,.0f}"


def _isoformat_or_text(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = _decode_text(value).strip()
    return text or None


def _notification_consumer_name() -> str:
    return f"{NOTIFICATION_CONSUMER_PREFIX}-{socket.gethostname()}-{os.getpid()}"


async def _acquire_listing_advisory_lock(
    conn: asyncpg.Connection,
    listing_id: str,
) -> None:
    listing_id_clean = str(listing_id or "").strip()
    if not listing_id_clean:
        return
    await conn.fetchval(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0));",
        listing_id_clean,
    )


def _normalize_stream_entry_fields(fields: Any) -> dict[str, str]:
    payload = dict(fields or {})
    return {_decode_text(key): _decode_text(value) for key, value in payload.items()}


def _normalize_stream_entries(raw_entries: Any) -> list[tuple[str, dict[str, str]]]:
    normalized: list[tuple[str, dict[str, str]]] = []
    for entry in list(raw_entries or []):
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        stream_event_id = _decode_text(entry[0]).strip()
        if not stream_event_id:
            continue
        normalized.append((stream_event_id, _normalize_stream_entry_fields(entry[1])))
    return normalized


def _normalize_xreadgroup_result(raw_result: Any) -> list[tuple[str, dict[str, str]]]:
    normalized: list[tuple[str, dict[str, str]]] = []
    for stream_payload in list(raw_result or []):
        if not isinstance(stream_payload, (list, tuple)) or len(stream_payload) < 2:
            continue
        normalized.extend(_normalize_stream_entries(stream_payload[1]))
    return normalized


def _normalize_xautoclaim_result(raw_result: Any) -> tuple[str, list[tuple[str, dict[str, str]]]]:
    if not isinstance(raw_result, (list, tuple)):
        return "0-0", []
    next_start_id = _decode_text(raw_result[0]).strip() or "0-0"
    entries = _normalize_stream_entries(raw_result[1] if len(raw_result) > 1 else [])
    return next_start_id, entries


def _build_telegram_card(event: ListingStreamEvent) -> str:
    title = escape(str(event.title or "Untitled listing"))
    price = _fmt_money(event.price)
    profit = _fmt_money(event.potential_profit)
    lines = [
        "📱 <b>New Listing</b>",
        f"<b>{title}</b>",
        f"Price: {price}",
        f"Potential Profit: {profit}",
    ]
    source = str(event.source or "").strip()
    if source:
        lines.append(f"Source: {escape(source)}")
    thumbnail_url = str(event.thumbnail_url or "").strip()
    if thumbnail_url:
        lines.append(f"Thumbnail: <a href=\"{escape(thumbnail_url, quote=True)}\">Preview</a>")
    url = str(event.url or "").strip()
    if url:
        lines.append(f"Link: <a href=\"{escape(url, quote=True)}\">Open Listing</a>")
    return "\n".join(lines)


async def _send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured, skipping notification send")
        return False

    try:
        import aiohttp

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            async with session.post(url, json=payload) as response:
                if response.status != 200:
                    body = await response.text()
                    logger.warning(
                        "Telegram API returned %d: %s",
                        response.status,
                        body[:200],
                    )
                    return False
                return True
    except Exception as exc:
        logger.warning("Telegram send failed: %s", str(exc)[:200])
        return False


def _init_firebase() -> bool:
    global _firebase_initialized
    if _firebase_initialized:
        return True

    try:
        import firebase_admin
        from firebase_admin import credentials
    except ImportError:
        return False

    if not os.path.exists(FCM_SERVICE_ACCOUNT_PATH):
        return False

    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(FCM_SERVICE_ACCOUNT_PATH)
            firebase_admin.initialize_app(cred)
        _firebase_initialized = True
        return True
    except Exception as exc:
        logger.warning("Failed to initialize Firebase Admin SDK: %s", exc)
        return False


async def _send_fcm_push(event: ListingStreamEvent) -> bool:
    if not _init_firebase():
        return False

    try:
        from firebase_admin import messaging
    except ImportError:
        return False

    message = messaging.Message(
        data={
            "notification_type": "new_listing",
            "listing_id": str(event.listing_id),
            "title": str(event.title or ""),
            "price": str(event.price or ""),
            "profit": str(event.potential_profit or ""),
            "url": str(event.url or ""),
            "source": str(event.source or ""),
            "persisted_at": str(event.persisted_at or ""),
        },
        topic=FCM_TOPIC,
        android=messaging.AndroidConfig(priority="high"),
        apns=messaging.APNSConfig(
            headers={"apns-priority": "10"},
            payload=messaging.APNSPayload(
                aps=messaging.Aps(content_available=True)
            ),
        ),
    )

    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, messaging.send, message)
        return True
    except Exception as exc:
        logger.warning("FCM push failed: %s", exc)
        return False


def _notification_gate(event: ListingStreamEvent) -> tuple[bool, str]:
    if str(event.event_name or "").strip() != "listing_created":
        return False, "suppressed_non_created"
    title = str(event.title or "").strip()
    if not title or title == "Untitled listing":
        return False, "suppressed_invalid_title"
    profit = event.potential_profit
    if profit is None:
        return False, "suppressed_missing_profit"
    if profit < TELEGRAM_NOTIFY_MIN_PROFIT:
        return False, "suppressed_below_profit_threshold"
    return True, "eligible"


@dataclass
class NotificationLedgerEntry:
    listing_id: str
    first_stream_event_id: str
    last_stream_event_id: str
    status: str
    attempt_count: int
    last_error: str | None = None
    last_attempt_at: str | None = None
    sent_at: str | None = None


class NotificationLedgerStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @staticmethod
    def _entry_from_row(row: asyncpg.Record | None) -> NotificationLedgerEntry | None:
        if row is None:
            return None
        return NotificationLedgerEntry(
            listing_id=str(row["listing_id"]),
            first_stream_event_id=str(row["first_stream_event_id"] or ""),
            last_stream_event_id=str(row["last_stream_event_id"] or ""),
            status=str(row["status"] or ""),
            attempt_count=int(row["attempt_count"] or 0),
            last_error=str(row["last_error"]) if row["last_error"] is not None else None,
            last_attempt_at=_isoformat_or_text(row["last_attempt_at"]),
            sent_at=_isoformat_or_text(row["sent_at"]),
        )

    async def get(self, listing_id: str) -> NotificationLedgerEntry | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT
                    listing_id,
                    first_stream_event_id,
                    last_stream_event_id,
                    status,
                    attempt_count,
                    last_error,
                    last_attempt_at,
                    sent_at
                FROM {NOTIFICATION_LEDGER_TABLE}
                WHERE listing_id = $1
                """,
                listing_id,
            )
        return self._entry_from_row(row)

    async def prepare(self, listing_id: str, stream_event_id: str, should_send: bool, reason: str) -> str:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _acquire_listing_advisory_lock(conn, listing_id)
                row = await conn.fetchrow(
                    f"""
                    SELECT
                        listing_id,
                        first_stream_event_id,
                        last_stream_event_id,
                        status,
                        attempt_count,
                        last_error,
                        last_attempt_at,
                        sent_at
                    FROM {NOTIFICATION_LEDGER_TABLE}
                    WHERE listing_id = $1
                    FOR UPDATE
                    """,
                    listing_id,
                )
                entry = self._entry_from_row(row)

                if entry is not None and entry.sent_at:
                    await conn.execute(
                        f"""
                        UPDATE {NOTIFICATION_LEDGER_TABLE}
                        SET last_stream_event_id = $2,
                            updated_at = NOW()
                        WHERE listing_id = $1
                        """,
                        listing_id,
                        stream_event_id,
                    )
                    return "duplicate_already_sent"

                if not should_send:
                    if entry is None:
                        await conn.execute(
                            f"""
                            INSERT INTO {NOTIFICATION_LEDGER_TABLE} (
                                listing_id,
                                first_stream_event_id,
                                last_stream_event_id,
                                status,
                                attempt_count,
                                last_error,
                                last_attempt_at,
                                sent_at
                            ) VALUES ($1, $2, $2, 'suppressed', 0, $3, NULL, NULL)
                            """,
                            listing_id,
                            stream_event_id,
                            reason,
                        )
                    else:
                        await conn.execute(
                            f"""
                            UPDATE {NOTIFICATION_LEDGER_TABLE}
                            SET last_stream_event_id = $2,
                                status = 'suppressed',
                                last_error = $3,
                                updated_at = NOW()
                            WHERE listing_id = $1
                            """,
                            listing_id,
                            stream_event_id,
                            reason,
                        )
                    return "suppressed"

                if entry is None:
                    await conn.execute(
                        f"""
                        INSERT INTO {NOTIFICATION_LEDGER_TABLE} (
                            listing_id,
                            first_stream_event_id,
                            last_stream_event_id,
                            status,
                            attempt_count,
                            last_error,
                            last_attempt_at,
                            sent_at
                        ) VALUES ($1, $2, $2, 'pending', 0, NULL, NULL, NULL)
                        """,
                        listing_id,
                        stream_event_id,
                    )
                else:
                    await conn.execute(
                        f"""
                        UPDATE {NOTIFICATION_LEDGER_TABLE}
                        SET last_stream_event_id = $2,
                            updated_at = NOW()
                        WHERE listing_id = $1
                        """,
                        listing_id,
                        stream_event_id,
                    )
        return "send"

    async def mark_attempt_started(self, listing_id: str, stream_event_id: str) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _acquire_listing_advisory_lock(conn, listing_id)
                await conn.execute(
                    f"""
                    UPDATE {NOTIFICATION_LEDGER_TABLE}
                    SET last_stream_event_id = $2,
                        status = 'sending',
                        attempt_count = attempt_count + 1,
                        last_attempt_at = NOW(),
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE listing_id = $1
                    """,
                    listing_id,
                    stream_event_id,
                )

    async def mark_retry_pending(self, listing_id: str, stream_event_id: str, error: str) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _acquire_listing_advisory_lock(conn, listing_id)
                await conn.execute(
                    f"""
                    UPDATE {NOTIFICATION_LEDGER_TABLE}
                    SET last_stream_event_id = $2,
                        status = 'retry_pending',
                        last_error = $3,
                        updated_at = NOW()
                    WHERE listing_id = $1
                    """,
                    listing_id,
                    stream_event_id,
                    error,
                )

    async def mark_failed_terminal(self, listing_id: str, stream_event_id: str, error: str) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _acquire_listing_advisory_lock(conn, listing_id)
                await conn.execute(
                    f"""
                    UPDATE {NOTIFICATION_LEDGER_TABLE}
                    SET last_stream_event_id = $2,
                        status = 'failed_terminal',
                        last_error = $3,
                        updated_at = NOW()
                    WHERE listing_id = $1
                    """,
                    listing_id,
                    stream_event_id,
                    error,
                )

    async def mark_sent(self, listing_id: str, stream_event_id: str) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _acquire_listing_advisory_lock(conn, listing_id)
                await conn.execute(
                    f"""
                    UPDATE {NOTIFICATION_LEDGER_TABLE}
                    SET last_stream_event_id = $2,
                        status = 'sent',
                        last_error = NULL,
                        sent_at = NOW(),
                        updated_at = NOW()
                    WHERE listing_id = $1
                    """,
                    listing_id,
                    stream_event_id,
                )


class NotificationSendPacer:
    def __init__(self, min_interval_seconds: float = NOTIFICATION_SEND_MIN_INTERVAL_SECONDS) -> None:
        self._min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._next_allowed_at = 0.0

    async def wait_turn(self) -> None:
        now = time.monotonic()
        delay = self._next_allowed_at - now
        if delay > 0:
            await asyncio.sleep(delay)
            now = time.monotonic()
        self._next_allowed_at = max(self._next_allowed_at, now) + self._min_interval_seconds


@dataclass
class NotificationStats:
    events_received: int = 0
    notifications_sent: int = 0
    suppressed: int = 0
    duplicates: int = 0
    dead_lettered: int = 0
    errors: int = 0
    started_at: float = field(default_factory=time.monotonic)


class NotificationConsumer:
    def __init__(
        self,
        redis_client: Redis,
        feature_flags: RedisFeatureFlags,
        ledger: NotificationLedgerStore,
        runtime_config: RedisRuntimeConfig | None = None,
        *,
        consumer_group: str = NOTIFICATION_CONSUMER_GROUP,
        consumer_name: str | None = None,
        send_telegram_func: Any = _send_telegram,
        send_fcm_func: Any = _send_fcm_push,
        pacer: NotificationSendPacer | None = None,
        retry_attempts: int = NOTIFICATION_RETRY_ATTEMPTS,
        retry_base_delay_seconds: float = NOTIFICATION_RETRY_BASE_DELAY_SECONDS,
        claim_idle_ms: int = NOTIFICATION_CLAIM_IDLE_MS,
        read_count: int = NOTIFICATION_STREAM_READ_COUNT,
        block_ms: int = NOTIFICATION_STREAM_BLOCK_MS,
    ) -> None:
        self._redis = redis_client
        self._feature_flags = feature_flags
        self._runtime_config = runtime_config or RedisRuntimeConfig(None)
        self._ledger = ledger
        self._consumer_group = str(consumer_group or NOTIFICATION_CONSUMER_GROUP)
        self._consumer_name = str(consumer_name or _notification_consumer_name())
        self._send_telegram = send_telegram_func
        self._send_fcm = send_fcm_func
        self._pacer = pacer or NotificationSendPacer()
        self._retry_attempts = max(1, int(retry_attempts or 1))
        self._retry_base_delay_seconds = max(0.0, float(retry_base_delay_seconds or 0.0))
        self._claim_idle_ms = max(1, int(claim_idle_ms or 1))
        self._read_count = max(1, int(read_count or 1))
        self._block_ms = max(1, int(block_ms or 1))
        self.stats = NotificationStats()
        self._last_drain_state: bool | None = None

    @property
    def consumer_group(self) -> str:
        return self._consumer_group

    @property
    def consumer_name(self) -> str:
        return self._consumer_name

    async def ensure_consumer_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                LISTING_STREAM_NAME,
                self._consumer_group,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _ack(self, stream_event_id: str) -> None:
        await self._redis.xack(
            LISTING_STREAM_NAME,
            self._consumer_group,
            stream_event_id,
        )

    async def _claim_idle_entries(self) -> list[tuple[str, dict[str, str]]]:
        raw_result = await self._redis.xautoclaim(
            LISTING_STREAM_NAME,
            self._consumer_group,
            self._consumer_name,
            self._claim_idle_ms,
            "0-0",
            count=self._read_count,
        )
        _, entries = _normalize_xautoclaim_result(raw_result)
        return entries

    async def _read_new_entries(self) -> list[tuple[str, dict[str, str]]]:
        raw_result = await self._redis.xreadgroup(
            self._consumer_group,
            self._consumer_name,
            {LISTING_STREAM_NAME: ">"},
            count=self._read_count,
            block=self._block_ms,
        )
        return _normalize_xreadgroup_result(raw_result)

    async def _publish_dead_letter(
        self,
        event: ListingStreamEvent,
        *,
        stream_event_id: str,
        failure_status: str,
        last_error: str,
        attempt_count: int,
    ) -> tuple[str, str]:
        dead_lettered_at = utc_now_iso()
        payload = build_notification_dead_letter_event(
            stream_event_id=stream_event_id,
            event=event,
            failure_status=failure_status,
            last_error=last_error,
            attempt_count=attempt_count,
            dead_lettered_at=dead_lettered_at,
        )
        dead_letter_event_id = await self._redis.xadd(
            NOTIFICATION_DEAD_LETTER_STREAM_NAME,
            payload.to_redis_fields(),
            maxlen=NOTIFICATION_DEAD_LETTER_STREAM_MAXLEN,
            approximate=True,
        )
        return str(dead_letter_event_id), dead_lettered_at

    async def _drain_mode_enabled(self) -> bool:
        enabled = await self._runtime_config.get_bool("NOTIFICATION_CONSUMER_DRAIN")
        if self._last_drain_state is None or self._last_drain_state != enabled:
            self._last_drain_state = enabled
            emit_json_log(
                "notification_consumer_drain_state",
                service="notification_worker",
                stream_name=LISTING_STREAM_NAME,
                consumer_group=self._consumer_group,
                consumer_name=self._consumer_name,
                drain_enabled=enabled,
            )
        return enabled

    async def _attempt_delivery(self, event: ListingStreamEvent, stream_event_id: str) -> str:
        message = _build_telegram_card(event)
        last_error = "telegram_delivery_failed"

        for attempt in range(1, self._retry_attempts + 1):
            await self._ledger.mark_attempt_started(str(event.listing_id), stream_event_id)
            await self._pacer.wait_turn()
            delivery_started = time.monotonic()
            try:
                sent = await self._send_telegram(message)
            except Exception as exc:
                sent = False
                last_error = str(exc)[:500] or "telegram_delivery_failed"
            else:
                if not sent:
                    last_error = "telegram_delivery_failed"

            if sent:
                notification_sent_ts = utc_now_iso()
                await self._ledger.mark_sent(str(event.listing_id), stream_event_id)
                self.stats.notifications_sent += 1
                emit_json_log(
                    "notification_delivery_result",
                    listing_id=str(event.listing_id),
                    event_id=stream_event_id,
                    worker_name=str(event.worker_name or "") or None,
                    route_name=str(event.route_name or "") or None,
                    event_name=str(event.event_name or ""),
                    stream_name=LISTING_STREAM_NAME,
                    stream_event_id=stream_event_id,
                    notification_channel="telegram",
                    notification_status="sent",
                    persisted_at=str(event.persisted_at or "") or None,
                    notification_sent_ts=notification_sent_ts,
                    notification_delivery_latency_ms=monotonic_duration_ms(delivery_started),
                    persist_to_notification_latency_ms=timestamp_delta_ms(event.persisted_at, notification_sent_ts),
                    attempt=attempt,
                )
                if self._send_fcm is not None:
                    try:
                        await self._send_fcm(event)
                    except Exception:
                        logger.exception("FCM push failed for listing %s", event.listing_id)
                return "sent"

            if attempt < self._retry_attempts:
                await asyncio.sleep(self._retry_base_delay_seconds * (2 ** (attempt - 1)))

        try:
            dead_letter_event_id, dead_lettered_at = await self._publish_dead_letter(
                event,
                stream_event_id=stream_event_id,
                failure_status="failed_terminal",
                last_error=last_error,
                attempt_count=self._retry_attempts,
            )
        except Exception:
            await self._ledger.mark_retry_pending(str(event.listing_id), stream_event_id, last_error)
            emit_json_log(
                "notification_dead_letter_failed",
                listing_id=str(event.listing_id),
                event_id=stream_event_id,
                worker_name=str(event.worker_name or "") or None,
                route_name=str(event.route_name or "") or None,
                event_name=str(event.event_name or ""),
                stream_name=LISTING_STREAM_NAME,
                stream_event_id=stream_event_id,
                dead_letter_stream_name=NOTIFICATION_DEAD_LETTER_STREAM_NAME,
                notification_channel="telegram",
                notification_status="retry_pending",
                persisted_at=str(event.persisted_at or "") or None,
                last_error=last_error,
                attempt=self._retry_attempts,
            )
            return "retry_pending"

        await self._ledger.mark_failed_terminal(str(event.listing_id), stream_event_id, last_error)
        self.stats.dead_lettered += 1
        emit_json_log(
            "notification_dead_letter_written",
            listing_id=str(event.listing_id),
            event_id=stream_event_id,
            worker_name=str(event.worker_name or "") or None,
            route_name=str(event.route_name or "") or None,
            event_name=str(event.event_name or ""),
            stream_name=LISTING_STREAM_NAME,
            stream_event_id=stream_event_id,
            dead_letter_stream_name=NOTIFICATION_DEAD_LETTER_STREAM_NAME,
            dead_letter_event_id=dead_letter_event_id,
            dead_lettered_at=dead_lettered_at,
            notification_channel="telegram",
            notification_status="failed_terminal",
            persisted_at=str(event.persisted_at or "") or None,
            notification_sent_ts=None,
            last_error=last_error,
            attempt=self._retry_attempts,
        )
        return "dead_lettered"

    async def process_stream_entry(self, stream_event_id: str, fields: dict[str, str]) -> str:
        self.stats.events_received += 1
        event = ListingStreamEvent.from_redis_fields(fields)
        should_send, reason = _notification_gate(event)
        action = await self._ledger.prepare(
            listing_id=str(event.listing_id),
            stream_event_id=stream_event_id,
            should_send=should_send,
            reason=reason,
        )

        if action == "duplicate_already_sent":
            await self._ack(stream_event_id)
            self.stats.duplicates += 1
            emit_json_log(
                "notification_delivery_result",
                listing_id=str(event.listing_id),
                event_id=stream_event_id,
                worker_name=str(event.worker_name or "") or None,
                route_name=str(event.route_name or "") or None,
                event_name=str(event.event_name or ""),
                stream_name=LISTING_STREAM_NAME,
                stream_event_id=stream_event_id,
                notification_channel="telegram",
                notification_status="duplicate_already_sent",
                persisted_at=str(event.persisted_at or "") or None,
            )
            return action

        if action == "suppressed":
            await self._ack(stream_event_id)
            self.stats.suppressed += 1
            emit_json_log(
                "notification_delivery_result",
                listing_id=str(event.listing_id),
                event_id=stream_event_id,
                worker_name=str(event.worker_name or "") or None,
                route_name=str(event.route_name or "") or None,
                event_name=str(event.event_name or ""),
                stream_name=LISTING_STREAM_NAME,
                stream_event_id=stream_event_id,
                notification_channel="telegram",
                notification_status="suppressed",
                suppression_reason=reason,
                persisted_at=str(event.persisted_at or "") or None,
            )
            return action

        result = await self._attempt_delivery(event, stream_event_id)
        if result in {"sent", "dead_lettered"}:
            await self._ack(stream_event_id)
        return result

    async def run_once(self) -> int:
        if not await self._feature_flags.is_enabled("ENABLE_NOTIFICATION_CONSUMER"):
            await asyncio.sleep(1.0)
            return 0

        processed = 0
        drain_mode_enabled = await self._drain_mode_enabled()
        for stream_event_id, fields in await self._claim_idle_entries():
            try:
                await self.process_stream_entry(stream_event_id, fields)
                processed += 1
            except Exception:
                self.stats.errors += 1
                logger.exception("Failed to process reclaimed stream entry %s", stream_event_id)

        if not drain_mode_enabled:
            for stream_event_id, fields in await self._read_new_entries():
                try:
                    await self.process_stream_entry(stream_event_id, fields)
                    processed += 1
                except Exception:
                    self.stats.errors += 1
                    logger.exception("Failed to process new stream entry %s", stream_event_id)

        if processed and self.stats.events_received % 100 == 0:
            logger.info(
                "Notification stats: received=%d sent=%d suppressed=%d duplicates=%d dead_lettered=%d errors=%d",
                self.stats.events_received,
                self.stats.notifications_sent,
                self.stats.suppressed,
                self.stats.duplicates,
                self.stats.dead_lettered,
                self.stats.errors,
            )
        return processed


async def run_notification_worker() -> None:
    logger.info(
        "Notification worker starting (stream=%s group=%s min_profit=%s retry_attempts=%d min_interval=%.2fs)",
        LISTING_STREAM_NAME,
        NOTIFICATION_CONSUMER_GROUP,
        TELEGRAM_NOTIFY_MIN_PROFIT,
        NOTIFICATION_RETRY_ATTEMPTS,
        NOTIFICATION_SEND_MIN_INTERVAL_SECONDS,
    )

    pool = await asyncpg.create_pool(
        host=PGHOST,
        port=PGPORT,
        database=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        min_size=1,
        max_size=5,
    )
    await ensure_common_worker_tables(pool=pool, include_triggers=False)

    redis_client = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD or None,
        decode_responses=True,
    )
    feature_flags = RedisFeatureFlags(redis_client, hash_key=FLAG_HASH_KEY)
    runtime_config = RedisRuntimeConfig(redis_client, hash_key=RUNTIME_CONFIG_HASH_KEY)
    emit_json_log(
        "feature_flag_snapshot",
        service="notification_worker",
        flag_hash_key=FLAG_HASH_KEY,
        flags=await feature_flags.snapshot(),
    )
    emit_json_log(
        "runtime_config_snapshot",
        service="notification_worker",
        runtime_config_hash_key=RUNTIME_CONFIG_HASH_KEY,
        runtime_config=await runtime_config.snapshot(),
    )

    consumer = NotificationConsumer(
        redis_client=redis_client,
        feature_flags=feature_flags,
        runtime_config=runtime_config,
        ledger=NotificationLedgerStore(pool),
    )
    await consumer.ensure_consumer_group()
    emit_json_log(
        "notification_consumer_started",
        service="notification_worker",
        stream_name=LISTING_STREAM_NAME,
        consumer_group=consumer.consumer_group,
        consumer_name=consumer.consumer_name,
    )

    try:
        while True:
            await consumer.run_once()
    finally:
        await redis_client.aclose()
        await pool.close()
        logger.info("Notification worker shut down")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run_notification_worker())


if __name__ == "__main__":
    main()
