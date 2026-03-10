"""
Background listing enrichment consumer.

Consumes deferred cold-field enrichment jobs from Redis Streams and updates
listing rows without blocking the worker discovery-to-alert hot path.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
from dataclasses import dataclass, field
from typing import Any

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from server.services.common.enrichment_events import LISTING_ENRICHMENT_STREAM_NAME, ListingEnrichmentEvent
from server.services.common.feature_flags import FLAG_HASH_KEY, RedisFeatureFlags
from server.services.common.observability import emit_json_log, monotonic_duration_ms, timestamp_delta_ms, utc_now_iso
from server.services.common.runtime_config import RUNTIME_CONFIG_HASH_KEY, RedisRuntimeConfig
from server.services.common.schema_ensure import ensure_worker_tables as ensure_common_worker_tables

logger = logging.getLogger(__name__)

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "iphone_flipper")
PGUSER = os.getenv("PGUSER", "flipper_app")
PGPASSWORD = os.getenv("PGPASSWORD", "")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
LISTING_EVENT_CHANNEL = os.getenv("LISTING_EVENT_CHANNEL", "listing_events")

ENRICHMENT_CONSUMER_GROUP = "listing_enrichment"
ENRICHMENT_CONSUMER_PREFIX = "enrichment-worker"
ENRICHMENT_STREAM_BLOCK_MS = 1000
ENRICHMENT_STREAM_READ_COUNT = 10
ENRICHMENT_CLAIM_IDLE_MS = 5000
ENRICHMENT_RETRY_ATTEMPTS = 3
ENRICHMENT_RETRY_BASE_DELAY_SECONDS = 1.0
ENRICHMENT_LEDGER_TABLE = "listing_enrichment_ledger"


def _decode_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _isoformat_or_text(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    text = _decode_text(value).strip()
    return text or None


def _consumer_name() -> str:
    return f"{ENRICHMENT_CONSUMER_PREFIX}-{socket.gethostname()}-{os.getpid()}"


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


async def _acquire_listing_advisory_lock(conn: asyncpg.Connection, listing_id: str) -> None:
    listing_id_clean = str(listing_id or "").strip()
    if not listing_id_clean:
        return
    await conn.fetchval(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0));",
        listing_id_clean,
    )


@dataclass
class ListingEnrichmentLedgerEntry:
    listing_id: str
    first_stream_event_id: str
    last_stream_event_id: str
    enrichment_source_hash: str
    status: str
    attempt_count: int
    last_error: str | None = None
    last_attempt_at: str | None = None
    enriched_at: str | None = None


class ListingEnrichmentLedgerStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @staticmethod
    def _entry_from_row(row: asyncpg.Record | None) -> ListingEnrichmentLedgerEntry | None:
        if row is None:
            return None
        return ListingEnrichmentLedgerEntry(
            listing_id=str(row["listing_id"]),
            first_stream_event_id=str(row["first_stream_event_id"] or ""),
            last_stream_event_id=str(row["last_stream_event_id"] or ""),
            enrichment_source_hash=str(row["enrichment_source_hash"] or ""),
            status=str(row["status"] or ""),
            attempt_count=int(row["attempt_count"] or 0),
            last_error=str(row["last_error"]) if row["last_error"] is not None else None,
            last_attempt_at=_isoformat_or_text(row["last_attempt_at"]),
            enriched_at=_isoformat_or_text(row["enriched_at"]),
        )

    async def get(self, listing_id: str) -> ListingEnrichmentLedgerEntry | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT
                    listing_id,
                    first_stream_event_id,
                    last_stream_event_id,
                    enrichment_source_hash,
                    status,
                    attempt_count,
                    last_error,
                    last_attempt_at,
                    enriched_at
                FROM {ENRICHMENT_LEDGER_TABLE}
                WHERE listing_id = $1
                """,
                listing_id,
            )
        return self._entry_from_row(row)

    async def prepare(self, listing_id: str, stream_event_id: str, enrichment_source_hash: str) -> str:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await _acquire_listing_advisory_lock(conn, listing_id)
                row = await conn.fetchrow(
                    f"""
                    SELECT
                        listing_id,
                        first_stream_event_id,
                        last_stream_event_id,
                        enrichment_source_hash,
                        status,
                        attempt_count,
                        last_error,
                        last_attempt_at,
                        enriched_at
                    FROM {ENRICHMENT_LEDGER_TABLE}
                    WHERE listing_id = $1
                    """,
                    listing_id,
                )
                entry = self._entry_from_row(row)
                if entry is None:
                    await conn.execute(
                        f"""
                        INSERT INTO {ENRICHMENT_LEDGER_TABLE} (
                            listing_id,
                            first_stream_event_id,
                            last_stream_event_id,
                            enrichment_source_hash,
                            status,
                            attempt_count,
                            updated_at
                        ) VALUES ($1, $2, $2, $3, 'pending', 0, NOW())
                        """,
                        listing_id,
                        stream_event_id,
                        enrichment_source_hash,
                    )
                    return "enrich"

                if entry.enrichment_source_hash == enrichment_source_hash:
                    await conn.execute(
                        f"""
                        UPDATE {ENRICHMENT_LEDGER_TABLE}
                        SET last_stream_event_id = $2, updated_at = NOW()
                        WHERE listing_id = $1
                        """,
                        listing_id,
                        stream_event_id,
                    )
                    if entry.status == "complete":
                        return "duplicate_complete"
                    if entry.status in {"pending", "processing"}:
                        return "duplicate_inflight"

                await conn.execute(
                    f"""
                    UPDATE {ENRICHMENT_LEDGER_TABLE}
                    SET
                        last_stream_event_id = $2,
                        enrichment_source_hash = $3,
                        status = 'pending',
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE listing_id = $1
                    """,
                    listing_id,
                    stream_event_id,
                    enrichment_source_hash,
                )
                return "enrich"

    async def mark_attempt_started(self, listing_id: str, stream_event_id: str, enrichment_source_hash: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {ENRICHMENT_LEDGER_TABLE}
                SET
                    last_stream_event_id = $2,
                    enrichment_source_hash = $3,
                    status = 'processing',
                    attempt_count = COALESCE(attempt_count, 0) + 1,
                    last_error = NULL,
                    last_attempt_at = NOW(),
                    updated_at = NOW()
                WHERE listing_id = $1
                """,
                listing_id,
                stream_event_id,
                enrichment_source_hash,
            )

    async def mark_complete(self, listing_id: str, stream_event_id: str, enrichment_source_hash: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {ENRICHMENT_LEDGER_TABLE}
                SET
                    last_stream_event_id = $2,
                    enrichment_source_hash = $3,
                    status = 'complete',
                    last_error = NULL,
                    enriched_at = NOW(),
                    updated_at = NOW()
                WHERE listing_id = $1
                """,
                listing_id,
                stream_event_id,
                enrichment_source_hash,
            )

    async def mark_failed(
        self,
        listing_id: str,
        stream_event_id: str,
        enrichment_source_hash: str,
        error_text: str,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {ENRICHMENT_LEDGER_TABLE}
                SET
                    last_stream_event_id = $2,
                    enrichment_source_hash = $3,
                    status = 'failed',
                    last_error = $4,
                    updated_at = NOW()
                WHERE listing_id = $1
                """,
                listing_id,
                stream_event_id,
                enrichment_source_hash,
                error_text[:2000] or None,
            )


@dataclass
class EnrichmentStats:
    events_received: int = 0
    completed: int = 0
    duplicates: int = 0
    failures: int = 0
    started_at: float = field(default_factory=time.monotonic)


class EnrichmentConsumer:
    def __init__(
        self,
        redis_client: Redis,
        feature_flags: RedisFeatureFlags,
        pool: asyncpg.Pool,
        ledger: ListingEnrichmentLedgerStore,
        *,
        consumer_group: str = ENRICHMENT_CONSUMER_GROUP,
        consumer_name: str | None = None,
        retry_attempts: int = ENRICHMENT_RETRY_ATTEMPTS,
        retry_base_delay_seconds: float = ENRICHMENT_RETRY_BASE_DELAY_SECONDS,
        claim_idle_ms: int = ENRICHMENT_CLAIM_IDLE_MS,
        read_count: int = ENRICHMENT_STREAM_READ_COUNT,
        block_ms: int = ENRICHMENT_STREAM_BLOCK_MS,
    ) -> None:
        self._redis = redis_client
        self._feature_flags = feature_flags
        self._pool = pool
        self._ledger = ledger
        self._consumer_group = str(consumer_group or ENRICHMENT_CONSUMER_GROUP)
        self._consumer_name = str(consumer_name or _consumer_name())
        self._retry_attempts = max(1, int(retry_attempts or 1))
        self._retry_base_delay_seconds = max(0.0, float(retry_base_delay_seconds or 0.0))
        self._claim_idle_ms = max(1, int(claim_idle_ms or 1))
        self._read_count = max(1, int(read_count or 1))
        self._block_ms = max(1, int(block_ms or 1))
        self.stats = EnrichmentStats()

    @property
    def consumer_group(self) -> str:
        return self._consumer_group

    @property
    def consumer_name(self) -> str:
        return self._consumer_name

    async def ensure_consumer_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                LISTING_ENRICHMENT_STREAM_NAME,
                self._consumer_group,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _ack(self, stream_event_id: str) -> None:
        await self._redis.xack(
            LISTING_ENRICHMENT_STREAM_NAME,
            self._consumer_group,
            stream_event_id,
        )

    async def _claim_idle_entries(self) -> list[tuple[str, dict[str, str]]]:
        raw_result = await self._redis.xautoclaim(
            LISTING_ENRICHMENT_STREAM_NAME,
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
            {LISTING_ENRICHMENT_STREAM_NAME: ">"},
            count=self._read_count,
            block=self._block_ms,
        )
        return _normalize_xreadgroup_result(raw_result)

    async def _publish_listing_updated(self, event: ListingEnrichmentEvent) -> None:
        payload = {
            "event": "listing_updated",
            "at": utc_now_iso(),
            "worker": event.worker_name,
            "route_name": event.route_name,
            "query": event.query,
            "query_index": event.query_index,
            "query_total": event.query_total,
            "listing": {"id": event.listing_id},
            "source": "enrichment",
        }
        await self._redis.publish(LISTING_EVENT_CHANNEL, json.dumps(payload, default=str))

    async def _apply_enrichment(self, event: ListingEnrichmentEvent) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE listings
                SET
                    description = COALESCE(NULLIF($2, ''), description),
                    seller_name = COALESCE(NULLIF($3, ''), seller_name),
                    thumbnail_url = COALESCE(NULLIF($4, ''), thumbnail_url),
                    enrichment_status = 'complete',
                    enrichment_source_hash = $5,
                    enrichment_last_error = NULL,
                    enriched_at = NOW(),
                    updated_at = NOW()
                WHERE id = $1
                RETURNING id
                """,
                event.listing_id,
                event.description,
                event.seller_name,
                event.thumbnail_url,
                event.enrichment_source_hash,
            )
        return row is not None

    async def process_stream_entry(self, stream_event_id: str, fields: dict[str, str]) -> str:
        self.stats.events_received += 1
        event = ListingEnrichmentEvent.from_redis_fields(fields)
        action = await self._ledger.prepare(
            listing_id=event.listing_id,
            stream_event_id=stream_event_id,
            enrichment_source_hash=event.enrichment_source_hash,
        )
        if action in {"duplicate_complete", "duplicate_inflight"}:
            await self._ack(stream_event_id)
            self.stats.duplicates += 1
            emit_json_log(
                "listing_enrichment_completed",
                listing_id=event.listing_id,
                event_id=stream_event_id,
                worker_name=event.worker_name or None,
                route_name=event.route_name or None,
                enrichment_stream_name=LISTING_ENRICHMENT_STREAM_NAME,
                enrichment_source_hash=event.enrichment_source_hash,
                enrichment_status=action,
                persisted_at=event.persisted_at or None,
                enqueued_at=event.enqueued_at or None,
                queue_latency_ms=timestamp_delta_ms(event.enqueued_at, utc_now_iso()),
                processing_latency_ms=0,
            )
            return action

        last_error = "listing_not_found"
        for attempt in range(1, self._retry_attempts + 1):
            await self._ledger.mark_attempt_started(
                event.listing_id,
                stream_event_id,
                event.enrichment_source_hash,
            )
            started_at = time.monotonic()
            emit_json_log(
                "listing_enrichment_started",
                listing_id=event.listing_id,
                event_id=stream_event_id,
                worker_name=event.worker_name or None,
                route_name=event.route_name or None,
                enrichment_stream_name=LISTING_ENRICHMENT_STREAM_NAME,
                enrichment_source_hash=event.enrichment_source_hash,
                attempt=attempt,
                persisted_at=event.persisted_at or None,
                enqueued_at=event.enqueued_at or None,
            )
            try:
                updated = await self._apply_enrichment(event)
                if not updated:
                    last_error = "listing_not_found"
                else:
                    if any(
                        (
                            str(event.description or "").strip(),
                            str(event.seller_name or "").strip(),
                            str(event.thumbnail_url or "").strip(),
                        )
                    ):
                        await self._publish_listing_updated(event)
                    await self._ledger.mark_complete(
                        event.listing_id,
                        stream_event_id,
                        event.enrichment_source_hash,
                    )
                    await self._ack(stream_event_id)
                    self.stats.completed += 1
                    enrichment_completed_ts = utc_now_iso()
                    emit_json_log(
                        "listing_enrichment_completed",
                        listing_id=event.listing_id,
                        event_id=stream_event_id,
                        worker_name=event.worker_name or None,
                        route_name=event.route_name or None,
                        enrichment_stream_name=LISTING_ENRICHMENT_STREAM_NAME,
                        enrichment_source_hash=event.enrichment_source_hash,
                        enrichment_status="complete",
                        persisted_at=event.persisted_at or None,
                        enqueued_at=event.enqueued_at or None,
                        enrichment_completed_ts=enrichment_completed_ts,
                        queue_latency_ms=timestamp_delta_ms(event.enqueued_at, enrichment_completed_ts),
                        processing_latency_ms=monotonic_duration_ms(started_at),
                        attempt=attempt,
                    )
                    return "complete"
            except Exception as exc:
                last_error = str(exc)[:500] or "enrichment_failed"

            if attempt < self._retry_attempts:
                await asyncio.sleep(self._retry_base_delay_seconds * (2 ** (attempt - 1)))

        await self._ledger.mark_failed(
            event.listing_id,
            stream_event_id,
            event.enrichment_source_hash,
            last_error,
        )
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE listings
                SET
                    enrichment_status = 'failed',
                    enrichment_source_hash = $2,
                    enrichment_last_error = $3,
                    updated_at = NOW()
                WHERE id = $1
                """,
                event.listing_id,
                event.enrichment_source_hash,
                last_error[:2000] or None,
            )
        await self._ack(stream_event_id)
        self.stats.failures += 1
        emit_json_log(
            "listing_enrichment_failed",
            listing_id=event.listing_id,
            event_id=stream_event_id,
            worker_name=event.worker_name or None,
            route_name=event.route_name or None,
            enrichment_stream_name=LISTING_ENRICHMENT_STREAM_NAME,
            enrichment_source_hash=event.enrichment_source_hash,
            persisted_at=event.persisted_at or None,
            enqueued_at=event.enqueued_at or None,
            queue_latency_ms=timestamp_delta_ms(event.enqueued_at, utc_now_iso()),
            error=last_error,
            attempt=self._retry_attempts,
        )
        return "failed"

    async def run_once(self) -> int:
        if not await self._feature_flags.is_enabled("ENABLE_BACKGROUND_ENRICHMENT"):
            await asyncio.sleep(1.0)
            return 0

        processed = 0
        for stream_event_id, fields in await self._claim_idle_entries():
            try:
                await self.process_stream_entry(stream_event_id, fields)
                processed += 1
            except Exception:
                logger.exception("Failed to process reclaimed enrichment entry %s", stream_event_id)

        for stream_event_id, fields in await self._read_new_entries():
            try:
                await self.process_stream_entry(stream_event_id, fields)
                processed += 1
            except Exception:
                logger.exception("Failed to process new enrichment entry %s", stream_event_id)

        return processed


async def run_enrichment_worker() -> None:
    logger.info(
        "Enrichment worker starting (stream=%s group=%s retry_attempts=%d)",
        LISTING_ENRICHMENT_STREAM_NAME,
        ENRICHMENT_CONSUMER_GROUP,
        ENRICHMENT_RETRY_ATTEMPTS,
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
        service="enrichment_worker",
        flag_hash_key=FLAG_HASH_KEY,
        flags=await feature_flags.snapshot(),
    )
    emit_json_log(
        "runtime_config_snapshot",
        service="enrichment_worker",
        runtime_config_hash_key=RUNTIME_CONFIG_HASH_KEY,
        runtime_config=await runtime_config.snapshot(),
    )
    consumer = EnrichmentConsumer(
        redis_client=redis_client,
        feature_flags=feature_flags,
        pool=pool,
        ledger=ListingEnrichmentLedgerStore(pool),
    )
    await consumer.ensure_consumer_group()
    emit_json_log(
        "listing_enrichment_consumer_started",
        service="enrichment_worker",
        stream_name=LISTING_ENRICHMENT_STREAM_NAME,
        consumer_group=consumer.consumer_group,
        consumer_name=consumer.consumer_name,
    )

    try:
        while True:
            await consumer.run_once()
    finally:
        await redis_client.aclose()
        await pool.close()
        logger.info("Enrichment worker shut down")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    asyncio.run(run_enrichment_worker())


if __name__ == "__main__":
    main()
