from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from server.services.common.stream_events import (
    LISTING_STREAM_SCHEMA_VERSION,
    ListingStreamEvent,
    _normalize_numeric,
    _normalize_text,
    _parse_optional_float,
    _parse_optional_int,
)

NOTIFICATION_DEAD_LETTER_STREAM_NAME = "stream:notification_dead_letter"
NOTIFICATION_DEAD_LETTER_STREAM_MAXLEN = 10_000
NOTIFICATION_DEAD_LETTER_SCHEMA_VERSION = "1"


@dataclass(frozen=True)
class NotificationDeadLetterEvent:
    schema_version: str
    original_stream_event_id: str
    listing_id: str
    worker_name: str
    route_name: str
    event_name: str
    query: str
    query_index: int | None
    query_total: int | None
    query_shard_key: str
    persisted_at: str
    price: float | None
    potential_profit: float | None
    title: str
    url: str
    thumbnail_url: str
    source: str
    failure_status: str
    last_error: str
    attempt_count: int
    dead_lettered_at: str

    def to_redis_fields(self) -> dict[str, str]:
        return {
            "schema_version": _normalize_text(self.schema_version or NOTIFICATION_DEAD_LETTER_SCHEMA_VERSION),
            "original_stream_event_id": _normalize_text(self.original_stream_event_id),
            "listing_id": _normalize_text(self.listing_id),
            "worker_name": _normalize_text(self.worker_name),
            "route_name": _normalize_text(self.route_name),
            "event_name": _normalize_text(self.event_name),
            "query": _normalize_text(self.query),
            "query_index": _normalize_text(self.query_index),
            "query_total": _normalize_text(self.query_total),
            "query_shard_key": _normalize_text(self.query_shard_key),
            "persisted_at": _normalize_text(self.persisted_at),
            "price": _normalize_numeric(self.price),
            "potential_profit": _normalize_numeric(self.potential_profit),
            "title": _normalize_text(self.title or "Untitled listing"),
            "url": _normalize_text(self.url),
            "thumbnail_url": _normalize_text(self.thumbnail_url),
            "source": _normalize_text(self.source),
            "failure_status": _normalize_text(self.failure_status),
            "last_error": _normalize_text(self.last_error),
            "attempt_count": _normalize_text(self.attempt_count),
            "dead_lettered_at": _normalize_text(self.dead_lettered_at),
        }

    @classmethod
    def from_redis_fields(cls, fields: Mapping[str, Any]) -> "NotificationDeadLetterEvent":
        return cls(
            schema_version=_normalize_text(fields.get("schema_version") or NOTIFICATION_DEAD_LETTER_SCHEMA_VERSION),
            original_stream_event_id=_normalize_text(fields.get("original_stream_event_id")),
            listing_id=_normalize_text(fields.get("listing_id")),
            worker_name=_normalize_text(fields.get("worker_name")),
            route_name=_normalize_text(fields.get("route_name")),
            event_name=_normalize_text(fields.get("event_name")),
            query=_normalize_text(fields.get("query")),
            query_index=_parse_optional_int(fields.get("query_index")),
            query_total=_parse_optional_int(fields.get("query_total")),
            query_shard_key=_normalize_text(fields.get("query_shard_key")),
            persisted_at=_normalize_text(fields.get("persisted_at")),
            price=_parse_optional_float(fields.get("price")),
            potential_profit=_parse_optional_float(fields.get("potential_profit")),
            title=_normalize_text(fields.get("title") or "Untitled listing"),
            url=_normalize_text(fields.get("url")),
            thumbnail_url=_normalize_text(fields.get("thumbnail_url")),
            source=_normalize_text(fields.get("source")),
            failure_status=_normalize_text(fields.get("failure_status")),
            last_error=_normalize_text(fields.get("last_error")),
            attempt_count=_parse_optional_int(fields.get("attempt_count")) or 0,
            dead_lettered_at=_normalize_text(fields.get("dead_lettered_at")),
        )

    def to_listing_stream_event(self) -> ListingStreamEvent:
        return ListingStreamEvent(
            schema_version=LISTING_STREAM_SCHEMA_VERSION,
            event_name=self.event_name,
            listing_id=self.listing_id,
            worker_name=self.worker_name,
            route_name=self.route_name,
            query=self.query,
            query_index=self.query_index,
            query_total=self.query_total,
            query_shard_key=self.query_shard_key,
            discovery_ts="",
            persisted_at=self.persisted_at,
            price=self.price,
            potential_profit=self.potential_profit,
            title=self.title,
            url=self.url,
            thumbnail_url=self.thumbnail_url,
            source=self.source,
            dedupe_kind="",
            mutable_hash="",
        )


def build_notification_dead_letter_event(
    *,
    stream_event_id: str,
    event: ListingStreamEvent,
    failure_status: str,
    last_error: str,
    attempt_count: int,
    dead_lettered_at: str,
) -> NotificationDeadLetterEvent:
    return NotificationDeadLetterEvent(
        schema_version=NOTIFICATION_DEAD_LETTER_SCHEMA_VERSION,
        original_stream_event_id=_normalize_text(stream_event_id),
        listing_id=_normalize_text(event.listing_id),
        worker_name=_normalize_text(event.worker_name),
        route_name=_normalize_text(event.route_name),
        event_name=_normalize_text(event.event_name),
        query=_normalize_text(event.query),
        query_index=event.query_index,
        query_total=event.query_total,
        query_shard_key=_normalize_text(event.query_shard_key),
        persisted_at=_normalize_text(event.persisted_at),
        price=event.price,
        potential_profit=event.potential_profit,
        title=_normalize_text(event.title),
        url=_normalize_text(event.url),
        thumbnail_url=_normalize_text(event.thumbnail_url),
        source=_normalize_text(event.source),
        failure_status=_normalize_text(failure_status),
        last_error=_normalize_text(last_error),
        attempt_count=max(0, int(attempt_count or 0)),
        dead_lettered_at=_normalize_text(dead_lettered_at),
    )
