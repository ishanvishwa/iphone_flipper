from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

LISTING_STREAM_NAME = "stream:listings"
LISTING_STREAM_MAXLEN = 10_000
LISTING_STREAM_SCHEMA_VERSION = "1"

LISTING_STREAM_COMPARISON_FIELDS: tuple[str, ...] = (
    "title",
    "price",
    "url",
    "model",
    "condition",
    "status",
    "max_buy_price",
    "potential_profit",
    "location",
)


def _normalize_text(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _normalize_numeric(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, Decimal):
        value = format(value, "f")
    return str(value).strip()


def _parse_optional_int(value: Any) -> int | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _parse_optional_float(value: Any) -> float | None:
    text = _normalize_numeric(value)
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _get_listing_max_buy_price(listing: Mapping[str, Any]) -> Any:
    if "max_buy_price" in listing:
        return listing.get("max_buy_price")
    return listing.get("max_offer")


def extract_listing_stream_state(listing: Mapping[str, Any]) -> dict[str, str]:
    return {
        "title": _normalize_text(listing.get("title") or "Untitled listing"),
        "price": _normalize_numeric(listing.get("price")),
        "url": _normalize_text(listing.get("url")),
        "model": _normalize_text(listing.get("model")),
        "condition": _normalize_text(listing.get("condition")),
        "status": _normalize_text(listing.get("status") or "new"),
        "max_buy_price": _normalize_numeric(_get_listing_max_buy_price(listing)),
        "potential_profit": _normalize_numeric(listing.get("potential_profit")),
        "location": _normalize_text(listing.get("location")),
    }


def extract_row_stream_state(row: Mapping[str, Any] | None) -> dict[str, str] | None:
    if not row:
        return None
    return {
        "title": _normalize_text(row.get("title") or "Untitled listing"),
        "price": _normalize_numeric(row.get("price")),
        "url": _normalize_text(row.get("url")),
        "model": _normalize_text(row.get("model")),
        "condition": _normalize_text(row.get("condition")),
        "status": _normalize_text(row.get("status") or "new"),
        "max_buy_price": _normalize_numeric(row.get("max_buy_price")),
        "potential_profit": _normalize_numeric(row.get("potential_profit")),
        "location": _normalize_text(row.get("location")),
    }


def has_meaningful_stream_state_change(
    previous_state: Mapping[str, Any] | None,
    current_state: Mapping[str, Any],
) -> bool:
    previous = extract_row_stream_state(previous_state)
    if previous is None:
        return True
    normalized_current = {
        key: _normalize_text(current_state.get(key))
        for key in LISTING_STREAM_COMPARISON_FIELDS
    }
    return previous != normalized_current


@dataclass(frozen=True)
class ListingStreamEvent:
    schema_version: str
    event_name: str
    listing_id: str
    worker_name: str
    route_name: str
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

    def to_redis_fields(self) -> dict[str, str]:
        return {
            "schema_version": _normalize_text(self.schema_version or LISTING_STREAM_SCHEMA_VERSION),
            "event_name": _normalize_text(self.event_name),
            "listing_id": _normalize_text(self.listing_id),
            "worker_name": _normalize_text(self.worker_name),
            "route_name": _normalize_text(self.route_name),
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
        }

    @classmethod
    def from_redis_fields(cls, fields: Mapping[str, Any]) -> "ListingStreamEvent":
        return cls(
            schema_version=_normalize_text(fields.get("schema_version") or LISTING_STREAM_SCHEMA_VERSION),
            event_name=_normalize_text(fields.get("event_name")),
            listing_id=_normalize_text(fields.get("listing_id")),
            worker_name=_normalize_text(fields.get("worker_name")),
            route_name=_normalize_text(fields.get("route_name")),
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
        )


def build_listing_stream_event(
    *,
    event_name: str,
    listing: Mapping[str, Any],
    metadata: Mapping[str, Any],
    worker_name: str,
    persisted_at: str,
) -> ListingStreamEvent:
    return ListingStreamEvent(
        schema_version=LISTING_STREAM_SCHEMA_VERSION,
        event_name=_normalize_text(event_name),
        listing_id=_normalize_text(listing.get("id")),
        worker_name=_normalize_text(worker_name),
        route_name=_normalize_text(metadata.get("route_name")),
        query=_normalize_text(metadata.get("query")),
        query_index=_parse_optional_int(metadata.get("query_index")),
        query_total=_parse_optional_int(metadata.get("query_total")),
        query_shard_key=_normalize_text(metadata.get("query_shard_key")),
        persisted_at=_normalize_text(persisted_at),
        price=_parse_optional_float(listing.get("price")),
        potential_profit=_parse_optional_float(listing.get("potential_profit")),
        title=_normalize_text(listing.get("title") or "Untitled listing"),
        url=_normalize_text(listing.get("url")),
        thumbnail_url=_normalize_text(listing.get("thumbnail_url")),
        source=_normalize_text(listing.get("source") or metadata.get("source")),
    )
