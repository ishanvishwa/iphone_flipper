from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

LISTING_ENRICHMENT_STREAM_NAME = "stream:listing_enrichment"
LISTING_ENRICHMENT_STREAM_MAXLEN = 10_000
LISTING_ENRICHMENT_STREAM_SCHEMA_VERSION = "1"

HOT_LISTING_FIELDS: tuple[str, ...] = (
    "id",
    "title",
    "price",
    "location",
    "url",
    "thumbnail_url",
    "model",
    "condition",
    "max_buy_price",
    "max_offer",
    "potential_profit",
    "status",
    "source",
)

COLD_LISTING_FIELDS: tuple[str, ...] = (
    "description",
    "seller_name",
    "thumbnail_url",
)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_numeric(value: Any) -> str:
    if value is None or value == "":
        return ""
    return str(value).strip()


def _parse_optional_int(value: Any) -> int | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _pick_mapping_text(mapping: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, Mapping):
            nested = _pick_mapping_text(value, "uri", "url", "src")
            if nested:
                return nested
        text = _normalize_text(value)
        if text:
            return text
    return ""


def normalize_thumbnail_url(listing: Mapping[str, Any]) -> str:
    direct = _pick_mapping_text(
        listing,
        "thumbnail_url",
        "image_url",
        "photo_url",
        "image",
        "thumbnail",
        "cover_photo",
    )
    if direct:
        return direct

    for key in (
        "listing_photo",
        "primary_listing_photo",
        "photo",
        "primary_photo",
        "image",
        "thumbnail",
        "cover_photo",
    ):
        value = listing.get(key)
        if isinstance(value, Mapping):
            candidate = _pick_mapping_text(value, "image", "photo_image", "thumbnail_image", "uri", "url", "src")
            if candidate:
                return candidate

    photos = listing.get("listing_photos") or listing.get("photos") or listing.get("images")
    if isinstance(photos, list):
        for item in photos:
            if isinstance(item, Mapping):
                candidate = _pick_mapping_text(item, "image", "photo_image", "thumbnail_image", "uri", "url", "src")
                if candidate:
                    return candidate

    return ""


def build_enrichment_source_hash(cold_fields: Mapping[str, Any]) -> str:
    normalized = {
        key: _normalize_text(cold_fields.get(key))
        for key in COLD_LISTING_FIELDS
    }
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def split_listing_for_fast_path(listing: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, str], str]:
    thumbnail_url = normalize_thumbnail_url(listing)
    hot_listing: dict[str, Any] = {}
    for field in HOT_LISTING_FIELDS:
        if field == "thumbnail_url":
            hot_listing[field] = thumbnail_url or None
            continue
        if field in listing:
            hot_listing[field] = listing.get(field)

    if "max_buy_price" not in hot_listing and "max_offer" in listing:
        hot_listing["max_offer"] = listing.get("max_offer")

    cold_fields = {
        "description": _normalize_text(listing.get("description")),
        "seller_name": _normalize_text(listing.get("seller_name")),
        "thumbnail_url": thumbnail_url,
    }
    source_hash = build_enrichment_source_hash(cold_fields)
    return hot_listing, cold_fields, source_hash


def has_cold_enrichment_payload(cold_fields: Mapping[str, Any]) -> bool:
    return any(_normalize_text(cold_fields.get(field)) for field in COLD_LISTING_FIELDS)


@dataclass(frozen=True)
class ListingEnrichmentEvent:
    schema_version: str
    listing_id: str
    worker_name: str
    route_name: str
    query: str
    query_index: int | None
    query_total: int | None
    query_shard_key: str
    persisted_at: str
    enqueued_at: str
    enrichment_source_hash: str
    description: str
    seller_name: str
    thumbnail_url: str

    def to_redis_fields(self) -> dict[str, str]:
        return {
            "schema_version": _normalize_text(self.schema_version or LISTING_ENRICHMENT_STREAM_SCHEMA_VERSION),
            "listing_id": _normalize_text(self.listing_id),
            "worker_name": _normalize_text(self.worker_name),
            "route_name": _normalize_text(self.route_name),
            "query": _normalize_text(self.query),
            "query_index": _normalize_text(self.query_index),
            "query_total": _normalize_text(self.query_total),
            "query_shard_key": _normalize_text(self.query_shard_key),
            "persisted_at": _normalize_text(self.persisted_at),
            "enqueued_at": _normalize_text(self.enqueued_at),
            "enrichment_source_hash": _normalize_text(self.enrichment_source_hash),
            "description": _normalize_text(self.description),
            "seller_name": _normalize_text(self.seller_name),
            "thumbnail_url": _normalize_text(self.thumbnail_url),
        }

    @classmethod
    def from_redis_fields(cls, fields: Mapping[str, Any]) -> "ListingEnrichmentEvent":
        return cls(
            schema_version=_normalize_text(fields.get("schema_version") or LISTING_ENRICHMENT_STREAM_SCHEMA_VERSION),
            listing_id=_normalize_text(fields.get("listing_id")),
            worker_name=_normalize_text(fields.get("worker_name")),
            route_name=_normalize_text(fields.get("route_name")),
            query=_normalize_text(fields.get("query")),
            query_index=_parse_optional_int(fields.get("query_index")),
            query_total=_parse_optional_int(fields.get("query_total")),
            query_shard_key=_normalize_text(fields.get("query_shard_key")),
            persisted_at=_normalize_text(fields.get("persisted_at")),
            enqueued_at=_normalize_text(fields.get("enqueued_at")),
            enrichment_source_hash=_normalize_text(fields.get("enrichment_source_hash")),
            description=_normalize_text(fields.get("description")),
            seller_name=_normalize_text(fields.get("seller_name")),
            thumbnail_url=_normalize_text(fields.get("thumbnail_url")),
        )


def build_listing_enrichment_event(
    *,
    listing_id: str,
    metadata: Mapping[str, Any],
    worker_name: str,
    persisted_at: str,
    enqueued_at: str,
    cold_fields: Mapping[str, Any],
    enrichment_source_hash: str,
) -> ListingEnrichmentEvent:
    return ListingEnrichmentEvent(
        schema_version=LISTING_ENRICHMENT_STREAM_SCHEMA_VERSION,
        listing_id=_normalize_text(listing_id),
        worker_name=_normalize_text(worker_name),
        route_name=_normalize_text(metadata.get("route_name")),
        query=_normalize_text(metadata.get("query")),
        query_index=_parse_optional_int(metadata.get("query_index")),
        query_total=_parse_optional_int(metadata.get("query_total")),
        query_shard_key=_normalize_text(metadata.get("query_shard_key")),
        persisted_at=_normalize_text(persisted_at),
        enqueued_at=_normalize_text(enqueued_at),
        enrichment_source_hash=_normalize_text(enrichment_source_hash),
        description=_normalize_text(cold_fields.get("description")),
        seller_name=_normalize_text(cold_fields.get("seller_name")),
        thumbnail_url=_normalize_text(cold_fields.get("thumbnail_url")),
    )
