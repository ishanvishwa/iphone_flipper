import asyncio
import json
import os
import time
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping
from urllib.parse import unquote, urlparse
from uuid import uuid4

import asyncpg
from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from server.services.common.feature_flags import DEFAULT_FEATURE_FLAGS, FLAG_HASH_KEY, RedisFeatureFlags
from server.services.common.v42_family_catalog import (
    V42_FAMILY_CATALOG_VERSION,
    V42_FAMILY_PRESETS,
)
from server.services.common.notification_dead_letter import (
    NOTIFICATION_DEAD_LETTER_STREAM_NAME,
    NotificationDeadLetterEvent,
)
from server.services.common.observability import emit_json_log, monotonic_duration_ms, utc_now_iso
from server.services.common.route_lanes import normalize_route_lane
from server.services.common.runtime_config import (
    DEFAULT_RUNTIME_CONFIG,
    RUNTIME_CONFIG_HASH_KEY,
    RedisRuntimeConfig,
    parse_runtime_config_value,
)
from server.services.common.schema_ensure import ensure_worker_tables as ensure_common_worker_tables
from server.services.common.stream_events import LISTING_STREAM_MAXLEN, LISTING_STREAM_NAME, ListingStreamEvent
from server.services.common.enrichment_events import LISTING_ENRICHMENT_STREAM_NAME

APP_ENV = os.getenv("APP_ENV", "production")
API_TOKEN = os.getenv("APP_API_TOKEN", "")
LISTING_EVENT_CHANNEL = os.getenv("LISTING_EVENT_CHANNEL", "listing_events")

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "iphone_flipper")
PGUSER = os.getenv("PGUSER", "flipper_app")
PGPASSWORD = os.getenv("PGPASSWORD", "")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
try:
    WORKER_MIN_ENABLED_ROUTES_WARN = max(1, int(os.getenv("WORKER_MIN_ENABLED_ROUTES_WARN", "2")))
except (TypeError, ValueError):
    WORKER_MIN_ENABLED_ROUTES_WARN = 2

app = FastAPI(title="iPhone Flipper API", version="1.0.0")

LISTING_SELECT_COLUMNS = """
    seq_id,
    id,
    title,
    price,
    location,
    url,
    description,
    seller_name,
    thumbnail_url,
    model,
    condition,
    max_buy_price,
    potential_profit,
    status,
    source_seen_at,
    created_at,
    updated_at
"""
WEBSOCKET_PING_INTERVAL_SECONDS = 15.0
WEBSOCKET_PONG_TIMEOUT_SECONDS = 35.0


@dataclass(slots=True)
class ManagedWebSocketClient:
    websocket: WebSocket
    client_id: str = field(default_factory=lambda: uuid4().hex[:12])
    connected_at: float = field(default_factory=time.monotonic)
    last_pong_at: float = field(default_factory=time.monotonic)
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send_json(self, payload: dict[str, Any]) -> None:
        async with self.send_lock:
            await self.websocket.send_json(payload)

    async def close(self, *, code: int = 1000, reason: str = "") -> None:
        async with self.send_lock:
            try:
                await self.websocket.close(code=code, reason=reason)
            except TypeError:
                await self.websocket.close(code=code)


class WebSocketConnectionManager:
    def __init__(self) -> None:
        self._clients: dict[str, ManagedWebSocketClient] = {}
        self._lock = asyncio.Lock()

    async def register(self, websocket: WebSocket) -> ManagedWebSocketClient:
        client = ManagedWebSocketClient(websocket=websocket)
        async with self._lock:
            self._clients[client.client_id] = client
        return client

    async def unregister(self, client: ManagedWebSocketClient) -> None:
        async with self._lock:
            self._clients.pop(client.client_id, None)

    async def snapshot(self) -> list[ManagedWebSocketClient]:
        async with self._lock:
            return list(self._clients.values())

    async def active_count(self) -> int:
        async with self._lock:
            return len(self._clients)


class WorkerRouteUpsert(BaseModel):
    is_enabled: bool = True
    proxy_server: str | None = Field(default=None, max_length=512)
    proxy_username: str | None = None
    proxy_password: str | None = None
    proxy_mode: str = Field(default="fixed", max_length=32)
    proxy_pool: str | None = None
    user_data_dir: str | None = None
    search_queries: str | None = None
    priority: int = Field(default=100, ge=0, le=100000)
    lane_override: str | None = Field(default=None, max_length=16)
    route_interval_seconds: int | None = Field(default=None, ge=1, le=86400)
    route_status: str | None = Field(default=None, max_length=32)
    status_reason: str | None = None
    manual_login_required: bool | None = None
    manual_login_reason: str | None = None


class CentralRouteUpsert(BaseModel):
    is_enabled: bool = True
    proxy_server: str | None = Field(default=None, max_length=512)
    proxy_username: str | None = None
    proxy_password: str | None = None
    proxy_mode: str = Field(default="fixed", max_length=32)
    proxy_pool: str | None = None
    search_queries: str | None = None
    priority: int = Field(default=100, ge=0, le=100000)
    lane_override: str | None = Field(default=None, max_length=16)
    route_interval_seconds: int | None = Field(default=None, ge=1, le=86400)
    route_status: str | None = Field(default=None, max_length=32)
    status_reason: str | None = None


class RouteQuerySetUpdateRequest(BaseModel):
    queries: list[str] = Field(default_factory=list)


class WorkerRouteRetestRequest(BaseModel):
    reason: str | None = None


class WorkerRouteBulkClearRequest(BaseModel):
    reason: str | None = None


class ExecutionProfileManualLoginClearRequest(BaseModel):
    user_data_dir: str
    reason: str | None = None


class ProxyStatsResetRequest(BaseModel):
    proxy_key: str | None = None
    proxy_server: str | None = None
    proxy_username: str | None = None
    clear_failures: bool = True


class RuntimeConfigUpdateRequest(BaseModel):
    feature_flags: dict[str, bool | None] | None = None
    runtime_config: dict[str, bool | float | None] | None = None


class NotificationReplayRequest(BaseModel):
    window_minutes: int = Field(default=60, ge=1, le=1440)
    limit: int = Field(default=100, ge=1, le=500)
    dry_run: bool = False
    listing_id: str | None = Field(default=None, max_length=255)


class QueryVariantUpsertRequest(BaseModel):
    query_text: str = Field(min_length=1, max_length=512)
    validation_state: str = Field(default="pending_validation", max_length=64)
    is_enabled: bool = True
    weight: float = Field(default=1.0, ge=0.0, le=1000.0)
    notes: str | None = None


class QueryFamilyUpsertRequest(BaseModel):
    previous_name: str | None = Field(default=None, max_length=255)
    is_enabled: bool = True
    priority: int = Field(default=100, ge=0, le=100000)
    lane: str = Field(default="warm", max_length=16)
    min_gap_s: int = Field(default=5, ge=0, le=86400)
    max_gap_s: int | None = Field(default=None, ge=0, le=86400)
    legacy_route_name: str | None = Field(default=None, max_length=255)
    legacy_worker_name: str | None = Field(default=None, max_length=255)
    variants: list[QueryVariantUpsertRequest] = Field(default_factory=list)


def _assert_api_token(token: str | None) -> None:
    if not API_TOKEN:
        if APP_ENV == "production":
            raise HTTPException(status_code=401, detail="APP_API_TOKEN is not configured.")
        return
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


async def _auth_rest(x_api_token: str | None) -> None:
    _assert_api_token(x_api_token)


def _payload_field_is_set(payload: BaseModel, field_name: str) -> bool:
    fields = getattr(payload, "model_fields_set", None)
    if isinstance(fields, set):
        return field_name in fields
    legacy_fields = getattr(payload, "__fields_set__", set())
    return field_name in legacy_fields


def _serialize_datetimes(item: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    for key in keys:
        value = item.get(key)
        if hasattr(value, "isoformat"):
            item[key] = value.isoformat()
    return item


def _extract_listing_observation_fields(payload: dict[str, Any]) -> dict[str, Any] | None:
    event_name = str(payload.get("event") or "").strip()
    if event_name not in {"listing_created", "listing_updated"}:
        return None

    listing = payload.get("listing")
    listing_payload = listing if isinstance(listing, dict) else {}
    listing_id = str(listing_payload.get("id") or payload.get("listing_id") or "").strip()
    if not listing_id:
        return None

    worker_name = (
        str(payload.get("worker_name") or "").strip()
        or str(payload.get("worker") or "").strip()
        or None
    )
    route_name = str(payload.get("route_name") or "").strip() or None
    event_id = str(payload.get("event_id") or "").strip() or None
    return {
        "listing_id": listing_id,
        "event_id": event_id,
        "worker_name": worker_name,
        "route_name": route_name,
        "event_name": event_name,
    }


def _client_host(websocket: WebSocket) -> str | None:
    client = getattr(websocket, "client", None)
    if client is None:
        return None
    host = getattr(client, "host", None)
    if host:
        return str(host)
    if isinstance(client, tuple) and client:
        return str(client[0])
    return None


async def _is_gui_websocket_push_enabled() -> bool:
    feature_flags: RedisFeatureFlags | None = getattr(app.state, "feature_flags", None)
    if feature_flags is None:
        return False
    return await feature_flags.is_enabled("ENABLE_GUI_WEBSOCKET_PUSH")


async def _runtime_config_snapshot() -> dict[str, bool | float]:
    runtime_config: RedisRuntimeConfig | None = getattr(app.state, "runtime_config", None)
    if runtime_config is None:
        return dict(DEFAULT_RUNTIME_CONFIG)
    return await runtime_config.snapshot()


def _stream_id_from_epoch_ms(epoch_ms: int) -> str:
    return f"{max(0, int(epoch_ms))}-0"


def _normalize_stream_info(raw_info: Any) -> dict[str, Any]:
    if not isinstance(raw_info, dict):
        return {"length": 0, "last_generated_id": None, "last_entry_id": None}
    last_entry = raw_info.get("last-entry")
    last_entry_id = None
    if isinstance(last_entry, (list, tuple)) and last_entry:
        last_entry_id = str(last_entry[0])
    return {
        "length": int(raw_info.get("length") or 0),
        "last_generated_id": str(raw_info.get("last-generated-id") or "") or None,
        "last_entry_id": last_entry_id,
    }


def _normalize_group_info(raw_groups: Any) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    for raw_group in list(raw_groups or []):
        if not isinstance(raw_group, dict):
            continue
        groups.append(
            {
                "name": str(raw_group.get("name") or ""),
                "consumers": int(raw_group.get("consumers") or 0),
                "pending": int(raw_group.get("pending") or 0),
                "lag": int(raw_group.get("lag") or 0),
                "last_delivered_id": str(raw_group.get("last-delivered-id") or "") or None,
                "entries_read": int(raw_group.get("entries-read") or 0),
            }
        )
    return groups


def _normalize_xpending_summary(raw_pending: Any) -> dict[str, Any]:
    if isinstance(raw_pending, dict):
        consumers = raw_pending.get("consumers") or []
        normalized_consumers: list[dict[str, Any]] = []
        for item in list(consumers):
            if isinstance(item, dict):
                normalized_consumers.append(
                    {
                        "name": str(item.get("name") or ""),
                        "pending": int(item.get("pending") or 0),
                    }
                )
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                normalized_consumers.append({"name": str(item[0] or ""), "pending": int(item[1] or 0)})
        return {
            "pending": int(raw_pending.get("pending") or 0),
            "min": str(raw_pending.get("min") or "") or None,
            "max": str(raw_pending.get("max") or "") or None,
            "consumers": normalized_consumers,
        }
    if isinstance(raw_pending, (list, tuple)) and len(raw_pending) >= 4:
        consumers = []
        for item in list(raw_pending[3] or []):
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                consumers.append({"name": str(item[0] or ""), "pending": int(item[1] or 0)})
        return {
            "pending": int(raw_pending[0] or 0),
            "min": str(raw_pending[1] or "") or None,
            "max": str(raw_pending[2] or "") or None,
            "consumers": consumers,
        }
    return {"pending": 0, "min": None, "max": None, "consumers": []}


async def _fetch_stream_backlog(stream_name: str) -> dict[str, Any]:
    redis_client: Redis = app.state.redis
    try:
        info = await redis_client.xinfo_stream(stream_name)
    except ResponseError as exc:
        if "no such key" in str(exc).lower():
            return {"stream_name": stream_name, "length": 0, "groups": [], "exists": False}
        raise

    groups_raw = await redis_client.xinfo_groups(stream_name)
    groups = _normalize_group_info(groups_raw)
    for group in groups:
        try:
            pending_raw = await redis_client.xpending(stream_name, group["name"])
        except ResponseError:
            pending_raw = {}
        group["pending_summary"] = _normalize_xpending_summary(pending_raw)

    normalized_info = _normalize_stream_info(info)
    return {
        "stream_name": stream_name,
        "exists": True,
        "length": normalized_info["length"],
        "last_generated_id": normalized_info["last_generated_id"],
        "last_entry_id": normalized_info["last_entry_id"],
        "groups": groups,
    }


async def _load_dead_letter_entries(
    *,
    window_minutes: int,
    limit: int,
    listing_id: str | None = None,
) -> list[tuple[str, NotificationDeadLetterEvent]]:
    redis_client: Redis = app.state.redis
    now_ms = int(time.time() * 1000)
    min_id = _stream_id_from_epoch_ms(now_ms - (max(1, int(window_minutes)) * 60 * 1000))
    raw_entries = await redis_client.xrange(
        NOTIFICATION_DEAD_LETTER_STREAM_NAME,
        min=min_id,
        max="+",
        count=max(1, int(limit)),
    )
    entries: list[tuple[str, NotificationDeadLetterEvent]] = []
    listing_filter = (listing_id or "").strip()
    for raw_id, raw_fields in list(raw_entries or []):
        event = NotificationDeadLetterEvent.from_redis_fields(dict(raw_fields or {}))
        if listing_filter and event.listing_id != listing_filter:
            continue
        entries.append((str(raw_id), event))
    return entries


def _listing_row_to_item(row: asyncpg.Record) -> dict[str, Any]:
    return dict(jsonable_encoder(dict(row)))


async def _fetch_listing_item(listing_id: str) -> dict[str, Any] | None:
    listing_id_clean = str(listing_id or "").strip()
    if not listing_id_clean:
        return None

    async with app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            SELECT {LISTING_SELECT_COLUMNS}
            FROM listings
            WHERE id = $1
            LIMIT 1
            """,
            listing_id_clean,
        )

    if row is None:
        return None
    return _listing_row_to_item(row)


def _build_listing_snapshot_message(
    *,
    item: dict[str, Any],
    listing_fields: dict[str, Any],
    observed_at: str,
) -> dict[str, Any]:
    return {
        "event": "listing_snapshot",
        "at": observed_at,
        "cursor": int(item["seq_id"]),
        "item": item,
        "worker_name": listing_fields.get("worker_name"),
        "route_name": listing_fields.get("route_name"),
        "source": "websocket",
    }


async def _close_ws_client(
    client: ManagedWebSocketClient,
    *,
    payload: dict[str, Any] | None = None,
    code: int = 1000,
    reason: str = "",
) -> None:
    if payload is not None:
        with suppress(Exception):
            await client.send_json(payload)
    await app.state.ws_manager.unregister(client)
    with suppress(Exception):
        await client.close(code=code, reason=reason)


async def _close_all_ws_clients_disabled(reason: str) -> None:
    manager: WebSocketConnectionManager = app.state.ws_manager
    clients = await manager.snapshot()
    disabled_payload = {
        "event": "websocket_disabled",
        "at": utc_now_iso(),
        "reason": reason,
    }
    for client in clients:
        emit_json_log(
            "websocket_client_disabled",
            service="api",
            client_id=client.client_id,
            client_host=_client_host(client.websocket),
            reason=reason,
        )
        await _close_ws_client(client, payload=disabled_payload, reason=reason)


def _normalize_proxy_mode(raw: str | None) -> str:
    value = str(raw or "").strip().lower()
    if value in {"auto", "rotate", "rotation", "auto_rotation"}:
        return "auto_rotation"
    return "fixed"


def _normalize_route_status(raw: str | None) -> str | None:
    value = str(raw or "").strip().upper()
    if not value:
        return None
    allowed = {"ENABLED", "DEGRADED", "THROTTLED", "COOLDOWN", "NEEDS_LOGIN", "DISABLED"}
    return value if value in allowed else "ENABLED"


def _canonical_proxy_key(proxy_server: str, proxy_username: str | None = None) -> str | None:
    raw_proxy_server = str(proxy_server or "").strip()
    if not raw_proxy_server:
        return None
    base = raw_proxy_server if "://" in raw_proxy_server else f"socks5://{raw_proxy_server}"
    parsed = urlparse(base)
    scheme = (parsed.scheme or "socks5").strip().lower()
    host = (parsed.hostname or "").strip().lower()
    port = int(parsed.port or 0)
    username = str(proxy_username or unquote(parsed.username or "")).strip()
    if not host or port <= 0:
        return None
    return f"{scheme}:{host}:{port}:{username}"


def _resolve_proxy_key(
    proxy_key: str | None,
    proxy_server: str | None,
    proxy_username: str | None,
) -> str:
    key = str(proxy_key or "").strip()
    if key:
        return key
    canonical = _canonical_proxy_key(proxy_server=str(proxy_server or ""), proxy_username=proxy_username)
    if canonical:
        return canonical
    raise HTTPException(
        status_code=400,
        detail="Provide proxy_key or proxy_server (+ optional proxy_username).",
    )


async def _assert_unique_profile_dir(
    conn: asyncpg.Connection,
    worker_name: str,
    route_name: str,
    profile_dir: str | None,
    is_enabled: bool,
) -> None:
    profile_dir_clean = str(profile_dir or "").strip()
    if not is_enabled or not profile_dir_clean:
        return

    existing = await conn.fetchrow(
        """
        SELECT worker_name, route_name
        FROM worker_routes
        WHERE is_enabled = TRUE
          AND COALESCE(BTRIM(user_data_dir), '') = $1
          AND NOT (worker_name = $2 AND route_name = $3)
        LIMIT 1
        """,
        profile_dir_clean,
        worker_name,
        route_name,
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail=(
                f"user_data_dir '{profile_dir_clean}' is already assigned to enabled route "
                f"{existing['worker_name']}/{existing['route_name']}. Use a unique profile directory per route."
            ),
        )


async def _count_enabled_routes_for_worker(conn: asyncpg.Connection, worker_name: str) -> int:
    count = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM worker_routes
        WHERE worker_name = $1
          AND is_enabled = TRUE
          AND COALESCE(status, 'ENABLED') <> 'DISABLED'
        """,
        worker_name,
    )
    return int(count or 0)


async def _build_route_config_warnings(conn: asyncpg.Connection, worker_name: str) -> list[str]:
    warnings: list[str] = []
    enabled_count = await _count_enabled_routes_for_worker(conn=conn, worker_name=worker_name)
    if enabled_count < WORKER_MIN_ENABLED_ROUTES_WARN:
        warnings.append(
            f"Worker '{worker_name}' has {enabled_count} enabled route(s). "
            "Rotation resilience is reduced until at least "
            f"{WORKER_MIN_ENABLED_ROUTES_WARN} routes are enabled."
        )
    return warnings


def _normalize_query_items(queries: list[str] | None = None, query_csv: str | None = None) -> list[str]:
    raw_items: list[str] = []
    if queries is not None:
        raw_items.extend(str(item or "") for item in queries)
    if query_csv is not None:
        raw_items.extend(str(query_csv or "").split(","))
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_item in raw_items:
        query = str(raw_item or "").strip()
        if not query:
            continue
        identity = query.lower()
        if identity in seen:
            continue
        seen.add(identity)
        normalized.append(query)
    return normalized or ["BUCKETS"]


def _normalize_variant_state(value: str | None) -> str:
    state = str(value or "").strip().lower()
    if state in {"validated", "pending_validation", "rejected"}:
        return state
    return "pending_validation"


def _normalize_family_variant_items(
    variants: list[QueryVariantUpsertRequest] | None,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, variant in enumerate(list(variants or [])):
        query_text = str(variant.query_text or "").strip()
        if not query_text:
            continue
        identity = query_text.lower()
        if identity in seen:
            continue
        seen.add(identity)
        normalized.append(
            {
                "query_text": query_text,
                "validation_state": _normalize_variant_state(variant.validation_state),
                "is_enabled": bool(variant.is_enabled),
                "weight": float(variant.weight),
                "variant_order": index,
                "notes": str(variant.notes or "").strip() or None,
            }
        )
    return normalized


def _normalize_family_name(value: str) -> str:
    return str(value or "").strip()


def _normalize_family_lane(value: str | None) -> str:
    lane = normalize_route_lane(value)
    return lane or "warm"


async def _replace_route_queries(
    conn: asyncpg.Connection,
    *,
    route_name: str,
    query_items: list[str],
) -> None:
    normalized_queries = _normalize_query_items(queries=query_items)
    await conn.execute(
        """
        DELETE FROM route_queries
        WHERE route_name = $1
          AND NOT (query_text = ANY($2::TEXT[]))
        """,
        route_name,
        normalized_queries,
    )
    for index, query in enumerate(normalized_queries):
        await conn.execute(
            """
            INSERT INTO route_queries (
                route_name,
                query_text,
                query_order,
                is_enabled
            ) VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (route_name, query_text) DO UPDATE SET
                query_order = EXCLUDED.query_order,
                is_enabled = EXCLUDED.is_enabled
            """,
            route_name,
            query,
            index,
        )


async def _load_route_queries_by_route_name(
    conn: asyncpg.Connection,
    route_names: list[str],
) -> dict[str, list[dict[str, Any]]]:
    cleaned_route_names = [str(item).strip() for item in route_names if str(item).strip()]
    if not cleaned_route_names:
        return {}
    rows = await conn.fetch(
        """
        SELECT
            route_name,
            query_text,
            query_order,
            is_enabled,
            last_selected_at,
            last_success_at,
            avg_result_count,
            profitable_hit_rate,
            recent_duplicate_ratio,
            selection_count,
            last_error,
            created_at,
            updated_at
        FROM route_queries
        WHERE route_name = ANY($1::TEXT[])
        ORDER BY route_name ASC, query_order ASC, query_text ASC
        """,
        cleaned_route_names,
    )
    query_map: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        route_name = str(row["route_name"] or "").strip()
        if not route_name:
            continue
        item = _serialize_datetimes(
            dict(row),
            ("last_selected_at", "last_success_at", "created_at", "updated_at"),
        )
        query_map.setdefault(route_name, []).append(item)
    return query_map


def _serialize_central_route_row(
    row: Mapping[str, Any],
    query_items: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    item = _serialize_datetimes(
        dict(row),
        (
            "last_selected_at",
            "last_success_at",
            "status_since",
            "next_run_at",
            "cooldown_until",
            "priority_score_updated_at",
            "preferred_proxy_updated_at",
            "created_at",
            "updated_at",
        ),
    )
    queries = list(query_items or [])
    enabled_query_texts = [
        str(query.get("query_text") or "").strip()
        for query in queries
        if bool(query.get("is_enabled", True)) and str(query.get("query_text") or "").strip()
    ]
    item["queries"] = queries
    item["search_queries"] = ", ".join(enabled_query_texts) if enabled_query_texts else "BUCKETS"
    item["query_count"] = len(queries)
    return item


def _serialize_query_family_row(
    row: Mapping[str, Any],
    variants: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    item = _serialize_datetimes(
        dict(row),
        (
            "next_due_at",
            "last_claimed_at",
            "last_discovery_at",
            "last_success_at",
            "family_lease_expires_at",
            "created_at",
            "updated_at",
        ),
    )
    family_variants = list(variants or [])
    validated_variants = [
        variant
        for variant in family_variants
        if bool(variant.get("is_enabled", True))
        and str(variant.get("validation_state") or "pending_validation").strip().lower() == "validated"
    ]
    enabled_variants = [
        variant for variant in family_variants if bool(variant.get("is_enabled", True))
    ]
    item["variants"] = family_variants
    item["validated_variant_count"] = len(validated_variants)
    item["enabled_variant_count"] = len(enabled_variants)
    item["search_queries"] = ", ".join(
        str(variant.get("query_text") or "").strip()
        for variant in validated_variants
        if str(variant.get("query_text") or "").strip()
    )
    if not item["search_queries"]:
        item["search_queries"] = ", ".join(
            str(variant.get("query_text") or "").strip()
            for variant in enabled_variants
            if str(variant.get("query_text") or "").strip()
        )
    item["search_queries"] = item["search_queries"] or "-"
    item["has_active_worker"] = bool(str(item.get("active_worker_name") or "").strip())
    item["is_leased"] = bool(item.get("family_lease_token"))
    return item


async def _load_query_variants_by_family_id(
    conn: asyncpg.Connection,
    family_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    cleaned_ids = [int(item) for item in family_ids if int(item or 0) > 0]
    if not cleaned_ids:
        return {}
    rows = await conn.fetch(
        """
        SELECT
            variant_id,
            family_id,
            query_text,
            url_template,
            validation_state,
            weight,
            variant_order,
            is_enabled,
            last_selected_at,
            last_success_at,
            notes,
            created_at,
            updated_at
        FROM query_variants
        WHERE family_id = ANY($1::BIGINT[])
        ORDER BY family_id ASC, variant_order ASC, variant_id ASC
        """,
        cleaned_ids,
    )
    variants_by_family: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        family_id = int(row["family_id"])
        variants_by_family.setdefault(family_id, []).append(
            _serialize_datetimes(
                dict(row),
                ("last_selected_at", "last_success_at", "created_at", "updated_at"),
            )
        )
    return variants_by_family


async def _bootstrap_query_family_presets(
    conn: asyncpg.Connection,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for preset in V42_FAMILY_PRESETS:
        family_row = await conn.fetchrow(
            """
            INSERT INTO query_families (
                name,
                legacy_route_name,
                legacy_worker_name,
                is_enabled,
                priority,
                lane,
                next_due_at,
                min_gap_s,
                max_gap_s,
                variant_cursor,
                variant_count,
                last_error
            ) VALUES (
                $1, $2, $3, $4, $5, $6, NOW(), $7, $8, 0, 0, NULL
            )
            ON CONFLICT (name) DO UPDATE SET
                legacy_route_name = COALESCE(EXCLUDED.legacy_route_name, query_families.legacy_route_name),
                legacy_worker_name = COALESCE(EXCLUDED.legacy_worker_name, query_families.legacy_worker_name),
                is_enabled = EXCLUDED.is_enabled,
                priority = EXCLUDED.priority,
                lane = EXCLUDED.lane,
                min_gap_s = EXCLUDED.min_gap_s,
                max_gap_s = EXCLUDED.max_gap_s,
                next_due_at = LEAST(query_families.next_due_at, NOW()),
                last_error = NULL
            RETURNING family_id, name
            """,
            preset.name,
            preset.legacy_route_name,
            preset.legacy_worker_name,
            preset.is_enabled,
            preset.priority,
            preset.lane,
            preset.min_gap_s,
            preset.max_gap_s,
        )
        if family_row is None:
            raise HTTPException(status_code=500, detail=f"Failed to bootstrap family {preset.name}.")
        family_id = int(family_row["family_id"])
        for variant_order, variant in enumerate(preset.variants):
            await conn.execute(
                """
                INSERT INTO query_variants (
                    family_id,
                    query_text,
                    url_template,
                    validation_state,
                    weight,
                    variant_order,
                    is_enabled,
                    notes
                ) VALUES (
                    $1, $2, NULL, $3, $4, $5, $6, $7
                )
                ON CONFLICT (family_id, query_text) DO UPDATE SET
                    validation_state = EXCLUDED.validation_state,
                    weight = EXCLUDED.weight,
                    variant_order = EXCLUDED.variant_order,
                    is_enabled = EXCLUDED.is_enabled,
                    notes = EXCLUDED.notes
                """,
                family_id,
                variant.query_text,
                _normalize_variant_state(variant.validation_state),
                float(variant.weight),
                variant_order,
                bool(variant.is_enabled),
                variant.notes,
            )
        await conn.execute(
            """
            UPDATE query_families
            SET
                variant_count = COALESCE((
                    SELECT COUNT(*)::INT
                    FROM query_variants
                    WHERE family_id = $1
                      AND COALESCE(is_enabled, TRUE) = TRUE
                      AND LOWER(COALESCE(validation_state, 'pending_validation')) = 'validated'
                ), 0),
                variant_cursor = 0
            WHERE family_id = $1
            """,
            family_id,
        )
        results.append({"family_id": family_id, "name": str(family_row["name"] or "").strip()})
    return results


async def _ensure_worker_tables(pool: asyncpg.Pool) -> None:
    await ensure_common_worker_tables(pool=pool, include_triggers=True)


@app.on_event("startup")
async def startup() -> None:
    app.state.db_pool = await asyncpg.create_pool(
        host=PGHOST,
        port=PGPORT,
        database=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        min_size=1,
        max_size=20,
    )
    await _ensure_worker_tables(app.state.db_pool)
    app.state.redis = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD or None,
        decode_responses=True,
    )
    app.state.feature_flags = RedisFeatureFlags(app.state.redis, hash_key=FLAG_HASH_KEY)
    app.state.runtime_config = RedisRuntimeConfig(app.state.redis, hash_key=RUNTIME_CONFIG_HASH_KEY)
    emit_json_log(
        "feature_flag_snapshot",
        service="api",
        flag_hash_key=FLAG_HASH_KEY,
        flags=await app.state.feature_flags.snapshot(),
    )
    emit_json_log(
        "runtime_config_snapshot",
        service="api",
        runtime_config_hash_key=RUNTIME_CONFIG_HASH_KEY,
        runtime_config=await app.state.runtime_config.snapshot(),
    )
    app.state.ws_manager = WebSocketConnectionManager()
    app.state.redis_listener_task = asyncio.create_task(_redis_listener())


@app.on_event("shutdown")
async def shutdown() -> None:
    listener_task: asyncio.Task | None = getattr(app.state, "redis_listener_task", None)
    if listener_task:
        listener_task.cancel()
        with suppress(asyncio.CancelledError):
            await listener_task

    ws_manager: WebSocketConnectionManager | None = getattr(app.state, "ws_manager", None)
    if ws_manager is not None:
        await _close_all_ws_clients_disabled("service_shutdown")

    redis_client: Redis | None = getattr(app.state, "redis", None)
    if redis_client:
        await redis_client.close()

    db_pool: asyncpg.Pool | None = getattr(app.state, "db_pool", None)
    if db_pool:
        await db_pool.close()


async def _broadcast(payload: dict[str, Any]) -> None:
    listing_fields = _extract_listing_observation_fields(payload)
    if listing_fields is None:
        return

    broadcast_started = time.monotonic()
    if not await _is_gui_websocket_push_enabled():
        await _close_all_ws_clients_disabled("feature_flag_off")
        return

    item = await _fetch_listing_item(str(listing_fields["listing_id"]))
    if item is None:
        emit_json_log(
            "listing_gui_push_skipped",
            service="api",
            **listing_fields,
            reason="listing_not_found",
        )
        return

    message = _build_listing_snapshot_message(
        item=item,
        listing_fields=listing_fields,
        observed_at=str(payload.get("at") or utc_now_iso()),
    )
    clients = await app.state.ws_manager.snapshot()
    if not clients:
        emit_json_log(
            "listing_gui_push",
            **listing_fields,
            cursor=int(item["seq_id"]),
            gui_pushed_ts=utc_now_iso(),
            websocket_broadcast_latency_ms=0,
            websocket_client_count=0,
            push_source="websocket",
        )
        return

    dead_clients: list[ManagedWebSocketClient] = []
    delivered_count = 0
    for client in clients:
        try:
            await client.send_json(message)
            delivered_count += 1
        except Exception:
            dead_clients.append(client)

    if dead_clients:
        for client in dead_clients:
            await _close_ws_client(client, reason="send_failed")

    emit_json_log(
        "listing_gui_push",
        **listing_fields,
        cursor=int(item["seq_id"]),
        gui_pushed_ts=utc_now_iso(),
        websocket_broadcast_latency_ms=monotonic_duration_ms(broadcast_started),
        websocket_client_count=max(0, delivered_count),
        push_source="websocket",
    )


async def _redis_listener() -> None:
    redis_client: Redis = app.state.redis
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(LISTING_EVENT_CHANNEL)

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message.get("type") == "message":
                raw_data = message.get("data")
                if isinstance(raw_data, bytes):
                    raw_data = raw_data.decode("utf-8", errors="replace")

                payload: dict[str, Any]
                try:
                    payload = json.loads(raw_data)
                except Exception:
                    payload = {
                        "event": "raw_message",
                        "data": str(raw_data),
                        "at": datetime.now(timezone.utc).isoformat(),
                    }

                if "at" not in payload:
                    payload["at"] = datetime.now(timezone.utc).isoformat()
                await _broadcast(payload)

            await asyncio.sleep(0.05)
    finally:
        with suppress(Exception):
            await pubsub.unsubscribe(LISTING_EVENT_CHANNEL)
        with suppress(Exception):
            await pubsub.close()


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    db_ok = False
    redis_ok = False

    try:
        async with app.state.db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
            db_ok = True
    except Exception:
        db_ok = False

    try:
        pong = await app.state.redis.ping()
        redis_ok = bool(pong)
    except Exception:
        redis_ok = False

    payload = {
        "ok": db_ok and redis_ok,
        "env": APP_ENV,
        "db": db_ok,
        "redis": redis_ok,
    }
    if not payload["ok"]:
        raise HTTPException(status_code=503, detail=payload)
    return payload


@app.get("/ops/runtime-config")
async def get_runtime_config(
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    feature_flags: RedisFeatureFlags = app.state.feature_flags
    runtime_config: RedisRuntimeConfig = app.state.runtime_config
    return {
        "feature_flags": await feature_flags.snapshot(),
        "runtime_config": await runtime_config.snapshot(),
    }


@app.put("/ops/runtime-config")
async def update_runtime_config(
    payload: RuntimeConfigUpdateRequest,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    feature_flags: RedisFeatureFlags = app.state.feature_flags
    runtime_config: RedisRuntimeConfig = app.state.runtime_config

    requested_flags = dict(payload.feature_flags or {})
    unsupported_flags = sorted(set(requested_flags) - set(DEFAULT_FEATURE_FLAGS))
    if unsupported_flags:
        raise HTTPException(status_code=400, detail=f"Unsupported feature_flags keys: {', '.join(unsupported_flags)}")

    requested_runtime = dict(payload.runtime_config or {})
    unsupported_runtime = sorted(set(requested_runtime) - set(DEFAULT_RUNTIME_CONFIG))
    if unsupported_runtime:
        raise HTTPException(status_code=400, detail=f"Unsupported runtime_config keys: {', '.join(unsupported_runtime)}")

    normalized_runtime: dict[str, bool | float | None] = {}
    for key, value in requested_runtime.items():
        if value is None:
            normalized_runtime[key] = None
        else:
            normalized_runtime[key] = parse_runtime_config_value(value, DEFAULT_RUNTIME_CONFIG[key])

    flags_snapshot = await feature_flags.set_flag_values(requested_flags) if requested_flags else await feature_flags.snapshot()
    runtime_snapshot = await runtime_config.set_values(normalized_runtime) if normalized_runtime else await runtime_config.snapshot()

    if requested_flags.get("ENABLE_GUI_WEBSOCKET_PUSH") is False:
        await _close_all_ws_clients_disabled("feature_flag_off")

    emit_json_log(
        "runtime_config_updated",
        service="api",
        feature_flag_updates=requested_flags or None,
        runtime_config_updates=normalized_runtime or None,
        feature_flags=flags_snapshot,
        runtime_config=runtime_snapshot,
    )
    return {
        "ok": True,
        "feature_flags": flags_snapshot,
        "runtime_config": runtime_snapshot,
    }


@app.get("/ops/stream-backlog")
async def get_stream_backlog(
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    listings = await _fetch_stream_backlog(LISTING_STREAM_NAME)
    enrichment = await _fetch_stream_backlog(LISTING_ENRICHMENT_STREAM_NAME)
    dead_letter = await _fetch_stream_backlog(NOTIFICATION_DEAD_LETTER_STREAM_NAME)
    emit_json_log(
        "stream_backlog_inspected",
        service="api",
        stream_names=[LISTING_STREAM_NAME, LISTING_ENRICHMENT_STREAM_NAME, NOTIFICATION_DEAD_LETTER_STREAM_NAME],
    )
    return {
        "streams": {
            "listings": listings,
            "listing_enrichment": enrichment,
            "notification_dead_letter": dead_letter,
        }
    }


@app.get("/ops/realtime-health")
async def get_realtime_health(
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    feature_flags: RedisFeatureFlags = app.state.feature_flags
    runtime_snapshot = await _runtime_config_snapshot()
    websocket_client_count = await app.state.ws_manager.active_count()
    stream_backlog = await get_stream_backlog(x_api_token=x_api_token)

    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                worker_name,
                route_name,
                status,
                updated_at,
                last_event_publish_at,
                last_event_publish_status,
                last_event_publish_error,
                last_stream_event_id
            FROM worker_heartbeats
            ORDER BY worker_name ASC
            """
        )
    worker_publish_health = [
        _serialize_datetimes(
            dict(row),
            ("updated_at", "last_event_publish_at"),
        )
        for row in rows
    ]

    listings_groups = stream_backlog["streams"]["listings"].get("groups") or []
    notification_group = next((group for group in listings_groups if group.get("name") == "listing_notifications"), None)
    dead_letter_length = int(stream_backlog["streams"]["notification_dead_letter"].get("length") or 0)
    payload = {
        "feature_flags": await feature_flags.snapshot(),
        "runtime_config": runtime_snapshot,
        "websocket_connection_count": websocket_client_count,
        "notification_consumer": {
            "drain_enabled": bool(runtime_snapshot.get("NOTIFICATION_CONSUMER_DRAIN")),
            "group": notification_group,
        },
        "dead_letter_queue_size": dead_letter_length,
        "worker_event_publish_health": worker_publish_health,
        "stream_backlog": stream_backlog["streams"],
    }
    emit_json_log(
        "realtime_health_inspected",
        service="api",
        websocket_connection_count=websocket_client_count,
        dead_letter_queue_size=dead_letter_length,
        drain_enabled=bool(runtime_snapshot.get("NOTIFICATION_CONSUMER_DRAIN")),
    )
    return payload


@app.get("/ops/notification-dead-letter")
async def get_notification_dead_letter(
    window_minutes: int = Query(default=60, ge=1, le=1440),
    limit: int = Query(default=100, ge=1, le=500),
    listing_id: str | None = Query(default=None),
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    try:
        entries = await _load_dead_letter_entries(
            window_minutes=window_minutes,
            limit=limit,
            listing_id=listing_id,
        )
    except ResponseError as exc:
        if "no such key" in str(exc).lower():
            entries = []
        else:
            raise
    items = [
        {
            "dead_letter_event_id": dead_letter_event_id,
            **event.to_redis_fields(),
        }
        for dead_letter_event_id, event in entries
    ]
    return {
        "count": len(items),
        "window_minutes": window_minutes,
        "items": items,
    }


@app.post("/ops/replay/notifications")
async def replay_notification_dead_letters(
    payload: NotificationReplayRequest,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    try:
        entries = await _load_dead_letter_entries(
            window_minutes=payload.window_minutes,
            limit=payload.limit,
            listing_id=payload.listing_id,
        )
    except ResponseError as exc:
        if "no such key" in str(exc).lower():
            entries = []
        else:
            raise
    replay_candidates = entries[: payload.limit]
    emit_json_log(
        "notification_replay_requested",
        service="api",
        window_minutes=payload.window_minutes,
        limit=payload.limit,
        dry_run=payload.dry_run,
        requested_listing_id=(payload.listing_id or "").strip() or None,
        candidate_count=len(replay_candidates),
    )

    replayed_items: list[dict[str, Any]] = []
    if payload.dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "count": len(replay_candidates),
            "items": [
                {"dead_letter_event_id": dead_letter_event_id, "listing_id": event.listing_id}
                for dead_letter_event_id, event in replay_candidates
            ],
        }

    async with app.state.db_pool.acquire() as conn:
        async with conn.transaction():
            for dead_letter_event_id, dead_letter in replay_candidates:
                row = await conn.fetchrow(
                    """
                    UPDATE notification_delivery_ledger
                    SET
                        status = 'pending',
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE listing_id = $1
                      AND status = 'failed_terminal'
                    RETURNING listing_id
                    """,
                    dead_letter.listing_id,
                )
                if row is None:
                    continue
                replay_event = dead_letter.to_listing_stream_event()
                replayed_stream_event_id = await app.state.redis.xadd(
                    LISTING_STREAM_NAME,
                    replay_event.to_redis_fields(),
                    maxlen=LISTING_STREAM_MAXLEN,
                    approximate=True,
                )
                replayed_items.append(
                    {
                        "dead_letter_event_id": dead_letter_event_id,
                        "listing_id": dead_letter.listing_id,
                        "replayed_stream_event_id": str(replayed_stream_event_id),
                    }
                )

    emit_json_log(
        "notification_replay_completed",
        service="api",
        window_minutes=payload.window_minutes,
        limit=payload.limit,
        replayed_count=len(replayed_items),
        requested_listing_id=(payload.listing_id or "").strip() or None,
    )
    return {
        "ok": True,
        "dry_run": False,
        "count": len(replayed_items),
        "items": replayed_items,
    }


@app.get("/listings")
async def get_listings(
    request: Request,
    limit: int = Query(default=200, ge=1, le=5000),
    since_id: int = Query(default=0, ge=0),
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)

    query = f"""
        SELECT {LISTING_SELECT_COLUMNS}
        FROM listings
        WHERE seq_id > $1
        ORDER BY seq_id ASC
        LIMIT $2
    """

    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch(query, since_id, limit)

    listings: list[dict[str, Any]] = []
    max_seq_id = since_id
    for row in rows:
        item = _serialize_datetimes(dict(row), ("source_seen_at", "created_at", "updated_at"))
        max_seq_id = max(max_seq_id, int(item["seq_id"]))
        listings.append(item)

    return {
        "count": len(listings),
        "since_id": since_id,
        "next_since_id": max_seq_id,
        "items": listings,
    }


@app.get("/routes")
async def get_central_routes(
    route_name: str | None = Query(default=None),
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    route_name_filter = (route_name or "").strip()
    async with app.state.db_pool.acquire() as conn:
        if route_name_filter:
            rows = await conn.fetch(
                """
                SELECT
                    route_name,
                    legacy_worker_name,
                    legacy_route_name,
                    is_enabled,
                    proxy_server,
                    proxy_username,
                    CASE WHEN proxy_password IS NOT NULL AND proxy_password <> '' THEN '****' ELSE NULL END AS proxy_password,
                    proxy_mode,
                    proxy_pool,
                    preferred_proxy_key,
                    preferred_proxy_updated_at,
                    priority,
                    status,
                    status_reason,
                    status_since,
                    next_run_at,
                    route_interval_seconds,
                    avg_result_count,
                    profitable_hit_rate,
                    recent_duplicate_ratio,
                    avg_page_load_ms,
                    successful_cycles,
                    last_selected_at,
                    last_success_at,
                    consecutive_failures,
                    cooldown_until,
                    lane_override,
                    computed_lane,
                    effective_lane,
                    priority_score,
                    priority_score_updated_at,
                    last_error,
                    created_at,
                    updated_at
                FROM central_routes
                WHERE route_name = $1
                ORDER BY priority ASC, route_name ASC
                """,
                route_name_filter,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    route_name,
                    legacy_worker_name,
                    legacy_route_name,
                    is_enabled,
                    proxy_server,
                    proxy_username,
                    CASE WHEN proxy_password IS NOT NULL AND proxy_password <> '' THEN '****' ELSE NULL END AS proxy_password,
                    proxy_mode,
                    proxy_pool,
                    preferred_proxy_key,
                    preferred_proxy_updated_at,
                    priority,
                    status,
                    status_reason,
                    status_since,
                    next_run_at,
                    route_interval_seconds,
                    avg_result_count,
                    profitable_hit_rate,
                    recent_duplicate_ratio,
                    avg_page_load_ms,
                    successful_cycles,
                    last_selected_at,
                    last_success_at,
                    consecutive_failures,
                    cooldown_until,
                    lane_override,
                    computed_lane,
                    effective_lane,
                    priority_score,
                    priority_score_updated_at,
                    last_error,
                    created_at,
                    updated_at
                FROM central_routes
                ORDER BY priority ASC, route_name ASC
                """
            )
        route_names = [str(row["route_name"] or "").strip() for row in rows if str(row["route_name"] or "").strip()]
        queries_by_route = await _load_route_queries_by_route_name(conn, route_names)

    items = [
        _serialize_central_route_row(dict(row), queries_by_route.get(str(row["route_name"] or "").strip(), []))
        for row in rows
    ]
    return {"count": len(items), "items": items}


@app.get("/routes/{route_name}/queries")
async def get_route_queries(
    route_name: str,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    route_name_clean = route_name.strip()
    if not route_name_clean:
        raise HTTPException(status_code=400, detail="route_name is required.")
    async with app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT route_name FROM central_routes WHERE route_name = $1",
            route_name_clean,
        )
        if row is None:
            raise HTTPException(status_code=404, detail="Route not found.")
        queries_by_route = await _load_route_queries_by_route_name(conn, [route_name_clean])
    items = queries_by_route.get(route_name_clean, [])
    return {"count": len(items), "items": items}


@app.put("/routes/{route_name}")
async def upsert_central_route(
    route_name: str,
    payload: CentralRouteUpsert,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    route_name_clean = route_name.strip()
    if not route_name_clean:
        raise HTTPException(status_code=400, detail="route_name is required.")

    lane_override_provided = _payload_field_is_set(payload, "lane_override")
    lane_override = normalize_route_lane(payload.lane_override) if lane_override_provided else None
    raw_lane_override = (payload.lane_override or "").strip()
    if (
        lane_override_provided
        and raw_lane_override
        and raw_lane_override.lower() not in {"auto", "default", "computed"}
        and lane_override is None
    ):
        raise HTTPException(status_code=400, detail="lane_override must be one of hot, warm, sweep, or null.")

    proxy_mode = _normalize_proxy_mode(payload.proxy_mode)
    route_status = _normalize_route_status(payload.route_status)
    status_reason = (payload.status_reason or "").strip() or None
    query_items = _normalize_query_items(query_csv=payload.search_queries)

    async with app.state.db_pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO central_routes (
                    route_name,
                    is_enabled,
                    proxy_server,
                    proxy_username,
                    proxy_password,
                    proxy_mode,
                    proxy_pool,
                    priority,
                    route_interval_seconds,
                    lane_override,
                    computed_lane,
                    effective_lane,
                    status,
                    status_reason,
                    status_since
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9,
                    $10, 'warm', COALESCE($10, 'warm'),
                    CASE
                        WHEN $2::BOOLEAN = FALSE THEN 'DISABLED'
                        ELSE COALESCE(NULLIF(BTRIM($11), ''), 'ENABLED')
                    END,
                    CASE
                        WHEN $2::BOOLEAN = FALSE THEN COALESCE(NULLIF($12, ''), 'disabled by operator')
                        ELSE NULLIF($12, '')
                    END,
                    NOW()
                )
                ON CONFLICT (route_name) DO UPDATE SET
                    is_enabled = EXCLUDED.is_enabled,
                    proxy_server = EXCLUDED.proxy_server,
                    proxy_username = EXCLUDED.proxy_username,
                    proxy_password = EXCLUDED.proxy_password,
                    proxy_mode = EXCLUDED.proxy_mode,
                    proxy_pool = EXCLUDED.proxy_pool,
                    priority = EXCLUDED.priority,
                    route_interval_seconds = EXCLUDED.route_interval_seconds,
                    lane_override = CASE
                        WHEN $13::BOOLEAN THEN $10
                        ELSE central_routes.lane_override
                    END,
                    effective_lane = CASE
                        WHEN $13::BOOLEAN THEN COALESCE($10, central_routes.computed_lane, 'warm')
                        ELSE central_routes.effective_lane
                    END,
                    status = CASE
                        WHEN EXCLUDED.is_enabled = FALSE THEN 'DISABLED'
                        WHEN COALESCE(NULLIF(BTRIM($11), ''), '') <> '' THEN COALESCE(NULLIF(BTRIM($11), ''), 'ENABLED')
                        WHEN central_routes.status IN ('DEGRADED', 'THROTTLED', 'COOLDOWN') THEN central_routes.status
                        ELSE 'ENABLED'
                    END,
                    status_reason = CASE
                        WHEN EXCLUDED.is_enabled = FALSE THEN COALESCE(NULLIF($12, ''), 'disabled by operator')
                        WHEN COALESCE(NULLIF(BTRIM($12), ''), '') <> '' THEN NULLIF($12, '')
                        ELSE central_routes.status_reason
                    END,
                    status_since = CASE
                        WHEN central_routes.status IS DISTINCT FROM CASE
                            WHEN EXCLUDED.is_enabled = FALSE THEN 'DISABLED'
                            WHEN COALESCE(NULLIF(BTRIM($11), ''), '') <> '' THEN COALESCE(NULLIF(BTRIM($11), ''), 'ENABLED')
                            WHEN central_routes.status IN ('DEGRADED', 'THROTTLED', 'COOLDOWN') THEN central_routes.status
                            ELSE 'ENABLED'
                        END THEN NOW()
                        ELSE central_routes.status_since
                    END
                RETURNING
                    route_name,
                    legacy_worker_name,
                    legacy_route_name,
                    is_enabled,
                    proxy_server,
                    proxy_username,
                    CASE WHEN proxy_password IS NOT NULL AND proxy_password <> '' THEN '****' ELSE NULL END AS proxy_password,
                    proxy_mode,
                    proxy_pool,
                    preferred_proxy_key,
                    preferred_proxy_updated_at,
                    priority,
                    status,
                    status_reason,
                    status_since,
                    next_run_at,
                    route_interval_seconds,
                    avg_result_count,
                    profitable_hit_rate,
                    recent_duplicate_ratio,
                    avg_page_load_ms,
                    successful_cycles,
                    last_selected_at,
                    last_success_at,
                    consecutive_failures,
                    cooldown_until,
                    lane_override,
                    computed_lane,
                    effective_lane,
                    priority_score,
                    priority_score_updated_at,
                    last_error,
                    created_at,
                    updated_at
                """,
                route_name_clean,
                bool(payload.is_enabled),
                (payload.proxy_server or "").strip(),
                (payload.proxy_username or "").strip() or None,
                (payload.proxy_password or "").strip() or None,
                proxy_mode,
                (payload.proxy_pool or "").strip() or None,
                int(payload.priority),
                payload.route_interval_seconds,
                lane_override,
                route_status,
                status_reason,
                lane_override_provided,
            )
            await _replace_route_queries(conn, route_name=route_name_clean, query_items=query_items)
            queries_by_route = await _load_route_queries_by_route_name(conn, [route_name_clean])

    if row is None:
        raise HTTPException(status_code=500, detail="Failed to save central route.")
    return {
        "ok": True,
        "item": _serialize_central_route_row(dict(row), queries_by_route.get(route_name_clean, [])),
        "warnings": [],
    }


@app.put("/routes/{route_name}/queries")
async def replace_route_queries(
    route_name: str,
    payload: RouteQuerySetUpdateRequest,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    route_name_clean = route_name.strip()
    if not route_name_clean:
        raise HTTPException(status_code=400, detail="route_name is required.")
    query_items = _normalize_query_items(queries=payload.queries)
    async with app.state.db_pool.acquire() as conn:
        async with conn.transaction():
            exists = await conn.fetchval(
                "SELECT 1 FROM central_routes WHERE route_name = $1",
                route_name_clean,
            )
            if not exists:
                raise HTTPException(status_code=404, detail="Route not found.")
            await _replace_route_queries(conn, route_name=route_name_clean, query_items=query_items)
            queries_by_route = await _load_route_queries_by_route_name(conn, [route_name_clean])
    items = queries_by_route.get(route_name_clean, [])
    return {
        "ok": True,
        "count": len(items),
        "search_queries": ", ".join(
            str(item.get("query_text") or "").strip()
            for item in items
            if bool(item.get("is_enabled", True)) and str(item.get("query_text") or "").strip()
        ) or "BUCKETS",
        "items": items,
    }


@app.delete("/routes/{route_name}")
async def delete_central_route(
    route_name: str,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    route_name_clean = route_name.strip()
    if not route_name_clean:
        raise HTTPException(status_code=400, detail="route_name is required.")
    async with app.state.db_pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM central_routes WHERE route_name = $1",
            route_name_clean,
        )
    deleted = result.split()[-1] != "0"
    return {"ok": True, "deleted": deleted}


@app.get("/query-families")
async def get_query_families(
    family_name: str | None = Query(default=None),
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    family_name_filter = _normalize_family_name(family_name or "")
    async with app.state.db_pool.acquire() as conn:
        if family_name_filter:
            rows = await conn.fetch(
                """
                SELECT
                    qf.family_id,
                    qf.name,
                    qf.legacy_route_name,
                    qf.legacy_worker_name,
                    qf.is_enabled,
                    qf.priority,
                    qf.priority_score,
                    qf.lane,
                    qf.next_due_at,
                    qf.min_gap_s,
                    qf.max_gap_s,
                    qf.variant_cursor,
                    qf.variant_count,
                    qf.consecutive_hits,
                    qf.consecutive_empty,
                    qf.last_claimed_at,
                    qf.last_discovery_at,
                    qf.last_success_at,
                    qf.last_error,
                    qf.family_lease_token,
                    qf.family_lease_expires_at,
                    qf.created_at,
                    qf.updated_at,
                    hb.worker_name AS active_worker_name,
                    hb.route_search_queries AS active_query_text,
                    hb.route_user_data_dir AS active_user_data_dir,
                    hb.status AS active_worker_status
                FROM query_families qf
                LEFT JOIN worker_heartbeats hb
                  ON LOWER(COALESCE(hb.route_source, '')) = 'v4'
                 AND hb.route_name = qf.name
                WHERE qf.name = $1
                ORDER BY qf.priority DESC, qf.name ASC
                """,
                family_name_filter,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    qf.family_id,
                    qf.name,
                    qf.legacy_route_name,
                    qf.legacy_worker_name,
                    qf.is_enabled,
                    qf.priority,
                    qf.priority_score,
                    qf.lane,
                    qf.next_due_at,
                    qf.min_gap_s,
                    qf.max_gap_s,
                    qf.variant_cursor,
                    qf.variant_count,
                    qf.consecutive_hits,
                    qf.consecutive_empty,
                    qf.last_claimed_at,
                    qf.last_discovery_at,
                    qf.last_success_at,
                    qf.last_error,
                    qf.family_lease_token,
                    qf.family_lease_expires_at,
                    qf.created_at,
                    qf.updated_at,
                    hb.worker_name AS active_worker_name,
                    hb.route_search_queries AS active_query_text,
                    hb.route_user_data_dir AS active_user_data_dir,
                    hb.status AS active_worker_status
                FROM query_families qf
                LEFT JOIN worker_heartbeats hb
                  ON LOWER(COALESCE(hb.route_source, '')) = 'v4'
                 AND hb.route_name = qf.name
                ORDER BY qf.priority DESC, qf.name ASC
                """
            )
        family_ids = [int(row["family_id"]) for row in rows if int(row["family_id"] or 0) > 0]
        variants_by_family = await _load_query_variants_by_family_id(conn, family_ids)

    items = [
        _serialize_query_family_row(dict(row), variants_by_family.get(int(row["family_id"]), []))
        for row in rows
    ]
    return {
        "count": len(items),
        "catalog_version": V42_FAMILY_CATALOG_VERSION,
        "items": items,
    }


@app.put("/query-families/{family_name}")
async def upsert_query_family(
    family_name: str,
    payload: QueryFamilyUpsertRequest,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    family_name_clean = _normalize_family_name(family_name)
    if not family_name_clean:
        raise HTTPException(status_code=400, detail="family_name is required.")
    previous_name = _normalize_family_name(payload.previous_name or "")
    lane = _normalize_family_lane(payload.lane)
    variants = _normalize_family_variant_items(payload.variants)
    if not variants:
        raise HTTPException(status_code=400, detail="At least one variant is required.")

    async with app.state.db_pool.acquire() as conn:
        async with conn.transaction():
            family_row = None
            if previous_name and previous_name != family_name_clean:
                existing_target = await conn.fetchval(
                    "SELECT 1 FROM query_families WHERE name = $1",
                    family_name_clean,
                )
                if existing_target:
                    raise HTTPException(status_code=409, detail="Target family name already exists.")
                family_row = await conn.fetchrow(
                    """
                    UPDATE query_families
                    SET
                        name = $1,
                        legacy_route_name = COALESCE($2, legacy_route_name),
                        legacy_worker_name = COALESCE($3, legacy_worker_name),
                        is_enabled = $4,
                        priority = $5,
                        lane = $6,
                        min_gap_s = $7,
                        max_gap_s = $8,
                        next_due_at = NOW()
                    WHERE name = $9
                    RETURNING *
                    """,
                    family_name_clean,
                    (payload.legacy_route_name or "").strip() or None,
                    (payload.legacy_worker_name or "").strip() or None,
                    bool(payload.is_enabled),
                    int(payload.priority),
                    lane,
                    int(payload.min_gap_s),
                    payload.max_gap_s,
                    previous_name,
                )
                if family_row is None:
                    raise HTTPException(status_code=404, detail="Previous family not found.")
            else:
                family_row = await conn.fetchrow(
                    """
                    INSERT INTO query_families (
                        name,
                        legacy_route_name,
                        legacy_worker_name,
                        is_enabled,
                        priority,
                        lane,
                        next_due_at,
                        min_gap_s,
                        max_gap_s,
                        variant_cursor,
                        variant_count,
                        last_error
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, NOW(), $7, $8, 0, 0, NULL
                    )
                    ON CONFLICT (name) DO UPDATE SET
                        legacy_route_name = COALESCE(EXCLUDED.legacy_route_name, query_families.legacy_route_name),
                        legacy_worker_name = COALESCE(EXCLUDED.legacy_worker_name, query_families.legacy_worker_name),
                        is_enabled = EXCLUDED.is_enabled,
                        priority = EXCLUDED.priority,
                        lane = EXCLUDED.lane,
                        min_gap_s = EXCLUDED.min_gap_s,
                        max_gap_s = EXCLUDED.max_gap_s,
                        next_due_at = NOW(),
                        last_error = NULL
                    RETURNING *
                    """,
                    family_name_clean,
                    (payload.legacy_route_name or "").strip() or None,
                    (payload.legacy_worker_name or "").strip() or None,
                    bool(payload.is_enabled),
                    int(payload.priority),
                    lane,
                    int(payload.min_gap_s),
                    payload.max_gap_s,
                )
            if family_row is None:
                raise HTTPException(status_code=500, detail="Failed to save query family.")

            family_id = int(family_row["family_id"])
            query_texts = [str(item["query_text"]) for item in variants]
            await conn.execute(
                """
                DELETE FROM query_variants
                WHERE family_id = $1
                  AND NOT (query_text = ANY($2::TEXT[]))
                """,
                family_id,
                query_texts,
            )
            for variant in variants:
                await conn.execute(
                    """
                    INSERT INTO query_variants (
                        family_id,
                        query_text,
                        url_template,
                        validation_state,
                        weight,
                        variant_order,
                        is_enabled,
                        notes
                    ) VALUES (
                        $1, $2, NULL, $3, $4, $5, $6, $7
                    )
                    ON CONFLICT (family_id, query_text) DO UPDATE SET
                        validation_state = EXCLUDED.validation_state,
                        weight = EXCLUDED.weight,
                        variant_order = EXCLUDED.variant_order,
                        is_enabled = EXCLUDED.is_enabled,
                        notes = EXCLUDED.notes
                    """,
                    family_id,
                    variant["query_text"],
                    variant["validation_state"],
                    variant["weight"],
                    variant["variant_order"],
                    variant["is_enabled"],
                    variant["notes"],
                )
            family_row = await conn.fetchrow(
                """
                UPDATE query_families
                SET
                    variant_count = COALESCE((
                        SELECT COUNT(*)::INT
                        FROM query_variants
                        WHERE family_id = $1
                          AND COALESCE(is_enabled, TRUE) = TRUE
                          AND LOWER(COALESCE(validation_state, 'pending_validation')) = 'validated'
                    ), 0),
                    variant_cursor = CASE
                        WHEN COALESCE((
                            SELECT COUNT(*)::INT
                            FROM query_variants
                            WHERE family_id = $1
                              AND COALESCE(is_enabled, TRUE) = TRUE
                              AND LOWER(COALESCE(validation_state, 'pending_validation')) = 'validated'
                        ), 0) <= 0 THEN 0
                        ELSE LEAST(
                            COALESCE(variant_cursor, 0),
                            COALESCE((
                                SELECT COUNT(*)::INT
                                FROM query_variants
                                WHERE family_id = $1
                                  AND COALESCE(is_enabled, TRUE) = TRUE
                                  AND LOWER(COALESCE(validation_state, 'pending_validation')) = 'validated'
                            ), 1) - 1
                        )
                    END,
                    next_due_at = NOW()
                WHERE family_id = $1
                RETURNING *
                """,
                family_id,
            )
            family_rows = await conn.fetch(
                """
                SELECT
                    qf.family_id,
                    qf.name,
                    qf.legacy_route_name,
                    qf.legacy_worker_name,
                    qf.is_enabled,
                    qf.priority,
                    qf.priority_score,
                    qf.lane,
                    qf.next_due_at,
                    qf.min_gap_s,
                    qf.max_gap_s,
                    qf.variant_cursor,
                    qf.variant_count,
                    qf.consecutive_hits,
                    qf.consecutive_empty,
                    qf.last_claimed_at,
                    qf.last_discovery_at,
                    qf.last_success_at,
                    qf.last_error,
                    qf.family_lease_token,
                    qf.family_lease_expires_at,
                    qf.created_at,
                    qf.updated_at,
                    hb.worker_name AS active_worker_name,
                    hb.route_search_queries AS active_query_text,
                    hb.route_user_data_dir AS active_user_data_dir,
                    hb.status AS active_worker_status
                FROM query_families qf
                LEFT JOIN worker_heartbeats hb
                  ON LOWER(COALESCE(hb.route_source, '')) = 'v4'
                 AND hb.route_name = qf.name
                WHERE qf.family_id = $1
                """,
                family_id,
            )
            variants_by_family = await _load_query_variants_by_family_id(conn, [family_id])

    item = _serialize_query_family_row(dict(family_rows[0]), variants_by_family.get(family_id, []))
    return {"ok": True, "catalog_version": V42_FAMILY_CATALOG_VERSION, "item": item}


@app.delete("/query-families/{family_name}")
async def delete_query_family(
    family_name: str,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    family_name_clean = _normalize_family_name(family_name)
    if not family_name_clean:
        raise HTTPException(status_code=400, detail="family_name is required.")

    async with app.state.db_pool.acquire() as conn:
        async with conn.transaction():
            family_row = await conn.fetchrow(
                """
                SELECT family_id, family_lease_token, family_lease_expires_at
                FROM query_families
                WHERE name = $1
                FOR UPDATE
                """,
                family_name_clean,
            )
            if family_row is None:
                return {"ok": True, "deleted": False}

            lease_token = str(family_row.get("family_lease_token") or "").strip()
            lease_expires_at = family_row.get("family_lease_expires_at")
            if lease_token and lease_expires_at and lease_expires_at > datetime.now(timezone.utc):
                raise HTTPException(
                    status_code=409,
                    detail="Cannot delete a query family while it has an active V4 lease.",
                )

            result = await conn.execute(
                "DELETE FROM query_families WHERE family_id = $1",
                int(family_row["family_id"]),
            )

    deleted = result.split()[-1] != "0"
    return {"ok": True, "deleted": deleted}


@app.post("/query-families/bootstrap-presets")
async def bootstrap_query_family_presets(
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    async with app.state.db_pool.acquire() as conn:
        async with conn.transaction():
            seeded = await _bootstrap_query_family_presets(conn)
        rows = await conn.fetch(
            """
            SELECT
                qf.family_id,
                qf.name,
                qf.legacy_route_name,
                qf.legacy_worker_name,
                qf.is_enabled,
                qf.priority,
                qf.priority_score,
                qf.lane,
                qf.next_due_at,
                qf.min_gap_s,
                qf.max_gap_s,
                qf.variant_cursor,
                qf.variant_count,
                qf.consecutive_hits,
                qf.consecutive_empty,
                qf.last_claimed_at,
                qf.last_discovery_at,
                qf.last_success_at,
                qf.last_error,
                qf.family_lease_token,
                qf.family_lease_expires_at,
                qf.created_at,
                qf.updated_at,
                hb.worker_name AS active_worker_name,
                hb.route_search_queries AS active_query_text,
                hb.route_user_data_dir AS active_user_data_dir,
                hb.status AS active_worker_status
            FROM query_families qf
            LEFT JOIN worker_heartbeats hb
              ON LOWER(COALESCE(hb.route_source, '')) = 'v4'
             AND hb.route_name = qf.name
            WHERE qf.name = ANY($1::TEXT[])
            ORDER BY qf.priority DESC, qf.name ASC
            """,
            [preset.name for preset in V42_FAMILY_PRESETS],
        )
        variants_by_family = await _load_query_variants_by_family_id(
            conn,
            [int(row["family_id"]) for row in rows if int(row["family_id"] or 0) > 0],
        )

    items = [
        _serialize_query_family_row(dict(row), variants_by_family.get(int(row["family_id"]), []))
        for row in rows
    ]
    return {
        "ok": True,
        "catalog_version": V42_FAMILY_CATALOG_VERSION,
        "seeded_count": len(seeded),
        "items": items,
    }


@app.get("/execution-profiles")
async def get_execution_profiles(
    worker_name: str | None = Query(default=None),
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    worker_name_filter = (worker_name or "").strip()
    async with app.state.db_pool.acquire() as conn:
        if worker_name_filter:
            rows = await conn.fetch(
                """
                SELECT
                    user_data_dir,
                    worker_name,
                    is_enabled,
                    status,
                    status_reason,
                    status_since,
                    cooldown_until,
                    manual_login_required,
                    manual_login_reason,
                    manual_login_required_at,
                    quarantined_at,
                    quarantine_reason,
                    quarantine_evidence,
                    last_selected_at,
                    last_success_at,
                    consecutive_failures,
                    last_error,
                    created_at,
                    updated_at
                FROM execution_profiles
                WHERE worker_name = $1
                ORDER BY worker_name ASC, user_data_dir ASC
                """,
                worker_name_filter,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    user_data_dir,
                    worker_name,
                    is_enabled,
                    status,
                    status_reason,
                    status_since,
                    cooldown_until,
                    manual_login_required,
                    manual_login_reason,
                    manual_login_required_at,
                    quarantined_at,
                    quarantine_reason,
                    quarantine_evidence,
                    last_selected_at,
                    last_success_at,
                    consecutive_failures,
                    last_error,
                    created_at,
                    updated_at
                FROM execution_profiles
                ORDER BY worker_name ASC, user_data_dir ASC
                """
            )
    items = [
        _serialize_datetimes(
            dict(row),
            (
                "status_since",
                "cooldown_until",
                "manual_login_required_at",
                "quarantined_at",
                "last_selected_at",
                "last_success_at",
                "created_at",
                "updated_at",
            ),
        )
        for row in rows
    ]
    return {"count": len(items), "items": items}


@app.post("/execution-profiles/{worker_name}/clear-manual-login")
async def clear_execution_profile_manual_login(
    worker_name: str,
    payload: ExecutionProfileManualLoginClearRequest,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    worker_name_clean = worker_name.strip()
    user_data_dir = str(payload.user_data_dir or "").strip()
    if not worker_name_clean:
        raise HTTPException(status_code=400, detail="worker_name is required.")
    if not user_data_dir:
        raise HTTPException(status_code=400, detail="user_data_dir is required.")

    reason = (payload.reason or "").strip() or "manual login cleared by operator"
    async with app.state.db_pool.acquire() as conn:
        execution_profile = await conn.fetchrow(
            """
            UPDATE execution_profiles
            SET
                status = CASE
                    WHEN is_enabled = FALSE THEN 'DISABLED'
                    ELSE 'READY'
                END,
                status_reason = $3,
                status_since = NOW(),
                cooldown_until = NULL,
                manual_login_required = FALSE,
                manual_login_reason = NULL,
                manual_login_required_at = NULL,
                quarantined_at = NULL,
                quarantine_reason = NULL,
                quarantine_evidence = NULL,
                consecutive_failures = 0,
                last_error = NULL
            WHERE worker_name = $1
              AND user_data_dir = $2
            RETURNING
                user_data_dir,
                worker_name,
                is_enabled,
                status,
                status_reason,
                status_since,
                cooldown_until,
                manual_login_required,
                manual_login_reason,
                manual_login_required_at,
                quarantined_at,
                quarantine_reason,
                quarantine_evidence,
                last_selected_at,
                last_success_at,
                consecutive_failures,
                last_error,
                created_at,
                updated_at
            """,
            worker_name_clean,
            user_data_dir,
            reason,
        )
        if execution_profile is None:
            raise HTTPException(status_code=404, detail="Execution profile not found.")

        v4_profile = await conn.fetchrow(
            """
            UPDATE profiles
            SET
                status = CASE
                    WHEN is_enabled = FALSE THEN 'DISABLED'
                    ELSE 'READY'
                END,
                status_reason = $3,
                status_since = NOW(),
                available_after = NOW(),
                cooldown_until = NULL,
                manual_login_required = FALSE,
                manual_login_reason = NULL,
                manual_login_required_at = NULL,
                quarantined_at = NULL,
                quarantine_reason = NULL,
                quarantine_evidence = NULL,
                failure_count = 0,
                consecutive_empty_claims = 0,
                last_error = NULL
            WHERE worker_name = $1
              AND user_data_dir = $2
            RETURNING
                profile_id,
                worker_name,
                user_data_dir,
                is_enabled,
                status,
                status_reason,
                status_since,
                available_after,
                cooldown_until,
                manual_login_required,
                manual_login_reason,
                manual_login_required_at,
                quarantined_at,
                quarantine_reason,
                quarantine_evidence,
                failure_count,
                consecutive_empty_claims,
                last_started_at,
                last_success_at,
                last_failure_at,
                last_heartbeat_at,
                last_error,
                profile_lease_token,
                profile_lease_expires_at,
                created_at,
                updated_at
            """,
            worker_name_clean,
            user_data_dir,
            reason,
        )

    return {
        "ok": True,
        "worker_name": worker_name_clean,
        "user_data_dir": user_data_dir,
        "execution_profile": _serialize_datetimes(
            dict(execution_profile),
            (
                "status_since",
                "cooldown_until",
                "manual_login_required_at",
                "quarantined_at",
                "last_selected_at",
                "last_success_at",
                "created_at",
                "updated_at",
            ),
        ),
        "profile": (
            _serialize_datetimes(
                dict(v4_profile),
                (
                    "status_since",
                    "available_after",
                    "cooldown_until",
                    "manual_login_required_at",
                    "quarantined_at",
                    "last_started_at",
                    "last_success_at",
                    "last_failure_at",
                    "last_heartbeat_at",
                    "profile_lease_expires_at",
                    "created_at",
                    "updated_at",
                ),
            )
            if v4_profile
            else None
        ),
    }


@app.get("/worker-routes")
async def get_worker_routes(
    worker_name: str | None = Query(default=None),
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    worker_name_filter = (worker_name or "").strip()

    if worker_name_filter:
        query = """
            SELECT
                r.id,
                r.worker_name,
                r.route_name,
                r.is_enabled,
                r.proxy_server,
                r.proxy_username,
                CASE WHEN r.proxy_password IS NOT NULL AND r.proxy_password <> '' THEN '****' ELSE NULL END AS proxy_password,
                r.proxy_mode,
                r.proxy_pool,
                r.preferred_proxy_key,
                r.preferred_proxy_updated_at,
                r.user_data_dir,
                r.search_queries,
                r.priority,
                r.status,
                r.status_reason,
                r.status_since,
                r.next_run_at,
                r.route_interval_seconds,
                r.avg_result_count,
                r.profitable_hit_rate,
                r.recent_duplicate_ratio,
                r.avg_page_load_ms,
                r.successful_cycles,
                r.last_selected_at,
                r.last_success_at,
                r.consecutive_failures,
                r.cooldown_until,
                r.lane_override,
                r.computed_lane,
                r.effective_lane,
                r.priority_score,
                r.priority_score_updated_at,
                r.manual_login_required,
                r.manual_login_reason,
                r.manual_login_required_at,
                r.quarantined_at,
                r.quarantine_reason,
                r.quarantine_evidence,
                r.last_error,
                r.created_at,
                r.updated_at
            FROM worker_routes r
            WHERE r.worker_name = $1
            ORDER BY r.worker_name, r.priority, r.route_name
        """
        params = (worker_name_filter,)
    else:
        query = """
            SELECT
                r.id,
                r.worker_name,
                r.route_name,
                r.is_enabled,
                r.proxy_server,
                r.proxy_username,
                CASE WHEN r.proxy_password IS NOT NULL AND r.proxy_password <> '' THEN '****' ELSE NULL END AS proxy_password,
                r.proxy_mode,
                r.proxy_pool,
                r.preferred_proxy_key,
                r.preferred_proxy_updated_at,
                r.user_data_dir,
                r.search_queries,
                r.priority,
                r.status,
                r.status_reason,
                r.status_since,
                r.next_run_at,
                r.route_interval_seconds,
                r.avg_result_count,
                r.profitable_hit_rate,
                r.recent_duplicate_ratio,
                r.avg_page_load_ms,
                r.successful_cycles,
                r.last_selected_at,
                r.last_success_at,
                r.consecutive_failures,
                r.cooldown_until,
                r.lane_override,
                r.computed_lane,
                r.effective_lane,
                r.priority_score,
                r.priority_score_updated_at,
                r.manual_login_required,
                r.manual_login_reason,
                r.manual_login_required_at,
                r.quarantined_at,
                r.quarantine_reason,
                r.quarantine_evidence,
                r.last_error,
                r.created_at,
                r.updated_at
            FROM worker_routes r
            ORDER BY r.worker_name, r.priority, r.route_name
        """
        params = ()

    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    items = [
        _serialize_datetimes(
            dict(row),
            (
                "last_selected_at",
                "last_success_at",
                "status_since",
                "next_run_at",
                "cooldown_until",
                "priority_score_updated_at",
                "manual_login_required_at",
                "quarantined_at",
                "preferred_proxy_updated_at",
                "created_at",
                "updated_at",
            ),
        )
        for row in rows
    ]
    return {"count": len(items), "items": items}


@app.put("/worker-routes/{worker_name}/{route_name}")
async def upsert_worker_route(
    worker_name: str,
    route_name: str,
    payload: WorkerRouteUpsert,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    worker_name_clean = worker_name.strip()
    route_name_clean = route_name.strip()
    if not worker_name_clean or not route_name_clean:
        raise HTTPException(status_code=400, detail="worker_name and route_name are required.")

    query = """
        INSERT INTO worker_routes (
            worker_name,
            route_name,
            is_enabled,
            proxy_server,
            proxy_username,
            proxy_password,
            proxy_mode,
            proxy_pool,
            preferred_proxy_key,
            preferred_proxy_updated_at,
            user_data_dir,
            search_queries,
            priority,
            route_interval_seconds,
            lane_override,
            computed_lane,
            effective_lane,
            status,
            status_reason,
            status_since,
            manual_login_required,
            manual_login_reason,
            manual_login_required_at,
            quarantined_at,
            quarantine_reason
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            NULL, NULL,
            $9, $10, $11, $12,
            $17, 'warm', COALESCE($17, 'warm'),
            CASE
                WHEN $3::BOOLEAN = FALSE THEN 'DISABLED'
                WHEN COALESCE($15, FALSE) THEN 'NEEDS_LOGIN'
                ELSE COALESCE(NULLIF(BTRIM($13), ''), 'ENABLED')
            END,
            CASE
                WHEN $3::BOOLEAN = FALSE THEN COALESCE(NULLIF($14, ''), 'disabled by operator')
                WHEN COALESCE($15, FALSE) THEN COALESCE(NULLIF($16, ''), 'manual login required')
                ELSE NULLIF($14, '')
            END,
            NOW(),
            COALESCE($15, FALSE),
            CASE
                WHEN COALESCE($15, FALSE) THEN COALESCE(NULLIF($16, ''), 'manual login required')
                ELSE NULL
            END,
            CASE
                WHEN COALESCE($15, FALSE) THEN NOW()
                ELSE NULL
            END,
            CASE
                WHEN COALESCE($15, FALSE) THEN NOW()
                ELSE NULL
            END,
            CASE
                WHEN COALESCE($15, FALSE) THEN COALESCE(NULLIF($16, ''), 'manual login required')
                ELSE NULL
            END
        )
        ON CONFLICT (worker_name, route_name) DO UPDATE SET
            is_enabled = EXCLUDED.is_enabled,
            proxy_server = EXCLUDED.proxy_server,
            proxy_username = EXCLUDED.proxy_username,
            proxy_password = EXCLUDED.proxy_password,
            proxy_mode = EXCLUDED.proxy_mode,
            proxy_pool = EXCLUDED.proxy_pool,
            user_data_dir = EXCLUDED.user_data_dir,
            search_queries = EXCLUDED.search_queries,
            priority = EXCLUDED.priority,
            route_interval_seconds = EXCLUDED.route_interval_seconds,
            lane_override = CASE
                WHEN $18::BOOLEAN THEN $17
                ELSE worker_routes.lane_override
            END,
            effective_lane = CASE
                WHEN $18::BOOLEAN THEN COALESCE($17, worker_routes.computed_lane, 'warm')
                ELSE worker_routes.effective_lane
            END,
            status = CASE
                WHEN EXCLUDED.is_enabled = FALSE THEN 'DISABLED'
                WHEN COALESCE($15, worker_routes.manual_login_required) THEN 'NEEDS_LOGIN'
                WHEN COALESCE(NULLIF(BTRIM($13), ''), '') <> '' THEN COALESCE(NULLIF(BTRIM($13), ''), 'ENABLED')
                WHEN worker_routes.status IN ('DEGRADED', 'THROTTLED', 'COOLDOWN') THEN worker_routes.status
                ELSE 'ENABLED'
            END,
            status_reason = CASE
                WHEN EXCLUDED.is_enabled = FALSE THEN COALESCE(NULLIF($14, ''), 'disabled by operator')
                WHEN COALESCE($15, worker_routes.manual_login_required)
                    THEN COALESCE(NULLIF($16, ''), worker_routes.manual_login_reason, 'manual login required')
                WHEN COALESCE(NULLIF(BTRIM($14), ''), '') <> '' THEN NULLIF($14, '')
                WHEN worker_routes.status = 'NEEDS_LOGIN' AND NOT COALESCE($15, worker_routes.manual_login_required)
                    THEN 'manual login cleared by operator'
                ELSE worker_routes.status_reason
            END,
            status_since = CASE
                WHEN worker_routes.status IS DISTINCT FROM CASE
                    WHEN EXCLUDED.is_enabled = FALSE THEN 'DISABLED'
                    WHEN COALESCE($15, worker_routes.manual_login_required) THEN 'NEEDS_LOGIN'
                    WHEN COALESCE(NULLIF(BTRIM($13), ''), '') <> '' THEN COALESCE(NULLIF(BTRIM($13), ''), 'ENABLED')
                    WHEN worker_routes.status IN ('DEGRADED', 'THROTTLED', 'COOLDOWN') THEN worker_routes.status
                    ELSE 'ENABLED'
                END THEN NOW()
                ELSE worker_routes.status_since
            END,
            manual_login_required = COALESCE($15, worker_routes.manual_login_required),
            manual_login_reason = CASE
                WHEN COALESCE($15, worker_routes.manual_login_required)
                    THEN COALESCE(NULLIF($16, ''), worker_routes.manual_login_reason, 'manual login required')
                ELSE NULL
            END,
            manual_login_required_at = CASE
                WHEN COALESCE($15, worker_routes.manual_login_required)
                    THEN COALESCE(worker_routes.manual_login_required_at, NOW())
                ELSE NULL
            END,
            quarantined_at = CASE
                WHEN COALESCE($15, worker_routes.manual_login_required)
                    THEN COALESCE(worker_routes.quarantined_at, worker_routes.manual_login_required_at, NOW())
                ELSE worker_routes.quarantined_at
            END,
            quarantine_reason = CASE
                WHEN COALESCE($15, worker_routes.manual_login_required)
                    THEN COALESCE(
                        NULLIF($16, ''),
                        worker_routes.quarantine_reason,
                        worker_routes.manual_login_reason,
                        'manual login required'
                    )
                ELSE worker_routes.quarantine_reason
            END
        RETURNING
            id,
            worker_name,
            route_name,
            is_enabled,
            proxy_server,
            proxy_username,
            CASE WHEN proxy_password IS NOT NULL AND proxy_password <> '' THEN '****' ELSE NULL END AS proxy_password,
            proxy_mode,
            proxy_pool,
            preferred_proxy_key,
            preferred_proxy_updated_at,
            user_data_dir,
            search_queries,
            priority,
            status,
            status_reason,
            status_since,
            next_run_at,
            route_interval_seconds,
            avg_result_count,
            profitable_hit_rate,
            recent_duplicate_ratio,
            avg_page_load_ms,
            successful_cycles,
            last_selected_at,
            last_success_at,
            consecutive_failures,
            cooldown_until,
            lane_override,
            computed_lane,
            effective_lane,
            priority_score,
            priority_score_updated_at,
            manual_login_required,
            manual_login_reason,
            manual_login_required_at,
            quarantined_at,
            quarantine_reason,
            quarantine_evidence,
            last_error,
            created_at,
            updated_at
    """

    proxy_mode = _normalize_proxy_mode(payload.proxy_mode)
    proxy_pool = (payload.proxy_pool or "").strip() or None
    user_data_dir = (payload.user_data_dir or "").strip() or None
    route_status = _normalize_route_status(payload.route_status)
    status_reason = (payload.status_reason or "").strip() or None
    lane_override_provided = _payload_field_is_set(payload, "lane_override")
    lane_override = normalize_route_lane(payload.lane_override) if lane_override_provided else None
    raw_lane_override = (payload.lane_override or "").strip()
    if (
        lane_override_provided
        and raw_lane_override
        and raw_lane_override.lower() not in {"auto", "default", "computed"}
        and lane_override is None
    ):
        raise HTTPException(status_code=400, detail="lane_override must be one of hot, warm, sweep, or null.")

    async with app.state.db_pool.acquire() as conn:
        await _assert_unique_profile_dir(
            conn=conn,
            worker_name=worker_name_clean,
            route_name=route_name_clean,
            profile_dir=user_data_dir,
            is_enabled=bool(payload.is_enabled),
        )
        row = await conn.fetchrow(
            query,
            worker_name_clean,
            route_name_clean,
            bool(payload.is_enabled),
            (payload.proxy_server or "").strip(),
            (payload.proxy_username or "").strip() or None,
            (payload.proxy_password or "").strip() or None,
            proxy_mode,
            proxy_pool,
            user_data_dir,
            (payload.search_queries or "").strip() or None,
            int(payload.priority),
            payload.route_interval_seconds,
            route_status,
            status_reason,
            payload.manual_login_required,
            (payload.manual_login_reason or "").strip() or None,
            lane_override,
            lane_override_provided,
        )
        warnings = await _build_route_config_warnings(conn=conn, worker_name=worker_name_clean)

    if not row:
        raise HTTPException(status_code=500, detail="Failed to save worker route.")
    return {
        "ok": True,
        "item": _serialize_datetimes(
            dict(row),
            (
                "last_selected_at",
                "last_success_at",
                "status_since",
                "next_run_at",
                "cooldown_until",
                "priority_score_updated_at",
                "manual_login_required_at",
                "quarantined_at",
                "preferred_proxy_updated_at",
                "created_at",
                "updated_at",
            ),
        ),
        "warnings": warnings,
    }


@app.post("/worker-routes/bootstrap-from-health")
async def bootstrap_worker_routes_from_health(
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)

    heartbeat_query = """
        SELECT
            worker_name,
            COALESCE(NULLIF(BTRIM(route_name), ''), 'env_default') AS route_name,
            NULLIF(BTRIM(route_source), '') AS route_source,
            NULLIF(BTRIM(route_user_data_dir), '') AS route_user_data_dir,
            NULLIF(BTRIM(route_search_queries), '') AS route_search_queries,
            NULLIF(BTRIM(route_proxy_mode), '') AS route_proxy_mode,
            NULLIF(BTRIM(route_proxy_server), '') AS route_proxy_server,
            NULLIF(BTRIM(route_proxy_username), '') AS route_proxy_username,
            NULLIF(BTRIM(route_proxy_password), '') AS route_proxy_password,
            NULLIF(BTRIM(route_proxy_pool), '') AS route_proxy_pool
        FROM worker_heartbeats
        ORDER BY worker_name ASC
    """
    insert_query = """
        INSERT INTO worker_routes (
            worker_name,
            route_name,
            is_enabled,
            proxy_server,
            proxy_username,
            proxy_password,
            proxy_mode,
            proxy_pool,
            preferred_proxy_key,
            preferred_proxy_updated_at,
            user_data_dir,
            search_queries,
            priority,
            route_interval_seconds,
            lane_override,
            computed_lane,
            effective_lane,
            status,
            status_reason,
            status_since,
            manual_login_required,
            manual_login_reason,
            manual_login_required_at,
            quarantined_at,
            quarantine_reason
        ) VALUES (
            $1, $2, TRUE, $3, $4, $5, $6, $7,
            NULL, NULL,
            $8, $9, 100, NULL,
            NULL, 'warm', 'warm',
            'ENABLED', NULL, NOW(),
            FALSE, NULL, NULL, NULL, NULL
        )
        ON CONFLICT (worker_name, route_name) DO NOTHING
        RETURNING worker_name, route_name, user_data_dir, search_queries
    """

    created: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    warnings_by_worker: dict[str, list[str]] = {}

    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch(heartbeat_query)
        for row in rows:
            worker_name = str(row["worker_name"] or "").strip()
            route_name = str(row["route_name"] or "").strip() or "env_default"
            route_source = str(row["route_source"] or "").strip().lower() or "unknown"
            user_data_dir = str(row["route_user_data_dir"] or "").strip() or None
            search_queries = str(row["route_search_queries"] or "").strip() or None
            proxy_mode = _normalize_proxy_mode(row["route_proxy_mode"])
            proxy_server = str(row["route_proxy_server"] or "").strip()
            proxy_username = str(row["route_proxy_username"] or "").strip() or None
            proxy_password = str(row["route_proxy_password"] or "").strip() or None
            proxy_pool = str(row["route_proxy_pool"] or "").strip() or None

            if not worker_name:
                continue
            if route_source not in {"env", "db"}:
                skipped.append({"worker_name": worker_name, "route_name": route_name, "reason": "unsupported_route_source"})
                continue
            if not user_data_dir and not search_queries:
                skipped.append({"worker_name": worker_name, "route_name": route_name, "reason": "no_route_snapshot"})
                continue

            existing_route = await conn.fetchval(
                """
                SELECT 1
                FROM worker_routes
                WHERE worker_name = $1
                LIMIT 1
                """,
                worker_name,
            )
            if existing_route:
                skipped.append({"worker_name": worker_name, "route_name": route_name, "reason": "routes_already_exist"})
                continue

            try:
                await _assert_unique_profile_dir(
                    conn=conn,
                    worker_name=worker_name,
                    route_name=route_name,
                    profile_dir=user_data_dir,
                    is_enabled=True,
                )
            except HTTPException as exc:
                skipped.append(
                    {
                        "worker_name": worker_name,
                        "route_name": route_name,
                        "reason": str(exc.detail or "profile_dir_conflict"),
                    }
                )
                continue

            inserted = await conn.fetchrow(
                insert_query,
                worker_name,
                route_name,
                proxy_server,
                proxy_username,
                proxy_password,
                proxy_mode,
                proxy_pool,
                user_data_dir,
                search_queries,
            )
            if inserted:
                warnings_by_worker[worker_name] = await _build_route_config_warnings(conn=conn, worker_name=worker_name)
                created.append(dict(inserted))
            else:
                skipped.append({"worker_name": worker_name, "route_name": route_name, "reason": "not_inserted"})

    return {
        "ok": True,
        "created_count": len(created),
        "skipped_count": len(skipped),
        "created": created,
        "skipped": skipped,
        "warnings_by_worker": warnings_by_worker,
    }


@app.post("/worker-routes/{worker_name}/{route_name}/retest")
async def retest_worker_route(
    worker_name: str,
    route_name: str,
    payload: WorkerRouteRetestRequest | None = None,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    worker_name_clean = worker_name.strip()
    route_name_clean = route_name.strip()
    if not worker_name_clean or not route_name_clean:
        raise HTTPException(status_code=400, detail="worker_name and route_name are required.")

    reason = ((payload.reason if payload else "") or "").strip() or "manual-login retest requested by operator"
    async with app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE worker_routes
            SET
                manual_login_required = FALSE,
                manual_login_reason = NULL,
                manual_login_required_at = NULL,
                status = CASE
                    WHEN is_enabled = FALSE THEN 'DISABLED'
                    ELSE 'ENABLED'
                END,
                status_reason = $3,
                status_since = NOW(),
                cooldown_until = NULL,
                consecutive_failures = 0,
                next_run_at = CASE
                    WHEN is_enabled = TRUE THEN NOW()
                    ELSE next_run_at
                END,
                last_error = NULL
            WHERE worker_name = $1 AND route_name = $2
            RETURNING
                id,
                worker_name,
                route_name,
                is_enabled,
                proxy_server,
                proxy_username,
                CASE WHEN proxy_password IS NOT NULL AND proxy_password <> '' THEN '****' ELSE NULL END AS proxy_password,
                proxy_mode,
                proxy_pool,
                preferred_proxy_key,
                preferred_proxy_updated_at,
                user_data_dir,
                search_queries,
                priority,
                status,
                status_reason,
                status_since,
                next_run_at,
                route_interval_seconds,
                avg_result_count,
                avg_page_load_ms,
                successful_cycles,
                last_selected_at,
                last_success_at,
                consecutive_failures,
                cooldown_until,
                manual_login_required,
                manual_login_reason,
                manual_login_required_at,
                quarantined_at,
                quarantine_reason,
                quarantine_evidence,
                last_error,
                created_at,
                updated_at
            """,
            worker_name_clean,
            route_name_clean,
            reason,
        )
        warnings = await _build_route_config_warnings(conn=conn, worker_name=worker_name_clean)

    if not row:
        raise HTTPException(status_code=404, detail="Route not found.")

    return {
        "ok": True,
        "item": _serialize_datetimes(
            dict(row),
            (
                "last_selected_at",
                "last_success_at",
                "status_since",
                "next_run_at",
                "cooldown_until",
                "manual_login_required_at",
                "quarantined_at",
                "preferred_proxy_updated_at",
                "created_at",
                "updated_at",
            ),
        ),
        "warnings": warnings,
    }


@app.post("/worker-routes/{worker_name}/bulk-clear-manual-login")
async def bulk_clear_manual_login(
    worker_name: str,
    payload: WorkerRouteBulkClearRequest | None = None,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    worker_name_clean = worker_name.strip()
    if not worker_name_clean:
        raise HTTPException(status_code=400, detail="worker_name is required.")

    reason = ((payload.reason if payload else "") or "").strip() or "manual-login cleared in bulk by operator"
    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE worker_routes
            SET
                manual_login_required = FALSE,
                manual_login_reason = NULL,
                manual_login_required_at = NULL,
                status = CASE
                    WHEN is_enabled = FALSE THEN 'DISABLED'
                    ELSE 'ENABLED'
                END,
                status_reason = $2,
                status_since = NOW(),
                cooldown_until = NULL,
                consecutive_failures = 0,
                next_run_at = CASE
                    WHEN is_enabled = TRUE THEN NOW()
                    ELSE next_run_at
                END
            WHERE worker_name = $1
              AND (COALESCE(manual_login_required, FALSE) = TRUE OR COALESCE(status, '') = 'NEEDS_LOGIN')
            RETURNING route_name
            """,
            worker_name_clean,
            reason,
        )
        warnings = await _build_route_config_warnings(conn=conn, worker_name=worker_name_clean)

    route_names = sorted({str(row["route_name"] or "").strip() for row in rows if str(row["route_name"] or "").strip()})
    return {
        "ok": True,
        "worker_name": worker_name_clean,
        "cleared_count": len(route_names),
        "route_names": route_names,
        "warnings": warnings,
    }


@app.delete("/worker-routes/{worker_name}/{route_name}")
async def delete_worker_route(
    worker_name: str,
    route_name: str,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    worker_name_clean = worker_name.strip()
    route_name_clean = route_name.strip()
    if not worker_name_clean or not route_name_clean:
        raise HTTPException(status_code=400, detail="worker_name and route_name are required.")

    async with app.state.db_pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM worker_routes WHERE worker_name = $1 AND route_name = $2",
            worker_name_clean,
            route_name_clean,
        )
        warnings = await _build_route_config_warnings(conn=conn, worker_name=worker_name_clean)

    deleted = result.split()[-1] != "0"
    return {"ok": True, "deleted": deleted, "warnings": warnings}


@app.get("/worker-health")
async def get_worker_health(
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    query = """
        SELECT
            hb.worker_name,
            hb.route_name,
            hb.status,
            hb.listings_saved,
            hb.query_count,
            hb.last_run_started_at,
            hb.last_run_finished_at,
            hb.route_source,
            hb.route_user_data_dir,
            hb.route_search_queries,
            hb.route_proxy_mode,
            hb.route_proxy_server,
            hb.route_proxy_username,
            hb.route_proxy_password,
            hb.route_proxy_pool,
            hb.last_error,
            hb.updated_at,
            COALESCE(s.listings_scraped_last_minute, 0)::INT AS listings_scraped_last_minute,
            COALESCE(cr.cooldown_until, r.cooldown_until) AS route_cooldown_until,
            GREATEST(
                0,
                COALESCE(EXTRACT(EPOCH FROM (COALESCE(cr.cooldown_until, r.cooldown_until) - NOW())), 0)
            )::INT AS cooldown_remaining_seconds,
            l.proxy_server AS leased_proxy_server,
            l.leased_by_route AS leased_proxy_route,
            l.lease_until AS leased_proxy_until,
            GREATEST(0, COALESCE(EXTRACT(EPOCH FROM (l.lease_until - NOW())), 0))::INT AS lease_remaining_seconds
        FROM worker_heartbeats hb
        LEFT JOIN LATERAL (
            SELECT
                COALESCE(SUM(scraped_count), 0) AS listings_scraped_last_minute
            FROM worker_scrape_events
            WHERE worker_name = hb.worker_name
              AND observed_at >= NOW() - INTERVAL '60 seconds'
        ) s ON TRUE
        LEFT JOIN worker_routes r
               ON r.worker_name = hb.worker_name
              AND r.route_name = hb.route_name
        LEFT JOIN central_routes cr
               ON cr.route_name = hb.route_name
        LEFT JOIN LATERAL (
            SELECT
                proxy_server,
                leased_by_route,
                lease_until
            FROM worker_proxy_leases
            WHERE leased_by_worker = hb.worker_name
              AND COALESCE(lease_until, TIMESTAMPTZ '-infinity') > NOW()
            ORDER BY lease_until DESC NULLS LAST
            LIMIT 1
        ) l ON TRUE
        ORDER BY hb.worker_name ASC
    """
    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch(query)

    items: list[dict[str, Any]] = []
    for row in rows:
        item = _serialize_datetimes(
            dict(row),
            (
                "last_run_started_at",
                "last_run_finished_at",
                "updated_at",
                "route_cooldown_until",
                "leased_proxy_until",
            ),
        )
        item["has_active_lease"] = bool(item.get("leased_proxy_server")) and int(
            item.get("lease_remaining_seconds") or 0
        ) > 0
        item["is_route_in_cooldown"] = int(item.get("cooldown_remaining_seconds") or 0) > 0
        items.append(item)
    return {"count": len(items), "items": items}


@app.get("/proxy-provider-stats")
async def get_proxy_provider_stats(
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    """Return live proxy gateway health metrics from the provider API."""
    await _auth_rest(x_api_token)
    import os as _os

    token = _os.getenv("PROXY_PROVIDER_GATEWAY_TOKEN", "").strip()
    if not token:
        return {"ok": False, "error": "PROXY_PROVIDER_GATEWAY_TOKEN not configured"}

    import aiohttp as _aiohttp

    url = f"https://api.proxyrotator.com/proxy-gateways/realtime-usage/{token}/"
    try:
        async with _aiohttp.ClientSession(
            timeout=_aiohttp.ClientTimeout(total=10)
        ) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    return {
                        "ok": False,
                        "error": f"Provider API returned HTTP {resp.status}",
                        "detail": error_text[:500],
                    }
                data = await resp.json()
    except Exception as exc:
        return {"ok": False, "error": f"Request failed: {str(exc)[:200]}"}

    threads_connected = int(data.get("threadsConnected") or 0)
    threads_utilization_raw = str(data.get("threadsUtilization") or "0/5")
    parts = threads_utilization_raw.split("/")
    threads_used = int(parts[0]) if len(parts) >= 1 else 0
    threads_total = int(parts[1]) if len(parts) >= 2 else 5
    threads_total = max(1, threads_total)

    success_count = int(data.get("successfulConnections") or 0)
    thread_limit_errors = int(data.get("threadLimitReachedErrors") or 0)
    total_attempts = success_count + thread_limit_errors
    error_rate = (thread_limit_errors / total_attempts) if total_attempts > 0 else 0.0

    return {
        "ok": True,
        "threads_connected": threads_connected,
        "threads_used": threads_used,
        "threads_total": threads_total,
        "utilization_ratio": round(threads_used / threads_total, 4),
        "success_count": success_count,
        "thread_limit_errors": thread_limit_errors,
        "error_rate": round(error_rate, 4),
        "bandwidth_mb": str(data.get("bandwidthTotalMB") or "0"),
        "is_saturated": (threads_used / threads_total) >= 0.80,
        "raw": data,
    }


@app.get("/proxy-stats")
async def get_proxy_stats(
    only_banned: bool = Query(default=False),
    limit: int = Query(default=500, ge=1, le=5000),
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    where_clause = "WHERE COALESCE(banned_until > NOW(), FALSE)" if only_banned else ""
    query = f"""
        SELECT
            proxy_key,
            consecutive_failures,
            last_success_at,
            banned_until,
            avg_latency_ms,
            updated_at,
            COALESCE(banned_until > NOW(), FALSE) AS is_banned,
            GREATEST(0, COALESCE(EXTRACT(EPOCH FROM (banned_until - NOW())), 0))::INT AS banned_remaining_seconds
        FROM proxy_stats
        {where_clause}
        ORDER BY
            COALESCE(banned_until > NOW(), FALSE) DESC,
            consecutive_failures DESC,
            updated_at DESC
        LIMIT $1
    """

    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch(query, limit)

    items = [
        _serialize_datetimes(
            dict(row),
            (
                "last_success_at",
                "banned_until",
                "updated_at",
            ),
        )
        for row in rows
    ]
    return {"count": len(items), "items": items}


@app.post("/proxy-stats/reset")
async def reset_proxy_stats(
    payload: ProxyStatsResetRequest,
    x_api_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await _auth_rest(x_api_token)
    proxy_key = _resolve_proxy_key(
        proxy_key=payload.proxy_key,
        proxy_server=payload.proxy_server,
        proxy_username=payload.proxy_username,
    )
    clear_failures = bool(payload.clear_failures)

    async with app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO proxy_stats (
                proxy_key,
                consecutive_failures,
                banned_until,
                updated_at
            ) VALUES (
                $1,
                0,
                NULL,
                NOW()
            )
            ON CONFLICT (proxy_key) DO UPDATE SET
                consecutive_failures = CASE
                    WHEN $2::BOOLEAN THEN 0
                    ELSE proxy_stats.consecutive_failures
                END,
                banned_until = NULL,
                updated_at = NOW()
            RETURNING
                proxy_key,
                consecutive_failures,
                last_success_at,
                banned_until,
                avg_latency_ms,
                updated_at,
                COALESCE(banned_until > NOW(), FALSE) AS is_banned,
                GREATEST(0, COALESCE(EXTRACT(EPOCH FROM (banned_until - NOW())), 0))::INT AS banned_remaining_seconds
            """,
            proxy_key,
            clear_failures,
        )

    if not row:
        raise HTTPException(status_code=500, detail="Failed to reset proxy stats.")

    return {
        "ok": True,
        "clear_failures": clear_failures,
        "item": _serialize_datetimes(
            dict(row),
            (
                "last_success_at",
                "banned_until",
                "updated_at",
            ),
        ),
    }


@app.websocket("/ws/listings")
async def ws_listings(websocket: WebSocket, token: str | None = Query(default=None)) -> None:
    _assert_api_token(token)
    await websocket.accept()
    if not await _is_gui_websocket_push_enabled():
        emit_json_log(
            "websocket_client_disabled",
            service="api",
            client_host=_client_host(websocket),
            reason="feature_flag_off",
        )
        with suppress(Exception):
            await websocket.send_json(
                {
                    "event": "websocket_disabled",
                    "at": utc_now_iso(),
                    "reason": "feature_flag_off",
                }
            )
        with suppress(Exception):
            await websocket.close(code=1000)
        return

    client = await app.state.ws_manager.register(websocket)
    emit_json_log(
        "websocket_client_connected",
        service="api",
        client_id=client.client_id,
        client_host=_client_host(websocket),
        websocket_client_count=await app.state.ws_manager.active_count(),
    )
    heartbeat_task = asyncio.create_task(_ws_heartbeat_loop(client))

    disconnect_reason = "client_disconnected"
    try:
        while True:
            message = await websocket.receive()
            message_type = str(message.get("type") or "")
            if message_type == "websocket.disconnect":
                disconnect_reason = "client_disconnected"
                break
            if message_type != "websocket.receive":
                continue

            raw_text = message.get("text")
            if not raw_text:
                continue

            try:
                payload = json.loads(raw_text)
            except Exception:
                continue

            event_name = str(payload.get("event") or "").strip().lower()
            if event_name == "pong":
                client.last_pong_at = time.monotonic()
    except WebSocketDisconnect:
        disconnect_reason = "client_disconnected"
    except Exception as exc:
        disconnect_reason = f"error:{str(exc)[:200]}"
    finally:
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task
        await app.state.ws_manager.unregister(client)
        emit_json_log(
            "websocket_client_disconnected",
            service="api",
            client_id=client.client_id,
            client_host=_client_host(websocket),
            reason=disconnect_reason,
            websocket_client_count=await app.state.ws_manager.active_count(),
        )
        with suppress(Exception):
            await client.close(code=1000, reason=disconnect_reason)


async def _ws_heartbeat_loop(client: ManagedWebSocketClient) -> None:
    while True:
        await asyncio.sleep(WEBSOCKET_PING_INTERVAL_SECONDS)
        if not await _is_gui_websocket_push_enabled():
            emit_json_log(
                "websocket_client_disabled",
                service="api",
                client_id=client.client_id,
                client_host=_client_host(client.websocket),
                reason="feature_flag_off",
            )
            await _close_ws_client(
                client,
                payload={
                    "event": "websocket_disabled",
                    "at": utc_now_iso(),
                    "reason": "feature_flag_off",
                },
                reason="feature_flag_off",
            )
            return

        if (time.monotonic() - client.last_pong_at) > WEBSOCKET_PONG_TIMEOUT_SECONDS:
            emit_json_log(
                "websocket_client_timeout",
                service="api",
                client_id=client.client_id,
                client_host=_client_host(client.websocket),
                reason="pong_timeout",
            )
            await _close_ws_client(client, reason="pong_timeout")
            return

        try:
            await client.send_json({"event": "ping", "at": utc_now_iso()})
        except Exception:
            await _close_ws_client(client, reason="ping_send_failed")
            return
