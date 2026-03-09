import asyncio
import json
import os
import time
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urlparse
from uuid import uuid4

import asyncpg
from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from server.services.common.feature_flags import FLAG_HASH_KEY, RedisFeatureFlags
from server.services.common.observability import emit_json_log, monotonic_duration_ms, utc_now_iso
from server.services.common.schema_ensure import ensure_worker_tables as ensure_common_worker_tables

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
    proxy_server: str = Field(min_length=1, max_length=512)
    proxy_username: str | None = None
    proxy_password: str | None = None
    proxy_mode: str = Field(default="fixed", max_length=32)
    proxy_pool: str | None = None
    user_data_dir: str | None = None
    search_queries: str | None = None
    priority: int = Field(default=100, ge=0, le=100000)
    route_interval_seconds: int | None = Field(default=None, ge=1, le=86400)
    route_status: str | None = Field(default=None, max_length=32)
    status_reason: str | None = None
    manual_login_required: bool | None = None
    manual_login_reason: str | None = None


class WorkerRouteRetestRequest(BaseModel):
    reason: str | None = None


class WorkerRouteBulkClearRequest(BaseModel):
    reason: str | None = None


class ProxyStatsResetRequest(BaseModel):
    proxy_key: str | None = None
    proxy_server: str | None = None
    proxy_username: str | None = None
    clear_failures: bool = True


def _assert_api_token(token: str | None) -> None:
    if not API_TOKEN:
        if APP_ENV == "production":
            raise HTTPException(status_code=401, detail="APP_API_TOKEN is not configured.")
        return
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


async def _auth_rest(x_api_token: str | None) -> None:
    _assert_api_token(x_api_token)


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
    emit_json_log(
        "feature_flag_snapshot",
        service="api",
        flag_hash_key=FLAG_HASH_KEY,
        flags=await app.state.feature_flags.snapshot(),
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
                r.avg_page_load_ms,
                r.successful_cycles,
                r.last_selected_at,
                r.last_success_at,
                r.consecutive_failures,
                r.cooldown_until,
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
                r.avg_page_load_ms,
                r.successful_cycles,
                r.last_selected_at,
                r.last_success_at,
                r.consecutive_failures,
                r.cooldown_until,
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
    """

    proxy_mode = _normalize_proxy_mode(payload.proxy_mode)
    proxy_pool = (payload.proxy_pool or "").strip() or None
    user_data_dir = (payload.user_data_dir or "").strip() or None
    route_status = _normalize_route_status(payload.route_status)
    status_reason = (payload.status_reason or "").strip() or None

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
            payload.proxy_server.strip(),
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
                "manual_login_required_at",
                "quarantined_at",
                "preferred_proxy_updated_at",
                "created_at",
                "updated_at",
            ),
        ),
        "warnings": warnings,
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
            hb.last_error,
            hb.updated_at,
            COALESCE(s.listings_scraped_last_minute, 0)::INT AS listings_scraped_last_minute,
            r.cooldown_until AS route_cooldown_until,
            GREATEST(0, COALESCE(EXTRACT(EPOCH FROM (r.cooldown_until - NOW())), 0))::INT AS cooldown_remaining_seconds,
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
