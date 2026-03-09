from __future__ import annotations

import hashlib
import re
import uuid
from urllib.parse import unquote, urlparse

import asyncpg


def canonical_proxy_key(server: str, username: str | None = None) -> str:
    parsed = urlparse(server or "")
    scheme = (parsed.scheme or "socks5").strip().lower()
    host = (parsed.hostname or "").strip().lower()
    port = parsed.port or 0
    user = (username or unquote(parsed.username or "")).strip()
    return f"{scheme}:{host}:{int(port)}:{user}"


def proxy_identity(server: str, username: str, password: str = "") -> str:
    _ = password
    # Keep password out of identity so fixed/auto routes cannot collide on endpoint+username.
    return canonical_proxy_key(server=server, username=username)


def normalize_query_lock_key(query: str) -> str:
    normalized_query = re.sub(r"\s+", " ", str(query or "").strip().lower())
    digest = hashlib.sha1(normalized_query.encode("utf-8")).hexdigest()[:24]
    return f"query-lock:{digest}"


async def try_acquire_query_lock(
    redis_client: object,
    *,
    worker_name: str,
    route_name: str,
    query: str,
    lease_seconds: int,
) -> dict[str, str] | None:
    query_text = str(query or "").strip()
    if not query_text:
        return None
    lock_key = normalize_query_lock_key(query_text)
    lock_token = f"{(worker_name or 'worker').strip()}:{(route_name or 'route').strip()}:{uuid.uuid4().hex[:12]}"
    safe_lease_seconds = max(30, int(lease_seconds or 30))
    acquired = await redis_client.set(lock_key, lock_token, ex=safe_lease_seconds, nx=True)
    if not acquired:
        return None
    return {
        "lock_key": lock_key,
        "lock_token": lock_token,
        "query": query_text,
    }


async def release_query_lock(
    redis_client: object,
    *,
    lock_key: str | None,
    lock_token: str | None,
) -> None:
    key = str(lock_key or "").strip()
    token = str(lock_token or "").strip()
    if not key or not token:
        return
    await redis_client.eval(
        """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        end
        return 0
        """,
        1,
        key,
        token,
    )


async def try_acquire_proxy_lease(
    pool: asyncpg.Pool,
    worker_name: str,
    route_name: str,
    candidate: dict[str, str],
    lease_seconds: int,
    reuse_cooldown_seconds: int,
) -> dict[str, str] | None:
    proxy_server = str(candidate.get("proxy_server") or "").strip()
    proxy_username = str(candidate.get("proxy_username") or "").strip()
    proxy_password = str(candidate.get("proxy_password") or "").strip()
    if not proxy_server:
        return None

    proxy_id = proxy_identity(proxy_server, proxy_username, proxy_password)
    safe_lease_seconds = max(30, int(lease_seconds or 30))
    safe_reuse_cooldown_seconds = max(0, int(reuse_cooldown_seconds or 0))
    route_name_value = str(route_name or "unknown").strip() or "unknown"
    worker_name_value = str(worker_name or "").strip() or None

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO worker_proxy_leases (
                proxy_id,
                proxy_server,
                proxy_username,
                proxy_password,
                leased_by_worker,
                leased_by_route,
                leased_at,
                lease_until,
                last_used_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                NOW(),
                NOW() + ($7::INT * INTERVAL '1 second'),
                NOW(),
                NOW()
            )
            ON CONFLICT (proxy_id) DO UPDATE SET
                proxy_server = EXCLUDED.proxy_server,
                proxy_username = EXCLUDED.proxy_username,
                proxy_password = EXCLUDED.proxy_password,
                leased_by_worker = EXCLUDED.leased_by_worker,
                leased_by_route = EXCLUDED.leased_by_route,
                leased_at = NOW(),
                lease_until = NOW() + ($7::INT * INTERVAL '1 second'),
                updated_at = NOW()
            WHERE (
                COALESCE(worker_proxy_leases.lease_until, TIMESTAMPTZ '-infinity') <= NOW()
                AND (
                    $8::INT = 0
                    OR COALESCE(worker_proxy_leases.last_used_at, TIMESTAMPTZ '-infinity')
                       <= NOW() - ($8::INT * INTERVAL '1 second')
                )
            )
            RETURNING proxy_id
            """,
            proxy_id,
            proxy_server,
            proxy_username or None,
            proxy_password or None,
            worker_name_value,
            route_name_value,
            safe_lease_seconds,
            safe_reuse_cooldown_seconds,
        )
    if not row:
        return None

    leased = dict(candidate)
    leased["proxy_id"] = proxy_id
    return leased


async def release_proxy_lease(
    pool: asyncpg.Pool,
    worker_name: str,
    route_name: str,
    proxy_id: str | None,
) -> None:
    proxy_id_value = str(proxy_id or "").strip()
    if not proxy_id_value:
        return

    route_name_value = str(route_name or "unknown").strip() or "unknown"
    worker_name_value = str(worker_name or "").strip()
    if not worker_name_value:
        return

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE worker_proxy_leases
            SET
                leased_by_worker = NULL,
                leased_by_route = NULL,
                leased_at = NULL,
                lease_until = NOW(),
                last_used_at = NOW(),
                updated_at = NOW()
            WHERE proxy_id = $1
              AND leased_by_worker = $2
              AND leased_by_route = $3
            """,
            proxy_id_value,
            worker_name_value,
            route_name_value,
        )


async def refresh_proxy_lease(
    pool: asyncpg.Pool,
    worker_name: str,
    route_name: str,
    proxy_id: str | None,
    lease_seconds: int,
) -> bool:
    proxy_id_value = str(proxy_id or "").strip()
    if not proxy_id_value:
        return False

    route_name_value = str(route_name or "unknown").strip() or "unknown"
    worker_name_value = str(worker_name or "").strip()
    if not worker_name_value:
        return False

    safe_lease_seconds = max(30, int(lease_seconds or 30))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE worker_proxy_leases
            SET
                lease_until = NOW() + ($4::INT * INTERVAL '1 second'),
                updated_at = NOW()
            WHERE proxy_id = $1
              AND leased_by_worker = $2
              AND leased_by_route = $3
            RETURNING proxy_id
            """,
            proxy_id_value,
            worker_name_value,
            route_name_value,
            safe_lease_seconds,
        )
    return bool(row)
