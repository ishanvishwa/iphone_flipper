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


def _safe_lease_seconds(lease_seconds: int, *, minimum: int = 30) -> int:
    return max(minimum, int(lease_seconds or minimum))


def _row_to_dict(row: object) -> dict[str, object] | None:
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


async def claim_next_profile(
    pool: asyncpg.Pool,
    *,
    worker_name: str,
    lease_token: str,
    lease_seconds: int,
) -> dict[str, object] | None:
    worker_name_value = str(worker_name or "").strip()
    lease_token_value = str(lease_token or "").strip()
    if not worker_name_value or not lease_token_value:
        return None

    safe_lease_seconds = _safe_lease_seconds(lease_seconds)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE profiles
            SET
                profile_lease_token = $2,
                profile_lease_expires_at = NOW() + ($3::INT * INTERVAL '1 second'),
                last_started_at = NOW(),
                last_heartbeat_at = NOW()
            WHERE profile_id = (
                SELECT profile_id
                FROM profiles
                WHERE is_enabled = TRUE
                  AND status IN ('READY', 'DEGRADED', 'THROTTLED')
                  AND COALESCE(manual_login_required, FALSE) = FALSE
                  AND (cooldown_until IS NULL OR cooldown_until <= NOW())
                  AND (available_after IS NULL OR available_after <= NOW())
                  AND (profile_lease_expires_at IS NULL OR profile_lease_expires_at <= NOW())
                ORDER BY
                    CASE
                        WHEN worker_name = $1 THEN 0
                        ELSE 1
                    END,
                    CASE status
                        WHEN 'READY' THEN 0
                        WHEN 'DEGRADED' THEN 1
                        WHEN 'THROTTLED' THEN 2
                        ELSE 9
                    END,
                    last_started_at ASC NULLS FIRST,
                    profile_id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
            """,
            worker_name_value,
            lease_token_value,
            safe_lease_seconds,
        )
    return _row_to_dict(row)


async def heartbeat_profile_lease(
    pool: asyncpg.Pool,
    *,
    profile_id: int | None,
    lease_token: str,
    lease_seconds: int,
) -> dict[str, object] | None:
    if profile_id is None:
        return None
    lease_token_value = str(lease_token or "").strip()
    if not lease_token_value:
        return None

    safe_lease_seconds = _safe_lease_seconds(lease_seconds)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE profiles
            SET
                profile_lease_expires_at = NOW() + ($3::INT * INTERVAL '1 second'),
                last_heartbeat_at = NOW()
            WHERE profile_id = $1
              AND profile_lease_token = $2
            RETURNING *
            """,
            int(profile_id),
            lease_token_value,
            safe_lease_seconds,
        )
    return _row_to_dict(row)


async def release_profile_lease(
    pool: asyncpg.Pool,
    *,
    profile_id: int | None,
    lease_token: str,
    available_after_seconds: int | None = None,
) -> dict[str, object] | None:
    if profile_id is None:
        return None
    lease_token_value = str(lease_token or "").strip()
    if not lease_token_value:
        return None

    safe_available_after = None
    if available_after_seconds is not None:
        safe_available_after = max(0, int(available_after_seconds))

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE profiles
            SET
                profile_lease_token = NULL,
                profile_lease_expires_at = NULL,
                available_after = CASE
                    WHEN $3::INT IS NULL OR $3::INT <= 0 THEN profiles.available_after
                    ELSE NOW() + ($3::INT * INTERVAL '1 second')
                END,
                last_heartbeat_at = NOW()
            WHERE profile_id = $1
              AND profile_lease_token = $2
            RETURNING *
            """,
            int(profile_id),
            lease_token_value,
            safe_available_after,
        )
    return _row_to_dict(row)


async def claim_next_due_family(
    pool: asyncpg.Pool,
    *,
    lease_token: str,
    lease_seconds: int,
    family_names: list[str] | tuple[str, ...] | None = None,
) -> dict[str, object] | None:
    lease_token_value = str(lease_token or "").strip()
    if not lease_token_value:
        return None

    safe_lease_seconds = _safe_lease_seconds(lease_seconds)
    allowlisted_family_names = [
        str(name).strip().lower()
        for name in (family_names or [])
        if str(name).strip()
    ]
    family_filter_sql = ""
    query_args: list[object] = [lease_token_value, safe_lease_seconds]
    if allowlisted_family_names:
        family_filter_sql = "\n                  AND LOWER(name) = ANY($3::TEXT[])"
        query_args.append(allowlisted_family_names)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            UPDATE query_families
            SET
                family_lease_token = $1,
                family_lease_expires_at = NOW() + ($2::INT * INTERVAL '1 second'),
                last_claimed_at = NOW()
            WHERE family_id = (
                SELECT family_id
                FROM query_families
                WHERE is_enabled = TRUE
                  AND next_due_at <= NOW()
                  AND (family_lease_expires_at IS NULL OR family_lease_expires_at <= NOW())
                  {family_filter_sql}
                  AND EXISTS (
                        SELECT 1
                        FROM query_variants
                        WHERE query_variants.family_id = query_families.family_id
                          AND COALESCE(query_variants.is_enabled, TRUE) = TRUE
                          AND LOWER(COALESCE(query_variants.validation_state, 'pending_validation')) = 'validated'
                  )
                ORDER BY
                    priority_score DESC NULLS LAST,
                    priority DESC,
                    next_due_at ASC,
                    family_id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
            """,
            *query_args,
        )
    return _row_to_dict(row)


async def heartbeat_family_lease(
    pool: asyncpg.Pool,
    *,
    family_id: int | None,
    lease_token: str,
    lease_seconds: int,
) -> dict[str, object] | None:
    if family_id is None:
        return None
    lease_token_value = str(lease_token or "").strip()
    if not lease_token_value:
        return None

    safe_lease_seconds = _safe_lease_seconds(lease_seconds)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE query_families
            SET
                family_lease_expires_at = NOW() + ($3::INT * INTERVAL '1 second')
            WHERE family_id = $1
              AND family_lease_token = $2
            RETURNING *
            """,
            int(family_id),
            lease_token_value,
            safe_lease_seconds,
        )
    return _row_to_dict(row)


async def release_family_lease(
    pool: asyncpg.Pool,
    *,
    family_id: int | None,
    lease_token: str,
) -> dict[str, object] | None:
    if family_id is None:
        return None
    lease_token_value = str(lease_token or "").strip()
    if not lease_token_value:
        return None

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE query_families
            SET
                family_lease_token = NULL,
                family_lease_expires_at = NULL
            WHERE family_id = $1
              AND family_lease_token = $2
            RETURNING *
            """,
            int(family_id),
            lease_token_value,
        )
    return _row_to_dict(row)
