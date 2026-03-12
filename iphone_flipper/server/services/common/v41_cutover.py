from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from server.services.common.feature_flags import FLAG_HASH_KEY, parse_feature_flag_value

V41_FAMILY_NAME = "iphone_broad"
V41_BROAD_QUERY = "iPhone"
V41_MIN_GAP_SECONDS = 5
ROLLBACK_FLAG_DEFAULTS: dict[str, str] = {
    "ENABLE_CENTRAL_ROUTE_DISPATCH": "1",
    "ENABLE_V4_WARM_RUNTIME": "0",
    "ENABLE_V4_FIRST_SEEN_DEDUPE": "0",
    "ENABLE_V4_UPDATE_EVENTS": "0",
}


@dataclass(frozen=True)
class SeedRouteCandidate:
    route_name: str
    legacy_worker_name: str | None
    legacy_route_name: str | None
    priority: int
    priority_score: float | None
    effective_lane: str
    route_interval_seconds: int | None
    queries: tuple[str, ...]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalized_queries(raw_queries: Sequence[object] | None) -> tuple[str, ...]:
    tokens: list[str] = []
    seen: set[str] = set()
    for raw_query in raw_queries or ():
        query = str(raw_query or "").strip()
        if not query:
            continue
        identity = query.lower()
        if identity in seen:
            continue
        seen.add(identity)
        tokens.append(query)
    return tuple(tokens)


def _normalize_route_candidate(route: Mapping[str, Any]) -> SeedRouteCandidate:
    return SeedRouteCandidate(
        route_name=str(route.get("route_name") or "").strip(),
        legacy_worker_name=str(route.get("legacy_worker_name") or "").strip() or None,
        legacy_route_name=str(route.get("legacy_route_name") or "").strip() or None,
        priority=max(0, int(route.get("priority") or 100)),
        priority_score=float(route["priority_score"]) if route.get("priority_score") is not None else None,
        effective_lane=str(route.get("effective_lane") or "warm").strip().lower() or "warm",
        route_interval_seconds=int(route["route_interval_seconds"]) if route.get("route_interval_seconds") else None,
        queries=_normalized_queries(route.get("queries")),
    )


def choose_v41_seed_route(
    routes: Sequence[Mapping[str, Any]],
    *,
    preferred_route_name: str | None = None,
    broad_query: str = V41_BROAD_QUERY,
) -> SeedRouteCandidate | None:
    candidates = [_normalize_route_candidate(route) for route in routes]
    if not candidates:
        return None

    preferred_name = str(preferred_route_name or "").strip()
    if preferred_name:
        for candidate in candidates:
            if candidate.route_name == preferred_name:
                return candidate
        return None

    broad_query_identity = str(broad_query or "").strip().lower()

    def _score(candidate: SeedRouteCandidate) -> tuple[int, int, int, float, int, str]:
        query_identities = {query.lower() for query in candidate.queries}
        exact_single = 1 if len(candidate.queries) == 1 and broad_query_identity in query_identities else 0
        contains_broad = 1 if broad_query_identity in query_identities else 0
        lane_rank = 1 if candidate.effective_lane == "hot" else 0
        priority_score = float(candidate.priority_score or 0.0)
        fewer_queries_bias = -len(candidate.queries)
        return (
            exact_single,
            contains_broad,
            lane_rank,
            priority_score,
            fewer_queries_bias,
            candidate.route_name,
        )

    return max(candidates, key=_score)


def build_flag_restore_mapping(snapshot: Mapping[str, Any] | None) -> dict[str, str]:
    feature_flags = (snapshot or {}).get("feature_flags") if isinstance(snapshot, Mapping) else None
    raw_flags = feature_flags if isinstance(feature_flags, Mapping) else {}
    restored: dict[str, str] = {}
    for flag_name, default_value in ROLLBACK_FLAG_DEFAULTS.items():
        raw_value = raw_flags.get(flag_name)
        if raw_value in {None, ""}:
            restored[flag_name] = default_value
            continue
        restored[flag_name] = "1" if parse_feature_flag_value(raw_value, default=default_value == "1") else "0"
    return restored


async def _ensure_worker_tables(pool: Any) -> None:
    from server.services.common.schema_ensure import ensure_worker_tables

    await ensure_worker_tables(pool)


async def fetch_feature_flags(redis_client: Any | None) -> dict[str, str]:
    if redis_client is None:
        return {}
    payload = await redis_client.hgetall(FLAG_HASH_KEY)
    return {
        str(key.decode("utf-8") if isinstance(key, bytes) else key): str(
            value.decode("utf-8") if isinstance(value, bytes) else value
        )
        for key, value in payload.items()
    }


async def set_feature_flags(redis_client: Any | None, updates: Mapping[str, str]) -> dict[str, str]:
    if redis_client is None:
        raise RuntimeError("Redis client is required for feature-flag updates.")
    normalized = {
        str(flag_name): "1" if str(flag_value).strip() in {"1", "true", "TRUE"} else "0"
        for flag_name, flag_value in dict(updates or {}).items()
        if str(flag_name).strip()
    }
    if normalized:
        await redis_client.hset(FLAG_HASH_KEY, mapping=normalized)
    return await fetch_feature_flags(redis_client)


async def fetch_central_route_candidates(pool: Any) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                cr.route_name,
                cr.legacy_worker_name,
                cr.legacy_route_name,
                cr.priority,
                cr.priority_score,
                cr.effective_lane,
                cr.route_interval_seconds,
                ARRAY_REMOVE(
                    ARRAY_AGG(rq.query_text ORDER BY rq.query_order)
                    FILTER (WHERE COALESCE(rq.is_enabled, TRUE) = TRUE),
                    NULL
                ) AS queries
            FROM central_routes cr
            LEFT JOIN route_queries rq
              ON rq.route_name = cr.route_name
            WHERE COALESCE(cr.is_enabled, TRUE) = TRUE
            GROUP BY
                cr.route_name,
                cr.legacy_worker_name,
                cr.legacy_route_name,
                cr.priority,
                cr.priority_score,
                cr.effective_lane,
                cr.route_interval_seconds
            ORDER BY cr.priority ASC, cr.route_name ASC
            """
        )
    return [dict(row) for row in rows]


async def seed_v41_broad_family(
    pool: asyncpg.Pool,
    *,
    family_name: str = V41_FAMILY_NAME,
    broad_query: str = V41_BROAD_QUERY,
    preferred_route_name: str | None = None,
    min_gap_seconds: int = V41_MIN_GAP_SECONDS,
) -> dict[str, Any]:
    await _ensure_worker_tables(pool)
    routes = await fetch_central_route_candidates(pool)
    candidate = choose_v41_seed_route(
        routes,
        preferred_route_name=preferred_route_name,
        broad_query=broad_query,
    )
    if preferred_route_name and candidate is None:
        raise ValueError(f"Preferred source route not found: {preferred_route_name}")

    family_name_clean = str(family_name or V41_FAMILY_NAME).strip() or V41_FAMILY_NAME
    broad_query_clean = str(broad_query or V41_BROAD_QUERY).strip() or V41_BROAD_QUERY
    safe_min_gap_seconds = max(1, int(min_gap_seconds or V41_MIN_GAP_SECONDS))

    priority = candidate.priority if candidate is not None else 100
    priority_score = candidate.priority_score if candidate is not None else None
    route_interval_seconds = candidate.route_interval_seconds if candidate is not None else None
    legacy_worker_name = candidate.legacy_worker_name if candidate is not None else None
    legacy_route_name = (
        candidate.legacy_route_name if candidate is not None and candidate.legacy_route_name else None
    ) or (candidate.route_name if candidate is not None else None)

    async with pool.acquire() as conn:
        async with conn.transaction():
            family_row = await conn.fetchrow(
                """
                INSERT INTO query_families (
                    name,
                    legacy_route_name,
                    legacy_worker_name,
                    is_enabled,
                    priority,
                    priority_score,
                    lane,
                    next_due_at,
                    min_gap_s,
                    max_gap_s,
                    variant_cursor,
                    variant_count,
                    last_error
                ) VALUES (
                    $1, $2, $3, TRUE, $4, $5, 'hot', NOW(), $6, $7, 0, 0, NULL
                )
                ON CONFLICT (name) DO UPDATE SET
                    legacy_route_name = COALESCE(EXCLUDED.legacy_route_name, query_families.legacy_route_name),
                    legacy_worker_name = COALESCE(EXCLUDED.legacy_worker_name, query_families.legacy_worker_name),
                    is_enabled = TRUE,
                    priority = EXCLUDED.priority,
                    priority_score = COALESCE(EXCLUDED.priority_score, query_families.priority_score),
                    lane = 'hot',
                    next_due_at = NOW(),
                    min_gap_s = EXCLUDED.min_gap_s,
                    max_gap_s = EXCLUDED.max_gap_s,
                    family_lease_token = NULL,
                    family_lease_expires_at = NULL,
                    last_error = NULL
                RETURNING *
                """,
                family_name_clean,
                legacy_route_name,
                legacy_worker_name,
                priority,
                priority_score,
                safe_min_gap_seconds,
                route_interval_seconds,
            )
            if family_row is None:
                raise RuntimeError("Failed to create or update the V4.1 broad family.")

            family_id = int(family_row["family_id"])
            await conn.execute(
                """
                UPDATE query_variants
                SET
                    is_enabled = FALSE,
                    validation_state = 'pending_validation',
                    notes = CASE
                        WHEN COALESCE(notes, '') = '' THEN 'Disabled during V4.1 broad-family rollout'
                        ELSE notes
                    END
                WHERE family_id = $1
                  AND LOWER(query_text) <> LOWER($2)
                """,
                family_id,
                broad_query_clean,
            )
            variant_row = await conn.fetchrow(
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
                    $1, $2, NULL, 'validated', 1.0, 0, TRUE,
                    'Validated by V4.1 cutover tooling for the initial broad-family rollout'
                )
                ON CONFLICT (family_id, query_text) DO UPDATE SET
                    url_template = NULL,
                    validation_state = 'validated',
                    weight = 1.0,
                    variant_order = 0,
                    is_enabled = TRUE,
                    notes = EXCLUDED.notes
                RETURNING *
                """,
                family_id,
                broad_query_clean,
            )
            if variant_row is None:
                raise RuntimeError("Failed to create or update the V4.1 broad variant.")

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
                    variant_cursor = 0,
                    next_due_at = NOW(),
                    min_gap_s = $2,
                    lane = 'hot'
                WHERE family_id = $1
                RETURNING *
                """,
                family_id,
                safe_min_gap_seconds,
            )
            if family_row is None:
                raise RuntimeError("Failed to finalize the V4.1 broad family.")

    return {
        "family_name": family_name_clean,
        "family_id": int(family_row["family_id"]),
        "variant_id": int(variant_row["variant_id"]),
        "source_route_name": candidate.route_name if candidate is not None else None,
        "source_queries": list(candidate.queries) if candidate is not None else [],
        "validated_query": broad_query_clean,
        "min_gap_seconds": safe_min_gap_seconds,
        "next_due_at": family_row["next_due_at"].isoformat() if family_row["next_due_at"] is not None else None,
    }


async def collect_v41_status(
    pool: Any,
    redis_client: Any | None,
    *,
    family_name: str = V41_FAMILY_NAME,
) -> dict[str, Any]:
    await _ensure_worker_tables(pool)
    async with pool.acquire() as conn:
        counts_row = await conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*)::INT FROM central_routes) AS central_routes,
                (SELECT COUNT(*)::INT FROM route_queries) AS route_queries,
                (SELECT COUNT(*)::INT FROM execution_profiles) AS execution_profiles,
                (SELECT COUNT(*)::INT FROM profiles) AS profiles,
                (SELECT COUNT(*)::INT FROM query_families) AS query_families,
                (SELECT COUNT(*)::INT FROM query_variants) AS query_variants
            """
        )
        family_row = await conn.fetchrow(
            """
            SELECT
                family_id,
                name,
                legacy_route_name,
                legacy_worker_name,
                is_enabled,
                priority,
                lane,
                next_due_at,
                min_gap_s,
                max_gap_s,
                variant_count,
                family_lease_token IS NOT NULL AS leased
            FROM query_families
            WHERE name = $1
            """,
            family_name,
        )
        variant_rows = await conn.fetch(
            """
            SELECT
                variant_id,
                query_text,
                validation_state,
                is_enabled,
                variant_order
            FROM query_variants
            WHERE family_id = (
                SELECT family_id
                FROM query_families
                WHERE name = $1
            )
            ORDER BY variant_order ASC, variant_id ASC
            """,
            family_name,
        )
        profile_rows = await conn.fetch(
            """
            SELECT
                profile_id,
                worker_name,
                user_data_dir,
                status,
                available_after,
                profile_lease_token IS NOT NULL AS leased
            FROM profiles
            ORDER BY profile_id ASC
            """
        )

    feature_flags = await fetch_feature_flags(redis_client)
    return {
        "captured_at": _utc_now_iso(),
        "feature_flags": feature_flags,
        "counts": dict(counts_row) if counts_row is not None else {},
        "family": dict(family_row) if family_row is not None else None,
        "variants": [dict(row) for row in variant_rows],
        "profiles": [dict(row) for row in profile_rows],
    }


async def activate_v41(
    pool: Any,
    redis_client: Any | None,
    *,
    family_name: str = V41_FAMILY_NAME,
    broad_query: str = V41_BROAD_QUERY,
    preferred_route_name: str | None = None,
    min_gap_seconds: int = V41_MIN_GAP_SECONDS,
    enable_first_seen_dedupe: bool = True,
) -> dict[str, Any]:
    seed_summary = await seed_v41_broad_family(
        pool,
        family_name=family_name,
        broad_query=broad_query,
        preferred_route_name=preferred_route_name,
        min_gap_seconds=min_gap_seconds,
    )
    updates = {
        "ENABLE_V4_WARM_RUNTIME": "1",
        "ENABLE_V4_FIRST_SEEN_DEDUPE": "1" if enable_first_seen_dedupe else "0",
        "ENABLE_V4_UPDATE_EVENTS": "0",
    }
    flags = await set_feature_flags(redis_client, updates)
    return {
        "seed": seed_summary,
        "feature_flags": flags,
        "status": await collect_v41_status(pool, redis_client, family_name=family_name),
    }


async def rollback_v41(
    redis_client: Any | None,
    *,
    snapshot: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    updates = build_flag_restore_mapping(snapshot)
    return await set_feature_flags(redis_client, updates)


def write_snapshot(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return path
