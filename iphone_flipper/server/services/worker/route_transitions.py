from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import asyncpg

from .runtime import RouteStatus


async def record_route_outcome(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    success: bool,
    error: str = "",
    count_failure: bool = True,
) -> None:
    route_source = str(route.get("source") or "").strip().lower()
    if route_source not in {"db", "central"}:
        return

    async with pool.acquire() as conn:
        if success:
            if route_source == "central":
                await conn.execute(
                    """
                    UPDATE central_routes
                    SET
                        last_success_at = NOW(),
                        last_error = NULL,
                        consecutive_failures = 0
                    WHERE route_name = $1
                    """,
                    route.get("route_name"),
                )
            else:
                await conn.execute(
                    """
                    UPDATE worker_routes
                    SET
                        last_success_at = NOW(),
                        last_error = NULL,
                        consecutive_failures = 0
                    WHERE worker_name = $1 AND route_name = $2
                    """,
                    route.get("worker_name"),
                    route.get("route_name"),
                )
            return

        error_text = error[:2000] if error else "unknown error"
        if count_failure:
            if route_source == "central":
                await conn.execute(
                    """
                    UPDATE central_routes
                    SET
                        last_error = $2,
                        consecutive_failures = GREATEST(0, COALESCE(consecutive_failures, 0)) + 1
                    WHERE route_name = $1
                    """,
                    route.get("route_name"),
                    error_text,
                )
            else:
                await conn.execute(
                    """
                    UPDATE worker_routes
                    SET
                        last_error = $3,
                        consecutive_failures = GREATEST(0, COALESCE(consecutive_failures, 0)) + 1
                    WHERE worker_name = $1 AND route_name = $2
                    """,
                    route.get("worker_name"),
                    route.get("route_name"),
                    error_text,
                )
            return

        if route_source == "central":
            await conn.execute(
                """
                UPDATE central_routes
                SET
                    last_error = $2
                WHERE route_name = $1
                """,
                route.get("route_name"),
                error_text,
            )
        else:
            await conn.execute(
                """
                UPDATE worker_routes
                SET
                    last_error = $3
                WHERE worker_name = $1 AND route_name = $2
                """,
                route.get("worker_name"),
                route.get("route_name"),
                error_text,
            )


async def put_route_on_cooldown(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    reason: str,
    cooldown_seconds: int,
) -> datetime | None:
    route_source = str(route.get("source") or "").strip().lower()
    if route_source not in {"db", "central"}:
        return None

    safe_seconds = max(60, int(cooldown_seconds))
    reason_text = (reason or "").strip()[:2000] or "route put on cooldown"

    async with pool.acquire() as conn:
        if route_source == "central":
            row = await conn.fetchrow(
                """
                UPDATE central_routes
                SET
                    cooldown_until = NOW() + ($2::INT * INTERVAL '1 second'),
                    status = $4,
                    status_reason = $3,
                    status_since = NOW(),
                    last_error = $3,
                    consecutive_failures = 0
                WHERE route_name = $1
                RETURNING cooldown_until
                """,
                route.get("route_name"),
                safe_seconds,
                reason_text,
                RouteStatus.COOLDOWN.value,
            )
        else:
            row = await conn.fetchrow(
                """
                UPDATE worker_routes
                SET
                    cooldown_until = NOW() + ($3::INT * INTERVAL '1 second'),
                    status = $5,
                    status_reason = $4,
                    status_since = NOW(),
                    last_error = $4,
                    consecutive_failures = 0
                WHERE worker_name = $1 AND route_name = $2
                RETURNING cooldown_until
                """,
                route.get("worker_name"),
                route.get("route_name"),
                safe_seconds,
                reason_text,
                RouteStatus.COOLDOWN.value,
            )

    if not row:
        return None

    route["status"] = RouteStatus.COOLDOWN.value
    route["status_reason"] = reason_text
    route["status_since"] = datetime.now(timezone.utc)
    return row["cooldown_until"]


async def mark_route_manual_login_required(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    reason: str,
    evidence: dict[str, Any] | None = None,
) -> datetime | None:
    route_source = str(route.get("source") or "").strip().lower()
    if route_source not in {"db", "central"}:
        return None

    reason_text = (reason or "").strip()[:2000] or "Manual login required."
    evidence_payload: str | None = None
    if evidence:
        try:
            evidence_payload = json.dumps(evidence, default=str)
        except Exception:
            evidence_payload = None

    async with pool.acquire() as conn:
        if route_source == "central":
            row = await conn.fetchrow(
                """
                UPDATE central_routes
                SET
                    status = $3,
                    status_reason = $2,
                    status_since = NOW(),
                    cooldown_until = NULL,
                    consecutive_failures = 0,
                    last_error = $2
                WHERE route_name = $1
                RETURNING NOW() AS manual_login_required_at, NOW() AS quarantined_at, $2::TEXT AS quarantine_reason, $4::JSONB AS quarantine_evidence
                """,
                route.get("route_name"),
                reason_text,
                RouteStatus.NEEDS_LOGIN.value,
                evidence_payload,
            )
        else:
            row = await conn.fetchrow(
                """
                UPDATE worker_routes
                SET
                    status = $4,
                    status_reason = $3,
                    status_since = NOW(),
                    manual_login_required = TRUE,
                    manual_login_reason = $3,
                    manual_login_required_at = NOW(),
                    quarantined_at = NOW(),
                    quarantine_reason = $3,
                    quarantine_evidence = COALESCE($5::JSONB, quarantine_evidence),
                    cooldown_until = NULL,
                    consecutive_failures = 0,
                    last_error = $3
                WHERE worker_name = $1 AND route_name = $2
                RETURNING manual_login_required_at, quarantined_at, quarantine_reason, quarantine_evidence
                """,
                route.get("worker_name"),
                route.get("route_name"),
                reason_text,
                RouteStatus.NEEDS_LOGIN.value,
                evidence_payload,
            )

    if not row:
        return None

    route["status"] = RouteStatus.NEEDS_LOGIN.value
    route["status_reason"] = reason_text
    route["status_since"] = datetime.now(timezone.utc)
    route["quarantined_at"] = row["quarantined_at"]
    route["quarantine_reason"] = row["quarantine_reason"]
    route["quarantine_evidence"] = row["quarantine_evidence"]
    return row["manual_login_required_at"]
