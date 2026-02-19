from __future__ import annotations

import random
from datetime import datetime
from typing import Any

import asyncpg

from .runtime import compute_next_run_at, select_due_route


def select_next_route(routes: list[dict[str, Any]], now: datetime | None = None) -> dict[str, Any] | None:
    return select_due_route(routes=routes, now=now)


def route_interval_seconds(route: dict[str, Any], fallback_seconds: float) -> float:
    configured = route.get("route_interval_seconds")
    try:
        value = float(configured)
    except (TypeError, ValueError):
        value = 0.0
    if value <= 0:
        value = float(fallback_seconds)
    return max(1.0, value)


async def schedule_route_next_run_at(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    cycle_started_at: datetime,
    interval_seconds: float,
    jitter_pct: float,
    rng: Any = random,
) -> datetime | None:
    if route.get("source") != "db":
        return None

    next_run_at = compute_next_run_at(
        cycle_started_at=cycle_started_at,
        interval_seconds=interval_seconds,
        jitter_pct=jitter_pct,
        rng=rng,
    )

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE worker_routes
            SET next_run_at = $3
            WHERE worker_name = $1 AND route_name = $2
            RETURNING next_run_at
            """,
            route.get("worker_name"),
            route.get("route_name"),
            next_run_at,
        )

    return row["next_run_at"] if row else None
