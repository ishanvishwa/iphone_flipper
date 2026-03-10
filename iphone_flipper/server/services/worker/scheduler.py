from __future__ import annotations

import random
import re
from datetime import datetime, timezone
from typing import Any

import asyncpg

from server.services.common.runtime_config import DEFAULT_RUNTIME_CONFIG
from server.services.common.route_lanes import VALID_ROUTE_LANES, normalize_route_lane

from .runtime import RouteStatus, compute_next_run_at, select_due_route

_LANE_RANK = {"hot": 0, "warm": 1, "sweep": 2}
_LANE_INTERVAL_MULTIPLIER = {"hot": 1.0, "warm": 1.35, "sweep": 2.0}
_HOT_QUERY_CATEGORY_SCORES = {"exact": 3.0, "flipper": 2.7, "misspelling": 2.0, "broad": 1.0}
_WARM_QUERY_CATEGORY_SCORES = {"exact": 2.5, "flipper": 2.1, "misspelling": 1.8, "broad": 1.6}
_SWEEP_QUERY_CATEGORY_SCORES = {"exact": 1.6, "flipper": 1.4, "misspelling": 2.4, "broad": 3.0}


def _normalize_dt(value: datetime | None, fallback: datetime) -> datetime:
    if value is None:
        return fallback
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _lane_thresholds(config: dict[str, Any] | None = None) -> dict[str, float]:
    snapshot = dict(DEFAULT_RUNTIME_CONFIG)
    if config:
        snapshot.update(config)
    return {
        "hot_score_min": _safe_float(snapshot.get("ROUTE_LANE_HOT_SCORE_MIN"), 5.5),
        "hot_profit_rate_min": _safe_float(snapshot.get("ROUTE_LANE_HOT_PROFITABLE_HIT_RATE_MIN"), 0.35),
        "sweep_score_max": _safe_float(snapshot.get("ROUTE_LANE_SWEEP_SCORE_MAX"), 2.4),
        "sweep_profit_rate_max": _safe_float(snapshot.get("ROUTE_LANE_SWEEP_PROFITABLE_HIT_RATE_MAX"), 0.10),
        "sweep_avg_result_count_max": _safe_float(snapshot.get("ROUTE_LANE_SWEEP_AVG_RESULT_COUNT_MAX"), 2.0),
    }


def lane_interval_multiplier(lane: Any) -> float:
    normalized = normalize_route_lane(lane, allow_none=False)
    return _LANE_INTERVAL_MULTIPLIER.get(normalized, 1.0)


def _query_category(query: str) -> str:
    text = str(query or "").strip().lower()
    if not text:
        return "broad"
    if any(
        marker in text
        for marker in (
            "broken",
            "cracked",
            "unlock",
            "clean imei",
            "icloud",
            "need gone",
            "box alone",
        )
    ):
        return "flipper"
    if re.search(r"\bi\s+phone\b|\bipon\b", text):
        return "misspelling"
    if re.search(r"\biphone\s+\d", text):
        return "exact"
    if len(text.split()) <= 1:
        return "broad"
    return "exact"


def _query_category_score(lane: str, category: str) -> float:
    normalized_lane = normalize_route_lane(lane, allow_none=False)
    if normalized_lane == "hot":
        return _HOT_QUERY_CATEGORY_SCORES.get(category, 1.0)
    if normalized_lane == "sweep":
        return _SWEEP_QUERY_CATEGORY_SCORES.get(category, 1.0)
    return _WARM_QUERY_CATEGORY_SCORES.get(category, 1.0)


def route_interval_seconds(route: dict[str, Any], fallback_seconds: float) -> float:
    configured = route.get("route_interval_seconds")
    try:
        value = float(configured)
    except (TypeError, ValueError):
        value = 0.0
    if value <= 0:
        value = float(fallback_seconds)
    return max(1.0, value)


def compute_route_priority(
    route: dict[str, Any],
    *,
    now: datetime | None = None,
    fallback_interval_seconds: float = 60.0,
    runtime_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now_dt = _normalize_dt(now, datetime.now(timezone.utc))
    thresholds = _lane_thresholds(runtime_config)
    interval_seconds = route_interval_seconds(route=route, fallback_seconds=fallback_interval_seconds)
    next_run_at = _normalize_dt(route.get("next_run_at"), now_dt)
    last_selected_at = route.get("last_selected_at")
    last_selected_dt = _normalize_dt(last_selected_at, now_dt) if last_selected_at else now_dt

    due_age_seconds = max(0.0, (now_dt - next_run_at).total_seconds())
    revisit_age_seconds = max(0.0, (now_dt - last_selected_dt).total_seconds())

    configured_priority = max(0.0, _safe_float(route.get("priority"), 100.0))
    avg_result_count = max(0.0, _safe_float(route.get("avg_result_count")))
    profitable_hit_rate = _clamp(_safe_float(route.get("profitable_hit_rate")), 0.0, 1.0)
    recent_duplicate_ratio = _clamp(_safe_float(route.get("recent_duplicate_ratio")), 0.0, 1.0)
    consecutive_failures = max(0.0, _safe_float(route.get("consecutive_failures")))
    status = str(route.get("status") or RouteStatus.ENABLED.value).strip().upper() or RouteStatus.ENABLED.value

    priority_component = max(0.0, 1.0 - min(configured_priority, 500.0) / 500.0) * 2.0
    due_pressure = min(2.75, due_age_seconds / max(1.0, interval_seconds))
    revisit_pressure = min(1.75, revisit_age_seconds / max(1.0, interval_seconds))
    yield_component = min(2.0, avg_result_count / 10.0)
    profit_component = profitable_hit_rate * 3.0
    duplicate_penalty = recent_duplicate_ratio * 2.0

    status_penalty = 0.0
    if status == RouteStatus.DEGRADED.value:
        status_penalty = 0.75
    elif status == RouteStatus.THROTTLED.value:
        status_penalty = 1.5
    elif status == RouteStatus.COOLDOWN.value:
        status_penalty = 4.0
    elif status in {RouteStatus.NEEDS_LOGIN.value, RouteStatus.DISABLED.value}:
        status_penalty = 6.0
    failure_penalty = min(1.5, consecutive_failures * 0.25)

    priority_score = (
        1.0
        + priority_component
        + due_pressure
        + revisit_pressure
        + yield_component
        + profit_component
        - duplicate_penalty
        - status_penalty
        - failure_penalty
    )

    computed_lane = "warm"
    if (
        priority_score >= thresholds["hot_score_min"]
        or profitable_hit_rate >= thresholds["hot_profit_rate_min"]
    ):
        computed_lane = "hot"
    elif (
        priority_score < thresholds["sweep_score_max"]
        and profitable_hit_rate < thresholds["sweep_profit_rate_max"]
        and avg_result_count < thresholds["sweep_avg_result_count_max"]
    ):
        computed_lane = "sweep"

    lane_override = normalize_route_lane(route.get("lane_override"))
    effective_lane = lane_override or computed_lane
    components = {
        "priority": round(priority_component, 4),
        "due_pressure": round(due_pressure, 4),
        "revisit_pressure": round(revisit_pressure, 4),
        "yield": round(yield_component, 4),
        "profit": round(profit_component, 4),
        "duplicate_penalty": round(duplicate_penalty, 4),
        "status_penalty": round(status_penalty, 4),
        "failure_penalty": round(failure_penalty, 4),
    }

    return {
        "lane_override": lane_override,
        "computed_lane": computed_lane,
        "effective_lane": effective_lane,
        "priority_score": round(priority_score, 4),
        "priority_components": components,
        "due_age_seconds": round(due_age_seconds, 3),
        "revisit_age_seconds": round(revisit_age_seconds, 3),
        "lane_interval_multiplier": lane_interval_multiplier(effective_lane),
        "lane_thresholds": thresholds,
    }


def annotate_routes_with_priority(
    routes: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    fallback_interval_seconds: float = 60.0,
    runtime_config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    now_dt = _normalize_dt(now, datetime.now(timezone.utc))
    for route in routes:
        route.update(
            compute_route_priority(
                route,
                now=now_dt,
                fallback_interval_seconds=fallback_interval_seconds,
                runtime_config=runtime_config,
            )
        )
    return routes


def select_next_route(
    routes: list[dict[str, Any]],
    now: datetime | None = None,
    *,
    use_priority_scheduler: bool = False,
    runtime_config: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not routes:
        return None
    if not use_priority_scheduler:
        return select_due_route(routes=routes, now=now)

    now_dt = _normalize_dt(now, datetime.now(timezone.utc))
    due_routes: list[tuple[int, float, datetime, int, str, dict[str, Any]]] = []
    for route in routes:
        due_at = _normalize_dt(route.get("next_run_at"), now_dt)
        if due_at > now_dt:
            continue
        if "priority_score" not in route or "effective_lane" not in route:
            route.update(compute_route_priority(route, now=now_dt, runtime_config=runtime_config))
        lane = normalize_route_lane(route.get("effective_lane"), allow_none=False)
        score = _safe_float(route.get("priority_score"))
        due_routes.append(
            (
                _LANE_RANK.get(lane, _LANE_RANK["warm"]),
                -score,
                due_at,
                int(route.get("priority") or 100),
                str(route.get("route_name") or ""),
                route,
            )
        )
    if not due_routes:
        return None
    due_routes.sort(key=lambda item: (item[0], item[1], item[2], item[3], item[4]))
    return due_routes[0][5]


def rank_route_queries(route: dict[str, Any], queries: list[str]) -> list[str]:
    normalized_lane = normalize_route_lane(route.get("effective_lane"), allow_none=False)
    duplicate_ratio = _clamp(_safe_float(route.get("recent_duplicate_ratio")), 0.0, 1.0)
    ranked: list[tuple[float, int, str]] = []
    seen: set[str] = set()

    for index, raw_query in enumerate(queries):
        query = str(raw_query or "").strip()
        if not query:
            continue
        query_identity = query.lower()
        if query_identity in seen:
            continue
        seen.add(query_identity)
        category = _query_category(query)
        category_score = _query_category_score(normalized_lane, category)
        specificity_bonus = min(1.0, len(query.split()) * 0.15)
        duplicate_penalty = duplicate_ratio * (0.9 if category == "broad" else 0.3)
        ranked.append((category_score + specificity_bonus - duplicate_penalty, index, query))

    ranked.sort(key=lambda item: (-item[0], item[1], item[2]))
    return [item[2] for item in ranked]


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
