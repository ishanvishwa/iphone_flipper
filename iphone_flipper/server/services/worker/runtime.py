from __future__ import annotations

import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


class CycleOutcome(str, Enum):
    OK = "ok"
    WAIT_PROXY = "wait_proxy"
    WAIT_PROXY_MISMATCH = "wait_proxy_mismatch"
    WAIT_PROXY_PROVIDER = "wait_proxy_provider"
    WAIT_PROFILE_LOCK = "wait_profile_lock"
    WAIT_QUERY_SHARD = "wait_query_shard"
    WAIT_SIGNAL_PAUSE = "wait_signal_pause"
    FAIL = "fail"
    NEEDS_LOGIN = "needs_login"


class ErrorCategory(str, Enum):
    NONE = "none"
    NO_PROXY_AVAILABLE = "no_proxy_available"
    PROXY_MISMATCH = "proxy_mismatch"
    PROXY_PROVIDER_SATURATED = "proxy_provider_saturated"
    PROFILE_LOCK_UNAVAILABLE = "profile_lock_unavailable"
    QUERY_SHARD_LOCK_UNAVAILABLE = "query_shard_lock_unavailable"
    SIGNAL_RISK = "signal_risk"
    AUTH_REQUIRED = "auth_required"
    PROXY_CONNECT = "proxy_connect"
    NAV_TIMEOUT = "nav_timeout"
    HTTP_5XX = "http_5xx"
    PARSE_FAILED = "parse_failed"
    UNKNOWN = "unknown"


class RouteStatus(str, Enum):
    ENABLED = "ENABLED"
    DEGRADED = "DEGRADED"
    THROTTLED = "THROTTLED"
    COOLDOWN = "COOLDOWN"
    NEEDS_LOGIN = "NEEDS_LOGIN"
    DISABLED = "DISABLED"


@dataclass
class CycleResult:
    metrics: dict[str, int]
    outcome: CycleOutcome
    reason: str | None = None
    error_category: ErrorCategory = ErrorCategory.NONE
    retry_count: int = 0
    details: dict[str, Any] | None = None


class NoProxyAvailableError(RuntimeError):
    pass


class ProfileLockUnavailableError(RuntimeError):
    pass


class QueryShardLockUnavailableError(RuntimeError):
    pass


class PreCheckpointSignalError(RuntimeError):
    pass


def _normalize_jitter_pct(jitter_pct: float) -> float:
    try:
        value = float(jitter_pct)
    except (TypeError, ValueError):
        return 0.0
    if value < 0:
        return 0.0
    if value > 0.95:
        return 0.95
    return value


def _normalize_dt(value: datetime | None, fallback: datetime) -> datetime:
    if value is None:
        return fallback
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def compute_sleep_seconds(
    base_interval: float,
    elapsed: float,
    jitter_pct: float,
    rng: Any = random,
    min_sleep_seconds: float = 0.1,
) -> float:
    base = max(0.0, float(base_interval or 0))
    remaining = max(0.0, base - max(0.0, float(elapsed or 0)))
    if remaining <= 0:
        return 0.0

    jitter = _normalize_jitter_pct(jitter_pct)
    if jitter <= 0:
        return max(min_sleep_seconds, remaining)

    jittered = remaining * (1.0 + rng.uniform(-jitter, jitter))
    if jittered <= 0:
        return 0.0
    return max(min_sleep_seconds, jittered)


def compute_jittered_interval_seconds(
    base_interval: float,
    jitter_pct: float,
    rng: Any = random,
) -> float:
    base = max(0.0, float(base_interval or 0))
    if base <= 0:
        return 0.0
    jitter = _normalize_jitter_pct(jitter_pct)
    if jitter <= 0:
        return base
    jittered = base * (1.0 + rng.uniform(-jitter, jitter))
    return max(0.0, jittered)


def compute_worker_effective_interval(
    base_interval: float,
    enabled_count: int,
    eligible_count: int,
    max_multiplier: float = 5.0,
) -> float:
    base = max(1.0, float(base_interval or 1.0))
    enabled = max(1, int(enabled_count or 0))
    eligible = max(0, int(eligible_count or 0))
    ratio = eligible / enabled

    cap = max(1.0, float(max_multiplier or 1.0))
    min_ratio = 1.0 / cap
    ratio = min(1.0, max(min_ratio, ratio))
    multiplier = min(cap, 1.0 / ratio)
    return base * multiplier


def select_due_route(
    routes: list[dict[str, Any]],
    now: datetime | None = None,
) -> dict[str, Any] | None:
    if not routes:
        return None

    now_dt = _normalize_dt(now, datetime.now(timezone.utc))
    due_routes: list[tuple[datetime, int, str, dict[str, Any]]] = []
    for route in routes:
        due_at = _normalize_dt(route.get("next_run_at"), now_dt)
        if due_at <= now_dt:
            due_routes.append(
                (
                    due_at,
                    int(route.get("priority") or 100),
                    str(route.get("route_name") or ""),
                    route,
                )
            )

    if not due_routes:
        return None

    due_routes.sort(key=lambda item: (item[0], item[1], item[2]))
    return due_routes[0][3]


def seconds_until_next_route(
    routes: list[dict[str, Any]],
    now: datetime | None = None,
) -> float | None:
    if not routes:
        return None

    now_dt = _normalize_dt(now, datetime.now(timezone.utc))
    earliest_due_at: datetime | None = None
    for route in routes:
        due_at = _normalize_dt(route.get("next_run_at"), now_dt)
        if earliest_due_at is None or due_at < earliest_due_at:
            earliest_due_at = due_at

    if earliest_due_at is None:
        return None

    return max(0.0, (earliest_due_at - now_dt).total_seconds())


def compute_next_run_at(
    cycle_started_at: datetime,
    interval_seconds: float,
    jitter_pct: float,
    rng: Any = random,
) -> datetime:
    started_at = _normalize_dt(cycle_started_at, datetime.now(timezone.utc))
    delay_seconds = compute_jittered_interval_seconds(interval_seconds, jitter_pct, rng=rng)
    return started_at + timedelta(seconds=delay_seconds)


def classify_error(reason: str | None) -> ErrorCategory:
    text = str(reason or "").strip().lower()
    if not text:
        return ErrorCategory.NONE

    if "no available proxy" in text or "no proxy" in text:
        return ErrorCategory.NO_PROXY_AVAILABLE
    if "proxy_mismatch" in text or "proxy mismatch" in text:
        return ErrorCategory.PROXY_MISMATCH
    if "profile lock unavailable" in text:
        return ErrorCategory.PROFILE_LOCK_UNAVAILABLE
    if "query shard lock unavailable" in text:
        return ErrorCategory.QUERY_SHARD_LOCK_UNAVAILABLE
    if "pre-checkpoint signal" in text or "signal risk" in text:
        return ErrorCategory.SIGNAL_RISK
    if (
        "manual_login_required" in text
        or "facebook checkpoint" in text
        or "checkpoint/login url detected" in text
        or "challenge text detected" in text
        or "confirm it's you" in text
        or "confirm your identity" in text
        or "security check" in text
    ):
        return ErrorCategory.AUTH_REQUIRED
    if "timeout" in text or "timed out" in text:
        return ErrorCategory.NAV_TIMEOUT
    if "http" in text and re.search(r"\b5\d\d\b", text):
        return ErrorCategory.HTTP_5XX
    if "proxy" in text and ("connect" in text or "connection" in text or "refused" in text):
        return ErrorCategory.PROXY_CONNECT
    if "parse" in text or "selector" in text or "dom" in text:
        return ErrorCategory.PARSE_FAILED
    return ErrorCategory.UNKNOWN


def is_retryable_category(category: ErrorCategory) -> bool:
    return category in {
        ErrorCategory.PROXY_CONNECT,
        ErrorCategory.NAV_TIMEOUT,
        ErrorCategory.HTTP_5XX,
    }


def counts_toward_bad_cycles(outcome: CycleOutcome, category: ErrorCategory) -> bool:
    if outcome != CycleOutcome.FAIL:
        return False
    return category not in {
        ErrorCategory.NO_PROXY_AVAILABLE,
        ErrorCategory.PROXY_MISMATCH,
        ErrorCategory.PROXY_PROVIDER_SATURATED,
        ErrorCategory.PROFILE_LOCK_UNAVAILABLE,
        ErrorCategory.QUERY_SHARD_LOCK_UNAVAILABLE,
        ErrorCategory.SIGNAL_RISK,
        ErrorCategory.AUTH_REQUIRED,
        ErrorCategory.NONE,
    }


def next_bad_cycle_count(
    current_bad_cycles: int,
    outcome: CycleOutcome,
    category: ErrorCategory,
) -> int:
    if outcome in {CycleOutcome.OK, CycleOutcome.NEEDS_LOGIN}:
        return 0
    if not counts_toward_bad_cycles(outcome, category):
        return max(0, int(current_bad_cycles or 0))
    return max(0, int(current_bad_cycles or 0)) + 1


def route_status_interval_multiplier(status: str | RouteStatus | None) -> float:
    if isinstance(status, RouteStatus):
        value = status.value
    else:
        value = str(status or RouteStatus.ENABLED.value).strip().upper()
    if value == RouteStatus.THROTTLED.value:
        return 4.0
    if value == RouteStatus.DEGRADED.value:
        return 2.0
    return 1.0


def derive_route_status_for_bad_cycles(
    bad_cycles: int,
    degraded_after: int,
    throttled_after: int,
) -> RouteStatus:
    value = max(0, int(bad_cycles or 0))
    degraded_threshold = max(1, int(degraded_after or 1))
    throttled_threshold = max(degraded_threshold + 1, int(throttled_after or degraded_threshold + 1))
    if value >= throttled_threshold:
        return RouteStatus.THROTTLED
    if value >= degraded_threshold:
        return RouteStatus.DEGRADED
    return RouteStatus.ENABLED


def compute_retry_backoff_seconds(base_seconds: int, attempt: int, max_seconds: int = 60) -> int:
    base = max(1, int(base_seconds or 1))
    attempt_index = max(1, int(attempt or 1))
    max_cap = max(base, int(max_seconds or base))
    return min(max_cap, base * (2 ** (attempt_index - 1)))
