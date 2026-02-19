import asyncio
import hashlib
import inspect
import ipaddress
import json
import logging
import os
import random
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import asyncpg
from redis.asyncio import Redis

# Must be set before importing scraper, because scraper resolves DB_PATH at import time.
os.environ.setdefault("IPHONE_FLIPPER_DB_PATH", "/app/runtime/worker_1.db")
Path(os.environ["IPHONE_FLIPPER_DB_PATH"]).parent.mkdir(parents=True, exist_ok=True)

# Ensure repository root (contains scraper.py) is importable when running via file path.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scraper import (  # noqa: E402
    _start_local_socks5_auth_bridge,
    cleanup_bridge_process,
    init_db,
    is_accessory_only_listing,
    scrape_marketplace,
)
from notifications import notify_telegram_listing_card, send_telegram  # noqa: E402
from server.services.common.schema_ensure import ensure_worker_tables as ensure_common_worker_tables  # noqa: E402
from server.services.worker.proxy_monitor import get_proxy_health, start_monitor as start_proxy_monitor  # noqa: E402
from server.services.worker.lease_manager import (  # noqa: E402
    canonical_proxy_key as canonical_proxy_key_from_module,
    proxy_identity as proxy_identity_from_module,
    refresh_proxy_lease as refresh_proxy_lease_from_module,
    release_proxy_lease as release_proxy_lease_from_module,
    try_acquire_proxy_lease as try_acquire_proxy_lease_from_module,
)
from server.services.worker.persona import generate_persona, persona_hash  # noqa: E402
from server.services.worker.route_transitions import (  # noqa: E402
    mark_route_manual_login_required as mark_route_manual_login_required_from_module,
    put_route_on_cooldown as put_route_on_cooldown_from_module,
    record_route_outcome as record_route_outcome_from_module,
)
from server.services.worker.runtime import (  # noqa: E402
    CycleOutcome,
    CycleResult,
    ErrorCategory,
    NoProxyAvailableError,
    PreCheckpointSignalError,
    QueryShardLockUnavailableError,
    ProfileLockUnavailableError,
    RouteStatus,
    classify_error,
    compute_retry_backoff_seconds,
    compute_sleep_seconds,
    compute_worker_effective_interval,
    counts_toward_bad_cycles,
    derive_route_status_for_bad_cycles,
    is_retryable_category,
    next_bad_cycle_count,
    route_status_interval_multiplier,
    seconds_until_next_route,
)
from server.services.worker.scheduler import (  # noqa: E402
    route_interval_seconds as route_interval_seconds_from_module,
    schedule_route_next_run_at as schedule_route_next_run_at_from_module,
    select_next_route as select_next_route_from_module,
)
from server.services.worker.signal_detector import analyze_cycle_signals  # noqa: E402
from server.services.worker.telemetry import build_cycle_telemetry_payload  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

LISTING_EVENT_CHANNEL = os.getenv("LISTING_EVENT_CHANNEL", "listing_events")

try:
    SCRAPE_MARKETPLACE_SUPPORTED_KWARGS = set(inspect.signature(scrape_marketplace).parameters.keys())
except Exception:
    SCRAPE_MARKETPLACE_SUPPORTED_KWARGS = {
        "headless",
        "progress_callback",
        "stop_event",
        "user_data_dir",
        "proxy",
        "search_queries",
        "scroll_target_cards_override",
        "scroll_max_rounds_override",
    }

PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "iphone_flipper")
PGUSER = os.getenv("PGUSER", "flipper_app")
PGPASSWORD = os.getenv("PGPASSWORD", "")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

WORKER_NAME = os.getenv("WORKER_NAME", "worker_1")
SCRAPE_INTERVAL_SECONDS = max(1, int(os.getenv("SCRAPE_INTERVAL_SECONDS", "8")))
WORKER_USER_DATA_DIR = os.getenv("WORKER_USER_DATA_DIR", "").strip() or None


def _parse_bool(raw: str | None, default: bool = True) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_optional_int(raw: str | None, minimum: int = 1) -> int | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return max(minimum, int(value))
    except (TypeError, ValueError):
        return None


def _parse_int(raw: str | None, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(str(raw or "").strip()))
    except (TypeError, ValueError):
        return max(minimum, int(default))


def _parse_float(raw: str | None, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(str(raw or "").strip()))
    except (TypeError, ValueError):
        return max(minimum, float(default))


WORKER_HEADLESS = _parse_bool(os.getenv("WORKER_HEADLESS", "1"), default=True)
WORKER_SCROLL_TARGET_CARDS = _parse_optional_int(os.getenv("WORKER_SCROLL_TARGET_CARDS"), minimum=20)
WORKER_SCROLL_MAX_ROUNDS = _parse_optional_int(os.getenv("WORKER_SCROLL_MAX_ROUNDS"), minimum=2)
WORKER_CYCLE_RETRY_ATTEMPTS = _parse_int(os.getenv("WORKER_CYCLE_RETRY_ATTEMPTS"), default=3, minimum=1)
WORKER_CYCLE_RETRY_BACKOFF_SECONDS = _parse_int(
    os.getenv("WORKER_CYCLE_RETRY_BACKOFF_SECONDS"),
    default=3,
    minimum=1,
)
WORKER_CYCLE_RETRY_BACKOFF_MAX_SECONDS = _parse_int(
    os.getenv("WORKER_CYCLE_RETRY_BACKOFF_MAX_SECONDS"),
    default=60,
    minimum=1,
)
WORKER_DEGRADED_CONSECUTIVE_CYCLES = _parse_int(
    os.getenv("WORKER_DEGRADED_CONSECUTIVE_CYCLES"),
    default=3,
    minimum=1,
)
WORKER_THROTTLED_CONSECUTIVE_CYCLES = max(
    WORKER_DEGRADED_CONSECUTIVE_CYCLES + 1,
    _parse_int(
        os.getenv("WORKER_THROTTLED_CONSECUTIVE_CYCLES"),
        default=5,
        minimum=1,
    ),
)
# Keep this independently operator-configurable. If unset, default remains throttled+1.
WORKER_ROUTE_COOLDOWN_BAD_CYCLES = _parse_int(
    os.getenv("WORKER_ROUTE_COOLDOWN_BAD_CYCLES"),
    default=WORKER_THROTTLED_CONSECUTIVE_CYCLES + 1,
    minimum=1,
)
WORKER_ROUTE_COOLDOWN_SECONDS = _parse_int(os.getenv("WORKER_ROUTE_COOLDOWN_SECONDS"), default=900, minimum=60)
SCRAPE_INTERVAL_JITTER_PCT = _parse_float(
    os.getenv("SCRAPE_INTERVAL_JITTER_PCT"),
    default=0.25,
    minimum=0.0,
)
WORKER_WAIT_BACKOFF_SECONDS = _parse_int(os.getenv("WORKER_WAIT_BACKOFF_SECONDS"), default=3, minimum=1)
WORKER_MAX_BACKOFF_MULTIPLIER = _parse_float(
    os.getenv("WORKER_MAX_BACKOFF_MULTIPLIER"),
    default=5.0,
    minimum=1.0,
)
WORKER_SINGLE_ROUTE_REST_MULTIPLIER = _parse_float(
    os.getenv("WORKER_SINGLE_ROUTE_REST_MULTIPLIER"),
    default=3.0,
    minimum=1.0,
)
WORKER_MIN_ENABLED_ROUTES_WARN = _parse_int(
    os.getenv("WORKER_MIN_ENABLED_ROUTES_WARN"),
    default=2,
    minimum=1,
)
try:
    WORKER_PROXY_REUSE_COOLDOWN_SECONDS = max(
        0,
        int(os.getenv("WORKER_PROXY_REUSE_COOLDOWN_SECONDS", "120")),
    )
except ValueError:
    WORKER_PROXY_REUSE_COOLDOWN_SECONDS = 120
try:
    WORKER_PROXY_LEASE_SECONDS = max(
        30,
        int(os.getenv("WORKER_PROXY_LEASE_SECONDS", "600")),
    )
except ValueError:
    WORKER_PROXY_LEASE_SECONDS = 600
PROXY_HEALTH_FAILURE_BAN_AFTER = _parse_int(os.getenv("PROXY_HEALTH_FAILURE_BAN_AFTER"), default=3, minimum=1)
PROXY_HEALTH_BASE_BAN_SECONDS = _parse_int(os.getenv("PROXY_HEALTH_BASE_BAN_SECONDS"), default=300, minimum=30)
PROXY_HEALTH_MAX_BAN_SECONDS = _parse_int(os.getenv("PROXY_HEALTH_MAX_BAN_SECONDS"), default=7200, minimum=60)
WORKER_QUERY_SHARD_LEASE_SECONDS = _parse_int(
    os.getenv("WORKER_QUERY_SHARD_LEASE_SECONDS"),
    default=600,
    minimum=30,
)
ENABLE_SIGNAL_DETECTION = _parse_bool(os.getenv("ENABLE_SIGNAL_DETECTION", "1"), default=True)
SIGNAL_RESULT_RATIO_THRESHOLD = _parse_float(
    os.getenv("SIGNAL_RESULT_RATIO_THRESHOLD"),
    default=0.5,
    minimum=0.0,
)
SIGNAL_LOAD_TIME_MULTIPLIER = _parse_float(
    os.getenv("SIGNAL_LOAD_TIME_MULTIPLIER"),
    default=2.0,
    minimum=1.0,
)
SIGNAL_ROLLING_ALPHA = min(
    1.0,
    _parse_float(
        os.getenv("SIGNAL_ROLLING_ALPHA"),
        default=0.2,
        minimum=0.0,
    ),
)
SIGNAL_PAUSE_SECONDS = _parse_int(os.getenv("SIGNAL_PAUSE_SECONDS"), default=900, minimum=30)
SIGNAL_THROTTLE_MULTIPLIER = _parse_float(
    os.getenv("SIGNAL_THROTTLE_MULTIPLIER"),
    default=2.0,
    minimum=1.0,
)
SIGNAL_THROTTLE_DURATION = _parse_int(os.getenv("SIGNAL_THROTTLE_DURATION"), default=600, minimum=60)
SIGNAL_MIN_SUCCESS_CYCLES = _parse_int(os.getenv("SIGNAL_MIN_SUCCESS_CYCLES"), default=5, minimum=1)
ENABLE_FINGERPRINT_VARIATION = _parse_bool(os.getenv("ENABLE_FINGERPRINT_VARIATION", "1"), default=True)
VERIFY_PROXY_IP = _parse_bool(os.getenv("VERIFY_PROXY_IP", "1"), default=True)
PROXY_IP_CHECK_URL = str(os.getenv("PROXY_IP_CHECK_URL", "https://api.ipify.org?format=json") or "").strip()
if not PROXY_IP_CHECK_URL:
    PROXY_IP_CHECK_URL = "https://api.ipify.org?format=json"
PROXY_IP_CHECK_TIMEOUT_MS = _parse_int(
    os.getenv("PROXY_IP_CHECK_TIMEOUT_MS"),
    default=20000,
    minimum=1000,
)
if WORKER_NAME == "worker_3" and WORKER_SCROLL_TARGET_CARDS is not None:
    # Keep worker_3 on medium depth only.
    WORKER_SCROLL_TARGET_CARDS = min(WORKER_SCROLL_TARGET_CARDS, 100)
TELEGRAM_NOTIFICATIONS_ENABLED = bool(
    (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    and (os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()
)
try:
    TELEGRAM_NOTIFY_MIN_PROFIT = float(os.getenv("TELEGRAM_NOTIFY_MIN_PROFIT", "0"))
except ValueError:
    TELEGRAM_NOTIFY_MIN_PROFIT = 0.0
try:
    PROFILE_FAILURE_ALERT_COOLDOWN_SECONDS = max(
        60,
        int(os.getenv("WORKER_PROFILE_FAILURE_ALERT_COOLDOWN_SECONDS", "900")),
    )
except ValueError:
    PROFILE_FAILURE_ALERT_COOLDOWN_SECONDS = 900
MANUAL_LOGIN_ALERT_DEDUP_SECONDS = _parse_int(
    os.getenv("WORKER_MANUAL_LOGIN_ALERT_DEDUP_SECONDS"),
    default=300,
    minimum=60,
)

# Quiet hours — slow down scraping during off-peak times to appear more human.
# Format: "HH:MM-HH:MM" in UTC (e.g. "02:00-06:00"). Empty = disabled.
WORKER_QUIET_HOURS_UTC = (os.getenv("WORKER_QUIET_HOURS_UTC") or "").strip()
WORKER_QUIET_HOURS_MULTIPLIER = _parse_float(
    os.getenv("WORKER_QUIET_HOURS_MULTIPLIER"),
    default=4.0,
    minimum=1.0,
)

# Session duration caps — restart browser after N total queries on the same route
# to avoid session-length fingerprinting. None/0 = unlimited.
WORKER_SESSION_MAX_QUERIES = _parse_optional_int(
    os.getenv("WORKER_SESSION_MAX_QUERIES"), minimum=5
)

MANUAL_LOGIN_REQUIRED_MARKERS = (
    "manual_login_required",
    "facebook checkpoint",
    "checkpoint/login url detected",
    "challenge text detected",
    "confirm it's you",
    "confirm your identity",
    "security check",
    "suspended",
    "temporarily locked",
    "temporarily blocked",
    "two-factor",
)

_PROFILE_FAILURE_ALERT_LAST_SENT_AT: dict[str, datetime] = {}
_MANUAL_LOGIN_ALERT_LAST_SENT_AT: dict[str, datetime] = {}
_ROUTE_CONSECUTIVE_BAD_CYCLES: dict[str, int] = {}
_ROUTE_SESSION_QUERY_COUNTS: dict[str, int] = {}
_SINGLE_ROUTE_ENFORCEMENT_ACTIVE = False


def _parse_quiet_hours(raw: str) -> tuple[tuple[int, int], tuple[int, int]] | None:
    """Parse 'HH:MM-HH:MM' into ((start_h, start_m), (end_h, end_m)) or None."""
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        parts = raw.split("-")
        if len(parts) != 2:
            return None
        start_h, start_m = (int(x) for x in parts[0].strip().split(":"))
        end_h, end_m = (int(x) for x in parts[1].strip().split(":"))
        if not (0 <= start_h < 24 and 0 <= start_m < 60):
            return None
        if not (0 <= end_h < 24 and 0 <= end_m < 60):
            return None
        return (start_h, start_m), (end_h, end_m)
    except (ValueError, IndexError):
        return None


def _is_quiet_hours(now: datetime | None = None) -> bool:
    """Return True if the current UTC time falls within the configured quiet window."""
    parsed = _parse_quiet_hours(WORKER_QUIET_HOURS_UTC)
    if parsed is None:
        return False
    (sh, sm), (eh, em) = parsed
    if now is None:
        now = datetime.now(timezone.utc)
    current_minutes = now.hour * 60 + now.minute
    start_minutes = sh * 60 + sm
    end_minutes = eh * 60 + em
    if start_minutes <= end_minutes:
        return start_minutes <= current_minutes < end_minutes
    # Wraps midnight (e.g. 22:00-06:00).
    return current_minutes >= start_minutes or current_minutes < end_minutes


def _split_query_csv(raw: str | None) -> list[str] | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    queries = [segment.strip() for segment in raw.split(",") if segment.strip()]
    return queries or None


def _split_proxy_pool(raw: str | None) -> list[str]:
    raw_value = (raw or "").strip()
    if not raw_value:
        return []
    entries = [segment.strip() for segment in re.split(r"[\n,;]+", raw_value) if segment.strip()]
    # Keep order stable but remove duplicates.
    return list(dict.fromkeys(entries))


def _normalize_proxy_mode(raw: str | None) -> str:
    value = (raw or "").strip().lower()
    if value in {"auto", "rotate", "rotation", "auto_rotation"}:
        return "auto_rotation"
    return "fixed"


def _route_key(route: dict[str, Any]) -> str:
    worker_name = str(route.get("worker_name") or WORKER_NAME).strip() or WORKER_NAME
    route_name = str(route.get("route_name") or "unknown").strip() or "unknown"
    return f"{worker_name}::{route_name}"


def _normalize_route_status(raw: str | None) -> str:
    value = str(raw or "").strip().upper()
    allowed = {
        RouteStatus.ENABLED.value,
        RouteStatus.DEGRADED.value,
        RouteStatus.THROTTLED.value,
        RouteStatus.COOLDOWN.value,
        RouteStatus.NEEDS_LOGIN.value,
        RouteStatus.DISABLED.value,
    }
    if value in allowed:
        return value
    return RouteStatus.ENABLED.value


def _build_env_route() -> dict[str, Any]:
    return {
        "worker_name": WORKER_NAME,
        "route_name": "env_default",
        "is_enabled": True,
        "proxy_server": (os.getenv("SCRAPER_PROXY_SERVER", "") or "").strip(),
        "proxy_username": (os.getenv("SCRAPER_PROXY_USERNAME", "") or "").strip() or None,
        "proxy_password": (os.getenv("SCRAPER_PROXY_PASSWORD", "") or "").strip() or None,
        "proxy_mode": _normalize_proxy_mode(os.getenv("SCRAPER_PROXY_MODE", "fixed")),
        "proxy_pool": (os.getenv("SCRAPER_PROXY_POOL", "") or "").strip() or None,
        "user_data_dir": WORKER_USER_DATA_DIR,
        "search_queries": (os.getenv("SCRAPER_QUERIES", "") or "").strip() or None,
        "priority": 100,
        "status": RouteStatus.ENABLED.value,
        "status_reason": None,
        "status_since": None,
        "next_run_at": None,
        "route_interval_seconds": None,
        "avg_result_count": None,
        "avg_page_load_ms": None,
        "successful_cycles": 0,
        "source": "env",
    }


def _parse_proxy_entry(raw_proxy: str | None, default_scheme: str = "socks5") -> dict[str, str] | None:
    raw = (raw_proxy or "").strip()
    if not raw:
        return None

    if "://" in raw:
        parsed = urlparse(raw)
        scheme = (parsed.scheme or "").strip().lower()
        if not scheme or not parsed.hostname or not parsed.port:
            return None
        return {
            "scheme": scheme,
            "server": f"{scheme}://{parsed.hostname}:{int(parsed.port)}",
            "username": unquote(parsed.username) if parsed.username else "",
            "password": unquote(parsed.password) if parsed.password else "",
        }

    parts = [segment.strip() for segment in raw.split(":")]
    if len(parts) < 2 or not parts[0] or not parts[1].isdigit():
        return None
    host = parts[0]
    port = int(parts[1])
    username = parts[2] if len(parts) >= 3 and parts[2] else ""
    password = parts[3] if len(parts) >= 4 and parts[3] else ""
    scheme = (default_scheme or "socks5").strip().lower()
    return {
        "scheme": scheme,
        "server": f"{scheme}://{host}:{port}",
        "username": username,
        "password": password,
    }


def _canonical_proxy_key(server: str, username: str | None = None) -> str:
    return canonical_proxy_key_from_module(server=server, username=username)


def _proxy_identity(server: str, username: str, password: str) -> str:
    return proxy_identity_from_module(server=server, username=username, password=password)


def _build_proxy_candidates(route: dict[str, Any]) -> list[dict[str, str]]:
    mode = _normalize_proxy_mode(route.get("proxy_mode"))
    route_username = str(route.get("proxy_username") or "").strip()
    route_password = str(route.get("proxy_password") or "").strip()

    if mode != "auto_rotation":
        fixed_proxy = str(route.get("proxy_server") or "").strip()
        if not fixed_proxy:
            return []
        return [
            {
                "proxy_server": fixed_proxy,
                "proxy_username": route_username,
                "proxy_password": route_password,
            }
        ]

    candidates: list[dict[str, str]] = []
    for item in _split_proxy_pool(route.get("proxy_pool")):
        parsed = _parse_proxy_entry(item, default_scheme="socks5")
        if not parsed:
            continue
        if parsed["scheme"] != "socks5":
            continue
        candidates.append(parsed)

    fallback = _parse_proxy_entry(route.get("proxy_server"), default_scheme="socks5")
    if fallback and fallback.get("server"):
        if route_username:
            fallback["username"] = route_username
        if route_password:
            fallback["password"] = route_password
        if fallback["scheme"] == "socks5":
            candidates.append(fallback)

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in candidates:
        server = str(item.get("server") or "").strip()
        username = str(item.get("username") or "").strip()
        identity = _canonical_proxy_key(server=server, username=username)
        if not server or identity in seen:
            continue
        seen.add(identity)
        deduped.append(item)

    return [
        {
            "proxy_server": str(item.get("server") or "").strip(),
            "proxy_username": str(item.get("username") or "").strip(),
            "proxy_password": str(item.get("password") or "").strip(),
        }
        for item in deduped
        if str(item.get("server") or "").strip()
    ]


def _is_profile_failed(metrics: dict[str, int]) -> bool:
    query_count = int(metrics.get("query_count", 0))
    query_result_count = int(metrics.get("query_result_count", 0))
    query_error_count = int(metrics.get("query_error_count", 0))
    max_page_cards = int(metrics.get("max_page_cards", 0))
    zero_page_queries = int(metrics.get("zero_page_queries", 0))
    if query_count <= 0 and query_result_count <= 0:
        return True
    if query_count > 0 and query_error_count >= query_count and query_result_count <= 0:
        return True
    return (
        query_result_count > 0
        and max_page_cards <= 0
        and zero_page_queries >= query_result_count
    )


def _profile_failure_reason(metrics: dict[str, int] | None = None) -> str:
    query_count = int((metrics or {}).get("query_count", 0))
    query_result_count = int((metrics or {}).get("query_result_count", 0))
    query_error_count = int((metrics or {}).get("query_error_count", 0))
    if query_count <= 0 and query_result_count <= 0:
        return (
            "No queries were executed in this scrape cycle; "
            "worker query shard may be empty or search never started."
        )
    if query_count > 0 and query_result_count <= 0:
        if query_error_count >= query_count:
            return (
                "All queries failed before yielding results; "
                "profile/proxy is likely blocked or manual login is required."
            )
        return (
            "Queries started but none yielded results; "
            "profile/proxy is likely unhealthy."
        )
    return (
        "No marketplace page cards were returned for any query; "
        "the browser profile or proxy is likely unhealthy."
    )


def _is_manual_login_required_error(reason: str | None) -> bool:
    reason_text = str(reason or "").strip().lower()
    if not reason_text:
        return False
    return any(marker in reason_text for marker in MANUAL_LOGIN_REQUIRED_MARKERS)


def _manual_login_required_reason(reason: str | None) -> str:
    reason_text = str(reason or "").strip()
    if not reason_text:
        return "Facebook checkpoint or login challenge detected."
    if reason_text.lower().startswith("manual_login_required:"):
        reason_text = reason_text.split(":", 1)[1].strip()
    return f"Manual login required. {reason_text}"


def _build_quarantine_evidence(
    cycle_id: str,
    cycle_result: CycleResult,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "cycle_id": cycle_id,
        "at": datetime.now(timezone.utc).isoformat(),
        "outcome": cycle_result.outcome.value,
        "error_category": cycle_result.error_category.value,
    }
    if cycle_result.reason:
        evidence["reason"] = cycle_result.reason[:500]
    details = cycle_result.details if isinstance(cycle_result.details, dict) else {}
    if details:
        for key in (
            "proxy_key",
            "proxy_expected_ip",
            "proxy_observed_ip",
            "proxy_ip_check_status",
            "query_shard_key",
            "persona_hash",
            "page_text_sample",
        ):
            value = details.get(key)
            if value is None:
                continue
            text_value = str(value).strip()
            if text_value:
                evidence[key] = text_value[:500]
        soft_signals = details.get("soft_signals")
        if isinstance(soft_signals, list):
            evidence["soft_signals"] = [str(item)[:100] for item in soft_signals if str(item).strip()]
        persona_payload = details.get("persona")
        if isinstance(persona_payload, dict):
            evidence["persona"] = {
                "viewport": (
                    f"{int(persona_payload.get('viewport_width') or 0)}x"
                    f"{int(persona_payload.get('viewport_height') or 0)}"
                ),
                "timezone_id": str(persona_payload.get("timezone_id") or "").strip()[:64],
                "locale": str(persona_payload.get("locale") or "").strip()[:32],
                "color_scheme": str(persona_payload.get("color_scheme") or "").strip()[:16],
            }
    return evidence


def _listing_profit(listing: dict[str, Any]) -> float | None:
    raw = listing.get("potential_profit")
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _should_notify_telegram(created: bool, listing: dict[str, Any]) -> bool:
    if not created or not TELEGRAM_NOTIFICATIONS_ENABLED:
        return False
    model = str(listing.get("model") or "").strip().lower()
    if not model or model == "unknown":
        return False
    profit = _listing_profit(listing)
    return profit is not None and profit >= TELEGRAM_NOTIFY_MIN_PROFIT


def _send_telegram_listing_notification(listing: dict[str, Any]) -> None:
    sent = notify_telegram_listing_card(listing)
    if not sent:
        logging.warning(
            "[%s] telegram notification failed for listing id=%s",
            WORKER_NAME,
            listing.get("id"),
        )


def _extract_profile_number(route: dict[str, Any]) -> str:
    candidates = [
        str(route.get("user_data_dir") or "").strip(),
        str(WORKER_USER_DATA_DIR or "").strip(),
        str(WORKER_NAME or "").strip(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        profile_match = re.search(r"(?:profile|worker)[_-]?(\d+)", candidate, flags=re.IGNORECASE)
        if profile_match:
            return profile_match.group(1)
    return "1" if WORKER_NAME == "worker" else "unknown"


def _profile_failure_alert_key(route: dict[str, Any]) -> str:
    route_name = str(route.get("route_name") or "unknown").strip() or "unknown"
    profile_number = _extract_profile_number(route)
    return f"{WORKER_NAME}::{route_name}::profile_{profile_number}"


def _manual_login_alert_key(route: dict[str, Any], reason: str) -> str:
    reason_text = str(reason or "").strip().lower()
    reason_hash = hashlib.sha1(reason_text.encode("utf-8")).hexdigest()[:12] if reason_text else "unknown"
    return f"{_profile_failure_alert_key(route)}::manual_login::{reason_hash}"


def _should_send_manual_login_alert(route: dict[str, Any], reason: str, now: datetime | None = None) -> bool:
    now_dt = now or datetime.now(timezone.utc)
    alert_key = _manual_login_alert_key(route, reason)
    last_sent_at = _MANUAL_LOGIN_ALERT_LAST_SENT_AT.get(alert_key)
    if last_sent_at:
        age_seconds = (now_dt - last_sent_at).total_seconds()
        if age_seconds < MANUAL_LOGIN_ALERT_DEDUP_SECONDS:
            return False
    _MANUAL_LOGIN_ALERT_LAST_SENT_AT[alert_key] = now_dt
    return True


def _extract_proxy_host_ip(server: str | None) -> str | None:
    host = str(urlparse(str(server or "")).hostname or "").strip()
    if not host:
        return None
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        return None


def _build_accept_language(locale: str | None) -> str | None:
    value = str(locale or "").strip()
    if not value:
        return None
    primary = value.split("-", 1)[0].strip()
    if not primary:
        primary = value
    return f"{value},{primary};q=0.9,en;q=0.8"


def _build_proxy_geo_hint(route: dict[str, Any], selected_proxy: dict[str, str] | None) -> dict[str, str] | None:
    payload: dict[str, str] = {}
    country = (
        str((selected_proxy or {}).get("proxy_country") or "").strip()
        or str((selected_proxy or {}).get("country_code") or "").strip()
        or str(route.get("proxy_country") or "").strip()
        or str(route.get("country_code") or "").strip()
    )
    timezone_id = (
        str((selected_proxy or {}).get("proxy_timezone") or "").strip()
        or str((selected_proxy or {}).get("timezone_id") or "").strip()
        or str(route.get("proxy_timezone") or "").strip()
        or str(route.get("timezone_id") or "").strip()
    )
    locale = (
        str((selected_proxy or {}).get("proxy_locale") or "").strip()
        or str((selected_proxy or {}).get("locale") or "").strip()
        or str(route.get("proxy_locale") or "").strip()
        or str(route.get("locale") or "").strip()
    )
    if country:
        payload["country_code"] = country.upper()
    if timezone_id:
        payload["timezone_id"] = timezone_id
    if locale:
        payload["locale"] = locale
    return payload or None


def _telemetry_proxy_key(route: dict[str, Any]) -> str | None:
    mode = _normalize_proxy_mode(route.get("proxy_mode"))
    if mode != "fixed":
        return "auto_rotation"
    server = str(route.get("proxy_server") or "").strip()
    if not server:
        return None
    username = str(route.get("proxy_username") or "").strip()
    return _canonical_proxy_key(server=server, username=username)


def _emit_cycle_telemetry(
    cycle_id: str,
    route: dict[str, Any],
    started_at: datetime,
    finished_at: datetime,
    result: CycleResult | None,
) -> None:
    payload = build_cycle_telemetry_payload(
        cycle_id=cycle_id,
        worker_name=WORKER_NAME,
        route_name=str(route.get("route_name") or "unknown"),
        started_at=started_at,
        finished_at=finished_at,
        result=result,
        fallback_proxy_key=_telemetry_proxy_key(route),
    )
    logging.info("%s", json.dumps(payload, default=str, separators=(",", ":")))


def _should_send_profile_failure_alert(alert_key: str, now: datetime | None = None) -> bool:
    now_dt = now or datetime.now(timezone.utc)
    last_sent_at = _PROFILE_FAILURE_ALERT_LAST_SENT_AT.get(alert_key)
    if last_sent_at:
        age_seconds = (now_dt - last_sent_at).total_seconds()
        if age_seconds < PROFILE_FAILURE_ALERT_COOLDOWN_SECONDS:
            return False
    _PROFILE_FAILURE_ALERT_LAST_SENT_AT[alert_key] = now_dt
    return True


def _send_telegram_profile_failure_alert(
    route: dict[str, Any],
    reason: str,
    metrics: dict[str, int],
    consecutive_bad_cycles: int = 1,
    cooldown_seconds: int | None = None,
) -> None:
    if not TELEGRAM_NOTIFICATIONS_ENABLED:
        return

    profile_number = _extract_profile_number(route)
    route_name = str(route.get("route_name") or "unknown").strip() or "unknown"
    message = (
        "⚠️ Scraper profile failure detected\n"
        f"Worker: {WORKER_NAME}\n"
        f"Profile: {profile_number}\n"
        f"Route: {route_name}\n"
        f"Reason: {reason}\n"
        f"Consecutive bad cycles: {max(1, int(consecutive_bad_cycles))}\n"
        f"Query starts: {int(metrics.get('query_count', 0))}\n"
        f"Query results: {int(metrics.get('query_result_count', 0))}\n"
        f"Max page cards: {int(metrics.get('max_page_cards', 0))}"
    )
    if cooldown_seconds and cooldown_seconds > 0:
        message = (
            f"{message}\n"
            f"Cooldown applied: {int(cooldown_seconds)}s. Worker will rotate to the next available profile/route."
        )
    sent = send_telegram(
        message=message,
        parse_mode=None,
        disable_web_page_preview=True,
    )
    if not sent:
        logging.warning(
            "[%s] telegram profile-failure alert failed for route=%s profile=%s",
            WORKER_NAME,
            route_name,
            profile_number,
        )


def _send_telegram_manual_login_required_alert(route: dict[str, Any], reason: str) -> None:
    if not TELEGRAM_NOTIFICATIONS_ENABLED:
        return

    profile_number = _extract_profile_number(route)
    route_name = str(route.get("route_name") or "unknown").strip() or "unknown"
    message = (
        "🚫 Scraper profile quarantined (manual login required)\n"
        f"Worker: {WORKER_NAME}\n"
        f"Profile: {profile_number}\n"
        f"Route: {route_name}\n"
        f"Reason: {reason}\n"
        "Action: Run VPS Manual Login for this profile, then set Manual Login Required to 0 to re-enable."
    )
    sent = send_telegram(
        message=message,
        parse_mode=None,
        disable_web_page_preview=True,
    )
    if not sent:
        logging.warning(
            "[%s] telegram manual-login-required alert failed for route=%s profile=%s",
            WORKER_NAME,
            route_name,
            profile_number,
        )


def _current_bad_cycle_count(route_runtime_key: str, route: dict[str, Any]) -> int:
    cached_value = _ROUTE_CONSECUTIVE_BAD_CYCLES.get(route_runtime_key)
    if cached_value is not None:
        try:
            return max(0, int(cached_value))
        except (TypeError, ValueError):
            return 0

    try:
        seeded_value = max(0, int(route.get("consecutive_failures") or 0))
    except (TypeError, ValueError):
        seeded_value = 0
    _ROUTE_CONSECUTIVE_BAD_CYCLES[route_runtime_key] = seeded_value
    return seeded_value


async def _ensure_worker_tables(pool: asyncpg.Pool) -> None:
    await ensure_common_worker_tables(pool=pool, include_triggers=False)


async def _load_worker_routes(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    query = """
        SELECT
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
            status,
            status_reason,
            status_since,
            next_run_at,
            route_interval_seconds,
            avg_result_count,
            avg_page_load_ms,
            successful_cycles,
            consecutive_failures,
            cooldown_until,
            manual_login_required,
            manual_login_reason,
            manual_login_required_at,
            quarantined_at,
            quarantine_reason,
            quarantine_evidence
        FROM worker_routes
        WHERE worker_name = $1
          AND is_enabled = TRUE
          AND COALESCE(status, 'ENABLED') IN ('ENABLED', 'DEGRADED', 'THROTTLED')
          AND COALESCE(manual_login_required, FALSE) = FALSE
          AND (cooldown_until IS NULL OR cooldown_until <= NOW())
        ORDER BY priority ASC, route_name ASC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, WORKER_NAME)

    routes: list[dict[str, Any]] = []
    for row in rows:
        routes.append(
            {
                "worker_name": str(row["worker_name"]),
                "route_name": str(row["route_name"]),
                "is_enabled": bool(row["is_enabled"]),
                "proxy_server": str(row["proxy_server"] or "").strip(),
                "proxy_username": str(row["proxy_username"] or "").strip() or None,
                "proxy_password": str(row["proxy_password"] or "").strip() or None,
                "proxy_mode": _normalize_proxy_mode(str(row["proxy_mode"] or "fixed")),
                "proxy_pool": str(row["proxy_pool"] or "").strip() or None,
                "preferred_proxy_key": str(row["preferred_proxy_key"] or "").strip() or None,
                "preferred_proxy_updated_at": row["preferred_proxy_updated_at"],
                "user_data_dir": str(row["user_data_dir"] or "").strip() or None,
                "search_queries": str(row["search_queries"] or "").strip() or None,
                "priority": int(row["priority"] or 100),
                "status": _normalize_route_status(str(row["status"] or RouteStatus.ENABLED.value)),
                "status_reason": str(row["status_reason"] or "").strip() or None,
                "status_since": row["status_since"],
                "next_run_at": row["next_run_at"],
                "route_interval_seconds": int(row["route_interval_seconds"] or 0) or None,
                "avg_result_count": float(row["avg_result_count"]) if row["avg_result_count"] is not None else None,
                "avg_page_load_ms": float(row["avg_page_load_ms"]) if row["avg_page_load_ms"] is not None else None,
                "successful_cycles": int(row["successful_cycles"] or 0),
                "consecutive_failures": int(row["consecutive_failures"] or 0),
                "cooldown_until": row["cooldown_until"],
                "manual_login_required": bool(row["manual_login_required"]),
                "manual_login_reason": str(row["manual_login_reason"] or "").strip() or None,
                "manual_login_required_at": row["manual_login_required_at"],
                "quarantined_at": row["quarantined_at"],
                "quarantine_reason": str(row["quarantine_reason"] or "").strip() or None,
                "quarantine_evidence": row["quarantine_evidence"],
                "source": "db",
            }
        )
    return routes


async def _has_configured_worker_routes(pool: asyncpg.Pool) -> bool:
    async with pool.acquire() as conn:
        row = await conn.fetchval(
            """
            SELECT 1
            FROM worker_routes
            WHERE worker_name = $1
              AND is_enabled = TRUE
            LIMIT 1
            """,
            WORKER_NAME,
        )
    return bool(row)


async def _count_manual_login_required_routes(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM worker_routes
            WHERE worker_name = $1
              AND is_enabled = TRUE
              AND COALESCE(manual_login_required, FALSE) = TRUE
            """,
            WORKER_NAME,
        )
    return int(count or 0)


async def _count_enabled_worker_routes(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM worker_routes
            WHERE worker_name = $1
              AND is_enabled = TRUE
              AND COALESCE(status, 'ENABLED') <> 'DISABLED'
            """,
            WORKER_NAME,
        )
    return int(count or 0)


async def _seconds_until_next_cooldown_release(pool: asyncpg.Pool) -> float | None:
    async with pool.acquire() as conn:
        seconds = await conn.fetchval(
            """
            SELECT EXTRACT(EPOCH FROM (MIN(cooldown_until) - NOW()))
            FROM worker_routes
            WHERE worker_name = $1
              AND is_enabled = TRUE
              AND cooldown_until > NOW()
            """,
            WORKER_NAME,
        )
    if seconds is None:
        return None
    try:
        return max(0.0, float(seconds))
    except (TypeError, ValueError):
        return None


async def _release_expired_route_cooldowns(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        status = await conn.execute(
            """
            UPDATE worker_routes
            SET
                cooldown_until = NULL,
                status = $2,
                status_reason = NULL,
                status_since = NOW()
            WHERE worker_name = $1
              AND is_enabled = TRUE
              AND status = $3
              AND COALESCE(manual_login_required, FALSE) = FALSE
              AND cooldown_until IS NOT NULL
              AND cooldown_until <= NOW()
            """,
            WORKER_NAME,
            RouteStatus.ENABLED.value,
            RouteStatus.COOLDOWN.value,
        )
    try:
        return int(str(status or "").split()[-1])
    except (TypeError, ValueError, IndexError):
        return 0


def _select_next_route(routes: list[dict[str, Any]], now: datetime | None = None) -> dict[str, Any] | None:
    return select_next_route_from_module(routes=routes, now=now)


async def _mark_route_selected(pool: asyncpg.Pool, route: dict[str, Any]) -> None:
    if route.get("source") != "db":
        return
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE worker_routes
            SET last_selected_at = NOW()
            WHERE worker_name = $1 AND route_name = $2
            """,
            route.get("worker_name"),
            route.get("route_name"),
        )


def _route_interval_seconds(route: dict[str, Any], fallback_seconds: float) -> float:
    return route_interval_seconds_from_module(route=route, fallback_seconds=fallback_seconds)


async def _schedule_route_next_run_at(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    cycle_started_at: datetime,
    interval_seconds: float,
) -> datetime | None:
    return await schedule_route_next_run_at_from_module(
        pool=pool,
        route=route,
        cycle_started_at=cycle_started_at,
        interval_seconds=interval_seconds,
        jitter_pct=SCRAPE_INTERVAL_JITTER_PCT,
        rng=random,
    )


async def _record_route_outcome(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    success: bool,
    error: str = "",
    count_failure: bool = True,
) -> None:
    await record_route_outcome_from_module(
        pool=pool,
        route=route,
        success=success,
        error=error,
        count_failure=count_failure,
    )


async def _put_route_on_cooldown(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    reason: str,
    cooldown_seconds: int,
) -> datetime | None:
    return await put_route_on_cooldown_from_module(
        pool=pool,
        route=route,
        reason=reason,
        cooldown_seconds=cooldown_seconds,
    )


async def _mark_route_manual_login_required(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    reason: str,
    evidence: dict[str, Any] | None = None,
) -> datetime | None:
    return await mark_route_manual_login_required_from_module(
        pool=pool,
        route=route,
        reason=reason,
        evidence=evidence,
    )


async def _try_acquire_proxy_lease(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    candidate: dict[str, str],
    reuse_cooldown_seconds: int,
) -> dict[str, str] | None:
    return await try_acquire_proxy_lease_from_module(
        pool=pool,
        worker_name=WORKER_NAME,
        route_name=str(route.get("route_name") or "unknown"),
        candidate=candidate,
        lease_seconds=WORKER_PROXY_LEASE_SECONDS,
        reuse_cooldown_seconds=max(0, int(reuse_cooldown_seconds)),
    )


async def _refresh_proxy_lease(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    proxy_id: str | None,
    lease_seconds: int,
) -> bool:
    return await refresh_proxy_lease_from_module(
        pool=pool,
        worker_name=WORKER_NAME,
        route_name=str(route.get("route_name") or "unknown"),
        proxy_id=proxy_id,
        lease_seconds=max(30, int(lease_seconds or 30)),
    )


def _proxy_candidate_is_unhealthy(candidate: dict[str, Any]) -> bool:
    failure_count = max(0, int(candidate.get("_consecutive_failures") or 0))
    # Treat near-ban proxies as unhealthy for sticky preference.
    return failure_count >= max(1, PROXY_HEALTH_FAILURE_BAN_AFTER - 1)


def _lease_refresh_interval_seconds(lease_seconds: int) -> float:
    safe_lease_seconds = max(30, int(lease_seconds or 30))
    return float(max(5, min(30, safe_lease_seconds // 3)))


async def _persist_route_preferred_proxy_key(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    proxy_key: str | None,
) -> None:
    if route.get("source") != "db":
        return
    key = str(proxy_key or "").strip() or None
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE worker_routes
            SET
                preferred_proxy_key = $3,
                preferred_proxy_updated_at = CASE
                    WHEN $3::TEXT IS NULL THEN NULL
                    ELSE NOW()
                END
            WHERE worker_name = $1 AND route_name = $2
            """,
            route.get("worker_name"),
            route.get("route_name"),
            key,
        )
    route["preferred_proxy_key"] = key
    route["preferred_proxy_updated_at"] = datetime.now(timezone.utc) if key else None


async def _acquire_proxy_for_route(pool: asyncpg.Pool, route: dict[str, Any]) -> dict[str, str]:
    mode = _normalize_proxy_mode(route.get("proxy_mode"))
    route_name = str(route.get("route_name") or "unknown").strip() or "unknown"
    preferred_proxy_key = str(route.get("preferred_proxy_key") or "").strip() or None
    if not preferred_proxy_key and str(route.get("proxy_server") or "").strip():
        preferred_proxy_key = _proxy_identity(
            str(route.get("proxy_server") or "").strip(),
            str(route.get("proxy_username") or "").strip(),
            str(route.get("proxy_password") or "").strip(),
        )
    candidates = _build_proxy_candidates(route)
    if not candidates:
        raise NoProxyAvailableError("No proxy is configured for the selected route.")

    candidate_keys = [
        _proxy_identity(
            str(candidate.get("proxy_server") or "").strip(),
            str(candidate.get("proxy_username") or "").strip(),
            str(candidate.get("proxy_password") or "").strip(),
        )
        for candidate in candidates
    ]
    proxy_stats = await _fetch_proxy_stats(pool=pool, proxy_keys=candidate_keys)
    eligible_candidates: list[dict[str, str]] = []
    now_dt = datetime.now(timezone.utc)
    for candidate in candidates:
        candidate_key = _proxy_identity(
            str(candidate.get("proxy_server") or "").strip(),
            str(candidate.get("proxy_username") or "").strip(),
            str(candidate.get("proxy_password") or "").strip(),
        )
        stats = proxy_stats.get(candidate_key) or {}
        banned_until = stats.get("banned_until")
        if banned_until is not None:
            try:
                if banned_until.tzinfo is None:
                    banned_until = banned_until.replace(tzinfo=timezone.utc)
                if banned_until > now_dt:
                    continue
            except Exception:
                pass
        candidate_with_stats = dict(candidate)
        candidate_with_stats["proxy_key"] = candidate_key
        candidate_with_stats["_consecutive_failures"] = int(stats.get("consecutive_failures") or 0)
        candidate_with_stats["_last_success_ts"] = (
            float(stats["last_success_at"].timestamp()) if stats.get("last_success_at") else 0.0
        )
        eligible_candidates.append(candidate_with_stats)

    if not eligible_candidates:
        raise NoProxyAvailableError(
            "Selected route has no available proxy right now. "
            "All candidates are currently banned due to recent failures."
        )

    preferred_candidate = None
    if preferred_proxy_key:
        for candidate in eligible_candidates:
            if str(candidate.get("proxy_key") or "").strip() == preferred_proxy_key:
                preferred_candidate = candidate
                break

    random.shuffle(eligible_candidates)
    eligible_candidates.sort(
        key=lambda item: (
            int(item.get("_consecutive_failures") or 0),
            0 if float(item.get("_last_success_ts") or 0) > 0 else 1,
            -float(item.get("_last_success_ts") or 0),
        )
    )
    ordered_candidates: list[dict[str, Any]] = []
    if preferred_candidate and not _proxy_candidate_is_unhealthy(preferred_candidate):
        ordered_candidates.append(preferred_candidate)
    elif preferred_proxy_key:
        if preferred_candidate is None:
            logging.info(
                "[%s/%s] sticky proxy preference unavailable: %s",
                WORKER_NAME,
                route_name,
                preferred_proxy_key,
            )
        else:
            logging.info(
                "[%s/%s] sticky proxy preference unhealthy (failures=%s): %s",
                WORKER_NAME,
                route_name,
                int(preferred_candidate.get("_consecutive_failures") or 0),
                preferred_proxy_key,
            )
    for candidate in eligible_candidates:
        if preferred_candidate is not None and candidate is preferred_candidate:
            continue
        ordered_candidates.append(candidate)

    reuse_cooldown_seconds = WORKER_PROXY_REUSE_COOLDOWN_SECONDS if mode == "auto_rotation" else 0
    for candidate in ordered_candidates:
        leased = await _try_acquire_proxy_lease(
            pool=pool,
            route=route,
            candidate=candidate,
            reuse_cooldown_seconds=reuse_cooldown_seconds,
        )
        if leased:
            leased["proxy_key"] = str(candidate.get("proxy_key") or leased.get("proxy_id") or "").strip() or None
            if mode == "auto_rotation":
                sticky_hit = preferred_proxy_key and leased["proxy_key"] == preferred_proxy_key
                logging.info(
                    "[%s/%s] auto-rotation selected proxy %s (pool=%s sticky_hit=%s)",
                    WORKER_NAME,
                    route_name,
                    leased.get("proxy_server"),
                    len(ordered_candidates),
                    "yes" if sticky_hit else "no",
                )
            return leased
    raise NoProxyAvailableError(
        "Selected route has no available proxy right now. "
        "It may be in active use by another profile or within reuse cooldown."
    )


async def _release_proxy_lease(pool: asyncpg.Pool, route: dict[str, Any], proxy_id: str | None) -> None:
    await release_proxy_lease_from_module(
        pool=pool,
        worker_name=WORKER_NAME,
        route_name=str(route.get("route_name") or "unknown"),
        proxy_id=proxy_id,
    )


def _profile_lock_proxy_id(profile_dir: str) -> str:
    return f"PROFILE:{(profile_dir or '').strip().lower()}"


async def _try_acquire_profile_lock(pool: asyncpg.Pool, route: dict[str, Any], profile_dir: str) -> str | None:
    profile_key = _profile_lock_proxy_id(profile_dir)
    if not profile_key or profile_key == "PROFILE:":
        return None
    route_name = str(route.get("route_name") or "unknown").strip() or "unknown"
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
                updated_at
            ) VALUES (
                $1, $2, NULL, NULL, $3, $4, NOW(),
                NOW() + ($5::INT * INTERVAL '1 second'),
                NOW()
            )
            ON CONFLICT (proxy_id) DO UPDATE SET
                leased_by_worker = EXCLUDED.leased_by_worker,
                leased_by_route = EXCLUDED.leased_by_route,
                leased_at = NOW(),
                lease_until = NOW() + ($5::INT * INTERVAL '1 second'),
                updated_at = NOW()
            WHERE
                COALESCE(worker_proxy_leases.lease_until, TIMESTAMPTZ '-infinity') <= NOW()
                OR (
                    COALESCE(worker_proxy_leases.lease_until, TIMESTAMPTZ '-infinity') > NOW()
                    AND COALESCE(worker_proxy_leases.leased_by_worker, '') = COALESCE($3::TEXT, '')
                    AND COALESCE(worker_proxy_leases.leased_by_route, '') = $4::TEXT
                )
            RETURNING proxy_id
            """,
            profile_key,
            f"profile://{profile_key}",
            WORKER_NAME,
            route_name,
            WORKER_PROXY_LEASE_SECONDS,
        )
    if not row:
        return None
    return str(row["proxy_id"])


async def _release_profile_lock(pool: asyncpg.Pool, route: dict[str, Any], profile_lock_id: str | None) -> None:
    lock_id = str(profile_lock_id or "").strip()
    if not lock_id:
        return
    await _release_proxy_lease(pool=pool, route=route, proxy_id=lock_id)


def _query_shard_canonical_key(route: dict[str, Any], query_override: list[str] | None) -> str | None:
    raw_queries = query_override if query_override is not None else _split_query_csv(route.get("search_queries"))
    normalized = sorted({str(item).strip().lower() for item in (raw_queries or []) if str(item).strip()})
    if not normalized:
        return None
    joined = "|".join(normalized)
    digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:24]
    return f"QUERY_SHARD:{digest}"


async def _try_acquire_query_shard_lock(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    query_shard_key: str,
) -> str | None:
    shard_key = str(query_shard_key or "").strip()
    if not shard_key:
        return None
    route_name = str(route.get("route_name") or "unknown").strip() or "unknown"
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
                updated_at
            ) VALUES (
                $1, $2, NULL, NULL, $3, $4, NOW(),
                NOW() + ($5::INT * INTERVAL '1 second'),
                NOW()
            )
            ON CONFLICT (proxy_id) DO UPDATE SET
                leased_by_worker = EXCLUDED.leased_by_worker,
                leased_by_route = EXCLUDED.leased_by_route,
                leased_at = NOW(),
                lease_until = NOW() + ($5::INT * INTERVAL '1 second'),
                updated_at = NOW()
            WHERE
                COALESCE(worker_proxy_leases.lease_until, TIMESTAMPTZ '-infinity') <= NOW()
                OR (
                    COALESCE(worker_proxy_leases.lease_until, TIMESTAMPTZ '-infinity') > NOW()
                    AND COALESCE(worker_proxy_leases.leased_by_worker, '') = COALESCE($3::TEXT, '')
                    AND COALESCE(worker_proxy_leases.leased_by_route, '') = $4::TEXT
                )
            RETURNING proxy_id
            """,
            shard_key,
            f"query-shard://{shard_key}",
            WORKER_NAME,
            route_name,
            WORKER_QUERY_SHARD_LEASE_SECONDS,
        )
    if not row:
        return None
    return str(row["proxy_id"])


async def _release_query_shard_lock(pool: asyncpg.Pool, route: dict[str, Any], query_lock_id: str | None) -> None:
    lock_id = str(query_lock_id or "").strip()
    if not lock_id:
        return
    await _release_proxy_lease(pool=pool, route=route, proxy_id=lock_id)


async def _fetch_proxy_stats(pool: asyncpg.Pool, proxy_keys: list[str]) -> dict[str, dict[str, Any]]:
    keys = [str(item).strip() for item in proxy_keys if str(item).strip()]
    if not keys:
        return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT proxy_key, consecutive_failures, last_success_at, banned_until, avg_latency_ms
            FROM proxy_stats
            WHERE proxy_key = ANY($1::TEXT[])
            """,
            keys,
        )
    return {
        str(row["proxy_key"]): {
            "consecutive_failures": int(row["consecutive_failures"] or 0),
            "last_success_at": row["last_success_at"],
            "banned_until": row["banned_until"],
            "avg_latency_ms": int(row["avg_latency_ms"] or 0) if row["avg_latency_ms"] is not None else None,
        }
        for row in rows
    }


async def _record_proxy_success(pool: asyncpg.Pool, proxy_key: str | None, latency_ms: int) -> None:
    key = str(proxy_key or "").strip()
    if not key:
        return
    safe_latency = max(0, int(latency_ms or 0))
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO proxy_stats (
                proxy_key,
                consecutive_failures,
                last_success_at,
                banned_until,
                avg_latency_ms,
                updated_at
            ) VALUES (
                $1, 0, NOW(), NULL, NULLIF($2::INT, 0), NOW()
            )
            ON CONFLICT (proxy_key) DO UPDATE SET
                consecutive_failures = 0,
                last_success_at = NOW(),
                banned_until = NULL,
                avg_latency_ms = CASE
                    WHEN $2::INT <= 0 THEN proxy_stats.avg_latency_ms
                    WHEN proxy_stats.avg_latency_ms IS NULL THEN $2::INT
                    ELSE ROUND((proxy_stats.avg_latency_ms * 0.7) + ($2::INT * 0.3))::INT
                END,
                updated_at = NOW()
            """,
            key,
            safe_latency,
        )


async def _record_proxy_failure(pool: asyncpg.Pool, proxy_key: str | None) -> None:
    key = str(proxy_key or "").strip()
    if not key:
        return
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO proxy_stats (
                proxy_key,
                consecutive_failures,
                banned_until,
                updated_at
            ) VALUES (
                $1, 1,
                CASE
                    WHEN 1 >= $2::INT THEN NOW() + ($3::INT * INTERVAL '1 second')
                    ELSE NULL
                END,
                NOW()
            )
            ON CONFLICT (proxy_key) DO UPDATE SET
                consecutive_failures = proxy_stats.consecutive_failures + 1,
                banned_until = CASE
                    WHEN proxy_stats.consecutive_failures + 1 >= $2::INT THEN
                        NOW() + (
                            LEAST(
                                $4::INT,
                                $3::INT * POWER(2, GREATEST(0, (proxy_stats.consecutive_failures + 1) - $2::INT))
                            )::INT * INTERVAL '1 second'
                        )
                    ELSE proxy_stats.banned_until
                END,
                updated_at = NOW()
            """,
            key,
            PROXY_HEALTH_FAILURE_BAN_AFTER,
            PROXY_HEALTH_BASE_BAN_SECONDS,
            PROXY_HEALTH_MAX_BAN_SECONDS,
        )


async def _set_route_status(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    status: str,
    reason: str | None = None,
) -> None:
    if route.get("source") != "db":
        return
    normalized_status = _normalize_route_status(status)
    reason_text = (reason or "").strip()[:2000] or None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE worker_routes
            SET
                status = $3,
                status_reason = $4,
                status_since = CASE
                    WHEN worker_routes.status IS DISTINCT FROM $3
                      OR worker_routes.status_reason IS DISTINCT FROM $4
                    THEN NOW()
                    ELSE worker_routes.status_since
                END
            WHERE worker_name = $1 AND route_name = $2
            RETURNING status, status_reason, status_since
            """,
            route.get("worker_name"),
            route.get("route_name"),
            normalized_status,
            reason_text,
        )
    if row:
        route["status"] = _normalize_route_status(str(row["status"] or normalized_status))
        route["status_reason"] = str(row["status_reason"] or "").strip() or None
        route["status_since"] = row["status_since"]
    else:
        route["status"] = normalized_status
        route["status_reason"] = reason_text
        route["status_since"] = datetime.now(timezone.utc)


async def _record_route_signal_baseline(
    pool: asyncpg.Pool,
    route: dict[str, Any],
    result_count: int,
    page_load_ms: int,
) -> None:
    if route.get("source") != "db":
        return
    result_value = max(0, int(result_count or 0))
    load_value = max(0, int(page_load_ms or 0))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE worker_routes
            SET
                avg_result_count = CASE
                    WHEN avg_result_count IS NULL THEN $3::DOUBLE PRECISION
                    ELSE ($5::DOUBLE PRECISION * $3::DOUBLE PRECISION)
                       + ((1 - $5::DOUBLE PRECISION) * avg_result_count)
                END,
                avg_page_load_ms = CASE
                    WHEN avg_page_load_ms IS NULL THEN $4::DOUBLE PRECISION
                    ELSE ($5::DOUBLE PRECISION * $4::DOUBLE PRECISION)
                       + ((1 - $5::DOUBLE PRECISION) * avg_page_load_ms)
                END,
                successful_cycles = GREATEST(0, COALESCE(successful_cycles, 0)) + 1
            WHERE worker_name = $1 AND route_name = $2
            RETURNING avg_result_count, avg_page_load_ms, successful_cycles
            """,
            route.get("worker_name"),
            route.get("route_name"),
            float(result_value),
            float(load_value),
            float(SIGNAL_ROLLING_ALPHA),
        )
    if row:
        route["avg_result_count"] = float(row["avg_result_count"]) if row["avg_result_count"] is not None else None
        route["avg_page_load_ms"] = float(row["avg_page_load_ms"]) if row["avg_page_load_ms"] is not None else None
        route["successful_cycles"] = int(row["successful_cycles"] or 0)


async def _upsert_worker_heartbeat(
    pool: asyncpg.Pool,
    route_name: str,
    status: str,
    listings_saved: int = 0,
    query_count: int = 0,
    last_error: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    query = """
        INSERT INTO worker_heartbeats (
            worker_name,
            route_name,
            status,
            listings_saved,
            query_count,
            last_run_started_at,
            last_run_finished_at,
            last_error,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, NOW()
        )
        ON CONFLICT (worker_name) DO UPDATE SET
            route_name = EXCLUDED.route_name,
            status = EXCLUDED.status,
            listings_saved = EXCLUDED.listings_saved,
            query_count = EXCLUDED.query_count,
            last_run_started_at = COALESCE(EXCLUDED.last_run_started_at, worker_heartbeats.last_run_started_at),
            last_run_finished_at = COALESCE(EXCLUDED.last_run_finished_at, worker_heartbeats.last_run_finished_at),
            last_error = EXCLUDED.last_error,
            updated_at = NOW()
    """
    async with pool.acquire() as conn:
        await conn.execute(
            query,
            WORKER_NAME,
            route_name,
            status,
            int(max(0, listings_saved)),
            int(max(0, query_count)),
            started_at,
            finished_at,
            (last_error or "").strip()[:2000] or None,
        )


async def _record_scrape_events(
    pool: asyncpg.Pool,
    route_name: str,
    scrape_events: list[tuple[datetime, int]],
) -> None:
    if not scrape_events:
        return
    values: list[tuple[str, str | None, int, datetime]] = []
    route_name_value = (route_name or "").strip()
    for observed_at, scraped_count in scrape_events:
        count_value = int(scraped_count or 0)
        if count_value <= 0:
            continue
        values.append(
            (
                WORKER_NAME,
                route_name_value or None,
                count_value,
                observed_at,
            )
        )
    if not values:
        return
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO worker_scrape_events (
                worker_name,
                route_name,
                scraped_count,
                observed_at
            ) VALUES ($1, $2, $3, $4)
            """,
            values,
        )


async def _cleanup_old_scrape_events(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM worker_scrape_events WHERE observed_at < NOW() - INTERVAL '2 days'"
        )


async def _build_playwright_proxy(
    pool: asyncpg.Pool,
    route: dict[str, Any],
) -> tuple[dict[str, str], Any, str | None, str | None, str | None, dict[str, str] | None]:
    selected_proxy = await _acquire_proxy_for_route(pool=pool, route=route)
    raw_server = str(selected_proxy.get("proxy_server") or "").strip()
    if not raw_server:
        raise NoProxyAvailableError("Selected proxy entry was missing proxy_server.")
    proxy_key = str(selected_proxy.get("proxy_key") or "").strip() or str(selected_proxy.get("proxy_id") or "").strip() or None
    proxy_geo_hint = _build_proxy_geo_hint(route=route, selected_proxy=selected_proxy)

    parsed = urlparse(raw_server)
    scheme = (parsed.scheme or "").lower()
    if not scheme:
        raise RuntimeError("SCRAPER_PROXY_SERVER must include a scheme (http://, https://, socks5://)")

    if not parsed.hostname or not parsed.port:
        raise RuntimeError("SCRAPER_PROXY_SERVER must include host:port")
    expected_proxy_ip = _extract_proxy_host_ip(raw_server)

    configured_username = str(selected_proxy.get("proxy_username") or "").strip()
    configured_password = str(selected_proxy.get("proxy_password") or "").strip()

    url_username = unquote(parsed.username) if parsed.username else ""
    url_password = unquote(parsed.password) if parsed.password else ""

    username = configured_username or url_username
    password = configured_password or url_password

    clean_server = f"{scheme}://{parsed.hostname}:{parsed.port}"

    if scheme == "socks5" and (username or password):
        bridge_process, bridge_proxy = _start_local_socks5_auth_bridge(
            {
                "host": parsed.hostname,
                "port": int(parsed.port),
                "username": username,
                "password": password,
            }
        )
        return (
            {"server": bridge_proxy},
            bridge_process,
            str(selected_proxy.get("proxy_id") or "").strip() or None,
            proxy_key,
            expected_proxy_ip,
            proxy_geo_hint,
        )

    proxy: dict[str, str] = {"server": clean_server}
    if username:
        proxy["username"] = username
    if password:
        proxy["password"] = password
    return (
        proxy,
        None,
        str(selected_proxy.get("proxy_id") or "").strip() or None,
        proxy_key,
        expected_proxy_ip,
        proxy_geo_hint,
    )


async def _upsert_listing(pool: asyncpg.Pool, listing: dict[str, Any]) -> bool:
    listing_id = str(listing.get("id") or "").strip()
    if not listing_id:
        return False

    source_seen_at = datetime.now(timezone.utc)

    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM listings WHERE id = $1", listing_id)
        await conn.execute(
            """
            INSERT INTO listings (
                id, title, price, location, url, description, seller_name,
                model, condition, max_buy_price, potential_profit, status,
                source_seen_at, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11, $12,
                $13, NOW(), NOW()
            )
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                location = EXCLUDED.location,
                url = EXCLUDED.url,
                description = EXCLUDED.description,
                seller_name = EXCLUDED.seller_name,
                model = EXCLUDED.model,
                condition = EXCLUDED.condition,
                max_buy_price = EXCLUDED.max_buy_price,
                potential_profit = EXCLUDED.potential_profit,
                status = EXCLUDED.status,
                source_seen_at = EXCLUDED.source_seen_at,
                updated_at = NOW()
            """,
            listing_id,
            listing.get("title") or "Untitled listing",
            listing.get("price"),
            listing.get("location"),
            listing.get("url"),
            listing.get("description"),
            listing.get("seller_name"),
            listing.get("model"),
            listing.get("condition"),
            listing.get("max_offer"),
            listing.get("potential_profit"),
            listing.get("status") or "new",
            source_seen_at,
        )
    return not bool(exists)


async def _publish_listing_event(
    redis_client: Redis,
    event_name: str,
    listing: dict[str, Any],
    metadata: dict[str, Any],
) -> None:
    payload = {
        "event": event_name,
        "at": datetime.now(timezone.utc).isoformat(),
        "worker": WORKER_NAME,
        "route_name": metadata.get("route_name"),
        "query": metadata.get("query"),
        "query_index": metadata.get("query_index"),
        "query_total": metadata.get("query_total"),
        "listing": listing,
    }
    await redis_client.publish(LISTING_EVENT_CHANNEL, json.dumps(payload, default=str))


async def _process_listing_event(
    pool: asyncpg.Pool,
    redis_client: Redis,
    listing: dict[str, Any],
    metadata: dict[str, Any],
) -> None:
    created = await _upsert_listing(pool, listing)
    await _publish_listing_event(
        redis_client=redis_client,
        event_name="listing_created" if created else "listing_updated",
        listing=listing,
        metadata=metadata,
    )
    if _should_notify_telegram(created, listing):
        await asyncio.to_thread(_send_telegram_listing_notification, listing)


async def _run_scrape_cycle(
    pool: asyncpg.Pool,
    redis_client: Redis,
    route: dict[str, Any],
) -> tuple[dict[str, int], dict[str, Any]]:
    query_override = _split_query_csv(route.get("search_queries"))
    profile_dir = str(route.get("user_data_dir") or WORKER_USER_DATA_DIR or "").strip() or None
    profile_lock_id: str | None = None
    query_shard_lock_id: str | None = None
    query_shard_key: str | None = None
    bridge_process = None
    proxy_lease_id: str | None = None
    proxy_key: str | None = None
    proxy_expected_ip: str | None = None
    proxy_observed_ip: str | None = None
    proxy_ip_check_status: str | None = None
    ingest_tasks: set[asyncio.Task] = set()
    scrape_event_tasks: set[asyncio.Task] = set()
    query_error_text_samples: list[str] = []
    persona_context_options: dict[str, Any] | None = None
    persona_extra_headers: dict[str, str] | None = None
    persona_details: dict[str, Any] | None = None
    persona_digest: str | None = None
    lease_keepalive_stop = asyncio.Event()
    lease_keepalive_tasks: list[asyncio.Task] = []
    lease_keepalive_failure: dict[str, str] = {}
    metrics: dict[str, int] = {
        "listings_saved": 0,
        "listings_scraped": 0,
        "query_count": 0,
        "query_result_count": 0,
        "zero_page_queries": 0,
        "max_page_cards": 0,
        "query_error_count": 0,
        "redirect_count": 0,
    }

    if profile_dir:
        profile_lock_id = await _try_acquire_profile_lock(pool=pool, route=route, profile_dir=profile_dir)
        if not profile_lock_id:
            raise ProfileLockUnavailableError(f"Profile lock unavailable for {profile_dir}")
    query_shard_key = _query_shard_canonical_key(route=route, query_override=query_override)
    if query_shard_key:
        query_shard_lock_id = await _try_acquire_query_shard_lock(
            pool=pool,
            route=route,
            query_shard_key=query_shard_key,
        )
        if not query_shard_lock_id:
            raise QueryShardLockUnavailableError(f"Query shard lock unavailable for {query_shard_key}")

    async def _lease_keepalive_loop(lock_id: str | None, lock_name: str, lease_seconds: int) -> None:
        lock_id_value = str(lock_id or "").strip()
        if not lock_id_value:
            return
        refresh_interval = _lease_refresh_interval_seconds(lease_seconds)
        while not lease_keepalive_stop.is_set():
            await asyncio.sleep(refresh_interval)
            if lease_keepalive_stop.is_set():
                break
            try:
                refreshed = await _refresh_proxy_lease(
                    pool=pool,
                    route=route,
                    proxy_id=lock_id_value,
                    lease_seconds=lease_seconds,
                )
            except Exception as exc:
                refreshed = False
                error_text = f"{lock_name} lease refresh failed: {exc}"
            else:
                error_text = f"{lock_name} lease lost during active session."
            if refreshed:
                continue
            if "error" not in lease_keepalive_failure:
                lease_keepalive_failure["error"] = error_text
            lease_keepalive_stop.set()
            return

    def _start_lease_keepalive(lock_id: str | None, lock_name: str, lease_seconds: int) -> None:
        lock_id_value = str(lock_id or "").strip()
        if not lock_id_value:
            return
        lease_keepalive_tasks.append(
            asyncio.create_task(
                _lease_keepalive_loop(lock_id=lock_id_value, lock_name=lock_name, lease_seconds=lease_seconds)
            )
        )

    def _on_progress(payload: dict[str, Any]) -> None:
        nonlocal proxy_expected_ip, proxy_observed_ip, proxy_ip_check_status
        event = payload.get("event")
        if event == "proxy_ip_check":
            expected_ip = str(payload.get("expected_ip") or "").strip()
            observed_ip = str(payload.get("observed_ip") or "").strip()
            status_value = str(payload.get("status") or "").strip().lower()
            if expected_ip:
                proxy_expected_ip = expected_ip
            if observed_ip:
                proxy_observed_ip = observed_ip
            if status_value:
                proxy_ip_check_status = status_value
            return

        if event == "query_start":
            metrics["query_count"] = metrics.get("query_count", 0) + 1
            logging.info(
                "[%s/%s] query %s/%s: %s",
                WORKER_NAME,
                route.get("route_name", "unknown"),
                payload.get("query_index", 0),
                payload.get("query_total", 0),
                payload.get("query"),
            )
            return

        if event == "query_result":
            try:
                found_count = int(payload.get("found", 0) or 0)
            except (TypeError, ValueError):
                found_count = 0
            try:
                page_cards = int(payload.get("page_cards", 0) or 0)
            except (TypeError, ValueError):
                page_cards = 0
            try:
                redirect_count = int(payload.get("redirect_count", 0) or 0)
            except (TypeError, ValueError):
                redirect_count = 0
            metrics["listings_scraped"] = metrics.get("listings_scraped", 0) + max(0, found_count)
            metrics["query_result_count"] = metrics.get("query_result_count", 0) + 1
            metrics["max_page_cards"] = max(metrics.get("max_page_cards", 0), page_cards)
            metrics["redirect_count"] = max(metrics.get("redirect_count", 0), max(0, redirect_count))
            if page_cards <= 0:
                metrics["zero_page_queries"] = metrics.get("zero_page_queries", 0) + 1
            if found_count > 0:
                task = asyncio.create_task(
                    _record_scrape_events(
                        pool=pool,
                        route_name=str(route.get("route_name") or ""),
                        scrape_events=[(datetime.now(timezone.utc), found_count)],
                    )
                )
                scrape_event_tasks.add(task)
                task.add_done_callback(lambda done_task: scrape_event_tasks.discard(done_task))

            logging.info(
                "[%s/%s] query result %s/%s found=%s page_cards=%s scroll_rounds=%s new_saved=%s",
                WORKER_NAME,
                route.get("route_name", "unknown"),
                payload.get("query_index", 0),
                payload.get("query_total", 0),
                found_count,
                page_cards,
                payload.get("scroll_rounds", 0),
                payload.get("new_saved", 0),
            )
            return

        if event == "query_error":
            metrics["query_error_count"] = metrics.get("query_error_count", 0) + 1
            error_text = str(payload.get("error") or "").strip()
            if error_text:
                query_error_text_samples.append(error_text[:300])
                del query_error_text_samples[:-5]
            return

        if event != "listing_saved":
            return

        listing = payload.get("listing")
        if not isinstance(listing, dict):
            return

        if is_accessory_only_listing(
            str(listing.get("title") or ""),
            str(listing.get("description") or ""),
            listing.get("price"),
        ):
            logging.info(
                "[%s/%s] accessory-only listing skipped id=%s",
                WORKER_NAME,
                route.get("route_name", "unknown"),
                listing.get("id"),
            )
            return

        metrics["listings_saved"] = metrics.get("listings_saved", 0) + 1
        metadata = {
            "route_name": route.get("route_name"),
            "query": payload.get("query"),
            "query_index": payload.get("query_index"),
            "query_total": payload.get("query_total"),
        }

        task = asyncio.create_task(_process_listing_event(pool, redis_client, listing, metadata))
        ingest_tasks.add(task)
        task.add_done_callback(lambda done_task: ingest_tasks.discard(done_task))

    try:
        (
            proxy_config,
            bridge_process,
            proxy_lease_id,
            proxy_key,
            proxy_expected_ip,
            proxy_geo_hint,
        ) = await _build_playwright_proxy(pool=pool, route=route)
        if ENABLE_FINGERPRINT_VARIATION:
            persona = generate_persona(proxy_geo=proxy_geo_hint, rng=random)
            persona_context_options = persona.to_context_options()
            persona_details = persona.to_telemetry_dict()
            persona_digest = persona_hash(persona)
            accept_language = _build_accept_language(persona.locale)
            if accept_language:
                persona_extra_headers = {"Accept-Language": accept_language}
        _start_lease_keepalive(proxy_lease_id, "proxy", WORKER_PROXY_LEASE_SECONDS)
        _start_lease_keepalive(profile_lock_id, "profile", WORKER_PROXY_LEASE_SECONDS)
        _start_lease_keepalive(query_shard_lock_id, "query_shard", WORKER_QUERY_SHARD_LEASE_SECONDS)
        scrape_started_monotonic = time.monotonic()
        try:
            scrape_kwargs = {
                "headless": WORKER_HEADLESS,
                "progress_callback": _on_progress,
                "user_data_dir": profile_dir,
                "proxy": proxy_config,
                "search_queries": query_override,
                "scroll_target_cards_override": WORKER_SCROLL_TARGET_CARDS,
                "scroll_max_rounds_override": WORKER_SCROLL_MAX_ROUNDS,
                "verify_proxy_ip": VERIFY_PROXY_IP,
                "proxy_ip_check_url": PROXY_IP_CHECK_URL,
                "proxy_ip_check_timeout_ms": PROXY_IP_CHECK_TIMEOUT_MS,
                "proxy_ip_expected": proxy_expected_ip,
                "browser_context_options": persona_context_options,
                "extra_http_headers": persona_extra_headers,
            }
            unsupported_kwargs = [
                key for key in scrape_kwargs.keys() if key not in SCRAPE_MARKETPLACE_SUPPORTED_KWARGS
            ]
            if unsupported_kwargs:
                route_name_value = str(route.get("route_name") or "unknown")
                logging.warning(
                    "[%s/%s] scrape_marketplace kwargs unsupported in current scraper build: %s",
                    WORKER_NAME,
                    route_name_value,
                    ", ".join(sorted(unsupported_kwargs)),
                )
                for key in unsupported_kwargs:
                    scrape_kwargs.pop(key, None)
            scrape_task = asyncio.create_task(scrape_marketplace(**scrape_kwargs))
            lease_watch_task = asyncio.create_task(lease_keepalive_stop.wait())
            done, _ = await asyncio.wait(
                {scrape_task, lease_watch_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if lease_watch_task in done and lease_keepalive_failure:
                if not scrape_task.done():
                    scrape_task.cancel()
                    try:
                        await scrape_task
                    except asyncio.CancelledError:
                        pass
                raise NoProxyAvailableError(
                    str(lease_keepalive_failure.get("error") or "Proxy lease lost during active session.")
                )
            if not scrape_task.done():
                await scrape_task
            else:
                scrape_task.result()
        except Exception as exc:
            if not isinstance(exc, NoProxyAvailableError):
                await _record_proxy_failure(pool=pool, proxy_key=proxy_key)
            raise
        finally:
            lease_keepalive_stop.set()

        if ingest_tasks:
            results = await asyncio.gather(*list(ingest_tasks), return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logging.error("[%s] listing ingest error: %s", WORKER_NAME, result)
        duration_ms = max(0, int((time.monotonic() - scrape_started_monotonic) * 1000))
        await _record_proxy_success(pool=pool, proxy_key=proxy_key, latency_ms=duration_ms)
        try:
            await _persist_route_preferred_proxy_key(pool=pool, route=route, proxy_key=proxy_key)
        except Exception as exc:
            logging.warning(
                "[%s/%s] failed to persist preferred proxy key: %s",
                WORKER_NAME,
                str(route.get("route_name") or "unknown"),
                exc,
            )
        cycle_meta: dict[str, Any] = {
            "proxy_key": proxy_key,
            "duration_ms": duration_ms,
            "query_shard_key": query_shard_key,
            "page_text_sample": " ".join(query_error_text_samples)[-500:],
            "proxy_expected_ip": proxy_expected_ip,
            "proxy_observed_ip": proxy_observed_ip,
            "proxy_ip_check_status": proxy_ip_check_status,
        }
        if persona_digest:
            cycle_meta["persona_hash"] = persona_digest
        if isinstance(persona_details, dict):
            cycle_meta["persona"] = persona_details
        return metrics, cycle_meta
    finally:
        lease_keepalive_stop.set()
        if lease_keepalive_tasks:
            for task in lease_keepalive_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*lease_keepalive_tasks, return_exceptions=True)
        try:
            if scrape_event_tasks:
                results = await asyncio.gather(*list(scrape_event_tasks), return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        logging.warning("[%s] failed to record scrape event: %s", WORKER_NAME, result)
            await _cleanup_old_scrape_events(pool)
        except Exception as exc:
            logging.warning("[%s] failed to record scrape events: %s", WORKER_NAME, exc)
        cleanup_bridge_process(bridge_process)
        try:
            await _release_proxy_lease(pool=pool, route=route, proxy_id=proxy_lease_id)
        except Exception as exc:
            logging.warning("[%s] failed to release proxy lease: %s", WORKER_NAME, exc)
        try:
            await _release_profile_lock(pool=pool, route=route, profile_lock_id=profile_lock_id)
        except Exception as exc:
            logging.warning("[%s] failed to release profile lock: %s", WORKER_NAME, exc)
        try:
            await _release_query_shard_lock(pool=pool, route=route, query_lock_id=query_shard_lock_id)
        except Exception as exc:
            logging.warning("[%s] failed to release query shard lock: %s", WORKER_NAME, exc)


async def _run_scrape_cycle_with_retry(
    pool: asyncpg.Pool,
    redis_client: Redis,
    route: dict[str, Any],
) -> CycleResult:
    route_name = str(route.get("route_name") or "unknown")
    last_metrics: dict[str, int] = {
        "listings_saved": 0,
        "listings_scraped": 0,
        "query_count": 0,
        "query_result_count": 0,
        "zero_page_queries": 0,
        "max_page_cards": 0,
        "query_error_count": 0,
        "redirect_count": 0,
    }
    last_reason: str | None = None
    retry_count = 0
    cycle_details: dict[str, Any] = {}

    for attempt in range(1, WORKER_CYCLE_RETRY_ATTEMPTS + 1):
        try:
            metrics, cycle_meta = await _run_scrape_cycle(pool, redis_client, route)
            last_metrics = metrics
            cycle_details = dict(cycle_meta or {})
            if not _is_profile_failed(metrics):
                if ENABLE_SIGNAL_DETECTION:
                    successful_cycles = int(route.get("successful_cycles") or 0)
                    if successful_cycles >= SIGNAL_MIN_SUCCESS_CYCLES:
                        signal_report = analyze_cycle_signals(
                            result_count=metrics.get("listings_scraped", 0),
                            expected_result_count=float(route.get("avg_result_count") or 0.0),
                            redirect_count=metrics.get("redirect_count", 0),
                            page_load_ms=int(cycle_details.get("duration_ms") or 0),
                            avg_page_load_ms=float(route.get("avg_page_load_ms") or 0.0),
                            page_text_sample=str(cycle_details.get("page_text_sample") or ""),
                            dom_has_captcha="captcha" in str(cycle_details.get("page_text_sample") or "").lower(),
                            result_ratio_threshold=SIGNAL_RESULT_RATIO_THRESHOLD,
                            load_time_multiplier=SIGNAL_LOAD_TIME_MULTIPLIER,
                        )
                        cycle_details["soft_signals"] = [signal.signal_type for signal in signal_report.signals]
                        cycle_details["signal_risk_score"] = signal_report.risk_score
                        cycle_details["signal_action"] = signal_report.recommended_action
                        if signal_report.recommended_action == "quarantine":
                            raise PreCheckpointSignalError(
                                "Pre-checkpoint signal risk score is critical; manual review/login required."
                            )
                        if signal_report.recommended_action == "pause":
                            return CycleResult(
                                metrics=metrics,
                                outcome=CycleOutcome.WAIT_SIGNAL_PAUSE,
                                reason=(
                                    f"Pre-checkpoint signal risk score={signal_report.risk_score:.2f}; "
                                    f"pausing route for {SIGNAL_PAUSE_SECONDS}s."
                                ),
                                error_category=ErrorCategory.SIGNAL_RISK,
                                retry_count=retry_count,
                                details=cycle_details,
                            )
                        if signal_report.recommended_action == "throttle":
                            cycle_details["signal_throttle_multiplier"] = SIGNAL_THROTTLE_MULTIPLIER
                            cycle_details["signal_throttle_duration_seconds"] = SIGNAL_THROTTLE_DURATION
                return CycleResult(
                    metrics=metrics,
                    outcome=CycleOutcome.OK,
                    retry_count=retry_count,
                    details=cycle_details,
                )

            last_reason = _profile_failure_reason(metrics)
            category = ErrorCategory.PARSE_FAILED
            await _record_proxy_failure(pool=pool, proxy_key=str(cycle_details.get("proxy_key") or "").strip() or None)
            return CycleResult(
                metrics=metrics,
                outcome=CycleOutcome.FAIL,
                reason=last_reason,
                error_category=category,
                retry_count=retry_count,
                details=cycle_details,
            )
        except Exception as exc:
            last_reason = str(exc) or "unknown scrape error"
            category = classify_error(last_reason)
            if _is_manual_login_required_error(last_reason) or category == ErrorCategory.AUTH_REQUIRED:
                return CycleResult(
                    metrics=last_metrics,
                    outcome=CycleOutcome.NEEDS_LOGIN,
                    reason=last_reason,
                    error_category=ErrorCategory.AUTH_REQUIRED,
                    retry_count=retry_count,
                    details=cycle_details,
                )
            if category == ErrorCategory.NO_PROXY_AVAILABLE:
                return CycleResult(
                    metrics=last_metrics,
                    outcome=CycleOutcome.WAIT_PROXY,
                    reason=last_reason,
                    error_category=category,
                    retry_count=retry_count,
                    details=cycle_details,
                )
            if category == ErrorCategory.PROXY_MISMATCH:
                return CycleResult(
                    metrics=last_metrics,
                    outcome=CycleOutcome.WAIT_PROXY_MISMATCH,
                    reason=last_reason,
                    error_category=category,
                    retry_count=retry_count,
                    details=cycle_details,
                )
            if category == ErrorCategory.PROFILE_LOCK_UNAVAILABLE:
                return CycleResult(
                    metrics=last_metrics,
                    outcome=CycleOutcome.WAIT_PROFILE_LOCK,
                    reason=last_reason,
                    error_category=category,
                    retry_count=retry_count,
                    details=cycle_details,
                )
            if category == ErrorCategory.QUERY_SHARD_LOCK_UNAVAILABLE:
                return CycleResult(
                    metrics=last_metrics,
                    outcome=CycleOutcome.WAIT_QUERY_SHARD,
                    reason=last_reason,
                    error_category=category,
                    retry_count=retry_count,
                    details=cycle_details,
                )
            if category == ErrorCategory.SIGNAL_RISK:
                return CycleResult(
                    metrics=last_metrics,
                    outcome=CycleOutcome.NEEDS_LOGIN,
                    reason=last_reason,
                    error_category=category,
                    retry_count=retry_count,
                    details=cycle_details,
                )

            if attempt < WORKER_CYCLE_RETRY_ATTEMPTS and is_retryable_category(category):
                retry_count += 1
                backoff_seconds = compute_retry_backoff_seconds(
                    base_seconds=WORKER_CYCLE_RETRY_BACKOFF_SECONDS,
                    attempt=retry_count,
                    max_seconds=WORKER_CYCLE_RETRY_BACKOFF_MAX_SECONDS,
                )
                logging.warning(
                    "[%s/%s] scrape attempt %s/%s failed (%s): %s (retry in %ss)",
                    WORKER_NAME,
                    route_name,
                    attempt,
                    WORKER_CYCLE_RETRY_ATTEMPTS,
                    category.value,
                    last_reason,
                    backoff_seconds,
                )
                await asyncio.sleep(backoff_seconds)
                continue

            logging.warning(
                "[%s/%s] scrape attempt %s/%s failed (%s): %s",
                WORKER_NAME,
                route_name,
                attempt,
                WORKER_CYCLE_RETRY_ATTEMPTS,
                category.value,
                last_reason,
            )
            return CycleResult(
                metrics=last_metrics,
                outcome=CycleOutcome.FAIL,
                reason=last_reason,
                error_category=category,
                retry_count=retry_count,
                details=cycle_details,
            )

    return CycleResult(
        metrics=last_metrics,
        outcome=CycleOutcome.FAIL,
        reason=last_reason or "unknown scrape error",
        error_category=classify_error(last_reason),
        retry_count=retry_count,
        details=cycle_details,
    )


async def _main() -> None:
    init_db()

    pool = await asyncpg.create_pool(
        host=PGHOST,
        port=PGPORT,
        database=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        min_size=1,
        max_size=10,
    )
    await _ensure_worker_tables(pool)
    redis_client = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD or None,
        decode_responses=True,
    )

    logging.info(
        "[%s] worker online (interval=%ss retries=%s degraded_after=%s throttled_after=%s cooldown_after=%s jitter=%.2f min_enabled_routes_warn=%s verify_proxy_ip=%s fingerprint_variation=%s)",
        WORKER_NAME,
        SCRAPE_INTERVAL_SECONDS,
        WORKER_CYCLE_RETRY_ATTEMPTS,
        WORKER_DEGRADED_CONSECUTIVE_CYCLES,
        WORKER_THROTTLED_CONSECUTIVE_CYCLES,
        WORKER_ROUTE_COOLDOWN_BAD_CYCLES,
        SCRAPE_INTERVAL_JITTER_PCT,
        WORKER_MIN_ENABLED_ROUTES_WARN,
        VERIFY_PROXY_IP,
        ENABLE_FINGERPRINT_VARIATION,
    )

    # Start proxy provider health monitor (no-op if no gateway token configured).
    await start_proxy_monitor()

    global _SINGLE_ROUTE_ENFORCEMENT_ACTIVE
    try:
        while True:
            cycle_started_monotonic = time.monotonic()
            route = None
            started_at = datetime.now(timezone.utc)
            try:
                released_cooldowns = await _release_expired_route_cooldowns(pool)
                if released_cooldowns > 0:
                    logging.info(
                        "[%s] released %s route(s) from expired cooldown windows.",
                        WORKER_NAME,
                        released_cooldowns,
                    )
                routes = await _load_worker_routes(pool)
                enabled_route_count = await _count_enabled_worker_routes(pool)
                eligible_route_count = len(routes)
                effective_interval_seconds = compute_worker_effective_interval(
                    base_interval=SCRAPE_INTERVAL_SECONDS,
                    enabled_count=enabled_route_count or eligible_route_count or 1,
                    eligible_count=eligible_route_count,
                    max_multiplier=WORKER_MAX_BACKOFF_MULTIPLIER,
                )
                single_route_enforcement = (
                    enabled_route_count > 0 and enabled_route_count < WORKER_MIN_ENABLED_ROUTES_WARN
                )
                if single_route_enforcement:
                    effective_interval_seconds *= WORKER_SINGLE_ROUTE_REST_MULTIPLIER
                if single_route_enforcement != _SINGLE_ROUTE_ENFORCEMENT_ACTIVE:
                    if single_route_enforcement:
                        logging.warning(
                            "[%s] min-route enforcement active: enabled routes=%s (< %s); applying %.2fx rest multiplier.",
                            WORKER_NAME,
                            enabled_route_count,
                            WORKER_MIN_ENABLED_ROUTES_WARN,
                            WORKER_SINGLE_ROUTE_REST_MULTIPLIER,
                        )
                    else:
                        logging.info(
                            "[%s] min-route enforcement lifted: enabled routes=%s (>= %s).",
                            WORKER_NAME,
                            enabled_route_count,
                            WORKER_MIN_ENABLED_ROUTES_WARN,
                        )
                    _SINGLE_ROUTE_ENFORCEMENT_ACTIVE = single_route_enforcement

                # During quiet hours, slow down even further.
                if _is_quiet_hours():
                    effective_interval_seconds *= WORKER_QUIET_HOURS_MULTIPLIER
                    logging.debug(
                        "[%s] quiet hours active — interval *= %.1f",
                        WORKER_NAME,
                        WORKER_QUIET_HOURS_MULTIPLIER,
                    )

                now_dt = datetime.now(timezone.utc)
                route = _select_next_route(routes, now=now_dt)
                if route is None:
                    has_db_routes = await _has_configured_worker_routes(pool)
                    if has_db_routes:
                        manual_required_count = await _count_manual_login_required_routes(pool)
                        due_in_seconds = seconds_until_next_route(routes=routes, now=now_dt) if routes else None
                        cooldown_in_seconds = await _seconds_until_next_cooldown_release(pool)

                        heartbeat_status = "scheduled_wait"
                        if routes and due_in_seconds is not None:
                            waiting_reason = (
                                f"No route is due yet. Next route becomes eligible in {int(max(0, due_in_seconds))}s."
                            )
                            sleep_target_seconds = max(0.1, due_in_seconds)
                        else:
                            heartbeat_status = "cooldown"
                            if manual_required_count > 0:
                                waiting_reason = (
                                    f"{manual_required_count} route(s) are quarantined for manual login. "
                                    "Run VPS Manual Login, then clear Manual Login Required on the route."
                                )
                            else:
                                waiting_reason = (
                                    "No eligible DB routes currently available (routes may be in cooldown or waiting for proxy reuse)."
                                )
                            if cooldown_in_seconds is not None:
                                sleep_target_seconds = max(0.1, cooldown_in_seconds)
                            else:
                                sleep_target_seconds = max(1.0, effective_interval_seconds)
                        await _upsert_worker_heartbeat(
                            pool=pool,
                            route_name="",
                            status=heartbeat_status,
                            listings_saved=0,
                            query_count=0,
                            last_error=waiting_reason,
                            started_at=started_at,
                            finished_at=datetime.now(timezone.utc),
                        )
                        logging.info("[%s] %s", WORKER_NAME, waiting_reason)
                        await asyncio.sleep(
                            compute_sleep_seconds(
                                base_interval=sleep_target_seconds,
                                elapsed=0,
                                jitter_pct=SCRAPE_INTERVAL_JITTER_PCT,
                                rng=random,
                                min_sleep_seconds=0.1,
                            )
                        )
                        continue

                    route = _build_env_route()
                    effective_interval_seconds = float(SCRAPE_INTERVAL_SECONDS)
                    has_proxy = bool(str(route.get("proxy_server") or "").strip()) or bool(
                        _split_proxy_pool(route.get("proxy_pool"))
                    )
                    if not has_proxy:
                        raise RuntimeError(
                            "No worker routes configured in DB and no SCRAPER_PROXY_SERVER/SCRAPER_PROXY_POOL in environment."
                        )

                route_started_at = datetime.now(timezone.utc)
                route_started_monotonic = time.monotonic()
                await _mark_route_selected(pool, route)
                route_name = str(route.get("route_name") or "unknown")
                await _upsert_worker_heartbeat(
                    pool=pool,
                    route_name=route_name,
                    status="running",
                    listings_saved=0,
                    query_count=0,
                    last_error=None,
                    started_at=started_at,
                )

                logging.info(
                    "[%s] selected route=%s source=%s profile=%s proxy_mode=%s",
                    WORKER_NAME,
                    route_name,
                    route.get("source", "unknown"),
                    route.get("user_data_dir") or WORKER_USER_DATA_DIR or "(default)",
                    _normalize_proxy_mode(route.get("proxy_mode")),
                )
                cycle_id = str(uuid.uuid4())

                # Check proxy provider health before running the cycle.
                proxy_health = get_proxy_health()
                if proxy_health.is_saturated and not proxy_health.is_stale:
                    logging.warning(
                        "[%s/%s] proxy provider saturated (pacing=%.1fx) — skipping cycle",
                        WORKER_NAME,
                        route_name,
                        proxy_health.pacing_multiplier,
                    )
                    wait_seconds = float(WORKER_WAIT_BACKOFF_SECONDS) * proxy_health.pacing_multiplier
                    await _record_route_outcome(
                        pool, route, success=False,
                        error="Proxy provider saturated — waiting",
                        count_failure=False,
                    )
                    await _upsert_worker_heartbeat(
                        pool=pool,
                        route_name=route_name,
                        status="wait_proxy_provider",
                        listings_saved=0,
                        query_count=0,
                        last_error=f"Proxy provider saturated (pacing={proxy_health.pacing_multiplier:.1f}x)",
                        finished_at=datetime.now(timezone.utc),
                    )
                    if route.get("source") == "db":
                        await _schedule_route_next_run_at(
                            pool=pool,
                            route=route,
                            cycle_started_at=route_started_at,
                            interval_seconds=wait_seconds,
                        )
                    else:
                        await asyncio.sleep(
                            compute_sleep_seconds(
                                base_interval=wait_seconds,
                                elapsed=0,
                                jitter_pct=SCRAPE_INTERVAL_JITTER_PCT,
                                rng=random,
                                min_sleep_seconds=0.1,
                            )
                        )
                    continue

                cycle_result = await _run_scrape_cycle_with_retry(pool, redis_client, route)
                metrics = cycle_result.metrics
                route_runtime_key = _route_key(route)
                finished_at = datetime.now(timezone.utc)
                outcome = cycle_result.outcome
                error_category = cycle_result.error_category
                reason = (cycle_result.reason or "").strip()
                route_interval_seconds = _route_interval_seconds(route, fallback_seconds=effective_interval_seconds)
                if (
                    single_route_enforcement
                    and route.get("source") == "db"
                    and route.get("route_interval_seconds")
                ):
                    route_interval_seconds *= WORKER_SINGLE_ROUTE_REST_MULTIPLIER
                next_interval_seconds = route_interval_seconds * route_status_interval_multiplier(route.get("status"))

                # Apply proxy provider pacing multiplier (if provider is under load).
                if not proxy_health.is_stale and proxy_health.pacing_multiplier > 1.0:
                    next_interval_seconds *= proxy_health.pacing_multiplier
                    logging.debug(
                        "[%s/%s] proxy provider pacing applied: interval *= %.1f",
                        WORKER_NAME,
                        route_name,
                        proxy_health.pacing_multiplier,
                    )

                if outcome == CycleOutcome.NEEDS_LOGIN:
                    quarantine_reason = _manual_login_required_reason(reason)
                    _ROUTE_CONSECUTIVE_BAD_CYCLES[route_runtime_key] = 0
                    await _record_route_outcome(
                        pool,
                        route,
                        success=False,
                        error=quarantine_reason,
                        count_failure=False,
                    )
                    quarantined_at = await _mark_route_manual_login_required(
                        pool=pool,
                        route=route,
                        reason=quarantine_reason,
                        evidence=_build_quarantine_evidence(cycle_id=cycle_id, cycle_result=cycle_result),
                    )
                    heartbeat_error = quarantine_reason
                    if quarantined_at:
                        heartbeat_error = (
                            f"{quarantine_reason} Route disabled for rotation at {quarantined_at.isoformat()}."
                        )
                    await _upsert_worker_heartbeat(
                        pool=pool,
                        route_name=route_name,
                        status="manual_login_required",
                        listings_saved=metrics.get("listings_saved", 0),
                        query_count=metrics.get("query_count", 0),
                        last_error=heartbeat_error,
                        finished_at=finished_at,
                    )
                    if _should_send_manual_login_alert(route=route, reason=quarantine_reason):
                        await asyncio.to_thread(
                            _send_telegram_manual_login_required_alert,
                            route,
                            quarantine_reason,
                        )
                elif outcome in {
                    CycleOutcome.WAIT_PROXY,
                    CycleOutcome.WAIT_PROXY_MISMATCH,
                    CycleOutcome.WAIT_PROFILE_LOCK,
                    CycleOutcome.WAIT_QUERY_SHARD,
                    CycleOutcome.WAIT_SIGNAL_PAUSE,
                }:
                    current_bad_cycles = _current_bad_cycle_count(route_runtime_key, route)
                    _ROUTE_CONSECUTIVE_BAD_CYCLES[route_runtime_key] = next_bad_cycle_count(
                        current_bad_cycles,
                        outcome=outcome,
                        category=error_category,
                    )
                    wait_status: str
                    wait_reason: str
                    if outcome == CycleOutcome.WAIT_PROXY:
                        wait_status = "wait_proxy"
                        wait_reason = reason or "No proxy available right now."
                        next_interval_seconds = float(WORKER_WAIT_BACKOFF_SECONDS)
                    elif outcome == CycleOutcome.WAIT_PROXY_MISMATCH:
                        wait_status = "wait_proxy_mismatch"
                        wait_reason = reason or "Proxy IP verification mismatch detected."
                        next_interval_seconds = float(WORKER_WAIT_BACKOFF_SECONDS)
                    elif outcome == CycleOutcome.WAIT_PROFILE_LOCK:
                        wait_status = "wait_profile_lock"
                        wait_reason = reason or "Profile lock unavailable."
                        next_interval_seconds = float(WORKER_WAIT_BACKOFF_SECONDS)
                    elif outcome == CycleOutcome.WAIT_QUERY_SHARD:
                        wait_status = "wait_query_shard"
                        wait_reason = reason or "Query shard lock unavailable."
                        next_interval_seconds = float(WORKER_WAIT_BACKOFF_SECONDS)
                    else:
                        wait_status = "wait_signal_pause"
                        wait_reason = reason or (
                            f"Pre-checkpoint signal risk detected; pausing route for {SIGNAL_PAUSE_SECONDS}s."
                        )
                        next_interval_seconds = max(
                            float(SIGNAL_PAUSE_SECONDS),
                            route_interval_seconds * max(1.0, SIGNAL_THROTTLE_MULTIPLIER),
                        )
                        await _set_route_status(
                            pool=pool,
                            route=route,
                            status=RouteStatus.THROTTLED.value,
                            reason=wait_reason,
                        )
                    await _record_route_outcome(
                        pool,
                        route,
                        success=False,
                        error=wait_reason,
                        count_failure=False,
                    )
                    await _upsert_worker_heartbeat(
                        pool=pool,
                        route_name=route_name,
                        status=wait_status,
                        listings_saved=metrics.get("listings_saved", 0),
                        query_count=metrics.get("query_count", 0),
                        last_error=wait_reason,
                        finished_at=finished_at,
                    )
                elif outcome == CycleOutcome.FAIL:
                    previous_bad_cycles = _current_bad_cycle_count(route_runtime_key, route)
                    bad_cycles = next_bad_cycle_count(
                        previous_bad_cycles,
                        outcome=outcome,
                        category=error_category,
                    )
                    _ROUTE_CONSECUTIVE_BAD_CYCLES[route_runtime_key] = bad_cycles

                    base_reason = reason or "unknown scrape failure"
                    failure_counts = counts_toward_bad_cycles(outcome, error_category)
                    derived_status = derive_route_status_for_bad_cycles(
                        bad_cycles=bad_cycles,
                        degraded_after=WORKER_DEGRADED_CONSECUTIVE_CYCLES,
                        throttled_after=WORKER_THROTTLED_CONSECUTIVE_CYCLES,
                    )
                    degraded = failure_counts and derived_status in {RouteStatus.DEGRADED, RouteStatus.THROTTLED}
                    cooldown_applied = failure_counts and bad_cycles >= WORKER_ROUTE_COOLDOWN_BAD_CYCLES
                    heartbeat_status = "error"
                    heartbeat_error = base_reason

                    if failure_counts:
                        next_interval_seconds = route_interval_seconds * route_status_interval_multiplier(
                            derived_status.value
                        )
                        if derived_status == RouteStatus.THROTTLED:
                            heartbeat_status = "throttled"
                            heartbeat_error = (
                                f"{base_reason} (consecutive bad cycles={bad_cycles}; state=THROTTLED)"
                            )
                        elif derived_status == RouteStatus.DEGRADED:
                            heartbeat_status = "degraded"
                            heartbeat_error = (
                                f"{base_reason} (consecutive bad cycles={bad_cycles}; state=DEGRADED)"
                            )
                        else:
                            heartbeat_error = (
                                f"Transient scrape issue ({bad_cycles}/{WORKER_DEGRADED_CONSECUTIVE_CYCLES}) - "
                                f"{base_reason}"
                            )
                        await _set_route_status(
                            pool=pool,
                            route=route,
                            status=derived_status.value,
                            reason=base_reason if derived_status != RouteStatus.ENABLED else None,
                        )

                    await _record_route_outcome(
                        pool,
                        route,
                        success=False,
                        error=heartbeat_error,
                        count_failure=failure_counts,
                    )

                    cooldown_for_alert: int | None = None
                    if cooldown_applied:
                        cooldown_until = await _put_route_on_cooldown(
                            pool=pool,
                            route=route,
                            reason=base_reason,
                            cooldown_seconds=WORKER_ROUTE_COOLDOWN_SECONDS,
                        )
                        if cooldown_until:
                            cooldown_for_alert = WORKER_ROUTE_COOLDOWN_SECONDS
                            heartbeat_status = "cooldown"
                            heartbeat_error = (
                                f"{base_reason} Route moved to cooldown until {cooldown_until.isoformat()}."
                            )
                            _ROUTE_CONSECUTIVE_BAD_CYCLES[route_runtime_key] = 0
                            next_interval_seconds = float(WORKER_ROUTE_COOLDOWN_SECONDS)
                            logging.warning(
                                "[%s/%s] route put on cooldown for %ss after %s bad cycles",
                                WORKER_NAME,
                                route_name,
                                WORKER_ROUTE_COOLDOWN_SECONDS,
                                bad_cycles,
                            )

                    if degraded:
                        alert_key = _profile_failure_alert_key(route)
                        if _should_send_profile_failure_alert(alert_key):
                            await asyncio.to_thread(
                                _send_telegram_profile_failure_alert,
                                route,
                                base_reason,
                                metrics,
                                bad_cycles,
                                cooldown_for_alert,
                            )

                    await _upsert_worker_heartbeat(
                        pool=pool,
                        route_name=route_name,
                        status=heartbeat_status,
                        listings_saved=metrics.get("listings_saved", 0),
                        query_count=metrics.get("query_count", 0),
                        last_error=heartbeat_error,
                        finished_at=finished_at,
                    )
                else:
                    _ROUTE_CONSECUTIVE_BAD_CYCLES[route_runtime_key] = 0

                    # Session query cap tracking — force browser restart after N queries.
                    cycle_query_count = metrics.get("query_count", 0)
                    _ROUTE_SESSION_QUERY_COUNTS[route_runtime_key] = (
                        _ROUTE_SESSION_QUERY_COUNTS.get(route_runtime_key, 0) + cycle_query_count
                    )
                    if (
                        WORKER_SESSION_MAX_QUERIES is not None
                        and _ROUTE_SESSION_QUERY_COUNTS[route_runtime_key] >= WORKER_SESSION_MAX_QUERIES
                    ):
                        logging.info(
                            "[%s/%s] session query cap reached (%d >= %d) — scheduling browser restart",
                            WORKER_NAME,
                            route_name,
                            _ROUTE_SESSION_QUERY_COUNTS[route_runtime_key],
                            WORKER_SESSION_MAX_QUERIES,
                        )
                        _ROUTE_SESSION_QUERY_COUNTS[route_runtime_key] = 0
                    details = cycle_result.details or {}
                    signal_action = str(details.get("signal_action") or "").strip().lower()
                    signal_risk_score: float | None = None
                    raw_risk_score = details.get("signal_risk_score")
                    if raw_risk_score is not None:
                        try:
                            signal_risk_score = float(raw_risk_score)
                        except (TypeError, ValueError):
                            signal_risk_score = None
                    await _record_route_outcome(pool, route, success=True)
                    await _record_route_signal_baseline(
                        pool=pool,
                        route=route,
                        result_count=metrics.get("listings_scraped", 0),
                        page_load_ms=int(details.get("duration_ms") or 0),
                    )
                    heartbeat_status = "ok"
                    heartbeat_error: str | None = None
                    if signal_action == "throttle":
                        throttle_multiplier = max(
                            1.0,
                            _parse_float(
                                str(details.get("signal_throttle_multiplier") or ""),
                                default=SIGNAL_THROTTLE_MULTIPLIER,
                                minimum=1.0,
                            ),
                        )
                        throttle_duration_seconds = _parse_int(
                            str(details.get("signal_throttle_duration_seconds") or ""),
                            default=SIGNAL_THROTTLE_DURATION,
                            minimum=60,
                        )
                        next_interval_seconds = max(
                            float(throttle_duration_seconds),
                            route_interval_seconds * throttle_multiplier,
                        )
                        if signal_risk_score is None:
                            heartbeat_error = "Pre-checkpoint soft signals detected; throttling route cadence."
                        else:
                            heartbeat_error = (
                                f"Pre-checkpoint soft signals detected (risk={signal_risk_score:.2f}); "
                                "throttling route cadence."
                            )
                        heartbeat_status = "throttled"
                        await _set_route_status(
                            pool=pool,
                            route=route,
                            status=RouteStatus.THROTTLED.value,
                            reason=heartbeat_error,
                        )
                    else:
                        await _set_route_status(
                            pool=pool,
                            route=route,
                            status=RouteStatus.ENABLED.value,
                            reason=None,
                        )
                    await _upsert_worker_heartbeat(
                        pool=pool,
                        route_name=route_name,
                        status=heartbeat_status,
                        listings_saved=metrics.get("listings_saved", 0),
                        query_count=metrics.get("query_count", 0),
                        last_error=heartbeat_error,
                        finished_at=finished_at,
                    )

                _emit_cycle_telemetry(
                    cycle_id=cycle_id,
                    route=route,
                    started_at=route_started_at,
                    finished_at=finished_at,
                    result=cycle_result,
                )

                if route.get("source") == "db":
                    await _schedule_route_next_run_at(
                        pool=pool,
                        route=route,
                        cycle_started_at=route_started_at,
                        interval_seconds=next_interval_seconds,
                    )
                else:
                    elapsed = time.monotonic() - route_started_monotonic
                    sleep_seconds = compute_sleep_seconds(
                        base_interval=next_interval_seconds,
                        elapsed=elapsed,
                        jitter_pct=SCRAPE_INTERVAL_JITTER_PCT,
                        rng=random,
                        min_sleep_seconds=0.1,
                    )
                    if sleep_seconds > 0:
                        await asyncio.sleep(sleep_seconds)
            except Exception as exc:
                logging.exception("[%s] scrape cycle failed: %s", WORKER_NAME, exc)
                route_name = str((route or {}).get("route_name") or "unknown")
                error_reason = str(exc) or "unknown scrape error"
                await _record_route_outcome(pool, route or {}, success=False, error=str(exc))
                await _upsert_worker_heartbeat(
                    pool=pool,
                    route_name=route_name,
                    status="error",
                    listings_saved=0,
                    query_count=0,
                    last_error=error_reason,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                )
                _emit_cycle_telemetry(
                    cycle_id=str(uuid.uuid4()),
                    route=route or {"route_name": route_name},
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    result=CycleResult(
                        metrics={
                            "listings_saved": 0,
                            "listings_scraped": 0,
                            "query_count": 0,
                            "query_result_count": 0,
                            "zero_page_queries": 0,
                            "max_page_cards": 0,
                        },
                        outcome=CycleOutcome.FAIL,
                        reason=error_reason,
                        error_category=classify_error(error_reason),
                        retry_count=0,
                    ),
                )
                elapsed = time.monotonic() - cycle_started_monotonic
                sleep_seconds = compute_sleep_seconds(
                    base_interval=SCRAPE_INTERVAL_SECONDS,
                    elapsed=elapsed,
                    jitter_pct=SCRAPE_INTERVAL_JITTER_PCT,
                    rng=random,
                    min_sleep_seconds=0.1,
                )
                if sleep_seconds > 0:
                    await asyncio.sleep(sleep_seconds)
    finally:
        await redis_client.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
