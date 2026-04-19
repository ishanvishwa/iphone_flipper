from __future__ import annotations

import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from server.services.worker.runtime import ErrorCategory, classify_error


@dataclass(frozen=True)
class WarmSessionConfig:
    max_queries: int
    max_age_seconds: int
    idle_close_seconds: int
    min_pause_seconds: float
    max_pause_seconds: float


@dataclass(frozen=True)
class ListingIdOverlapSummary:
    baseline_ids: tuple[str, ...]
    retry_ids: tuple[str, ...]
    overlap_ids: tuple[str, ...]
    baseline_only_ids: tuple[str, ...]
    retry_only_ids: tuple[str, ...]
    overlap_ratio: float
    identical: bool


class FamilyClaimOutcome(str, Enum):
    MATCHES = "matches"
    STALE_FEED = "stale_feed"
    EMPTY_FEED = "empty_feed"
    DOM_CHANGED = "dom_changed"
    CHECKPOINT = "checkpoint"
    INFRASTRUCTURE_ERROR = "infrastructure_error"


def normalize_warm_session_config(
    *,
    max_queries: int,
    max_age_seconds: int,
    idle_close_seconds: int,
    min_pause_seconds: float,
    max_pause_seconds: float,
) -> WarmSessionConfig:
    normalized_min_pause = max(0.0, float(min_pause_seconds or 0.0))
    normalized_max_pause = max(normalized_min_pause, float(max_pause_seconds or normalized_min_pause))
    return WarmSessionConfig(
        max_queries=max(1, int(max_queries or 1)),
        max_age_seconds=max(1, int(max_age_seconds or 1)),
        idle_close_seconds=max(1, int(idle_close_seconds or 1)),
        min_pause_seconds=normalized_min_pause,
        max_pause_seconds=normalized_max_pause,
    )


def next_pause_seconds(
    *,
    min_pause_seconds: float,
    max_pause_seconds: float,
    rng: Any = random,
) -> float:
    minimum = max(0.0, float(min_pause_seconds or 0.0))
    maximum = max(minimum, float(max_pause_seconds or minimum))
    if maximum <= minimum:
        return minimum
    return float(rng.uniform(minimum, maximum))


def build_marketplace_search_url(query: str) -> str:
    query_text = str(query or "").strip()
    if query_text.lower().startswith(("http://", "https://")):
        return query_text
    encoded_query = quote(query_text, safe="")
    return (
        "https://www.facebook.com/marketplace/perth/search?"
        f"query={encoded_query}&exact=false&sortBy=creation_time_descend"
    )


def build_cache_busted_search_urls(
    *,
    search_queries: Sequence[str] | None,
    search_urls: Sequence[str] | None,
    cache_bust_token: str,
    cache_bust_param: str = "__cb",
) -> list[str]:
    token = str(cache_bust_token or "").strip()
    param_name = str(cache_bust_param or "").strip() or "__cb"
    if not token:
        return []

    provided_queries = [str(item or "").strip() for item in (search_queries or ())]
    provided_urls = [str(item or "").strip() for item in (search_urls or ())]
    target_count = max(len(provided_queries), len(provided_urls))
    if target_count <= 0:
        return []

    busted_urls: list[str] = []
    for index in range(target_count):
        query_text = provided_queries[index] if index < len(provided_queries) else ""
        base_url = provided_urls[index] if index < len(provided_urls) else ""
        navigation_url = base_url or build_marketplace_search_url(query_text)
        if not navigation_url:
            continue
        split_url = urlsplit(navigation_url)
        query_items = [
            (key, value)
            for key, value in parse_qsl(split_url.query, keep_blank_values=True)
            if key != param_name
        ]
        query_items.append((param_name, f"{token}-{index + 1}"))
        busted_urls.append(
            urlunsplit(
                (
                    split_url.scheme,
                    split_url.netloc,
                    split_url.path,
                    urlencode(query_items, doseq=True),
                    split_url.fragment,
                )
            )
        )
    return busted_urls


def _normalize_listing_ids(listing_ids: Sequence[str] | None) -> tuple[str, ...]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw_listing_id in listing_ids or ():
        listing_id = str(raw_listing_id or "").strip()
        if not listing_id or listing_id in seen:
            continue
        seen.add(listing_id)
        normalized.append(listing_id)
    return tuple(normalized)


def summarize_listing_id_overlap(
    *,
    baseline_listing_ids: Sequence[str] | None,
    retry_listing_ids: Sequence[str] | None,
) -> ListingIdOverlapSummary:
    baseline_ids = _normalize_listing_ids(baseline_listing_ids)
    retry_ids = _normalize_listing_ids(retry_listing_ids)
    baseline_set = set(baseline_ids)
    retry_set = set(retry_ids)
    overlap_ids = tuple(listing_id for listing_id in baseline_ids if listing_id in retry_set)
    baseline_only_ids = tuple(listing_id for listing_id in baseline_ids if listing_id not in retry_set)
    retry_only_ids = tuple(listing_id for listing_id in retry_ids if listing_id not in baseline_set)
    union_count = len(baseline_set | retry_set)
    overlap_ratio = 1.0 if union_count <= 0 else len(overlap_ids) / union_count
    return ListingIdOverlapSummary(
        baseline_ids=baseline_ids,
        retry_ids=retry_ids,
        overlap_ids=overlap_ids,
        baseline_only_ids=baseline_only_ids,
        retry_only_ids=retry_only_ids,
        overlap_ratio=overlap_ratio,
        identical=baseline_ids == retry_ids,
    )


def evaluate_warm_session_state(
    *,
    started_at: datetime,
    last_activity_at: datetime,
    executed_claims: int,
    config: WarmSessionConfig,
    now: datetime | None = None,
) -> str | None:
    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=timezone.utc)
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    if last_activity_at.tzinfo is None:
        last_activity_at = last_activity_at.replace(tzinfo=timezone.utc)

    if int(executed_claims or 0) >= config.max_queries:
        return "query_budget"
    if (current_time - started_at).total_seconds() >= config.max_age_seconds:
        return "session_age"
    if (current_time - last_activity_at).total_seconds() >= config.idle_close_seconds:
        return "idle_close"
    return None


def classify_family_claim(
    *,
    listings_saved: int,
    listings_scraped: int,
    error_text: str | None,
    final_url: str | None,
    feed_present: bool,
    empty_state_detected: bool,
    error_category: ErrorCategory | None = None,
) -> FamilyClaimOutcome:
    category = error_category or classify_error(error_text)
    final_url_text = str(final_url or "").strip().lower()
    if category == ErrorCategory.AUTH_REQUIRED or "checkpoint" in final_url_text:
        return FamilyClaimOutcome.CHECKPOINT
    if "/login" in final_url_text and "marketplace" not in final_url_text:
        return FamilyClaimOutcome.CHECKPOINT

    if max(0, int(listings_saved or 0)) > 0:
        return FamilyClaimOutcome.MATCHES
    if max(0, int(listings_scraped or 0)) > 0:
        return FamilyClaimOutcome.STALE_FEED

    if error_text:
        if category == ErrorCategory.PARSE_FAILED:
            return FamilyClaimOutcome.DOM_CHANGED
        return FamilyClaimOutcome.INFRASTRUCTURE_ERROR

    if feed_present or empty_state_detected:
        return FamilyClaimOutcome.EMPTY_FEED
    return FamilyClaimOutcome.DOM_CHANGED


def family_claim_succeeded(outcome: FamilyClaimOutcome) -> bool:
    return outcome in {
        FamilyClaimOutcome.MATCHES,
        FamilyClaimOutcome.STALE_FEED,
        FamilyClaimOutcome.EMPTY_FEED,
    }


def next_variant_cursor(
    *,
    variant_count: int,
    current_cursor: int,
    outcome: FamilyClaimOutcome,
) -> int:
    safe_variant_count = max(0, int(variant_count or 0))
    if safe_variant_count <= 1:
        return 0 if safe_variant_count == 1 else max(0, int(current_cursor or 0))
    cursor = max(0, int(current_cursor or 0))
    if outcome not in {
        FamilyClaimOutcome.MATCHES,
        FamilyClaimOutcome.STALE_FEED,
        FamilyClaimOutcome.EMPTY_FEED,
    }:
        return min(cursor, safe_variant_count - 1)
    return (cursor + 1) % safe_variant_count


def next_family_due_seconds(
    *,
    family: dict[str, Any],
    outcome: FamilyClaimOutcome,
    listings_saved: int,
    consecutive_hits: int,
    consecutive_stale: int,
    consecutive_empty: int,
    consecutive_failures: int = 0,
    dom_backoff_seconds: int = 300,
    infra_backoff_seconds: int = 60,
) -> int:
    min_gap = max(0, int(family.get("min_gap_s") or 0))
    safe_min_gap = max(1, min_gap or 1)
    raw_max_gap = family.get("max_gap_s")
    if raw_max_gap is None:
        max_gap = max(safe_min_gap, max(dom_backoff_seconds, infra_backoff_seconds, safe_min_gap * 8))
    else:
        max_gap = max(safe_min_gap, int(raw_max_gap or safe_min_gap))

    if outcome == FamilyClaimOutcome.MATCHES:
        _ = consecutive_hits
        _ = listings_saved
        return min_gap
    if outcome == FamilyClaimOutcome.STALE_FEED:
        streak = max(1, int(consecutive_stale or 1))
        return min(max_gap, max(min_gap, safe_min_gap * (2 ** min(streak, 4))))
    if outcome == FamilyClaimOutcome.EMPTY_FEED:
        streak = max(1, int(consecutive_empty or 1))
        return min(max_gap, max(min_gap, safe_min_gap * (2 ** min(streak - 1, 4))))
    if outcome == FamilyClaimOutcome.CHECKPOINT:
        return min_gap
    if outcome == FamilyClaimOutcome.DOM_CHANGED:
        return min(max_gap, max(min_gap, int(dom_backoff_seconds or max_gap)))

    failure_streak = max(1, int(consecutive_failures or 1))
    return min(
        max_gap,
        max(min_gap, max(int(infra_backoff_seconds or safe_min_gap), safe_min_gap * (2 ** min(failure_streak - 1, 3)))),
    )


def should_abort_warm_session(outcome: FamilyClaimOutcome) -> bool:
    return outcome in {
        FamilyClaimOutcome.CHECKPOINT,
        FamilyClaimOutcome.DOM_CHANGED,
        FamilyClaimOutcome.INFRASTRUCTURE_ERROR,
    }


def worker_rollout_enabled(
    worker_name: str | None,
    rollout_allowlist: Sequence[str] | None,
) -> bool:
    normalized_worker_name = str(worker_name or "").strip().lower()
    normalized_allowlist = {
        str(item or "").strip().lower()
        for item in (rollout_allowlist or ())
        if str(item or "").strip()
    }
    if not normalized_allowlist:
        return True
    return normalized_worker_name in normalized_allowlist


def scrub_orphaned_chromium_locks(user_data_dir: str | None) -> list[str]:
    profile_dir = Path(str(user_data_dir or "").strip())
    if not str(profile_dir):
        return []

    removed: list[str] = []
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket", "DevToolsActivePort"):
        candidate = profile_dir / name
        try:
            if candidate.exists():
                os.remove(candidate)
                removed.append(str(candidate))
        except FileNotFoundError:
            continue
    return removed
