"""
Scraper core pipeline: marketplace scrape orchestration, accessory purging,
and listing financial recalculation.
"""

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote

from scraper.browser import (
    BrowserSessionError,
    BrowserSessionLostError,
    is_benign_browser_shutdown_error,
    is_browser_session_error,
    launch_browser_context,
    random_delay,
    stop_dolphin_profile,
)
from scraper.config import DB_PATH
from scraper.parsers import (
    assess_condition,
    calculate_max_offer,
    calculate_profit_for_listing,
    extract_currency_price_from_text,
    extract_shorthand_k_price_from_text,
    identify_model,
    is_accessory_only_listing,
    resolve_price_model_key,
)
from scraper.storage import (
    init_db,
    load_active_search_queries,
    load_price_list,
    load_runtime_scraper_settings,
    mark_search_query_polled,
)


# ---------------------------------------------------------------------------
# Main scraping pipeline
# ---------------------------------------------------------------------------


@dataclass
class MarketplaceSession:
    profile_id: str
    playwright: Any
    browser: Any
    context: Any
    page: Any
    conn: sqlite3.Connection
    cursor: sqlite3.Cursor
    price_data: Dict[str, Any]
    runtime_settings: Dict[str, Any]
    close_browser_explicitly: bool = True
    stop_profile_on_close: bool = True
    launch_mode: str = "unknown"


@dataclass
class MarketplaceClaimExecution:
    listings: List[Dict[str, Any]]
    query_diagnostics: List[Dict[str, Any]]
    cancelled: bool
    accessory_removed: int = 0


MARKETPLACE_FEED_SELECTORS: tuple[str, ...] = (
    '[data-testid="marketplace_feed_item"]',
    '[role="feed"]',
    '[data-pagelet*="BrowseFeed"]',
    'a[href*="/marketplace/item/"]',
)

MARKETPLACE_EMPTY_STATE_MARKERS: tuple[str, ...] = (
    "no listings found",
    "no results found",
    "try a different search",
    "we couldn't find anything",
    "no matches found",
    "there are no products matching your search",
    "sorry, this content isn't available right now",
)


def _resolve_active_queries(
    runtime_settings: Dict[str, Any],
    search_queries: Optional[List[str]] = None,
) -> List[str]:
    explicit_queries = [q.strip() for q in (search_queries or []) if str(q).strip()]
    active_queries = explicit_queries if explicit_queries else load_active_search_queries(
        max_queries=runtime_settings.get("max_queries_per_run", 5)
    )
    if not active_queries:
        from scraper.config import SEARCH_QUERIES

        active_queries = SEARCH_QUERIES[: runtime_settings.get("max_queries_per_run", 5)]
    return active_queries


def _default_search_url(query: str) -> str:
    query_text = str(query or "").strip()
    if query_text.lower().startswith(("http://", "https://")):
        return query_text
    encoded_query = quote(query_text, safe="")
    return (
        "https://www.facebook.com/marketplace/perth/search?"
        f"query={encoded_query}&exact=false&sortBy=creation_time_descend"
    )


def _resolve_query_targets(
    runtime_settings: Dict[str, Any],
    *,
    search_queries: Optional[List[str]] = None,
    search_urls: Optional[List[str]] = None,
) -> List[Tuple[str, str]]:
    explicit_queries = [str(q).strip() for q in (search_queries or []) if str(q).strip()]
    explicit_urls = [str(url).strip() for url in (search_urls or []) if str(url).strip()]
    if explicit_urls and not explicit_queries:
        explicit_queries = list(explicit_urls)
    active_queries = _resolve_active_queries(runtime_settings, explicit_queries)
    targets: List[Tuple[str, str]] = []
    for index, query in enumerate(active_queries):
        if index < len(explicit_urls):
            navigation_url = explicit_urls[index]
        else:
            navigation_url = _default_search_url(query)
        targets.append((query, navigation_url))
    return targets


def _resolve_scroll_settings(
    runtime_settings: Dict[str, Any],
    scroll_target_cards_override: Optional[int] = None,
    scroll_max_rounds_override: Optional[int] = None,
) -> tuple[int, int]:
    scroll_target_cards = runtime_settings.get("scrape_scroll_target_cards", 180)
    scroll_max_rounds = runtime_settings.get("scrape_scroll_max_rounds", 14)
    if scroll_target_cards_override is not None:
        try:
            scroll_target_cards = max(20, int(scroll_target_cards_override))
        except (TypeError, ValueError):
            pass
    if scroll_max_rounds_override is not None:
        try:
            scroll_max_rounds = max(2, int(scroll_max_rounds_override))
        except (TypeError, ValueError):
            pass
    return int(scroll_target_cards), int(scroll_max_rounds)


async def _inspect_marketplace_results_surface(page) -> Dict[str, Any]:
    try:
        payload = await page.evaluate(
            """
            () => {
                const href = String(window.location.href || "");
                const path = String(window.location.pathname || "");
                const text = String(document.body?.innerText || "").toLowerCase();
                const emptyMarkers = [
                    "no listings found",
                    "no results found",
                    "try a different search",
                    "we couldn't find anything",
                    "no matches found",
                    "there are no products matching your search",
                    "sorry, this content isn't available right now",
                ];
                const emptyStateDetected = emptyMarkers.some((marker) => text.includes(marker));
                const feedSurfaceSelectors = [
                    '[data-testid="marketplace_feed_item"]',
                    '[role="feed"]',
                    '[data-pagelet*="BrowseFeed"]',
                    'a[href*="/marketplace/item/"]',
                ];
                const feedSurfacePresent = feedSurfaceSelectors.some((selector) => {
                    try {
                        return Boolean(document.querySelector(selector));
                    } catch (error) {
                        return false;
                    }
                });
                return {
                    final_url: href,
                    marketplace_shell_detected: path.includes("/marketplace"),
                    feed_present: Boolean(feedSurfacePresent || emptyStateDetected),
                    empty_state_detected: emptyStateDetected,
                };
            }
            """
        )
    except Exception:
        payload = None
    if not isinstance(payload, dict):
        return {
            "final_url": "",
            "marketplace_shell_detected": False,
            "feed_present": False,
            "empty_state_detected": False,
        }
    return {
        "final_url": str(payload.get("final_url") or "").strip(),
        "marketplace_shell_detected": bool(payload.get("marketplace_shell_detected")),
        "feed_present": bool(payload.get("feed_present")),
        "empty_state_detected": bool(payload.get("empty_state_detected")),
    }


async def _wait_for_marketplace_results_surface(
    page: Any,
    *,
    timeout_ms: int = 20000,
) -> Dict[str, Any]:
    safe_timeout_ms = max(1000, int(timeout_ms or 0))
    networkidle_reached = False
    wait_error: str | None = None

    try:
        await page.wait_for_load_state("networkidle", timeout=min(safe_timeout_ms, 15000))
        networkidle_reached = True
    except Exception as exc:
        wait_error = str(exc) or exc.__class__.__name__

    try:
        await page.wait_for_function(
            """
            ({ feedSelectors, emptyMarkers }) => {
                const href = String(window.location.href || "").toLowerCase();
                if (href.includes("checkpoint")) {
                    return true;
                }
                if (href.includes("/login") && !href.includes("/marketplace")) {
                    return true;
                }

                const text = String(document.body?.innerText || "").toLowerCase();
                const hasEmptyState = emptyMarkers.some((marker) => text.includes(marker));
                if (hasEmptyState) {
                    return true;
                }

                return feedSelectors.some((selector) => {
                    try {
                        return Boolean(document.querySelector(selector));
                    } catch (error) {
                        return false;
                    }
                });
            }
            """,
            {
                "feedSelectors": list(MARKETPLACE_FEED_SELECTORS),
                "emptyMarkers": list(MARKETPLACE_EMPTY_STATE_MARKERS),
            },
            timeout=safe_timeout_ms,
        )
    except Exception as exc:
        if wait_error is None:
            wait_error = str(exc) or exc.__class__.__name__

    from scraper.legacy_utils import _detect_manual_login_required_state

    surface = await _inspect_marketplace_results_surface(page)
    return {
        "networkidle_reached": networkidle_reached,
        "wait_error": wait_error,
        "manual_login": await _detect_manual_login_required_state(page),
        **surface,
    }


async def open_profile_session(
    profile_id: str | None = None,
    *,
    user_data_dir: str | None = None,
    headless: bool = False,
) -> MarketplaceSession:
    init_db()
    price_data = load_price_list()
    runtime_settings = load_runtime_scraper_settings()
    runtime_identity = str(profile_id or user_data_dir or "").strip()
    if not runtime_identity:
        raise ValueError("A Dolphin profile_id or user_data_dir is required.")
    launch_result = await launch_browser_context(
        headless=headless,
        profile_id=profile_id,
        user_data_dir=user_data_dir,
    )
    if hasattr(launch_result, "playwright"):
        playwright = launch_result.playwright
        browser = launch_result.browser
        context = launch_result.context
        page = launch_result.page
        launch_mode = str(getattr(launch_result, "launch_mode", "unknown") or "unknown")
    else:
        playwright, browser, context, page = launch_result
        launch_mode = "unknown"
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    return MarketplaceSession(
        profile_id=runtime_identity,
        playwright=playwright,
        browser=browser,
        context=context,
        page=page,
        conn=conn,
        cursor=cursor,
        price_data=price_data,
        runtime_settings=runtime_settings,
        close_browser_explicitly=profile_id is not None,
        stop_profile_on_close=profile_id is not None,
        launch_mode=launch_mode,
    )


async def close_profile_session(
    session: MarketplaceSession,
    *,
    stop_profile: bool | None = None,
) -> None:
    errors: list[BaseException] = []
    try:
        session.conn.close()
    except Exception as exc:
        errors.append(exc)

    try:
        await session.context.close()
    except Exception as exc:
        if not is_benign_browser_shutdown_error(exc):
            errors.append(exc)
    if session.close_browser_explicitly and session.browser is not None:
        try:
            await session.browser.close()
        except Exception as exc:
            if not is_benign_browser_shutdown_error(exc):
                errors.append(exc)
    try:
        await session.playwright.stop()
    except Exception as exc:
        if not is_benign_browser_shutdown_error(exc):
            errors.append(exc)

    effective_stop_profile = session.stop_profile_on_close if stop_profile is None else bool(stop_profile)
    if effective_stop_profile:
        try:
            await stop_dolphin_profile(session.profile_id)
        except Exception as exc:
            errors.append(exc)

    if errors:
        raise RuntimeError("; ".join(str(error) for error in errors if str(error)))


async def _execute_queries_on_page(
    session: MarketplaceSession,
    page: Any,
    *,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    stop_event: Any = None,
    search_queries: Optional[List[str]] = None,
    search_urls: Optional[List[str]] = None,
    scroll_target_cards_override: Optional[int] = None,
    scroll_max_rounds_override: Optional[int] = None,
    apply_inter_query_delay: bool = True,
) -> List[Dict[str, Any]]:
    runtime_settings = load_runtime_scraper_settings()
    session.runtime_settings = runtime_settings
    query_targets = _resolve_query_targets(
        runtime_settings,
        search_queries=search_queries,
        search_urls=search_urls,
    )
    scroll_target_cards, scroll_max_rounds = _resolve_scroll_settings(
        runtime_settings,
        scroll_target_cards_override=scroll_target_cards_override,
        scroll_max_rounds_override=scroll_max_rounds_override,
    )

    def emit_progress(event: str, **payload):
        if not progress_callback:
            return
        try:
            progress_callback({"event": event, **payload})
        except Exception:
            pass

    processed_listings: List[Dict[str, Any]] = []
    seen_listing_ids = set()
    new_listings_count = 0
    total_queries = len(query_targets)
    processed_queries = 0
    cancelled = False
    manual_login_error = None
    conn = session.conn
    cursor = session.cursor
    price_data = session.price_data
    query_diagnostics: List[Dict[str, Any]] = []
    listing_discovery_ts: Dict[str, str] = {}

    def _mark_discovery_ts(listing_id: str) -> str:
        discovery_ts = listing_discovery_ts.get(listing_id)
        if discovery_ts:
            return discovery_ts
        discovery_ts = datetime.now(timezone.utc).isoformat()
        listing_discovery_ts[listing_id] = discovery_ts
        return discovery_ts

    for index, (query, search_url) in enumerate(query_targets, start=1):
        if stop_event and stop_event.is_set():
            cancelled = True
            emit_progress("cancelled", query_index=processed_queries, query_total=total_queries)
            break

        emit_progress("query_start", query=query, query_index=index, query_total=total_queries)
        print(f"  Searching for: {query}")

        from scraper.legacy_utils import (
            _detect_manual_login_required_state,
            _extract_dom_listing_candidates,
            _extract_text_value,
            _normalize_marketplace_url,
            _store_listing_candidate,
            decode_json_body,
            extract_listings_from_graphql_payload,
            merge_listing_candidates,
            progressive_marketplace_scroll,
        )
        from scraper.config import MANUAL_LOGIN_REQUIRED_PREFIX

        graphql_candidates = {}
        graphql_tasks = set()

        async def consume_graphql_response(response):
            try:
                request = response.request
                if request.method != "POST" or "/api/graphql/" not in response.url:
                    return
                payload = decode_json_body(await response.text())
                if not payload:
                    return
                listings_from_payload = extract_listings_from_graphql_payload(payload)
                for extracted in listings_from_payload:
                    listing_id = extracted.get("id")
                    if not listing_id:
                        continue
                    extracted["discovery_ts"] = _mark_discovery_ts(str(listing_id))
                    existing = graphql_candidates.get(listing_id)
                    if not existing:
                        graphql_candidates[listing_id] = extracted
                    else:
                        merged = merge_listing_candidates([existing, extracted])
                        if merged:
                            graphql_candidates[listing_id] = merged[0]
            except Exception:
                return

        def on_response(response):
            if "/api/graphql/" not in response.url:
                return
            task = asyncio.create_task(consume_graphql_response(response))
            graphql_tasks.add(task)
            task.add_done_callback(lambda done_task: graphql_tasks.discard(done_task))

        page.on("response", on_response)

        try:
            await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            page_wait = await _wait_for_marketplace_results_surface(page)
            await random_delay(0.8, 1.8)

            checkpoint_reason = await _detect_manual_login_required_state(page)
            if checkpoint_reason:
                raise RuntimeError(f"{MANUAL_LOGIN_REQUIRED_PREFIX} {checkpoint_reason}")

            dom_candidates_by_id = {}

            def _dom_candidate_score(candidate: Dict[str, Any]) -> int:
                title = str(candidate.get("title") or "")
                score = 0
                if title:
                    score += 1
                if "iphone" in title.lower():
                    score += 5
                if candidate.get("price"):
                    score += 3
                if len(title) > 15:
                    score += 1
                if candidate.get("url"):
                    score += 1
                return score

            async def capture_dom_snapshot() -> None:
                dom_snapshot = await _extract_dom_listing_candidates(page)
                for listing in dom_snapshot:
                    listing_id = _extract_text_value(listing.get("id"))
                    if not listing_id:
                        continue
                    candidate: Dict[str, Any] = {
                        "id": listing_id,
                        "title": _extract_text_value(listing.get("title")),
                        "url": _normalize_marketplace_url(listing.get("url"), listing_id),
                        "price": listing.get("price"),
                        "location": "",
                        "description": "",
                        "seller_name": "",
                        "discovery_ts": _mark_discovery_ts(listing_id),
                    }
                    candidate_score = _dom_candidate_score(candidate)
                    existing = dom_candidates_by_id.get(listing_id)
                    if not existing or candidate_score > int(existing.get("_score", 0)):
                        candidate["_score"] = candidate_score
                        dom_candidates_by_id[listing_id] = candidate

            scroll_stats = await progressive_marketplace_scroll(
                page,
                target_cards=scroll_target_cards,
                max_rounds=scroll_max_rounds,
                snapshot_callback=capture_dom_snapshot,
            )
            await random_delay(0.8, 1.8)

            checkpoint_reason = await _detect_manual_login_required_state(page)
            if checkpoint_reason:
                raise RuntimeError(f"{MANUAL_LOGIN_REQUIRED_PREFIX} {checkpoint_reason}")
            await capture_dom_snapshot()

            if graphql_tasks:
                await asyncio.wait(list(graphql_tasks), timeout=8)

            normalized_dom_listings = []
            for listing in dom_candidates_by_id.values():
                payload = dict(listing)
                payload.pop("_score", None)
                normalized_dom_listings.append(payload)

            query_listings = merge_listing_candidates(
                list(graphql_candidates.values()) + normalized_dom_listings
            )
            query_new_saved = 0
            for listing in query_listings:
                listing_id = _extract_text_value(listing.get("id"))
                if not listing_id or listing_id in seen_listing_ids:
                    continue
                seen_listing_ids.add(listing_id)
                listing["discovery_ts"] = (
                    _extract_text_value(listing.get("discovery_ts"))
                    or listing_discovery_ts.get(listing_id)
                    or _mark_discovery_ts(listing_id)
                )

                saved_listing = _store_listing_candidate(cursor, listing, price_data)
                if not saved_listing:
                    continue
                saved_listing["discovery_ts"] = listing["discovery_ts"]

                conn.commit()
                new_listings_count += 1
                query_new_saved += 1
                processed_listings.append(saved_listing)
                emit_progress(
                    "listing_saved",
                    query=query,
                    query_index=index,
                    query_total=total_queries,
                    new_count=new_listings_count,
                    discovery_ts=listing["discovery_ts"],
                    listing=saved_listing,
                )

            found_count = len(query_listings)
            graphql_count = len(graphql_candidates)
            dom_count = len(normalized_dom_listings)
            page_state = await _inspect_marketplace_results_surface(page)
            page_state["networkidle_reached"] = bool(page_wait.get("networkidle_reached"))
            page_state["surface_wait_error"] = page_wait.get("wait_error")
            emit_progress(
                "query_result",
                query=query,
                query_index=index,
                query_total=total_queries,
                found=found_count,
                graphql_found=graphql_count,
                dom_found=dom_count,
                page_cards=scroll_stats.get("visible_cards", 0),
                scroll_rounds=scroll_stats.get("scroll_rounds", 0),
                new_saved=query_new_saved,
                final_url=page_state["final_url"],
                marketplace_shell_detected=page_state["marketplace_shell_detected"],
                feed_present=page_state["feed_present"],
                empty_state_detected=page_state["empty_state_detected"],
            )
            query_diagnostics.append(
                {
                    "query": query,
                    "query_index": index,
                    "found": found_count,
                    "graphql_found": graphql_count,
                    "dom_found": dom_count,
                    "page_cards": int(scroll_stats.get("visible_cards", 0) or 0),
                    "scroll_rounds": int(scroll_stats.get("scroll_rounds", 0) or 0),
                    "new_saved": query_new_saved,
                    **page_state,
                }
            )

        except Exception as e:
            error_text = str(e)
            print(f"  Error scraping query '{query}': {error_text}")
            page_state = await _inspect_marketplace_results_surface(page)
            page_state["networkidle_reached"] = bool(page_wait.get("networkidle_reached")) if "page_wait" in locals() else False
            page_state["surface_wait_error"] = page_wait.get("wait_error") if "page_wait" in locals() else None
            emit_progress(
                "query_error",
                query=query,
                query_index=index,
                query_total=total_queries,
                error=error_text,
                final_url=page_state["final_url"],
                marketplace_shell_detected=page_state["marketplace_shell_detected"],
                feed_present=page_state["feed_present"],
                empty_state_detected=page_state["empty_state_detected"],
            )
            query_diagnostics.append(
                {
                    "query": query,
                    "query_index": index,
                    "error": error_text,
                    **page_state,
                }
            )
            if MANUAL_LOGIN_REQUIRED_PREFIX.lower() in error_text.lower():
                manual_login_error = error_text
            if is_browser_session_error(error_text):
                raise BrowserSessionLostError(
                    f"Browser session lost while executing query '{query}': {error_text}",
                    failure_stage="in_query",
                    launch_mode=session.launch_mode,
                    details={
                        "runtime_identity": session.profile_id,
                        "query": query,
                        "query_index": index,
                        "query_total": total_queries,
                    },
                ) from e
        finally:
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass
            mark_search_query_polled(query)

        processed_queries = index
        if manual_login_error:
            break

        if stop_event and stop_event.is_set():
            cancelled = True
            emit_progress("cancelled", query_index=processed_queries, query_total=total_queries)
            break

        if apply_inter_query_delay:
            await random_delay(
                runtime_settings.get("scrape_delay_min_seconds", 3),
                runtime_settings.get("scrape_delay_max_seconds", 6),
            )

    if manual_login_error:
        raise RuntimeError(manual_login_error)

    removed_accessory_count = purge_accessory_only_listings(cursor)
    if removed_accessory_count > 0:
        conn.commit()
        emit_progress("accessory_cleanup", removed=removed_accessory_count)

    emit_progress(
        "completed",
        new_count=new_listings_count,
        query_index=processed_queries,
        query_total=total_queries,
        cancelled=cancelled,
    )
    return MarketplaceClaimExecution(
        listings=processed_listings,
        query_diagnostics=query_diagnostics,
        cancelled=cancelled,
        accessory_removed=removed_accessory_count,
    )


async def execute_session_queries(
    session: MarketplaceSession,
    *,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    stop_event: Any = None,
    search_queries: Optional[List[str]] = None,
    search_urls: Optional[List[str]] = None,
    scroll_target_cards_override: Optional[int] = None,
    scroll_max_rounds_override: Optional[int] = None,
    apply_inter_query_delay: bool = True,
) -> List[Dict[str, Any]]:
    execution = await _execute_queries_on_page(
        session,
        session.page,
        progress_callback=progress_callback,
        stop_event=stop_event,
        search_queries=search_queries,
        search_urls=search_urls,
        scroll_target_cards_override=scroll_target_cards_override,
        scroll_max_rounds_override=scroll_max_rounds_override,
        apply_inter_query_delay=apply_inter_query_delay,
    )
    return execution.listings


async def execute_family_claim(
    session: MarketplaceSession,
    *,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    stop_event: Any = None,
    search_queries: Optional[List[str]] = None,
    search_urls: Optional[List[str]] = None,
    scroll_target_cards_override: Optional[int] = None,
    scroll_max_rounds_override: Optional[int] = None,
    apply_inter_query_delay: bool = False,
) -> MarketplaceClaimExecution:
    try:
        page = await session.context.new_page()
    except Exception as exc:
        if is_browser_session_error(exc):
            raise BrowserSessionLostError(
                f"Browser session lost before opening a fresh family page: {exc}",
                failure_stage="new_page",
                launch_mode=session.launch_mode,
                details={"runtime_identity": session.profile_id},
            ) from exc
        raise
    try:
        return await _execute_queries_on_page(
            session,
            page,
            progress_callback=progress_callback,
            stop_event=stop_event,
            search_queries=search_queries,
            search_urls=search_urls,
            scroll_target_cards_override=scroll_target_cards_override,
            scroll_max_rounds_override=scroll_max_rounds_override,
            apply_inter_query_delay=apply_inter_query_delay,
        )
    finally:
        try:
            await page.close()
        except Exception:
            pass


async def scrape_marketplace(
    profile_id: str,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    stop_event: Any = None,
    search_queries: Optional[List[str]] = None,
    scroll_target_cards_override: Optional[int] = None,
    scroll_max_rounds_override: Optional[int] = None,
    raise_browser_errors: bool = False,
) -> List[Dict[str, Any]]:
    """Run the main Facebook Marketplace scraping pipeline."""
    session: MarketplaceSession | None = None
    try:
        session = await open_profile_session(profile_id, headless=False)
        if progress_callback:
            try:
                progress_callback(
                    {
                        "event": "browser_session_ready",
                        "launch_mode": session.launch_mode,
                        "runtime_identity": session.profile_id,
                    }
                )
            except Exception:
                pass
        return await execute_session_queries(
            session,
            progress_callback=progress_callback,
            stop_event=stop_event,
            search_queries=search_queries,
            scroll_target_cards_override=scroll_target_cards_override,
            scroll_max_rounds_override=scroll_max_rounds_override,
        )
    except BrowserSessionError as e:
        if progress_callback:
            try:
                progress_callback(
                    {
                        "event": "browser_session_error",
                        "error": str(e),
                        "launch_mode": e.launch_mode,
                        "failure_stage": e.failure_stage,
                        **(e.details or {}),
                    }
                )
            except Exception:
                pass
        print(f"Scraper pipeline failed: {e}")
        if raise_browser_errors:
            raise
        return []
    except Exception as e:
        print(f"Scraper pipeline failed: {e}")
        return []
    finally:
        if session is not None:
            try:
                await close_profile_session(session)
            except Exception:
                pass



# ---------------------------------------------------------------------------
# Accessory purge
# ---------------------------------------------------------------------------


def purge_accessory_only_listings(
    cursor: sqlite3.Cursor,
    statuses: Optional[set[str]] = None,
) -> int:
    """Delete accessory-only rows for statuses managed by scraper automation."""
    target_statuses = statuses or {"new", "unclassified", "needs_pricing"}
    if not target_statuses:
        return 0

    placeholders = ",".join("?" for _ in target_statuses)
    cursor.execute(
        f"""
        SELECT id, title, description, price
        FROM listings
        WHERE status IN ({placeholders})
        """,
        tuple(sorted(target_statuses)),
    )
    rows = cursor.fetchall()
    delete_rows: List[Tuple[str]] = []
    for listing_id, title, description, price in rows:
        if is_accessory_only_listing(title or "", description or "", price):
            delete_rows.append((str(listing_id),))

    if not delete_rows:
        return 0

    cursor.executemany("DELETE FROM listings WHERE id = ?", delete_rows)
    return len(delete_rows)


# ---------------------------------------------------------------------------
# Financial recalculation
# ---------------------------------------------------------------------------


def recalculate_listing_financials(csv_path: str = "price_list.csv") -> int:
    """
    Recalculate model, condition, max buy, and profit for all existing listings.

    Useful after editing ``price_list.csv`` so GUI / CLI values are immediately
    refreshed.  Returns the number of listings updated.
    """
    init_db()
    price_data = load_price_list(csv_path=csv_path)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT id, title, description, price, status FROM listings")
    rows = cursor.fetchall()

    updated = 0
    managed_statuses = {"new", "unclassified", "needs_pricing"}
    delete_rows: List[Tuple[str]] = []

    for row in rows:
        title = (row["title"] or "").strip()
        description = row["description"] or ""
        existing_status = row["status"] or "new"
        listed_price = row["price"]
        inferred_price = (
            extract_currency_price_from_text(title)
            or extract_currency_price_from_text(description)
            or extract_shorthand_k_price_from_text(title)
            or extract_shorthand_k_price_from_text(description)
        )
        derived_price = listed_price
        if derived_price is None:
            derived_price = inferred_price
        elif inferred_price is not None and derived_price < 20 and inferred_price >= 100:
            derived_price = inferred_price

        if existing_status in managed_statuses and is_accessory_only_listing(title, description, derived_price):
            delete_rows.append((str(row["id"]),))
            continue

        detected_model = identify_model(title, description)
        resolved_model = resolve_price_model_key(detected_model, price_data)

        if existing_status in managed_statuses and not resolved_model:
            # Keep local queue aligned to price-sheet scope.
            delete_rows.append((str(row["id"]),))
            continue

        if resolved_model:
            model = resolved_model
            condition, issues = assess_condition(title, description)
            max_offer, _ = calculate_max_offer(model, condition, issues, price_data)
            profit = calculate_profit_for_listing(
                model=model,
                condition=condition,
                issues=issues,
                price_data=price_data,
                listed_price=derived_price,
                fallback_purchase_price=max_offer,
            )
            computed_status = "new"
        else:
            model = "Unknown"
            condition = "unknown"
            max_offer = 0.0
            profit = 0.0
            computed_status = "unclassified"

        next_status = computed_status if existing_status in managed_statuses else existing_status
        cursor.execute(
            """
            UPDATE listings
            SET price = ?, model = ?, condition = ?, max_buy_price = ?, potential_profit = ?, status = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                float(round(derived_price, 2)) if derived_price is not None else None,
                model,
                condition,
                float(round(max_offer, 2)),
                float(round(profit, 2)),
                next_status,
                datetime.now().isoformat(),
                row["id"],
            ),
        )
        updated += 1

    if delete_rows:
        cursor.executemany("DELETE FROM listings WHERE id = ?", delete_rows)

    conn.commit()
    conn.close()
    return updated
