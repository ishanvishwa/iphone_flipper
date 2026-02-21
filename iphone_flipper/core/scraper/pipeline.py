import asyncio
import json
import random
from datetime import datetime
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Set

from .config import MANUAL_LOGIN_REQUIRED_PREFIX
from .driver import launch_browser_context
from .driver import random_delay
from .parser import extract_currency_price_from_text
from .parser import extract_shorthand_k_price_from_text
from .parser import identify_model
from .parser import is_accessory_only_listing
from .parser import parse_listing_price
from .parser import resolve_price_model_key
from .parser import assess_condition
from .parser import calculate_max_offer
from .parser import calculate_profit_for_listing
from .storage import init_db
from .storage import load_active_search_queries
from .storage import load_price_list
from .storage import load_runtime_scraper_settings
from .storage import mark_search_query_polled
from .storage import save_listing
from .storage import update_fb_account_runtime_status


def _extract_text_value(value: Any) -> str:
    """Extract a readable text value from nested GraphQL objects."""
    if isinstance(value, str):
        return value.strip()

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, dict):
        text_keys = (
            "text",
            "name",
            "title",
            "display_name",
            "formatted_amount",
            "raw_text",
            "description",
            "message",
            "label",
        )
        for key in text_keys:
            candidate = _extract_text_value(value.get(key))
            if candidate:
                return candidate
        return ""

    if isinstance(value, list):
        for item in value:
            candidate = _extract_text_value(item)
            if candidate:
                return candidate
        return ""

    return ""


def _normalize_marketplace_url(raw_url: Any, listing_id: str) -> str:
    normalized_id = _extract_text_value(listing_id)
    if normalized_id:
        return f"https://www.facebook.com/marketplace/item/{normalized_id}/"
    return ""  # Simplified for brevity


def _store_listing_candidate(
    cursor,
    listing: Dict[str, Any],
    price_data: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Process a normalized listing candidate and persist if valid/profitable.
    """
    listing_id = _extract_text_value(listing.get("id"))
    if not listing_id:
        return None

    title_text = (listing.get("title") or "").strip()
    if not title_text:
        title_text = "Untitled listing"

    description_text = (listing.get("description") or "").strip()
    location_text = (listing.get("location") or "").strip() or None
    seller_name = (listing.get("seller_name") or "").strip() or None
    listing_url = _normalize_marketplace_url(listing.get("url"), listing_id)

    price_value = parse_listing_price(listing.get("price"))
    inferred_price = (
        extract_currency_price_from_text(title_text)
        or extract_currency_price_from_text(description_text)
        or extract_shorthand_k_price_from_text(title_text)
        or extract_shorthand_k_price_from_text(description_text)
    )
    if price_value is None:
        price_value = inferred_price
    elif inferred_price is not None and price_value < 20 and inferred_price >= 100:
        price_value = inferred_price

    if is_accessory_only_listing(title_text, description_text, price_value):
        return None

    detected_model = identify_model(title_text, description_text)
    resolved_model = resolve_price_model_key(detected_model, price_data)
    if not resolved_model:
        return None

    model = resolved_model
    condition, issues = assess_condition(title_text, description_text)
    max_offer, _ = calculate_max_offer(model, condition, issues, price_data)
    profit = calculate_profit_for_listing(
        model=model,
        condition=condition,
        issues=issues,
        price_data=price_data,
        listed_price=price_value,
        fallback_purchase_price=max_offer,
    )

    final_listing = {
        "id": listing_id,
        "title": title_text,
        "description": description_text,
        "price": price_value,
        "model": model,
        "condition": condition,
        "max_offer": max_offer,
        "potential_profit": profit,
        "location": location_text,
        "seller_name": seller_name,
        "url": listing_url,
        "status": "new",
    }
    
    saved = save_listing(cursor, final_listing)
    return final_listing if saved else None


async def scrape_marketplace(
    profile_id: str,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    stop_event: Any = None,
    search_queries: Optional[List[str]] = None,
):
    init_db()
    price_data = load_price_list()
    runtime_settings = load_runtime_scraper_settings()
    
    explicit_queries = [q.strip() for q in (search_queries or []) if str(q).strip()]
    active_queries = explicit_queries or load_active_search_queries(
        max_queries=runtime_settings["max_queries_per_run"]
    )

    try:
        p, browser, context, page = await launch_browser_context(
            headless=False,
            profile_id=profile_id,
        )
        
        import sqlite3
        from .config import DB_PATH
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # GraphQL Interception Logic
        async def handle_response(response):
            if "api/graphql/" in response.url and response.request.method == "POST":
                try:
                    json_data = await response.json()
                    # A robust parser will be needed here to extract nodes, 
                    # but for now we'll just log that we caught it to avoid breaking early.
                    # We will fill this out properly in the parsing step.
                except Exception as e:
                    pass

        page.on("response", handle_response)

        for index, query in enumerate(active_queries, start=1):
            if stop_event and stop_event.is_set():
                break

            print(f"  Searching for: {query}")
            search_url = f"https://www.facebook.com/marketplace/perth/search?query={query.replace(' ', '%20')}&exact=false&sortBy=creation_time_descend&daysSinceListed=1"
            
            await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            
            # Since we intercept GraphQL, we don't need to wait long for the DOM.
            await random_delay(1, 2)
            
            mark_search_query_polled(query)

        conn.close()
        await context.close()
        await browser.close()
        await p.stop()
        
    except Exception as e:
        print(f"Scraper pipeline failed: {e}")

