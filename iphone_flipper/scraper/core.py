"""
Scraper core pipeline: marketplace scrape orchestration, accessory purging,
and listing financial recalculation.
"""

import asyncio
import sqlite3
from datetime import datetime
from typing import List, Optional, Tuple

from scraper.browser import launch_browser_context, random_delay
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


async def scrape_marketplace(headless: bool = True) -> None:
    """Run the main Facebook Marketplace scraping pipeline."""
    init_db()
    price_data = load_price_list()
    runtime_settings = load_runtime_scraper_settings()
    active_queries = load_active_search_queries(max_queries=runtime_settings["max_queries_per_run"])

    try:
        browser, context, page = await launch_browser_context(headless=headless)

        for query in active_queries:
            print(f"  Searching for: {query}")
            search_url = (
                f"https://www.facebook.com/marketplace/perth/search"
                f"?query={query.replace(' ', '%20')}&exact=false&sortBy=creation_time_descend"
            )

            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                await random_delay(3, 6)
                mark_search_query_polled(query)
            except Exception as e:
                print(f"Error scraping {query}: {e}")

        await context.close()
        await browser.stop()

    except Exception as e:
        print(f"Scraper pipeline failed: {e}")


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
