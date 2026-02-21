import re

with open("iphone_flipper/scraper/core.py", "r") as f:
    core_content = f.read()

new_scrape = """async def scrape_marketplace(
    profile_id: str,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    stop_event: Any = None,
    search_queries: Optional[List[str]] = None,
    scroll_target_cards_override: Optional[int] = None,
    scroll_max_rounds_override: Optional[int] = None,
) -> List[Dict[str, Any]]:
    \"\"\"Run the main Facebook Marketplace scraping pipeline.\"\"\"
    init_db()
    price_data = load_price_list()
    runtime_settings = load_runtime_scraper_settings()

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
    
    explicit_queries = [q.strip() for q in (search_queries or []) if str(q).strip()]
    active_queries = explicit_queries if explicit_queries else load_active_search_queries(max_queries=runtime_settings.get("max_queries_per_run", 5))

    def emit_progress(event: str, **payload):
        if not progress_callback:
            return
        try:
            progress_callback({"event": event, **payload})
        except Exception:
            pass

    processed_listings = []
    seen_listing_ids = set()
    new_listings_count = 0

    try:
        p, browser, context, page = await launch_browser_context(headless=False, profile_id=profile_id)
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        total_queries = len(active_queries)
        processed_queries = 0
        cancelled = False
        manual_login_error = None
        
        for index, query in enumerate(active_queries, start=1):
            if stop_event and stop_event.is_set():
                cancelled = True
                emit_progress("cancelled", query_index=processed_queries, query_total=total_queries)
                break
                
            emit_progress("query_start", query=query, query_index=index, query_total=total_queries)
            print(f"  Searching for: {query}")
            
            search_url = f"https://www.facebook.com/marketplace/perth/search?query={query.replace(' ', '%20')}&exact=false&sortBy=creation_time_descend"
            
            from scraper.legacy_utils import extract_listings_from_graphql_payload, decode_json_body, merge_listing_candidates, _extract_text_value, _normalize_marketplace_url, _store_listing_candidate, purge_accessory_only_listings, progressive_marketplace_scroll, _detect_manual_login_required_state, _extract_dom_listing_candidates
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
                await random_delay(3, 6)
                
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
                    
                    saved_listing = _store_listing_candidate(cursor, listing, price_data)
                    if not saved_listing:
                        continue
                        
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
                        listing=saved_listing,
                    )
                
                found_count = len(query_listings)
                graphql_count = len(graphql_candidates)
                dom_count = len(normalized_dom_listings)
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
                )
                
            except Exception as e:
                error_text = str(e)
                print(f"  Error scraping query '{query}': {error_text}")
                emit_progress("query_error", query=query, query_index=index, query_total=total_queries, error=error_text)
                if MANUAL_LOGIN_REQUIRED_PREFIX.lower() in error_text.lower():
                    manual_login_error = error_text
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
                
            await random_delay(runtime_settings.get("scrape_delay_min_seconds", 3), runtime_settings.get("scrape_delay_max_seconds", 6))

        if manual_login_error:
            conn.close()
            await context.close()
            await browser.close()
            await p.stop()
            raise RuntimeError(manual_login_error)
            
        from scraper.legacy_utils import purge_accessory_only_listings
        removed_accessory_count = purge_accessory_only_listings(cursor)
        if removed_accessory_count > 0:
            conn.commit()
            emit_progress("accessory_cleanup", removed=removed_accessory_count)
            
        conn.close()
        await context.close()
        await browser.close()
        await p.stop()
        
        emit_progress(
            "completed",
            new_count=new_listings_count,
            query_index=processed_queries,
            query_total=total_queries,
            cancelled=cancelled,
        )
        return processed_listings

    except Exception as e:
        print(f"Scraper pipeline failed: {e}")
        return []
"""

core_content = re.sub(r"async def scrape_marketplace\(.*?except Exception as e:\n\s+print\(f\"Scraper pipeline failed: \{e\}\"\)", new_scrape, core_content, flags=re.DOTALL)
core_content = core_content.replace("from typing import List, Optional, Tuple", "from typing import Any, Callable, Dict, List, Optional, Tuple")

with open("iphone_flipper/scraper/core.py", "w") as f:
    f.write(core_content)

print("Patched.")
