import re, json, ipaddress, random, asyncio, sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Callable, Awaitable
from urllib.parse import urlparse, urlunparse
from scraper.parsers import parse_listing_price, extract_currency_price_from_text, extract_shorthand_k_price_from_text, is_accessory_only_listing, identify_model, resolve_price_model_key, assess_condition, calculate_profit_for_listing, calculate_max_offer

def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


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


def _get_nested(mapping: Any, *keys: str) -> Any:
    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    return current


def _normalize_marketplace_url(raw_url: Any, listing_id: str) -> str:
    normalized_id = _extract_text_value(listing_id)
    if normalized_id:
        return f"https://www.facebook.com/marketplace/item/{normalized_id}/"

    url = _extract_text_value(raw_url)
    if not url:
        return ""

    if url.startswith("/"):
        url = f"https://www.facebook.com{url}"

    if url.startswith("http://") or url.startswith("https://"):
        parsed = urlparse(url)
        if parsed.path:
            match = re.search(r"/marketplace/item/(\d+)", parsed.path)
            if match:
                return f"https://www.facebook.com/marketplace/item/{match.group(1)}/"
        return urlunparse(parsed._replace(query="", fragment=""))

    return url


def _extract_graphql_listing_candidates(payload: Any) -> List[Dict[str, Any]]:
    """Recursively traverse GraphQL payload and collect listing-shaped nodes."""
    candidates: List[Dict[str, Any]] = []

    def walk(node: Any):
        if isinstance(node, dict):
            listing_obj = node.get("listing")
            if isinstance(listing_obj, dict):
                candidates.append(listing_obj)

            if "id" in node and (
                "marketplace_listing_title" in node
                or "listing_price" in node
                or "marketplace_listing_seller" in node
                or "redacted_description" in node
            ):
                candidates.append(node)

            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return candidates


def _normalize_graphql_listing(candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a GraphQL listing object into scraper listing shape."""
    listing_id_raw = candidate.get("id") or candidate.get("listing_id")
    listing_id = _extract_text_value(listing_id_raw)
    if listing_id:
        id_match = re.search(r"\d{6,}", listing_id)
        if id_match:
            listing_id = id_match.group()

    url = _normalize_marketplace_url(
        candidate.get("url")
        or candidate.get("permalink")
        or _get_nested(candidate, "story", "url")
        or _get_nested(candidate, "story", "www_lite_url"),
        listing_id,
    )
    if not listing_id:
        id_from_url = re.search(r"/item/(\d+)", url)
        if id_from_url:
            listing_id = id_from_url.group(1)

    if not listing_id:
        return None

    title = _extract_text_value(candidate.get("marketplace_listing_title") or candidate.get("title"))
    description = _extract_text_value(candidate.get("redacted_description") or candidate.get("description"))
    price = None
    price_candidates = [
        candidate.get("listing_price"),
        candidate.get("price"),
        candidate.get("marketplace_listing_price"),
        candidate.get("listing_price_text"),
        _get_nested(candidate, "formatted_price"),
        _get_nested(candidate, "listing_price", "formatted_amount"),
    ]
    for raw_price in price_candidates:
        price = parse_listing_price(raw_price)
        if price is not None:
            break

    # Some GraphQL shapes flatten card text (e.g. "300 AU$iPhone ...") into title.
    if price is None:
        price = (
            extract_currency_price_from_text(title)
            or extract_currency_price_from_text(description)
            or extract_shorthand_k_price_from_text(title)
            or extract_shorthand_k_price_from_text(description)
        )
    else:
        shorthand_price = extract_shorthand_k_price_from_text(title) or extract_shorthand_k_price_from_text(description)
        if shorthand_price is not None and price < 20 and shorthand_price >= 100:
            price = shorthand_price

    seller_data = candidate.get("marketplace_listing_seller") or candidate.get("seller")
    seller_name = _extract_text_value(_get_nested(seller_data, "name") if isinstance(seller_data, dict) else seller_data)

    location_data = candidate.get("location") or {}
    location_name = _extract_text_value(
        _get_nested(location_data, "reverse_geocode", "city_page", "display_name")
        or _get_nested(location_data, "city_page", "display_name")
        or location_data.get("name")
        or location_data.get("display_name")
    )

    return {
        "id": listing_id,
        "title": title,
        "url": url,
        "price": price,
        "location": location_name,
        "description": description,
        "seller_name": seller_name,
    }


def _listing_quality_score(listing: Dict[str, Any]) -> int:
    title = _extract_text_value(listing.get("title"))
    score = 0
    if title:
        score += 3
    if re.search(r"iphone", title, re.IGNORECASE):
        score += 3
    if parse_listing_price(listing.get("price")) is not None:
        score += 2
    if _has_value(listing.get("url")):
        score += 2
    if _has_value(listing.get("location")):
        score += 1
    if _has_value(listing.get("description")):
        score += 1
    if _has_value(listing.get("seller_name")):
        score += 1
    return score


def merge_listing_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge duplicate listing IDs, preserving richer values."""
    merged: Dict[str, Dict[str, Any]] = {}
    fields = ("title", "url", "price", "location", "description", "seller_name")

    for candidate in candidates:
        listing_id = _extract_text_value(candidate.get("id"))
        if not listing_id:
            continue

        if listing_id not in merged:
            merged[listing_id] = dict(candidate)
            continue

        existing = merged[listing_id]
        existing_score = _listing_quality_score(existing)
        incoming_score = _listing_quality_score(candidate)

        preferred = dict(candidate) if incoming_score > existing_score else dict(existing)
        backup = existing if incoming_score > existing_score else candidate

        for field in fields:
            if not _has_value(preferred.get(field)) and _has_value(backup.get(field)):
                preferred[field] = backup[field]

        merged[listing_id] = preferred

    return list(merged.values())


def extract_listings_from_graphql_payload(payload: Any) -> List[Dict[str, Any]]:
    """Extract normalized listings from a Marketplace GraphQL payload."""
    normalized: List[Dict[str, Any]] = []
    for candidate in _extract_graphql_listing_candidates(payload):
        listing = _normalize_graphql_listing(candidate)
        if listing:
            normalized.append(listing)
    return merge_listing_candidates(normalized)


def decode_json_body(body_text: str) -> Optional[Any]:
    """Decode response body text into JSON, handling Facebook anti-prefixes."""
    if not body_text:
        return None

    payload = body_text.lstrip()
    if payload.startswith("for (;;);"):
        payload = payload[len("for (;;);"):].lstrip()

    if not payload:
        return None

    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def _extract_ip_from_text(value: str) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    for token in re.split(r"[\s,;]+", text):
        token_clean = token.strip().strip("[]")
        if not token_clean:
            continue
        try:
            return str(ipaddress.ip_address(token_clean))
        except ValueError:
            continue
    match = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text)
    if not match:
        return None
    try:
        return str(ipaddress.ip_address(match.group(0)))
    except ValueError:
        return None


def _extract_proxy_check_ip(payload: Any) -> Optional[str]:
    if payload is None:
        return None
    if isinstance(payload, str):
        return _extract_ip_from_text(payload)
    if isinstance(payload, list):
        for item in payload:
            found = _extract_proxy_check_ip(item)
            if found:
                return found
        return None
    if isinstance(payload, dict):
        for key in ("ip", "origin", "query", "address"):
            if key not in payload:
                continue
            found = _extract_proxy_check_ip(payload.get(key))
            if found:
                return found
        return None
    return _extract_ip_from_text(str(payload))


async def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0):
    """Add a random delay to mimic human behavior."""
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)


async def human_like_scroll(page, scroll_count: int = 3):
    """Scroll the page in a human-like manner with variable speeds."""
    for i in range(scroll_count):
        # Vary the scroll distance
        scroll_distance = random.randint(300, 800)
        await page.evaluate("window.scrollBy(0, {})".format(scroll_distance))
        # Variable delay between scrolls
        await random_delay(1.5, 4.0)
        # Occasionally pause longer (simulating reading)
        if random.random() < 0.3:
            await random_delay(3.0, 6.0)


async def _collect_marketplace_visible_ids(page) -> set[str]:
    """Collect unique listing IDs currently rendered in the Marketplace feed."""
    try:
        raw_ids = await page.evaluate(
            """
            () => {
                const ids = new Set();
                const links = document.querySelectorAll('a[href*="/marketplace/item/"]');
                links.forEach((link) => {
                    const href = link.getAttribute('href') || '';
                    const match = href.match(/\\/item\\/(\\d+)/);
                    if (match && match[1]) {
                        ids.add(match[1]);
                    } else if (href) {
                        ids.add(href);
                    }
                });
                return Array.from(ids);
            }
            """
        )
    except Exception:
        return set()

    ids: set[str] = set()
    for value in raw_ids or []:
        text = str(value or "").strip()
        if text:
            ids.add(text)
    return ids


async def _scroll_marketplace_feed(page, distance: int) -> bool:
    """Scroll the most likely Marketplace feed container. Returns True if movement was detected."""
    distance = int(distance)
    try:
        moved = await page.evaluate(
            """
            (distance) => {
                const root = document.scrollingElement || document.documentElement;
                let best = root;
                let bestRange = Math.max(0, (root.scrollHeight || 0) - (root.clientHeight || 0));

                const candidates = Array.from(document.querySelectorAll("div, main, section"));
                for (const el of candidates) {
                    const style = window.getComputedStyle(el);
                    if (!style || !/(auto|scroll)/i.test(style.overflowY || "")) {
                        continue;
                    }
                    const range = Math.max(0, (el.scrollHeight || 0) - (el.clientHeight || 0));
                    if (range > bestRange + 120) {
                        best = el;
                        bestRange = range;
                    }
                }

                const before = best.scrollTop || 0;
                if (typeof best.scrollBy === "function") {
                    best.scrollBy(0, distance);
                } else {
                    best.scrollTop = before + distance;
                }
                const after = best.scrollTop || 0;
                if (after !== before) {
                    return true;
                }

                const rootBefore = window.pageYOffset || document.documentElement.scrollTop || 0;
                window.scrollBy(0, distance);
                const rootAfter = window.pageYOffset || document.documentElement.scrollTop || 0;
                return rootAfter !== rootBefore;
            }
            """,
            distance,
        )
        return bool(moved)
    except Exception:
        return False


async def progressive_marketplace_scroll(
    page,
    target_cards: int = 180,
    max_rounds: int = 14,
    min_rounds: int = 1,
    snapshot_callback: Optional[Callable[[], Awaitable[None]]] = None,
) -> Dict[str, int]:
    """
    Scroll progressively until enough cards are loaded or growth stalls.
    This avoids getting stuck around the initial ~70-card viewport batch.
    """
    target_cards = max(40, int(target_cards))
    max_rounds = max(2, int(max_rounds))
    min_rounds = max(0, int(min_rounds))

    seen_listing_ids = await _collect_marketplace_visible_ids(page)
    best_visible = len(seen_listing_ids)
    idle_rounds = 0
    rounds = 0

    if snapshot_callback:
        try:
            await snapshot_callback()
        except Exception:
            pass

    for _ in range(max_rounds):
        # Enforce at least one scroll pass, even if the initial card count
        # already meets the target threshold.
        if rounds >= min_rounds and best_visible >= target_cards:
            break

        rounds += 1
        first_scroll = random.randint(900, 1700)
        moved = await _scroll_marketplace_feed(page, first_scroll)
        if not moved:
            try:
                await page.mouse.wheel(0, first_scroll)
            except Exception:
                pass
        await random_delay(1.0, 2.4)

        if random.random() < 0.35:
            second_scroll = random.randint(350, 850)
            moved_second = await _scroll_marketplace_feed(page, second_scroll)
            if not moved_second:
                try:
                    await page.mouse.wheel(0, second_scroll)
                except Exception:
                    pass
            await random_delay(0.8, 1.8)

        try:
            clicked_more = await page.evaluate(
                """
                () => {
                    const nodes = Array.from(
                        document.querySelectorAll('div[role="button"], a[role="button"], button')
                    );
                    for (const node of nodes) {
                        const text = (node.textContent || '').trim().toLowerCase();
                        if (text.includes('see more') || text.includes('more results') || text.includes('show more')) {
                            node.click();
                            return true;
                        }
                    }
                    return false;
                }
                """
            )
            if clicked_more:
                await random_delay(0.8, 1.8)
        except Exception:
            pass

        if snapshot_callback:
            try:
                await snapshot_callback()
            except Exception:
                pass

        current_ids = await _collect_marketplace_visible_ids(page)
        newly_seen = current_ids - seen_listing_ids
        if newly_seen:
            seen_listing_ids.update(newly_seen)
            best_visible = len(seen_listing_ids)
            idle_rounds = 0
        else:
            idle_rounds += 1

        if idle_rounds >= 5:
            break

    return {
        "scroll_rounds": rounds,
        "visible_cards": best_visible,
    }


async def _extract_dom_listing_candidates(page) -> List[Dict[str, str]]:
    """Extract visible DOM listing candidates for the current viewport snapshot."""
    return await page.evaluate("""
        () => {
            // Try multiple selectors as Facebook changes them
            const selectors = [
                '[data-testid="marketplace_feed_item"]',
                'a[href*="/marketplace/item/"]',
                'div[class*="x9f619"] a[href*="/marketplace/item/"]',
            ];

            let links = [];
            for (const selector of selectors) {
                const found = document.querySelectorAll(selector);
                if (found.length > 0) {
                    links = found;
                    break;
                }
            }

            const byId = new Map();

            const cleanText = (text) => (text || "").replace(/\\s+/g, " ").trim();
            const looksLikeLocationOnly = (text) =>
                /^(perth|wa|australia|listed|sponsored)(,\\s*wa)?$/i.test(text);
            const looksLikePrice = (text) => {
                if (!text) return false;
                return (
                    /(?:A?U?\\$)\\s?\\d/i.test(text) ||
                    /\\d[\\d,.]*\\s?(?:A?U?\\$)/i.test(text) ||
                    /^\\d{2,6}(?:[.,]\\d{3})*(?:\\.\\d{1,2})?$/.test(text.trim())
                );
            };

            const pickBestText = (texts) => {
                const filtered = texts
                    .map(cleanText)
                    .filter((t) => t.length >= 4 && t.length <= 220 && !looksLikeLocationOnly(t));

                if (!filtered.length) return "";

                const iphoneText = filtered.find((t) => /iphone/i.test(t));
                if (iphoneText) return iphoneText;

                const nonPrice = filtered.find((t) => !looksLikePrice(t));
                if (nonPrice) return nonPrice;

                return filtered.sort((a, b) => b.length - a.length)[0];
            };

            const pickBestPrice = (texts) => {
                const price = texts.find((t) => looksLikePrice(t));
                return cleanText(price || "");
            };

            links.forEach((el) => {
                try {
                    let linkEl = el;
                    if (!el.href || !el.href.includes('/marketplace/item/')) {
                        linkEl = el.querySelector('a[href*="/marketplace/item/"]');
                    }
                    if (!linkEl || !linkEl.href) return;

                    const idMatch = linkEl.href.match(/\\/item\\/(\\d+)/);
                    if (!idMatch) return;
                    const listingId = idMatch[1];

                    const container =
                        linkEl.closest('[data-testid="marketplace_feed_item"]') ||
                        linkEl.closest('[role="article"]') ||
                        linkEl.closest('div[class*="x9f619"]') ||
                        linkEl.parentElement;

                    const spanTexts = container
                        ? Array.from(container.querySelectorAll("span")).map((s) => s.textContent || "")
                        : [];
                    const textCandidates = [
                        linkEl.getAttribute("aria-label") || "",
                        linkEl.textContent || "",
                        ...spanTexts,
                    ];

                    const title = pickBestText(textCandidates);
                    const price = pickBestPrice(textCandidates);

                    let score = 0;
                    if (title) score += 1;
                    if (/iphone/i.test(title)) score += 5;
                    if (price) score += 3;
                    if (title.length > 15) score += 1;

                    const candidate = {
                        id: listingId,
                        title,
                        url: linkEl.href,
                        price,
                        _score: score,
                    };

                    const existing = byId.get(listingId);
                    if (!existing || candidate._score > existing._score) {
                        byId.set(listingId, candidate);
                    }
                } catch (e) {}
            });

            return Array.from(byId.values()).map((item) => ({
                id: item.id,
                title: item.title,
                url: item.url,
                price: item.price,
            }));
        }
    """)


async def _detect_manual_login_required_state(page) -> Optional[str]:
    """Check if Facebook put the account in a state requiring manual intervention."""
    try:
        url = page.url
        if "checkpoint" in url or "suspended" in url:
            return "checkpoint"
        if "login" in url and "marketplace" not in url:
            return "login"
        
        # Check for specific DOM elements indicating a lock
        is_locked = await page.evaluate(
            """
            () => {
                const text = document.body.innerText.toLowerCase();
                if (text.includes("we suspended your account") || text.includes("account restricted")) {
                    return "suspended";
                }
                if (text.includes("you must log in to continue") || document.querySelector('input[name="email"]')) {
                    return "login";
                }
                return null;
            }
            """
        )
        return is_locked
    except Exception:
        return None


async def apply_stealth_scripts(page):
    """Apply stealth JavaScript to hide automation signals."""
    # Hide webdriver property
    await page.add_init_script("""
        // Override the navigator.webdriver property
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });

        // Override the chrome property
        window.chrome = {
            runtime: {},
            loadTimes: function() {},
            csi: function() {},
            app: {},
        };

        // Override permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        // Override plugins to look more realistic
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                { name: 'Native Client', filename: 'internal-nacl-plugin' },
            ],
        });

        // Override languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-AU', 'en-US', 'en'],
        });

        // Remove automation-related properties
        delete navigator.__proto__.webdriver;

        // Override the platform
        Object.defineProperty(navigator, 'platform', {
            get: () => 'MacIntel',
        });

        // Override hardware concurrency
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 8,
        });

        // Override device memory
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => 8,
        });
    """)


def _store_listing_candidate(
    cursor: sqlite3.Cursor,
    listing: Dict[str, Any],
    price_data: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Persist a single normalized listing candidate if it is new.
    Returns the processed listing payload when inserted, else None.
    """
    listing_id = _extract_text_value(listing.get("id"))
    if not listing_id:
        return None

    cursor.execute("SELECT id FROM listings WHERE id = ?", (listing_id,))
    if cursor.fetchone():
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
        # Scope control: only keep listings for models present in price sheet.
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
    status = "new"

    created_at = datetime.now().isoformat()
    cursor.execute(
        """
        INSERT OR IGNORE INTO listings (
            id, title, price, location, url, description, seller_name,
            model, condition, max_buy_price, potential_profit, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            listing_id,
            title_text,
            price_value,
            location_text,
            listing_url,
            description_text,
            seller_name,
            model,
            condition,
            max_offer,
            profit,
            status,
            created_at,
        ),
    )
    if cursor.rowcount <= 0:
        return None

    return {
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
        "status": status,
    }


